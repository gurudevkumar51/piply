"""
Run service layer.
"""
import os
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from fastapi import BackgroundTasks

from ..database import PipelineRun, TaskRun, RunStatus, LogEntry
from ..schemas import RunCreate, RunResponse, RunRetryRequest
from ..tracker import tracked_execution
from piply.parser.yaml_parser import load_pipeline
from piply.engine.local_engine import LocalEngine
from piply.engine.prefect_engine import PrefectEngine
from piply.utils.logging import logger


class RunService:
    """Business logic for pipeline run management."""

    def __init__(self, db: Session, background_tasks: BackgroundTasks = None):
        self.db = db
        self.background_tasks = background_tasks

    def create_run(self, run_request: RunCreate) -> RunResponse:
        """
        Create and trigger a new pipeline run.
        Validates pipeline exists, creates database record, and schedules execution.
        """
        # Validate pipeline exists
        pipeline_path = self._find_pipeline_path(run_request.pipeline_name)
        if not pipeline_path:
            raise ValueError(
                f"Pipeline '{run_request.pipeline_name}' not found")

        # Create pipeline run record
        pipeline_run = PipelineRun(
            pipeline_name=run_request.pipeline_name,
            tenant=run_request.tenant,
            trigger_type=run_request.trigger_type,
            triggered_by=run_request.triggered_by,
            retry_of=run_request.retry_of,
            config={"pipeline_path": pipeline_path, **
                    (run_request.config_overrides or {})}
        )
        self.db.add(pipeline_run)
        self.db.commit()
        self.db.refresh(pipeline_run)

        # Execute pipeline in background if background_tasks available
        if self.background_tasks:
            self.background_tasks.add_task(
                self._execute_pipeline_background,
                pipeline_path=pipeline_path,
                run_id=pipeline_run.id,
                tenant=run_request.tenant,
                db_url="sqlite:///./piply.db"  # TODO: Get from config
            )

        return RunResponse(
            id=pipeline_run.id,
            pipeline_name=pipeline_run.pipeline_name,
            tenant=pipeline_run.tenant,
            status=pipeline_run.status.value,
            started_at=pipeline_run.started_at,
            completed_at=pipeline_run.completed_at,
            trigger_type=pipeline_run.trigger_type,
            task_count=0,
            tasks_completed=0
        )

    def retry_run(self, run_id: int, retry_request: RunRetryRequest) -> Dict[str, Any]:
        """
        Retry a failed pipeline run.
        Supports 'resume' (only failed tasks) and 'startover' modes.
        """
        original_run = self.db.query(PipelineRun).filter(
            PipelineRun.id == run_id
        ).first()

        if not original_run:
            raise ValueError("Run not found")

        if original_run.status != RunStatus.FAILED:
            raise ValueError("Only failed runs can be retried")

        # Get pipeline path from config
        pipeline_path = original_run.config.get("pipeline_path")
        if not pipeline_path or not os.path.exists(pipeline_path):
            raise ValueError("Original pipeline configuration not found")

        # Create new run
        new_run = PipelineRun(
            pipeline_name=original_run.pipeline_name,
            tenant=retry_request.tenant or original_run.tenant,
            trigger_type="retry",
            triggered_by="api",
            retry_of=run_id,
            config=original_run.config.copy()
        )
        self.db.add(new_run)
        self.db.commit()
        self.db.refresh(new_run)

        # Execute in background
        if self.background_tasks:
            self.background_tasks.add_task(
                self._execute_pipeline_background,
                pipeline_path=pipeline_path,
                run_id=new_run.id,
                tenant=retry_request.tenant or original_run.tenant,
                retry_mode=retry_request.trigger_type,  # Using trigger_type as mode for now
                retry_from_run_id=run_id,
                db_url="sqlite:///./piply.db"
            )

        return {"message": "Retry started", "run_id": new_run.id}

    def _find_pipeline_path(self, pipeline_name: str) -> Optional[str]:
        """Find the YAML file for a given pipeline name."""
        search_paths = [".", "examples"]
        for search_dir in search_paths:
            if not os.path.exists(search_dir):
                continue
            for root, dirs, files in os.walk(search_dir):
                for filename in files:
                    if filename.endswith(('.yaml', '.yml')):
                        filepath = os.path.join(root, filename)
                        try:
                            pipeline_obj = load_pipeline(filepath)
                            if pipeline_obj.config.get("name") == pipeline_name:
                                return filepath
                        except Exception:
                            continue
        return None

    def _execute_pipeline_background(
        self,
        pipeline_path: str,
        run_id: int,
        tenant: Optional[str] = None,
        retry_mode: Optional[str] = None,
        retry_from_run_id: Optional[int] = None,
        db_url: str = None
    ):
        """
        Background task to execute a pipeline and track results.
        This is a simplified version - the full implementation would be
        moved from the current main.py.
        """
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        engine = create_engine(db_url, connect_args={
                               "check_same_thread": False} if db_url.startswith("sqlite") else {})
        SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=engine)
        db = SessionLocal()

        pipeline_run = None

        try:
            # Update run status to running
            pipeline_run = db.query(PipelineRun).filter(
                PipelineRun.id == run_id).first()
            if pipeline_run:
                pipeline_run.status = RunStatus.RUNNING
                db.commit()

            # Load pipeline
            pipeline = load_pipeline(pipeline_path)

            with tracked_execution(db, run_id) as tracker:
                pipeline.context.tracker = tracker

                # Choose engine
                engine_choice = pipeline.config.get("engine", "local")
                if engine_choice == "prefect":
                    eng = PrefectEngine()
                else:
                    eng = LocalEngine()

                # Determine retry_failed mode
                retry_failed = (retry_mode == "resume")

                # For resume mode, pre-populate context with successful task outputs
                if retry_failed and retry_from_run_id:
                    previous_tasks = db.query(TaskRun).filter(
                        TaskRun.pipeline_run_id == retry_from_run_id,
                        TaskRun.status == RunStatus.SUCCESS
                    ).all()

                    for task in previous_tasks:
                        from piply.core.context import StepOutput
                        output = StepOutput(
                            step_name=task.task_name,
                            success=True,
                            result=task.result,
                            metadata={"attempts": task.attempt,
                                      "previous_run_id": retry_from_run_id}
                        )
                        pipeline.context.set_output(task.task_name, output)

                    print(
                        f"[Resume Mode] Skipping {len(previous_tasks)} successful tasks from previous run")

                # Run pipeline through the Pipeline interface so tenant/retry
                # handling stays consistent across engine implementations.
                pipeline.run(eng, tenant=tenant, retry_failed=retry_failed)

                # Update run status to success
                if pipeline_run:
                    pipeline_run.status = RunStatus.SUCCESS
                    pipeline_run.completed_at = datetime.utcnow()
                    db.commit()
            
        except Exception as exc:
            if pipeline_run:
                pipeline_run.status = RunStatus.FAILED
                pipeline_run.completed_at = datetime.utcnow()
                db.add(LogEntry(
                    level="ERROR",
                    logger="piply.api.run_service",
                    message=str(exc),
                    pipeline_run_id=run_id,
                    extra={"pipeline_path": pipeline_path}
                ))
                db.commit()
            logger.exception("Background pipeline execution failed for run %s", run_id)

        finally:
            db.close()
