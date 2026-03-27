"""
Execution tracker to monitor pipeline runs and update database.
"""
from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from .database import PipelineRun, TaskRun, RunStatus, LogEntry
from .db import get_db
import threading
import contextlib


class ExecutionTracker:
    """
    Tracks pipeline and task executions, updating the database in real-time.
    Integrates with the existing pipeline execution system.
    """

    def __init__(self, db: Session, pipeline_run_id: int):
        self.db = db
        self.pipeline_run_id = pipeline_run_id
        self._log_buffer = []
        self._log_lock = threading.Lock()

    def start_task(self, task_name: str) -> TaskRun:
        """Mark a task as starting."""
        task_run = TaskRun(
            pipeline_run_id=self.pipeline_run_id,
            task_name=task_name,
            status=RunStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        self.db.add(task_run)
        self.db.commit()
        self.db.refresh(task_run)
        return task_run

    def complete_task(
        self,
        task_name: str,
        success: bool,
        result: Any = None,
        error: str = None,
        attempt: Optional[int] = None
    ):
        """Mark a task as completed (success or failure)."""
        task_run = self.db.query(TaskRun).filter(
            TaskRun.pipeline_run_id == self.pipeline_run_id,
            TaskRun.task_name == task_name
        ).order_by(TaskRun.started_at.desc()).first()

        if task_run:
            task_run.status = RunStatus.SUCCESS if success else RunStatus.FAILED
            task_run.completed_at = datetime.utcnow()
            task_run.result = result
            task_run.error_message = error
            if attempt is not None:
                task_run.attempt = attempt
            self.db.commit()

    def fail_task(self, task_name: str, error: str, attempt: Optional[int] = None):
        """Mark a task as failed."""
        self.complete_task(task_name, False, error=error, attempt=attempt)

    def log(self, level: str, message: str, logger_name: str = None, task_name: str = None, **extra):
        """Capture a log entry."""
        task_run_id = None
        if task_name:
            task_run = self.db.query(TaskRun).filter(
                TaskRun.pipeline_run_id == self.pipeline_run_id,
                TaskRun.task_name == task_name
            ).order_by(TaskRun.started_at.desc()).first()
            if task_run:
                task_run_id = task_run.id

        with self._log_lock:
            self._log_buffer.append({
                "timestamp": datetime.utcnow(),
                "level": level,
                "logger": logger_name,
                "message": message,
                "pipeline_run_id": self.pipeline_run_id,
                "task_run_id": task_run_id,
                "extra": extra
            })

    def flush_logs(self):
        """Write buffered logs to database."""
        if not self._log_buffer:
            return

        with self._log_lock:
            logs = [
                LogEntry(
                    timestamp=entry["timestamp"],
                    level=entry["level"],
                    logger=entry["logger"],
                    message=entry["message"],
                    pipeline_run_id=entry["pipeline_run_id"],
                    task_run_id=entry["task_run_id"],
                    extra=entry["extra"]
                )
                for entry in self._log_buffer
            ]
            self.db.add_all(logs)
            self.db.commit()
            self._log_buffer.clear()

    def complete_pipeline(self, success: bool):
        """Mark the pipeline run as completed."""
        pipeline_run = self.db.query(PipelineRun).filter(
            PipelineRun.id == self.pipeline_run_id
        ).first()

        if pipeline_run:
            pipeline_run.status = RunStatus.SUCCESS if success else RunStatus.FAILED
            pipeline_run.completed_at = datetime.utcnow()
            self.db.commit()


@contextlib.contextmanager
def tracked_execution(db: Session, pipeline_run_id: int):
    """
    Context manager for tracking pipeline execution.

    Usage:
        with tracked_execution(db, run_id) as tracker:
            # Execute steps, tracker will capture logs and update status
    """
    tracker = ExecutionTracker(db, pipeline_run_id)
    try:
        yield tracker
    finally:
        tracker.flush_logs()
