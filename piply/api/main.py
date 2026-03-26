"""
FastAPI application for Piply UI.
"""
from fastapi import FastAPI, Depends, HTTPException, Query, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func, case
from typing import List, Optional
import os
import yaml
from datetime import datetime

from .db import get_db, init_db
from .database import PipelineRun, TaskRun, RunStatus, LogEntry, Base
from .schemas import (
    PipelineCreate, PipelineResponse, RunCreate, RunResponse,
    RunRetryRequest, TaskResponse, DashboardStats, TenantResponse
)
from .tracker import tracked_execution
from piply.parser.yaml_parser import load_pipeline
from piply.engine.local_engine import LocalEngine
from piply.engine.prefect_engine import PrefectEngine
from piply.core.context import PipelineContext, StepOutput

# Initialize FastAPI
app = FastAPI(
    title="Piply API",
    description="REST API for Piply pipeline orchestration",
    version="0.1.0"
)

# Get the directory where this file is located
current_dir = os.path.dirname(os.path.abspath(__file__))
templates_dir = os.path.join(current_dir, "templates")
static_dir = os.path.join(current_dir, "static")

# Create directories if they don't exist
os.makedirs(templates_dir, exist_ok=True)
os.makedirs(static_dir, exist_ok=True)

# Mount static files and templates
app.mount("/static", StaticFiles(directory=static_dir), name="static")
templates = Jinja2Templates(directory=templates_dir)


@app.on_event("startup")
def startup_event():
    """Initialize database on startup."""
    init_db()


# ========== Pipeline Endpoints ==========

@app.get("/api/pipelines", response_model=List[PipelineResponse])
def list_pipelines(db: Session = Depends(get_db)):
    """List all registered pipelines."""
    # Get distinct pipeline names with their stats
    pipelines = db.query(
        PipelineRun.pipeline_name,
        func.count().label("run_count"),
        func.max(PipelineRun.started_at).label("last_run")
    ).group_by(PipelineRun.pipeline_name).all()

    result = []
    for p in pipelines:
        result.append({
            "id": hash(p.pipeline_name) % (10**9),  # Simple deterministic ID
            "name": p.pipeline_name,
            "steps_count": 0,  # Will be populated from YAML
            "tenants": [],
            "schedule": None,
            "created_at": p.last_run or datetime.utcnow()
        })

    # Try to load actual pipeline configs to get steps/tenants
    for p in result:
        try:
            # Look for YAML file in current directory or examples/
            possible_paths = [
                f"{p['name']}.yaml",
                f"{p['name']}.yml",
                os.path.join("examples", f"{p['name']}.yaml"),
                "piply.yaml"
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    pipeline_obj = load_pipeline(path)
                    p["steps_count"] = len(pipeline_obj.steps)
                    p["tenants"] = pipeline_obj.tenants
                    p["schedule"] = pipeline_obj.config.get("schedule")
                    break
        except Exception:
            pass

    return result


@app.get("/api/pipelines/{pipeline_name}")
def get_pipeline(pipeline_name: str, db: Session = Depends(get_db)):
    """Get pipeline details including recent runs."""
    # Get pipeline metadata from runs
    recent_runs = db.query(PipelineRun).filter(
        PipelineRun.pipeline_name == pipeline_name
    ).order_by(PipelineRun.started_at.desc()).limit(10).all()

    # Try to load pipeline config
    pipeline_config = None
    try:
        possible_paths = [
            f"{pipeline_name}.yaml",
            f"{pipeline_name}.yml",
            os.path.join("examples", f"{pipeline_name}.yaml"),
            "piply.yaml"
        ]
        for path in possible_paths:
            if os.path.exists(path):
                pipeline_obj = load_pipeline(path)
                pipeline_config = pipeline_obj.config
                steps = [{"name": s.name, "type": s.config.get(
                    "type")} for s in pipeline_obj.steps]
                tenants = pipeline_obj.tenants
                break
    except Exception as e:
        raise HTTPException(
            status_code=404, detail=f"Pipeline config not found: {e}")

    return {
        "name": pipeline_name,
        "config": pipeline_config,
        "steps": steps if 'steps' in locals() else [],
        "tenants": tenants if 'tenants' in locals() else [],
        "recent_runs": recent_runs
    }


# ========== Run Endpoints ==========

@app.post("/api/runs", response_model=RunResponse)
def create_run(run_request: RunCreate, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Create and trigger a new pipeline run."""
    # Validate pipeline exists
    pipeline_name = run_request.pipeline_name
    try:
        possible_paths = [
            f"{pipeline_name}.yaml",
            f"{pipeline_name}.yml",
            os.path.join("examples", f"{pipeline_name}.yaml"),
            "piply.yaml"
        ]
        pipeline_path = None
        for path in possible_paths:
            if os.path.exists(path):
                pipeline_path = path
                break
        if not pipeline_path:
            raise HTTPException(
                status_code=404, detail=f"Pipeline '{pipeline_name}' not found")
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

    # Create pipeline run record
    pipeline_run = PipelineRun(
        pipeline_name=pipeline_name,
        tenant=run_request.tenant,
        trigger_type=run_request.trigger_type,
        triggered_by=run_request.triggered_by,
        retry_of=run_request.retry_of,
        config={"pipeline_path": pipeline_path, **
                run_request.config_overrides} if run_request.config_overrides else {"pipeline_path": pipeline_path}
    )
    db.add(pipeline_run)
    db.commit()
    db.refresh(pipeline_run)

    # Execute pipeline in background
    background_tasks.add_task(
        execute_pipeline_background,
        pipeline_path=pipeline_path,
        run_id=pipeline_run.id,
        tenant=run_request.tenant,
        db_url=os.getenv("PIPLY_DATABASE_URL", "sqlite:///./piply.db")
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


@app.get("/api/runs", response_model=List[RunResponse])
def list_runs(
    pipeline: Optional[str] = None,
    tenant: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    db: Session = Depends(get_db)
):
    """List pipeline runs with optional filters."""
    query = db.query(PipelineRun)

    if pipeline:
        query = query.filter(PipelineRun.pipeline_name == pipeline)
    if tenant:
        query = query.filter(PipelineRun.tenant == tenant)
    if status:
        query = query.filter(PipelineRun.status == RunStatus(status))

    runs = query.order_by(PipelineRun.started_at.desc()).limit(limit).all()

    result = []
    for run in runs:
        task_count = db.query(TaskRun).filter(
            TaskRun.pipeline_run_id == run.id).count()
        tasks_completed = db.query(TaskRun).filter(
            TaskRun.pipeline_run_id == run.id,
            TaskRun.status == RunStatus.SUCCESS
        ).count()

        result.append(RunResponse(
            id=run.id,
            pipeline_name=run.pipeline_name,
            tenant=run.tenant,
            status=run.status.value,
            started_at=run.started_at,
            completed_at=run.completed_at,
            trigger_type=run.trigger_type,
            task_count=task_count,
            tasks_completed=tasks_completed
        ))

    return result


@app.get("/api/runs/{run_id}")
def get_run(run_id: int, db: Session = Depends(get_db)):
    """Get detailed information about a specific run."""
    run = db.query(PipelineRun).filter(PipelineRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    tasks = db.query(TaskRun).filter(TaskRun.pipeline_run_id ==
                                     run_id).order_by(TaskRun.started_at).all()

    return {
        "run": run,
        "tasks": tasks
    }


@app.post("/api/runs/{run_id}/retry")
def retry_run(run_id: int, retry_request: RunRetryRequest, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Retry a pipeline run.

    Modes:
    - 'resume': Continue from failed tasks only
    - 'startover': Run all tasks from the beginning
    """
    original_run = db.query(PipelineRun).filter(
        PipelineRun.id == run_id).first()
    if not original_run:
        raise HTTPException(status_code=404, detail="Run not found")

    if original_run.status != RunStatus.FAILED:
        raise HTTPException(
            status_code=400, detail="Only failed runs can be retried")

    # Get pipeline path from config
    pipeline_path = original_run.config.get("pipeline_path")
    if not pipeline_path or not os.path.exists(pipeline_path):
        raise HTTPException(
            status_code=400, detail="Original pipeline configuration not found")

    # Create new run
    new_run = PipelineRun(
        pipeline_name=original_run.pipeline_name,
        tenant=retry_request.tenant or original_run.tenant,
        trigger_type="retry",
        triggered_by="api",
        retry_of=run_id,
        config=original_run.config.copy()
    )
    db.add(new_run)
    db.commit()
    db.refresh(new_run)

    # Execute in background
    background_tasks.add_task(
        execute_pipeline_background,
        pipeline_path=pipeline_path,
        run_id=new_run.id,
        tenant=retry_request.tenant or original_run.tenant,
        retry_mode=retry_request.mode,
        retry_from_run_id=run_id,
        db_url=os.getenv("PIPLY_DATABASE_URL", "sqlite:///./piply.db")
    )

    return {"message": "Retry started", "run_id": new_run.id}


# ========== Task Endpoints ==========

@app.get("/api/runs/{run_id}/tasks", response_model=List[TaskResponse])
def list_run_tasks(run_id: int, db: Session = Depends(get_db)):
    """Get all tasks for a specific run."""
    tasks = db.query(TaskRun).filter(TaskRun.pipeline_run_id ==
                                     run_id).order_by(TaskRun.started_at).all()
    return tasks


@app.get("/api/runs/{run_id}/tasks/{task_name}/logs")
def get_task_logs(run_id: int, task_name: str, db: Session = Depends(get_db)):
    """Get logs for a specific task."""
    task = db.query(TaskRun).filter(
        TaskRun.pipeline_run_id == run_id,
        TaskRun.task_name == task_name
    ).first()

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    # Get structured logs
    logs = db.query(LogEntry).filter(
        LogEntry.task_run_id == task.id
    ).order_by(LogEntry.timestamp).all()

    log_text = "\n".join([
        f"{log.timestamp.isoformat()} [{log.level}] {log.message}"
        for log in logs
    ])

    return {
        "task_name": task_name,
        "run_id": run_id,
        "logs": log_text,
        "structured_logs": logs
    }


# ========== Tenant Endpoints ==========

@app.get("/api/tenants", response_model=List[TenantResponse])
def list_tenants(db: Session = Depends(get_db)):
    """List all tenants with their run statistics."""
    # Get tenants from pipeline runs
    tenants_query = db.query(
        PipelineRun.tenant,
        func.count().label("run_count"),
        func.max(PipelineRun.started_at).label("last_run")
    ).filter(
        PipelineRun.tenant.isnot(None)
    ).group_by(PipelineRun.tenant).all()

    result = []
    for t in tenants_query:
        result.append({
            "name": t.tenant,
            "run_count": t.run_count,
            "last_run": t.last_run
        })

    return result


# ========== Dashboard Endpoints ==========

@app.get("/api/dashboard", response_model=DashboardStats)
def get_dashboard(db: Session = Depends(get_db)):
    """Get dashboard statistics."""
    total_pipelines = db.query(
        func.count(func.distinct(PipelineRun.pipeline_name))
    ).scalar() or 0

    total_runs = db.query(PipelineRun).count()
    running = db.query(PipelineRun).filter(
        PipelineRun.status == RunStatus.RUNNING).count()
    success = db.query(PipelineRun).filter(
        PipelineRun.status == RunStatus.SUCCESS).count()
    failed = db.query(PipelineRun).filter(
        PipelineRun.status == RunStatus.FAILED).count()

    recent_runs = db.query(PipelineRun).order_by(
        PipelineRun.started_at.desc()
    ).limit(20).all()

    recent_runs_data = []
    for run in recent_runs:
        recent_runs_data.append({
            "id": run.id,
            "pipeline": run.pipeline_name,
            "tenant": run.tenant,
            "status": run.status.value,
            "started_at": run.started_at,
            "completed_at": run.completed_at
        })

    # Tenant summary
    from sqlalchemy import case

    tenant_summary = db.query(
        PipelineRun.tenant,
        func.count().label("total_runs"),
        func.sum(case((PipelineRun.status == RunStatus.SUCCESS, 1), else_=0)).label(
            "successful"
        ),
        func.sum(case((PipelineRun.status == RunStatus.FAILED, 1), else_=0)).label(
            "failed"
        )
    ).filter(
        PipelineRun.tenant.isnot(None)
    ).group_by(PipelineRun.tenant).all()

    tenant_data = []
    for t in tenant_summary:
        tenant_data.append({
            "name": t.tenant,
            "total_runs": t.total_runs,
            "successful": t.successful or 0,
            "failed": t.failed or 0,
            "success_rate": (t.successful or 0) / t.total_runs if t.total_runs > 0 else 0
        })

    return DashboardStats(
        total_pipelines=total_pipelines,
        total_runs=total_runs,
        running=running,
        success=success,
        failed=failed,
        recent_runs=recent_runs_data,
        tenant_summary=tenant_data
    )


# ========== UI Endpoints ==========

@app.get("/", response_class=HTMLResponse)
def dashboard():
    """Main dashboard page."""
    with open(os.path.join(templates_dir, "dashboard.html"), "r") as f:
        return f.read()


@app.get("/pipelines", response_class=HTMLResponse)
def pipelines_page():
    """Pipelines listing page."""
    with open(os.path.join(templates_dir, "pipelines.html"), "r") as f:
        return f.read()


@app.get("/pipelines/{pipeline_name}", response_class=HTMLResponse)
def pipeline_detail_page(pipeline_name: str):
    """Pipeline detail page with DAG visualization."""
    with open(os.path.join(templates_dir, "pipeline_detail.html"), "r") as f:
        return f.read()


@app.get("/runs/{run_id}", response_class=HTMLResponse)
def run_detail_page(run_id: int):
    """Run detail page with task status and logs."""
    with open(os.path.join(templates_dir, "run_detail.html"), "r") as f:
        return f.read()


# ========== Background Execution ==========

def execute_pipeline_background(
    pipeline_path: str,
    run_id: int,
    tenant: Optional[str] = None,
    retry_mode: Optional[str] = None,
    retry_from_run_id: Optional[int] = None,
    db_url: str = None
):
    """
    Background task to execute a pipeline and track results.

    Args:
        pipeline_path: Path to the YAML pipeline file
        run_id: Database ID of the pipeline run
        tenant: Optional tenant to run for
        retry_mode: 'resume' or 'startover' for retries
        retry_from_run_id: Original run ID if this is a retry
        db_url: Database connection URL
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    # Create new DB session for this background thread
    engine = create_engine(db_url, connect_args={
                           "check_same_thread": False} if db_url.startswith("sqlite") else {})
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()

    try:
        # Update run status to running
        pipeline_run = db.query(PipelineRun).filter(
            PipelineRun.id == run_id).first()
        if pipeline_run:
            pipeline_run.status = RunStatus.RUNNING
            db.commit()

        # Load pipeline
        pipeline = load_pipeline(pipeline_path)

        # Create tracker
        tracker = tracked_execution(db, run_id)
        tracker_gen = tracker.__enter__()

        try:
            # Choose engine
            engine_choice = pipeline.config.get("engine", "local")
            if engine_choice == "prefect":
                eng = PrefectEngine()
            else:
                eng = LocalEngine()

            # Determine retry_failed mode
            # If retry_mode is 'resume', we only run tasks that failed in the original run
            retry_failed = (retry_mode == "resume")

            # For resume mode, we need to pre-populate the context with successful task outputs
            # from the previous run so they can be skipped
            if retry_failed and retry_from_run_id:
                # Get successful task outputs from the previous run
                previous_tasks = db.query(TaskRun).filter(
                    TaskRun.pipeline_run_id == retry_from_run_id,
                    TaskRun.status == RunStatus.SUCCESS
                ).all()

                # Pre-populate context with successful outputs
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

            # Run pipeline
            pipeline.run(eng, tenant=tenant, retry_failed=retry_failed)

            # Mark pipeline as successful
            tracker.complete_pipeline(True)
            if pipeline_run:
                pipeline_run.status = RunStatus.SUCCESS

        except Exception as e:
            # Mark pipeline as failed
            tracker.complete_pipeline(False)
            if pipeline_run:
                pipeline_run.status = RunStatus.FAILED
            tracker.log("ERROR", f"Pipeline execution failed: {e}", extra={
                        "exception": str(e)})
        finally:
            tracker.flush_logs()
            tracker_gen.__exit__(None, None, None)
            db.commit()

    except Exception as e:
        print(f"Critical error in background execution: {e}")
    finally:
        db.close()
