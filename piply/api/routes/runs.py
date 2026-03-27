"""
Run-related API routes.
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session

from ..db import get_db
from ..database import PipelineRun, TaskRun, RunStatus, LogEntry
from ..schemas import RunCreate, RunResponse, RunRetryRequest, TaskResponse
from ..services.run_service import RunService

router = APIRouter(prefix="/api/runs", tags=["runs"])


@router.post("", response_model=RunResponse)
def create_run(run_request: RunCreate, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Create and trigger a new pipeline run.
    """
    service = RunService(db, background_tasks)
    try:
        return service.create_run(run_request)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[RunResponse])
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


@router.get("/{run_id}")
def get_run(run_id: int, db: Session = Depends(get_db)):
    """Get detailed information about a specific run."""
    run = db.query(PipelineRun).filter(PipelineRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    tasks = db.query(TaskRun).filter(TaskRun.pipeline_run_id ==
                                     run_id).order_by(TaskRun.started_at).all()

    task_items = [
        TaskResponse(
            id=task.id,
            task_name=task.task_name,
            status=task.status.value,
            started_at=task.started_at,
            completed_at=task.completed_at,
            attempt=task.attempt,
            error_message=task.error_message
        )
        for task in tasks
    ]

    run_item = RunResponse(
        id=run.id,
        pipeline_name=run.pipeline_name,
        tenant=run.tenant,
        status=run.status.value,
        started_at=run.started_at,
        completed_at=run.completed_at,
        trigger_type=run.trigger_type,
        task_count=len(task_items),
        tasks_completed=sum(1 for task in task_items if task.status == "success")
    )

    return {"run": run_item, "tasks": task_items}


@router.post("/{run_id}/retry")
def retry_run(run_id: int, retry_request: RunRetryRequest, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Retry a pipeline run.
    Modes: 'resume' (continue from failed) or 'startover' (run all).
    """
    service = RunService(db, background_tasks)
    try:
        return service.retry_run(run_id, retry_request)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{run_id}/tasks", response_model=List)
def list_run_tasks(run_id: int, db: Session = Depends(get_db)):
    """Get all tasks for a specific run."""
    tasks = db.query(TaskRun).filter(TaskRun.pipeline_run_id ==
                                     run_id).order_by(TaskRun.started_at).all()
    return tasks


@router.get("/{run_id}/tasks/{task_name}/logs")
def get_task_logs(run_id: int, task_name: str, db: Session = Depends(get_db)):
    """Get logs for a specific task."""
    task = db.query(TaskRun).filter(
        TaskRun.pipeline_run_id == run_id,
        TaskRun.task_name == task_name
    ).first()

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    logs = db.query(LogEntry).filter(LogEntry.task_run_id ==
                                     task.id).order_by(LogEntry.timestamp).all()

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
