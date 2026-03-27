"""
Dashboard and tenant API routes.
"""
from typing import List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, case

from ..db import get_db
from ..database import PipelineRun, TaskRun, RunStatus
from ..schemas import DashboardStats, TenantResponse

router = APIRouter(prefix="/api", tags=["dashboard"])


@router.get("/dashboard", response_model=DashboardStats)
def get_dashboard(db: Session = Depends(get_db)):
    """Get dashboard statistics."""
    total_pipelines = db.query(func.count(
        func.distinct(PipelineRun.pipeline_name))).scalar() or 0

    total_runs = db.query(PipelineRun).count()
    running = db.query(PipelineRun).filter(
        PipelineRun.status == RunStatus.RUNNING).count()
    success = db.query(PipelineRun).filter(
        PipelineRun.status == RunStatus.SUCCESS).count()
    failed = db.query(PipelineRun).filter(
        PipelineRun.status == RunStatus.FAILED).count()

    recent_runs = db.query(PipelineRun).order_by(
        PipelineRun.started_at.desc()).limit(20).all()

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
    tenant_summary = db.query(
        PipelineRun.tenant,
        func.count().label("total_runs"),
        func.sum(case((PipelineRun.status == RunStatus.SUCCESS, 1), else_=0)).label(
            "successful"),
        func.sum(case((PipelineRun.status == RunStatus.FAILED, 1), else_=0)).label(
            "failed")
    ).filter(PipelineRun.tenant.isnot(None)).group_by(PipelineRun.tenant).all()

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


@router.get("/tenants", response_model=List[TenantResponse])
def list_tenants(db: Session = Depends(get_db)):
    """List all tenants with their run statistics."""
    tenants_query = db.query(
        PipelineRun.tenant,
        func.count().label("run_count"),
        func.max(PipelineRun.started_at).label("last_run")
    ).filter(PipelineRun.tenant.isnot(None)).group_by(PipelineRun.tenant).all()

    result = []
    for t in tenants_query:
        result.append({
            "name": t.tenant,
            "run_count": t.run_count,
            "last_run": t.last_run
        })

    return result
