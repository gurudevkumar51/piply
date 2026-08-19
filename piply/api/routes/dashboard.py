"""Dashboard routes expose global project and scheduler state."""

from __future__ import annotations

from fastapi import APIRouter, Request

from piply.api.auth import filter_by_pipeline, require_permission, visible_pipelines
from piply.api.schemas import (
    DashboardResponse,
    DashboardStatsResponse,
    PipelineResponse,
    RunResponse,
    SchedulerResponse,
)

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


def _get_service(request: Request):
    """Resolve the shared PipelineService from the app state."""
    return request.app.state.service


@router.get("", response_model=DashboardResponse)
def get_dashboard(request: Request) -> DashboardResponse:
    """Return the dashboard payload, narrowed to what the caller may see.

    Aggregate counts stay installation-wide; the listed pipelines and runs are
    filtered, so a restricted account never learns another tenant's run ids.
    """
    require_permission(request, "view")
    payload = _get_service(request).dashboard()
    return DashboardResponse(
        project=payload["project"],
        stats=DashboardStatsResponse.from_stats(payload["stats"]),
        pipelines=[PipelineResponse.from_summary(item) for item in visible_pipelines(request, payload["pipelines"])],
        recent_runs=[RunResponse.from_record(item) for item in filter_by_pipeline(request, payload["recent_runs"])],
        recent_failures=[
            RunResponse.from_record(item) for item in filter_by_pipeline(request, payload["recent_failures"])
        ],
        active_pipelines=[
            PipelineResponse.from_summary(item) for item in visible_pipelines(request, payload["active_pipelines"])
        ],
        runtime_trend=payload["runtime_trend"],
        runtime_metrics=payload["runtime_metrics"],
        scheduler=SchedulerResponse(**payload["scheduler"]),
    )


@router.get("/scheduler", response_model=SchedulerResponse)
def get_scheduler_snapshot(request: Request) -> SchedulerResponse:
    """Return lightweight scheduler status for nav and live UI polling."""
    require_permission(request, "view")
    return SchedulerResponse(**_get_service(request).scheduler_snapshot())
