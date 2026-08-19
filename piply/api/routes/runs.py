"""Run routes expose run history, logs, and retry actions."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from piply.api.auth import require_permission, visible_pipelines
from piply.api.schemas import (
    LogResponse,
    RetryRequest,
    RunDetailResponse,
    RunResponse,
    TaskDetailResponse,
    TaskOutputResponse,
    TaskRunResponse,
    UpcomingRunResponse,
)

router = APIRouter(prefix="/api/runs", tags=["runs"])


def _get_service(request: Request):
    """Resolve the shared PipelineService from the app state."""
    return request.app.state.service


def _guard_run(request: Request, run_id: str, action: str):
    """Check a permission against the pipeline that owns a run."""
    service = _get_service(request)
    run = service.store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Unknown run '{run_id}'")
    require_permission(request, action, run.pipeline_id)
    return run


@router.get("", response_model=list[RunResponse])
def list_runs(
    request: Request,
    pipeline_id: str | None = None,
    status: str | None = None,
    tenant: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
) -> list[RunResponse]:
    """List runs the caller may see."""
    require_permission(request, "view", pipeline_id)
    service = _get_service(request)
    runs = service.list_runs(pipeline_id=pipeline_id, status=status, tenant_id=tenant, limit=limit)
    allowed = {item.pipeline_id for item in visible_pipelines(request, service.list_pipelines())}
    return [RunResponse.from_record(item) for item in runs if item.pipeline_id in allowed]


@router.get("/{run_id}", response_model=RunDetailResponse)
def get_run(request: Request, run_id: str) -> RunDetailResponse:
    """Return one run with task runs and raw logs."""
    _guard_run(request, run_id, "view")
    service = _get_service(request)
    try:
        payload = service.get_run_detail(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return RunDetailResponse(
        run=RunResponse.from_record(payload["run"]),
        task_runs=[TaskRunResponse.from_record(item) for item in payload["task_runs"]],
        logs=[LogResponse.from_record(item) for item in payload["logs"]],
        upcoming_runs=[UpcomingRunResponse(**item) for item in payload["upcoming_runs"]],
        downstream=payload["downstream"],
        upstream=payload["upstream"],
        artifacts=payload["artifacts"],
        has_run_config=bool(payload["has_run_config"]),
    )


@router.post("/{run_id}/retry", response_model=RunResponse)
def retry_run(request: Request, run_id: str, payload: RetryRequest) -> RunResponse:
    """Create a retry run in startover or resume mode."""
    _guard_run(request, run_id, "run")
    service = _get_service(request)
    if payload.mode not in {"resume", "startover"}:
        raise HTTPException(status_code=400, detail="mode must be 'resume' or 'startover'")
    try:
        retry_run_record = service.retry_run(
            run_id,
            mode=payload.mode,  # type: ignore[arg-type]
            task_id=payload.task_id,
            wait=False,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RunResponse.from_record(retry_run_record)


@router.get("/{run_id}/logs")
def get_run_logs(
    request: Request,
    run_id: str,
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    """Return paginated raw logs for a specific run."""
    _guard_run(request, run_id, "view")
    service = _get_service(request)
    try:
        run = service.store.get_run(run_id)
        if not run:
            raise KeyError(run_id)
        logs = service.store.list_logs(run_id, limit=limit, offset=offset)
        return {
            "run_id": run_id,
            "total": run.log_count,
            "limit": limit,
            "offset": offset,
            "logs": [LogResponse.from_record(item).model_dump() for item in logs],
        }
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{run_id}/tasks/{task_id}", response_model=TaskDetailResponse)
def get_task_detail(request: Request, run_id: str, task_id: str) -> TaskDetailResponse:
    """Return one task run with logs and output metadata."""
    _guard_run(request, run_id, "view")
    service = _get_service(request)
    try:
        payload = service.get_task_detail(run_id, task_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    output = payload["output"]
    return TaskDetailResponse(
        run=RunResponse.from_record(payload["run"]),
        task_run=TaskRunResponse.from_record(payload["task_run"]),
        logs=[LogResponse.from_record(item) for item in payload["logs"]],
        output=TaskOutputResponse.from_record(output) if output is not None else None,
    )


@router.get("/{run_id}/tasks/{task_id}/output", response_model=TaskOutputResponse)
def get_task_output(request: Request, run_id: str, task_id: str) -> TaskOutputResponse:
    """Return captured task output metadata and JSON value when available."""
    _guard_run(request, run_id, "view")
    service = _get_service(request)
    try:
        output = service.get_task_output(run_id, task_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return TaskOutputResponse.from_record(output)


@router.post("/{run_id}/tasks/{task_id}/retry", response_model=RunResponse)
def retry_task_from_run(request: Request, run_id: str, task_id: str) -> RunResponse:
    """Resume a failed run from a selected task."""
    _guard_run(request, run_id, "run")
    service = _get_service(request)
    try:
        run = service.retry_run(run_id, mode="resume", task_id=task_id, wait=False)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RunResponse.from_record(run)


@router.post("/{run_id}/cancel", response_model=RunResponse)
def cancel_run(request: Request, run_id: str) -> RunResponse:
    """Cancel one queued or running run."""
    _guard_run(request, run_id, "run")
    service = _get_service(request)
    try:
        run = service.cancel_run(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RunResponse.from_record(run)


@router.delete("/{run_id}")
def delete_run(request: Request, run_id: str) -> dict[str, str]:
    """Delete one finished run from history."""
    _guard_run(request, run_id, "edit")
    service = _get_service(request)
    try:
        service.delete_run(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "deleted", "run_id": run_id}
