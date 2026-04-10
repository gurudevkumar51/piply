"""Run routes expose run history, logs, and retry actions."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from piply.api.schemas import (
    LogResponse,
    RetryRequest,
    RunDetailResponse,
    RunResponse,
    TaskRunResponse,
    UpcomingRunResponse,
)

router = APIRouter(prefix="/api/runs", tags=["runs"])


def _get_service(request: Request):
    """Resolve the shared PipelineService from the app state."""
    return request.app.state.service


@router.get("", response_model=list[RunResponse])
def list_runs(
    request: Request,
    pipeline_id: str | None = None,
    status: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
) -> list[RunResponse]:
    """List runs with optional filters."""
    service = _get_service(request)
    runs = service.list_runs(pipeline_id=pipeline_id, status=status, limit=limit)
    return [RunResponse.from_record(item) for item in runs]


@router.get("/{run_id}", response_model=RunDetailResponse)
def get_run(request: Request, run_id: str) -> RunDetailResponse:
    """Return one run with task runs and raw logs."""
    service = _get_service(request)
    try:
        payload = service.get_run_detail(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return RunDetailResponse(
        run=RunResponse.from_record(payload["run"]),
        task_runs=[TaskRunResponse.from_record(item) for item in payload["task_runs"]],
        logs=[LogResponse.from_record(item) for item in payload["logs"]],
        upcoming_runs=[
            UpcomingRunResponse(**item) for item in payload["upcoming_runs"]
        ],
    )


@router.post("/{run_id}/retry", response_model=RunResponse)
def retry_run(request: Request, run_id: str, payload: RetryRequest) -> RunResponse:
    """Create a retry run in startover or resume mode."""
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


@router.post("/{run_id}/cancel", response_model=RunResponse)
def cancel_run(request: Request, run_id: str) -> RunResponse:
    """Cancel one queued or running run."""
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
    service = _get_service(request)
    try:
        service.delete_run(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "deleted", "run_id": run_id}
