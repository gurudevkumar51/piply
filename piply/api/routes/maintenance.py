"""Preview, artifact browsing, backfill, and retention endpoints."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse

from piply.api.schemas import BackfillScheduleRequest, PreviewRequest, PruneRequest, RunResponse
from piply.core.artifacts import is_readable_artifact

router = APIRouter(tags=["operations"])


def _get_service(request: Request):
    """Resolve the shared PipelineService from the app state."""
    return request.app.state.service


@router.post("/api/pipelines/{pipeline_id}/preview", response_model=dict[str, object])
def preview_pipeline(
    request: Request,
    pipeline_id: str,
    payload: PreviewRequest | None = None,
) -> dict[str, object]:
    """Return a dry-run preview of what a run would execute."""
    service = _get_service(request)
    try:
        preview = service.preview_pipeline(
            pipeline_id,
            params=(payload.params if payload is not None else None),
            tenant_id=(payload.tenant_id if payload is not None else None),
            command_overrides=(payload.command_overrides if payload is not None else None),
            source_run_id=(payload.source_run_id if payload is not None else None),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return preview.as_dict()


@router.get("/api/pipelines/{pipeline_id}/preview", response_model=dict[str, object])
def get_pipeline_preview(request: Request, pipeline_id: str) -> dict[str, object]:
    """Return the dry-run preview for a pipeline using its configured values."""
    service = _get_service(request)
    try:
        return service.preview_pipeline(pipeline_id).as_dict()
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/api/runs/{run_id}/artifacts", response_model=list[dict[str, object]])
def list_run_artifacts(request: Request, run_id: str, task_id: str | None = None) -> list[dict[str, object]]:
    """List the files produced by one run."""
    service = _get_service(request)
    try:
        return service.list_run_artifacts(run_id, task_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/api/runs/{run_id}/artifacts/download")
def download_run_artifact(request: Request, run_id: str, path: str = Query(...)) -> FileResponse:
    """Download one recorded artifact.

    The requested path must be one this run actually recorded and must resolve
    inside an allowed root, so a crafted path cannot read unrelated files.
    """
    service = _get_service(request)
    try:
        artifacts = service.list_run_artifacts(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    match = next((item for item in artifacts if str(item["path"]) == path), None)
    if match is None:
        raise HTTPException(status_code=404, detail="This run did not record an artifact at that path.")
    candidate = Path(path)
    if not is_readable_artifact(candidate, service.artifact_roots()):
        raise HTTPException(status_code=404, detail="The artifact is no longer readable on disk.")
    return FileResponse(
        candidate,
        filename=str(match["name"]),
        media_type=str(match.get("content_type") or "application/octet-stream"),
    )


@router.post("/api/runs/{run_id}/backfill", response_model=RunResponse)
def backfill_run(request: Request, run_id: str) -> RunResponse:
    """Re-execute one historic run using the configuration it originally captured."""
    service = _get_service(request)
    try:
        run = service.backfill_run(run_id, wait=False)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RunResponse.from_record(run)


@router.post("/api/pipelines/{pipeline_id}/backfill", response_model=dict[str, object])
def backfill_schedule(request: Request, pipeline_id: str, payload: BackfillScheduleRequest) -> dict[str, object]:
    """Queue one run per scheduled slot inside a historic window."""
    service = _get_service(request)
    try:
        slots = service.backfill_schedule(
            pipeline_id,
            start=payload.start,
            end=payload.end,
            limit=payload.limit,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "pipeline_id": pipeline_id,
        "queued": len(slots),
        "slots": [slot.isoformat() for slot in slots],
    }


@router.post("/api/maintenance/prune", response_model=dict[str, object])
def prune_history(request: Request, payload: PruneRequest | None = None) -> dict[str, object]:
    """Delete history beyond the configured retention window."""
    service = _get_service(request)
    overrides: dict[str, int] = {}
    if payload is not None:
        if payload.run_retention_days is not None:
            overrides["run_retention_days"] = payload.run_retention_days
        if payload.log_retention_days is not None:
            overrides["log_retention_days"] = payload.log_retention_days
        if payload.max_runs_per_pipeline is not None:
            overrides["max_runs_per_pipeline"] = payload.max_runs_per_pipeline
    return service.prune(
        dry_run=bool(payload.dry_run) if payload is not None else False,
        vacuum=bool(payload.vacuum) if payload is not None else True,
        **overrides,
    )


@router.get("/api/runs/{run_id}/config", response_model=dict[str, object])
def get_run_config(request: Request, run_id: str) -> dict[str, object]:
    """Return the runtime configuration snapshot captured for one run."""
    service = _get_service(request)
    if service.store.get_run(run_id) is None:
        raise HTTPException(status_code=404, detail=f"Unknown run '{run_id}'")
    snapshot = service.store.get_run_config(run_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="This run was created before runtime configuration was captured.")
    return snapshot


@router.get("/api/logs/stream", response_model=list[dict[str, object]])
def stream_logs(
    request: Request,
    run_id: str | None = None,
    pipeline_id: str | None = None,
    task_id: str | None = None,
    after: datetime | None = None,
    limit: int = Query(default=500, ge=1, le=5000),
) -> list[dict[str, object]]:
    """Return log lines newer than ``after`` for incremental log following."""
    return _get_service(request).tail_logs(
        run_id=run_id,
        pipeline_id=pipeline_id,
        task_id=task_id,
        after=after,
        limit=limit,
    )
