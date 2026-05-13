"""Pipeline routes expose definitions, summaries, and run actions."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from piply.api.schemas import (
    PipelineDetailResponse,
    PipelineResponse,
    RunResponse,
    TaskResponse,
    TriggerRunRequest,
)

router = APIRouter(prefix="/api/pipelines", tags=["pipelines"])


def _get_service(request: Request):
    """Resolve the shared PipelineService from the app state."""
    return request.app.state.service


@router.get("", response_model=list[PipelineResponse])
def list_pipelines(request: Request) -> list[PipelineResponse]:
    """List configured pipelines."""
    service = _get_service(request)
    return [PipelineResponse.from_summary(item) for item in service.list_pipelines()]


@router.get("/{pipeline_id}", response_model=PipelineDetailResponse)
def get_pipeline(request: Request, pipeline_id: str) -> PipelineDetailResponse:
    """Return one pipeline with tasks and recent runs."""
    service = _get_service(request)
    try:
        payload = service.get_pipeline_detail(pipeline_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return PipelineDetailResponse.from_payload(payload)


@router.post("/{pipeline_id}/run", response_model=RunResponse)
def trigger_pipeline(
    request: Request,
    pipeline_id: str,
    payload: TriggerRunRequest | None = None,
) -> RunResponse:
    """Trigger one manual pipeline run."""
    service = _get_service(request)
    try:
        initial_context = {}
        if payload is not None and payload.params:
            initial_context["params"] = payload.params
        run = service.trigger_pipeline(
            pipeline_id,
            trigger="manual",
            wait=False,
            command_overrides=(payload.command_overrides if payload is not None else None),
            tenant_id=(payload.tenant_id if payload is not None else None),
            initial_context=initial_context,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RunResponse.from_record(run)


@router.get("/{pipeline_id}/tasks/{task_id}", response_model=TaskResponse)
def get_pipeline_task(request: Request, pipeline_id: str, task_id: str) -> TaskResponse:
    """Return one task definition from a pipeline."""
    service = _get_service(request)
    try:
        pipeline = service.get_pipeline(pipeline_id)
        task = pipeline.tasks[task_id]
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return TaskResponse.from_definition(task)


@router.post("/{pipeline_id}/tasks/{task_id}/run", response_model=RunResponse)
def trigger_pipeline_task(
    request: Request,
    pipeline_id: str,
    task_id: str,
    payload: TriggerRunRequest | None = None,
) -> RunResponse:
    """Trigger one task and its upstream dependencies as a focused manual run."""
    service = _get_service(request)
    try:
        run = service.trigger_task(
            pipeline_id,
            task_id,
            trigger="task",
            wait=False,
            command_overrides=(payload.command_overrides if payload is not None else None),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RunResponse.from_record(run)


@router.post("/{pipeline_id}/chain/{target_pipeline_id}", response_model=RunResponse)
def chain_pipeline(
    request: Request,
    pipeline_id: str,
    target_pipeline_id: str,
    payload: TriggerRunRequest | None = None,
) -> RunResponse:
    """Trigger a downstream pipeline while preserving parent and tenant context."""
    service = _get_service(request)
    try:
        service.get_pipeline(pipeline_id)
        initial_context = {"params": payload.params} if payload is not None and payload.params else {}
        run = service.trigger_pipeline(
            target_pipeline_id,
            trigger="pipeline",
            wait=False,
            parent_pipeline_id=pipeline_id,
            tenant_id=(payload.tenant_id if payload is not None else None),
            initial_context=initial_context,
            command_overrides=(payload.command_overrides if payload is not None else None),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RunResponse.from_record(run)


@router.post("/{pipeline_id}/pause", response_model=PipelineResponse)
def pause_pipeline(request: Request, pipeline_id: str) -> PipelineResponse:
    """Pause one pipeline schedule."""
    service = _get_service(request)
    try:
        summary = service.set_pipeline_paused(pipeline_id, True)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return PipelineResponse.from_summary(summary)


@router.post("/{pipeline_id}/resume", response_model=PipelineResponse)
def resume_pipeline(request: Request, pipeline_id: str) -> PipelineResponse:
    """Resume one pipeline schedule."""
    service = _get_service(request)
    try:
        summary = service.set_pipeline_paused(pipeline_id, False)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return PipelineResponse.from_summary(summary)


@router.delete("/{pipeline_id}")
def delete_pipeline(request: Request, pipeline_id: str) -> dict[str, str]:
    """Delete one pipeline definition and its stored history."""
    service = _get_service(request)
    try:
        service.delete_pipeline(pipeline_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "deleted", "pipeline_id": pipeline_id}
