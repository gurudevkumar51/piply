"""Pipeline routes expose definitions, summaries, and run actions."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from piply.api.auth import get_service, require_admin, require_permission, visible_pipelines
from piply.api.schemas import PipelineDetailResponse, PipelineResponse, RunResponse, TaskResponse, TriggerRunRequest

router = APIRouter(prefix="/api/pipelines", tags=["pipelines"])


def _actor(user) -> str | None:
    """Return the username to record against an action, if authenticated."""
    return None if user is None else user.username


def _guard_command_overrides(request: Request, payload: TriggerRunRequest | None) -> None:
    """Restrict command overrides to administrators.

    An override replaces the command a task runs, so allowing it under a plain
    ``run`` grant would turn "may run this one pipeline" into "may execute any
    command as the Piply process". Admins keep it for debugging.
    """
    if payload is not None and payload.command_overrides:
        require_admin(request, "Only administrators can override task commands.")


@router.get("", response_model=list[PipelineResponse])
def list_pipelines(request: Request) -> list[PipelineResponse]:
    """List the pipelines the caller may see."""
    require_permission(request, "view")
    service = get_service(request)
    return [PipelineResponse.from_summary(item) for item in visible_pipelines(request, service.list_pipelines())]


def _runtime_variables(request: Request, payload: TriggerRunRequest | None) -> dict[str, str] | None:
    """Validate the runtime values supplied with a manual run."""
    if payload is None or not payload.variables:
        return None
    service = get_service(request)
    try:
        return service.validate_runtime_inputs(payload.variables)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{pipeline_id}/runtime-inputs", response_model=dict[str, object])
def get_runtime_inputs(
    request: Request,
    pipeline_id: str,
    task_id: str | None = None,
    source_run_id: str | None = None,
) -> dict[str, object]:
    """Report the values a manual run of this pipeline still needs.

    The UI calls this before starting a run so it can ask for missing values
    instead of executing a command containing a literal `{practice}`.
    """
    require_permission(request, "run", pipeline_id)
    try:
        return get_service(request).runtime_inputs(
            pipeline_id,
            task_id=task_id,
            source_run_id=source_run_id,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{pipeline_id}", response_model=PipelineDetailResponse)
def get_pipeline(request: Request, pipeline_id: str) -> PipelineDetailResponse:
    """Return one pipeline with tasks and recent runs."""
    require_permission(request, "view", pipeline_id)
    service = get_service(request)
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
    user = require_permission(request, "run", pipeline_id)
    _guard_command_overrides(request, payload)
    service = get_service(request)
    runtime_variables = _runtime_variables(request, payload)
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
            inherited_variables=runtime_variables,
            actor=_actor(user),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RunResponse.from_record(run)


@router.get("/{pipeline_id}/tasks/{task_id}", response_model=TaskResponse)
def get_pipeline_task(request: Request, pipeline_id: str, task_id: str) -> TaskResponse:
    """Return one task definition from a pipeline."""
    require_permission(request, "view", pipeline_id)
    service = get_service(request)
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
    user = require_permission(request, "run", pipeline_id)
    _guard_command_overrides(request, payload)
    service = get_service(request)
    runtime_variables = _runtime_variables(request, payload)
    try:
        initial_context = {}
        if payload is not None and payload.params:
            initial_context["params"] = payload.params
        run = service.trigger_task(
            pipeline_id,
            task_id,
            trigger="task",
            wait=False,
            command_overrides=(payload.command_overrides if payload is not None else None),
            tenant_id=(payload.tenant_id if payload is not None else None),
            initial_context=initial_context,
            inherited_variables=runtime_variables,
            actor=_actor(user),
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
    require_permission(request, "run", pipeline_id)
    require_permission(request, "run", target_pipeline_id)
    _guard_command_overrides(request, payload)
    service = get_service(request)
    runtime_variables = _runtime_variables(request, payload)
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
            inherited_variables=runtime_variables,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RunResponse.from_record(run)


@router.post("/{pipeline_id}/pause", response_model=PipelineResponse)
def pause_pipeline(request: Request, pipeline_id: str) -> PipelineResponse:
    """Pause one pipeline schedule."""
    user = require_permission(request, "edit", pipeline_id)
    service = get_service(request)
    try:
        summary = service.set_pipeline_paused(pipeline_id, True, actor=_actor(user))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return PipelineResponse.from_summary(summary)


@router.post("/{pipeline_id}/resume", response_model=PipelineResponse)
def resume_pipeline(request: Request, pipeline_id: str) -> PipelineResponse:
    """Resume one pipeline schedule."""
    user = require_permission(request, "edit", pipeline_id)
    service = get_service(request)
    try:
        summary = service.set_pipeline_paused(pipeline_id, False, actor=_actor(user))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return PipelineResponse.from_summary(summary)


@router.delete("/{pipeline_id}")
def delete_pipeline(request: Request, pipeline_id: str) -> dict[str, str]:
    """Delete one pipeline definition and its stored history."""
    require_permission(request, "edit", pipeline_id)
    service = get_service(request)
    try:
        service.delete_pipeline(pipeline_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "deleted", "pipeline_id": pipeline_id}
