"""Server-rendered UI routes for the Piply dashboard."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, Response

from piply.api.schemas import RunResponse

router = APIRouter(tags=["ui"])


def _templates(request: Request):
    """Resolve the shared Jinja template environment."""
    return request.app.state.templates


def _service(request: Request):
    """Resolve the shared PipelineService from the app state."""
    return request.app.state.service


@router.get("/", response_class=HTMLResponse)
def dashboard_page(request: Request) -> HTMLResponse:
    """Render the dashboard page."""
    service = _service(request)
    payload = service.dashboard()
    return _templates(request).TemplateResponse(
        request,
        "dashboard.html",
        {
            "project": payload["project"],
            "stats": payload["stats"],
            "pipelines": payload["pipelines"],
            "recent_runs": payload["recent_runs"],
            "recent_failures": payload["recent_failures"],
            "active_pipelines": payload["active_pipelines"],
            "runtime_trend": payload["runtime_trend"],
            "runtime_metrics": payload["runtime_metrics"],
            "scheduler": payload["scheduler"],
            "page": "dashboard",
        },
    )


@router.get("/logout", response_class=HTMLResponse)
def logout_page(request: Request) -> Response:
    """Clear basic auth credentials by forcing a 401 challenge."""
    return Response(
        status_code=401,
        headers={"WWW-Authenticate": 'Basic realm="Piply"'},
        content="Logged out successfully. You can close this tab or return to the dashboard to log in again.",
    )


def _pipeline_group_key(summary) -> tuple[str, str]:
    """Return the grouping id and label for one pipeline summary.

    Deployments of the same template belong together, which matters when one
    template is deployed once per tenant.
    """
    if summary.template_id:
        return f"template:{summary.template_id}", f"Template: {summary.template_id}"
    if summary.tags:
        return f"tag:{summary.tags[0]}", f"Tag: {summary.tags[0]}"
    return "standalone", "Standalone pipelines"


@router.get("/pipelines", response_class=HTMLResponse)
def pipelines_page(request: Request) -> HTMLResponse:
    """Render the pipeline list page."""
    service = _service(request)
    summaries = service.list_pipelines()

    groups: dict[str, dict[str, object]] = {}
    for summary in summaries:
        group_id, group_label = _pipeline_group_key(summary)
        group = groups.setdefault(
            group_id,
            {"group_id": group_id, "label": group_label, "template_id": summary.template_id, "pipelines": []},
        )
        group["pipelines"].append(summary)  # type: ignore[union-attr]

    # Multi-deployment templates first, then everything else alphabetically.
    ordered_groups = sorted(
        groups.values(),
        key=lambda item: (
            0 if len(item["pipelines"]) > 1 and item["group_id"] != "standalone" else 1,  # type: ignore[arg-type]
            str(item["label"]).lower(),
        ),
    )

    payloads = [
        {
            "pipeline_id": summary.pipeline_id,
            "title": summary.title,
            "description": summary.description,
            "group_id": _pipeline_group_key(summary)[0],
            "template_id": summary.template_id,
            "deployment_id": summary.deployment_id,
            "tags": list(summary.tags),
            "enabled": summary.enabled,
            "paused": summary.paused,
            "schedule_text": summary.schedule_text,
            "next_run_at": None if summary.next_run_at is None else summary.next_run_at.isoformat(),
            "next_run_label": summary.next_run_label,
            "task_count": summary.task_count,
            "active_runs": summary.active_runs,
            "trigger_targets": list(summary.trigger_targets),
            "triggered_by": list(summary.triggered_by),
            "last_run_status": summary.last_run.status if summary.last_run else None,
            "last_run_id": summary.last_run.run_id if summary.last_run else None,
            "last_run_at": (
                (summary.last_run.started_at or summary.last_run.created_at).isoformat() if summary.last_run else None
            ),
            "last_run_duration_seconds": summary.last_run.duration_seconds if summary.last_run else None,
        }
        for summary in summaries
    ]

    return _templates(request).TemplateResponse(
        request,
        "pipelines.html",
        {
            "project": service.project,
            "pipelines": summaries,
            "pipeline_payloads": payloads,
            "groups": ordered_groups,
            "scheduler": service.scheduler_snapshot(),
            "page": "pipelines",
        },
    )


@router.get("/diagnostics", response_class=HTMLResponse)
def diagnostics_page(request: Request) -> HTMLResponse:
    """Render the runtime diagnostics page."""
    service = _service(request)
    payload = service.diagnostics()
    return _templates(request).TemplateResponse(
        request,
        "diagnostics.html",
        {
            "project": service.project,
            "diagnostics": payload,
            "scheduler": payload["scheduler"],
            "page": "diagnostics",
        },
    )


@router.get("/pipelines/{pipeline_id}", response_class=HTMLResponse)
def pipeline_detail_page(request: Request, pipeline_id: str) -> HTMLResponse:
    """Render the pipeline detail page."""
    service = _service(request)
    try:
        detail = service.get_pipeline_detail(pipeline_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    dag_tasks = [
        {
            "task_id": task.task_id,
            "title": task.title,
            "task_type": task.task_type,
            "depends_on": list(task.depends_on),
            "command_preview": task.command_preview,
            "priority": task.priority,
            "timeout_seconds": task.timeout_seconds,
            "run_if": task.run_if,
            "artifact_paths": list(task.artifact_paths),
        }
        for task in detail["pipeline"].tasks.values()
    ]

    # Find terminal nodes for downstream links
    all_deps = {dep for task in detail["pipeline"].tasks.values() for dep in task.depends_on}
    terminal_nodes = [task.task_id for task in detail["pipeline"].tasks.values() if task.task_id not in all_deps]

    for target in detail["pipeline"].triggers_on_success:
        dag_tasks.append(
            {
                "task_id": f"trigger_{target}",
                "title": f"Trigger: {target}",
                "task_type": "pipeline",
                "depends_on": terminal_nodes,
                "command_preview": f"Triggers pipeline '{target}'",
            }
        )
    task_state_map = dict(detail["summary"].latest_task_states)
    task_run_map = {task.task_id: task for task in detail["latest_task_runs"]}
    latest_task_run_payloads = [
        {
            "run_id": task.run_id,
            "task_id": task.task_id,
            "title": task.title,
            "task_type": task.task_type,
            "status": task.status,
            "position": task.position,
            "command_preview": task.command_preview,
            "priority": task.priority,
            "timeout_seconds": task.timeout_seconds,
            "run_if": task.run_if,
            "depends_on": list(task.depends_on),
            "log_count": task.log_count,
            "duration_seconds": task.duration_seconds,
            "error": task.error,
            "output_preview": task.output_preview,
            "output_type": task.output_type,
        }
        for task in detail["latest_task_runs"]
    ]
    return _templates(request).TemplateResponse(
        request,
        "pipeline_detail.html",
        {
            "project": service.project,
            "pipeline_definition": detail["pipeline"],
            "pipeline": detail["summary"],
            "tasks": list(detail["pipeline"].tasks.values()),
            "latest_task_runs": detail["latest_task_runs"],
            "latest_task_run_payloads": latest_task_run_payloads,
            "dag_tasks": dag_tasks,
            "task_state_map": task_state_map,
            "task_run_map": task_run_map,
            "runs": detail["recent_runs"],
            "scheduler": service.scheduler_snapshot(),
            "page": "pipelines",
        },
    )


@router.get("/runs", response_class=HTMLResponse)
def runs_page(request: Request) -> HTMLResponse:
    """Render the run history page."""
    service = _service(request)
    return _templates(request).TemplateResponse(
        request,
        "runs.html",
        {
            "project": service.project,
            "runs": service.list_runs(limit=80),
            "scheduler": service.scheduler_snapshot(),
            "page": "runs",
        },
    )


@router.get("/runs/{run_id}", response_class=HTMLResponse)
def run_detail_page(request: Request, run_id: str) -> HTMLResponse:
    """Render the run detail page."""
    service = _service(request)
    try:
        payload = service.get_run_detail(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    run = payload["run"]
    task_runs = payload["task_runs"]
    logs = payload["logs"]

    dag_tasks = [
        {
            "task_id": task.task_id,
            "title": task.title,
            "task_type": task.task_type,
            "depends_on": list(task.depends_on),
            "command_preview": task.command_preview,
            "status": task.status,
        }
        for task in task_runs
    ]
    task_run_payloads = [
        {
            "run_id": task.run_id,
            "task_id": task.task_id,
            "title": task.title,
            "task_type": task.task_type,
            "status": task.status,
            "position": task.position,
            "command_preview": task.command_preview,
            "priority": task.priority,
            "timeout_seconds": task.timeout_seconds,
            "run_if": task.run_if,
            "depends_on": list(task.depends_on),
            "log_count": task.log_count,
            "duration_seconds": task.duration_seconds,
            "error": task.error,
            "output_preview": task.output_preview,
            "output_type": task.output_type,
        }
        for task in task_runs
    ]
    log_payloads = [
        {
            "run_id": line.run_id,
            "task_id": line.task_id,
            "created_at": line.created_at.isoformat(),
            "time_label": line.created_at.astimezone().strftime("%H:%M:%S.%f")[:-3],
            "stream": line.stream,
            "message": line.message,
        }
        for line in logs
    ]
    upcoming_run_payloads = [
        {
            "scheduled_for": item["scheduled_for"].isoformat(),
            "label": item["label"],
        }
        for item in payload["upcoming_runs"]
    ]

    return _templates(request).TemplateResponse(
        request,
        "run_detail.html",
        {
            "project": service.project,
            "run": run,
            "run_payload": RunResponse.from_record(run).model_dump(mode="json"),
            "task_runs": task_runs,
            "dag_tasks": dag_tasks,
            "task_run_payloads": task_run_payloads,
            "logs": logs,
            "log_payloads": log_payloads,
            "upcoming_runs": payload["upcoming_runs"],
            "upcoming_run_payloads": upcoming_run_payloads,
            "downstream": payload["downstream"],
            "upstream": payload["upstream"],
            "artifacts": payload["artifacts"],
            "has_run_config": payload["has_run_config"],
            "scheduler": service.scheduler_snapshot(),
            "page": "runs",
        },
    )


@router.get("/execution-matrix", response_class=HTMLResponse)
def execution_matrix_page(
    request: Request,
    pipeline_id: str | None = None,
    tenant: str | None = None,
    status: str | None = None,
) -> HTMLResponse:
    """Render the execution matrix page."""
    service = _service(request)
    payload = service.execution_matrix(
        pipeline_id=pipeline_id,
        tenant_id=tenant,
        status=status,
    )
    return _templates(request).TemplateResponse(
        request,
        "execution_matrix.html",
        {
            "project": service.project,
            "matrix": payload,
            "scheduler": service.scheduler_snapshot(),
            "page": "matrix",
        },
    )


@router.get("/logs", response_class=HTMLResponse)
def logs_page(
    request: Request,
    q: str | None = None,
    pipeline_id: str | None = None,
    task_id: str | None = None,
) -> HTMLResponse:
    """Render searchable logs."""
    service = _service(request)
    return _templates(request).TemplateResponse(
        request,
        "logs.html",
        {
            "project": service.project,
            "logs": service.search_logs(query=q, pipeline_id=pipeline_id, task_id=task_id),
            "pipelines": service.list_pipelines(),
            "query": q or "",
            "selected_pipeline_id": pipeline_id or "",
            "selected_task_id": task_id or "",
            "scheduler": service.scheduler_snapshot(),
            "page": "logs",
        },
    )


@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request) -> HTMLResponse:
    """Render lightweight settings and runtime configuration."""
    service = _service(request)
    payload = service.dashboard()
    return _templates(request).TemplateResponse(
        request,
        "settings.html",
        {
            "project": service.project,
            "pipelines": payload["pipelines"],
            "settings": payload["settings"],
            "scheduler": payload["scheduler"],
            "runtime_metrics": payload["runtime_metrics"],
            "page": "settings",
        },
    )
