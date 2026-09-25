"""Server-rendered UI routes for the Piply dashboard."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from piply.api.auth import (
    filter_by_pipeline,
    get_service,
    require_admin,
    require_permission,
    resolve_user,
    visible_pipeline_ids,
    visible_pipelines,
)
from piply.api.routes.setup import database_is_env_managed
from piply.api.schemas import RunResponse
from piply.core.dialects import is_postgres_dsn
from piply.core.sql_adapters import mask_connection_secret

router = APIRouter(tags=["ui"])


def _templates(request: Request):
    """Resolve the shared Jinja template environment."""
    return request.app.state.templates


@router.get("/", response_class=HTMLResponse)
def dashboard_page(request: Request) -> HTMLResponse:
    """Render the dashboard page, narrowed to what the caller may see."""
    require_permission(request, "view")
    service = get_service(request)
    payload = service.dashboard()
    return _templates(request).TemplateResponse(
        request,
        "dashboard.html",
        {
            "project": payload["project"],
            "stats": payload["stats"],
            "pipelines": visible_pipelines(request, payload["pipelines"]),
            "recent_runs": filter_by_pipeline(request, payload["recent_runs"]),
            "recent_failures": filter_by_pipeline(request, payload["recent_failures"]),
            "active_pipelines": visible_pipelines(request, payload["active_pipelines"]),
            "runtime_trend": payload["runtime_trend"],
            "runtime_metrics": payload["runtime_metrics"],
            "scheduler": payload["scheduler"],
            "page": "dashboard",
        },
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
    require_permission(request, "view")
    service = get_service(request)
    summaries = visible_pipelines(request, service.list_pipelines())

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
            # Newest first; the template reverses so the dots read left to right.
            "recent_runs": [
                {
                    "run_id": item.run_id,
                    "status": item.status,
                    "trigger": item.trigger,
                    "started_at": (item.started_at or item.created_at).isoformat(),
                    "duration_seconds": item.duration_seconds,
                }
                for item in summary.recent_runs
            ],
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
    """Render the runtime diagnostics page. Administrators only."""
    require_admin(request, "Only administrators can view diagnostics.")
    service = get_service(request)
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
    require_permission(request, "view", pipeline_id)
    service = get_service(request)
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


#: Trigger types a run can carry, in the order the filter bar shows them.
TRIGGER_TYPES = ("manual", "schedule", "pipeline", "sensor", "retry", "api", "task")
RUN_STATUSES = ("queued", "running", "success", "failed", "timed_out", "cancelled", "interrupted")


@router.get("/runs", response_class=HTMLResponse)
def runs_page(
    request: Request,
    pipeline_id: str | None = None,
    status: str | None = None,
    trigger: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    sort: str = "started_desc",
    limit: int = 100,
) -> HTMLResponse:
    """Render the run history page with filters, sorting, and trigger lineage."""
    require_permission(request, "view")
    service = get_service(request)

    def _parse_moment(value: str | None, end_of_day: bool) -> datetime | None:
        """Accept a date or a datetime from the filter bar."""
        if not value:
            return None
        for pattern in ("%Y-%m-%dT%H:%M", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(value, pattern)
            except ValueError:
                continue
            if pattern == "%Y-%m-%d" and end_of_day:
                parsed = parsed.replace(hour=23, minute=59, second=59)
            return parsed.astimezone()
        return None

    runs = service.list_runs(
        pipeline_id=pipeline_id or None,
        status=status or None,
        trigger=trigger or None,
        created_after=_parse_moment(date_from, end_of_day=False),
        created_before=_parse_moment(date_to, end_of_day=True),
        sort=sort,
        limit=max(1, min(500, limit)),
    )
    allowed_pipelines = visible_pipelines(request, service.list_pipelines())
    allowed_ids = {item.pipeline_id for item in allowed_pipelines}
    runs = [item for item in runs if item.pipeline_id in allowed_ids]
    lineage = service.lineage_for_runs(runs)

    return _templates(request).TemplateResponse(
        request,
        "runs.html",
        {
            "project": service.project,
            "runs": runs,
            "lineage": lineage,
            "pipelines": allowed_pipelines,
            "trigger_types": TRIGGER_TYPES,
            "run_statuses": RUN_STATUSES,
            "filters": {
                "pipeline_id": pipeline_id or "",
                "status": status or "",
                "trigger": trigger or "",
                "date_from": date_from or "",
                "date_to": date_to or "",
                "sort": sort,
                "limit": limit,
            },
            "scheduler": service.scheduler_snapshot(),
            "page": "runs",
        },
    )


@router.get("/runs/{run_id}", response_class=HTMLResponse)
def run_detail_page(request: Request, run_id: str) -> HTMLResponse:
    """Render the run detail page."""
    service = get_service(request)
    existing = service.store.get_run(run_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Unknown run '{run_id}'")
    require_permission(request, "view", existing.pipeline_id)
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
    require_permission(request, "view", pipeline_id)
    service = get_service(request)
    allowed = visible_pipeline_ids(request)
    if pipeline_id is None and allowed is not None:
        # Fall back to a pipeline this caller may actually see, not just the first.
        pipeline_id = next((item.pipeline_id for item in service.list_pipelines() if item.pipeline_id in allowed), None)
    payload = service.execution_matrix(
        pipeline_id=pipeline_id,
        tenant_id=tenant,
        status=status,
    )
    payload["pipelines"] = visible_pipelines(request, payload["pipelines"])
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
    """Render searchable logs, restricted to pipelines the caller may see."""
    require_permission(request, "view", pipeline_id)
    service = get_service(request)
    return _templates(request).TemplateResponse(
        request,
        "logs.html",
        {
            "project": service.project,
            "logs": service.search_logs(
                query=q,
                pipeline_id=pipeline_id,
                pipeline_ids=visible_pipeline_ids(request),
                task_id=task_id,
            ),
            "pipelines": visible_pipelines(request, service.list_pipelines()),
            "query": q or "",
            "selected_pipeline_id": pipeline_id or "",
            "selected_task_id": task_id or "",
            "scheduler": service.scheduler_snapshot(),
            "page": "logs",
        },
    )


def _database_panel(request: Request) -> dict:
    """Summarise the metadata store for the admin settings panel."""
    service = get_service(request)
    settings = request.app.state.settings
    location = service.database_location
    return {
        "backend": "PostgreSQL" if is_postgres_dsn(location) else "SQLite",
        # Never render a DSN with its password in the page source.
        "location": mask_connection_secret(location) or location,
        "configured": settings.database_configured,
        "env_managed": database_is_env_managed(),
        "row_counts": service.store.row_counts(),
    }


@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request) -> HTMLResponse:
    """Render settings, plus SMTP and account administration for admins."""
    require_permission(request, "view")
    service = get_service(request)
    payload = service.dashboard()
    current = resolve_user(request)
    is_admin = current is None or current.is_admin
    return _templates(request).TemplateResponse(
        request,
        "settings.html",
        {
            "project": service.project,
            "pipelines": visible_pipelines(request, payload["pipelines"]),
            "settings": payload["settings"],
            "scheduler": payload["scheduler"],
            "runtime_metrics": payload["runtime_metrics"],
            "smtp": service.get_smtp_settings() if is_admin else None,
            "database": _database_panel(request) if is_admin else None,
            "users": [
                {
                    "username": item.username,
                    "role": item.role,
                    "is_active": item.is_active,
                    "last_login_at": item.last_login_at,
                    "permissions": {key: sorted(value) for key, value in item.permissions.items()},
                }
                for item in (service.list_users() if is_admin else [])
            ],
            "all_pipeline_ids": sorted(service.project.pipelines),
            "current_user": current,
            "is_admin": is_admin,
            "auth_required": service.auth_required,
            "page": "settings",
        },
    )
