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


@router.get("/pipelines", response_class=HTMLResponse)
def pipelines_page(request: Request) -> HTMLResponse:
    """Render the pipeline list page."""
    service = _service(request)
    return _templates(request).TemplateResponse(
        request,
        "pipelines.html",
        {
            "project": service.project,
            "pipelines": service.list_pipelines(),
            "scheduler": service.scheduler_snapshot(),
            "page": "pipelines",
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
        }
        for task in detail["pipeline"].tasks.values()
    ]

    # Find terminal nodes for downstream links
    all_deps = {dep for task in detail["pipeline"].tasks.values() for dep in task.depends_on}
    terminal_nodes = [task.task_id for task in detail["pipeline"].tasks.values() if task.task_id not in all_deps]

    for target in detail["pipeline"].triggers_on_success:
        dag_tasks.append({
            "task_id": f"trigger_{target}",
            "title": f"Trigger: {target}",
            "task_type": "pipeline",
            "depends_on": terminal_nodes,
            "command_preview": f"Triggers pipeline '{target}'",
        })
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
            "depends_on": list(task.depends_on),
            "log_count": task.log_count,
            "duration_seconds": task.duration_seconds,
            "error": task.error,
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
            "depends_on": list(task.depends_on),
            "log_count": task.log_count,
            "duration_seconds": task.duration_seconds,
            "error": task.error,
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
            "scheduler": service.scheduler_snapshot(),
            "page": "runs",
        },
    )
