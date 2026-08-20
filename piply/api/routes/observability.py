"""Prometheus metrics and runtime diagnostics endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from piply.api.auth import get_service, require_admin, require_permission, visible_pipeline_ids
from piply.version import get_version

router = APIRouter(tags=["observability"])

PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

_RUN_STATUSES = (
    "queued",
    "running",
    "success",
    "failed",
    "skipped",
    "cancelled",
    "interrupted",
    "timed_out",
)


def _escape_label(value: str) -> str:
    """Escape a Prometheus label value."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _metric(name: str, help_text: str, metric_type: str, samples: list[tuple[str, float]]) -> list[str]:
    """Render one Prometheus metric family."""
    lines = [f"# HELP {name} {help_text}", f"# TYPE {name} {metric_type}"]
    for labels, value in samples:
        suffix = f"{{{labels}}}" if labels else ""
        rendered = int(value) if float(value).is_integer() else value
        lines.append(f"{name}{suffix} {rendered}")
    return lines


def render_prometheus(service) -> str:
    """Render the full Piply metrics exposition."""
    counts = service.store.status_counts()
    durations = service.store.duration_metrics()
    scheduler = service.scheduler_snapshot()
    queue = scheduler["queue_metrics"]
    workers = scheduler["worker_metrics"]
    pipelines = service.project.pipelines
    sensors = service.sensor_health()

    run_counts = counts["runs"]
    task_counts = counts["tasks"]
    lines: list[str] = []

    lines += _metric(
        "piply_runs_total",
        "Total pipeline runs recorded, by terminal or current status.",
        "gauge",
        [(f'status="{status}"', run_counts.get(status, 0)) for status in _RUN_STATUSES],
    )
    lines += _metric(
        "piply_runs_by_trigger_total",
        "Total pipeline runs recorded, by trigger type.",
        "gauge",
        [(f'trigger="{_escape_label(trigger)}"', value) for trigger, value in sorted(counts["triggers"].items())],
    )
    lines += _metric(
        "piply_runs_success_total",
        "Pipeline runs that finished successfully.",
        "gauge",
        [("", run_counts.get("success", 0))],
    )
    lines += _metric(
        "piply_runs_failure_total",
        "Pipeline runs that ended in a failure-like state.",
        "gauge",
        [
            (
                "",
                run_counts.get("failed", 0) + run_counts.get("interrupted", 0) + run_counts.get("timed_out", 0),
            )
        ],
    )
    lines += _metric(
        "piply_runs_running",
        "Pipeline runs currently executing.",
        "gauge",
        [("", workers.get("running_runs", 0))],
    )
    lines += _metric(
        "piply_tasks_total",
        "Task executions recorded, by status.",
        "gauge",
        [(f'status="{status}"', task_counts.get(status, 0)) for status in _RUN_STATUSES],
    )
    lines += _metric(
        "piply_tasks_running",
        "Task executions currently running.",
        "gauge",
        [("", workers.get("running_tasks", 0))],
    )
    lines += _metric(
        "piply_queue_size",
        "Trigger queue depth, by queue status.",
        "gauge",
        [
            ('status="queued"', queue.get("queued", 0) or 0),
            ('status="due"', queue.get("due", 0) or 0),
            ('status="dispatching"', queue.get("dispatching", 0) or 0),
            ('status="failed"', queue.get("failed", 0) or 0),
        ],
    )
    lines += _metric(
        "piply_queue_oldest_age_seconds",
        "Age of the oldest due trigger queue item.",
        "gauge",
        [("", queue.get("oldest_queued_age_seconds") or 0)],
    )
    lines += _metric(
        "piply_scheduler_up",
        "1 when the scheduler heartbeat is fresh, 0 otherwise.",
        "gauge",
        [("", 1 if scheduler["running"] else 0)],
    )
    lines += _metric(
        "piply_scheduler_heartbeat_age_seconds",
        "Seconds since the scheduler last recorded a heartbeat.",
        "gauge",
        [("", scheduler.get("heartbeat_age_seconds") or 0)],
    )
    lines += _metric(
        "piply_run_duration_seconds_sum",
        "Total execution time across all completed runs.",
        "counter",
        [("", round(durations["total_seconds"], 3))],
    )
    lines += _metric(
        "piply_run_duration_seconds_count",
        "Number of completed runs contributing to the duration sum.",
        "counter",
        [("", durations["completed_runs"])],
    )
    lines += _metric(
        "piply_run_duration_seconds_avg",
        "Average execution time across all completed runs.",
        "gauge",
        [("", round(durations["average_seconds"], 3))],
    )
    lines += _metric(
        "piply_run_duration_seconds_max",
        "Longest recorded run duration.",
        "gauge",
        [("", round(durations["max_seconds"], 3))],
    )
    lines += _metric(
        "piply_pipelines_total",
        "Configured pipelines, split by whether they carry a schedule.",
        "gauge",
        [
            ('scheduled="true"', sum(1 for item in pipelines.values() if item.schedule is not None)),
            ('scheduled="false"', sum(1 for item in pipelines.values() if item.schedule is None)),
        ],
    )
    lines += _metric(
        "piply_sensor_health",
        "1 when a sensor polled successfully, 0 when its last poll failed.",
        "gauge",
        [
            (
                f'pipeline="{_escape_label(str(item["pipeline_id"]))}",sensor="{_escape_label(str(item["sensor_id"]))}"',
                0 if item["status"] == "failing" else 1,
            )
            for item in sensors
        ],
    )
    return "\n".join(lines) + "\n"


@router.get("/metrics", response_class=PlainTextResponse, include_in_schema=False)
def get_metrics(request: Request) -> PlainTextResponse:
    """Expose runtime metrics in the Prometheus text exposition format."""
    require_permission(request, "view")
    service = get_service(request)
    if not request.app.state.settings.metrics_enabled:
        raise HTTPException(status_code=404, detail="Metrics are disabled. Set PIPLY_METRICS_ENABLED=true to enable.")
    return PlainTextResponse(render_prometheus(service), media_type=PROMETHEUS_CONTENT_TYPE)


@router.get("/health", response_model=dict[str, object], include_in_schema=False)
def get_health(request: Request) -> JSONResponse:
    """Liveness probe for container orchestrators and load balancers.

    Public and deliberately cheap: it answers whether this process can serve a
    request and reach its metadata store, which is what a restart would fix.
    Scheduler state is reported but does not fail the check, because a paused
    or stopped scheduler is a valid operating state and restarting the
    container would not help.
    """
    service = get_service(request)
    try:
        scheduler_state = service.store.get_meta("scheduler_state") or "unknown"
    except Exception as exc:  # noqa: BLE001 - any store failure means unhealthy
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "detail": f"Metadata store unreachable: {exc}"},
        )
    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "version": get_version(),
            "scheduler": scheduler_state,
            "accepting_work": not service.is_shutting_down,
        },
    )


@router.get("/api/diagnostics", response_model=dict[str, object])
def get_diagnostics(request: Request) -> dict[str, object]:
    """Return scheduler, worker, sensor, and reconciliation diagnostics.

    Admin-only: the payload names filesystem paths, the config location, the
    process id, and the metadata store, none of which a delegated pipeline
    operator needs.
    """
    require_admin(request, "Only administrators can view diagnostics.")
    return get_service(request).diagnostics()


@router.get("/api/sensors", response_model=list[dict[str, object]])
def get_sensor_health(request: Request) -> list[dict[str, object]]:
    """Return the health of sensors on pipelines the caller may see."""
    require_permission(request, "view")
    sensors = get_service(request).sensor_health()
    allowed = visible_pipeline_ids(request)
    if allowed is None:
        return sensors
    return [item for item in sensors if item.get("pipeline_id") in allowed]
