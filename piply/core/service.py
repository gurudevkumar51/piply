"""High-level runtime service used by the CLI, API, and scheduler."""

from __future__ import annotations

import inspect
import json
import os
import re
import secrets
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from piply.engine.base import BaseEngine
from piply.engine.local_engine import LocalEngine
from piply.settings import PiplySettings, load_settings, read_secret

from .auth import (
    ALL_PIPELINES,
    ROLES,
    AuthError,
    LoginThrottle,
    User,
    generate_password,
    hash_password,
    normalize_permissions,
    normalize_username,
    verify_password,
)
from .dialects import is_postgres_dsn
from .graph import upstream_closure
from .loader import discover_config, load_project
from .mailer import build_message, load_smtp_settings, save_smtp_settings, send_message
from .models import (
    PipelineDefinition,
    PipelineSummary,
    ProjectDefinition,
    RetryMode,
    RetryPolicy,
    RunRecord,
    TaskDefinition,
)
from .preview import PipelinePreview, build_pipeline_preview
from .processes import is_process_alive
from .retry import build_retry_plan
from .sensors import poll_api_sensor, poll_file_sensor, poll_sql_sensor
from .store import RunStore

_RUNTIME_VARIABLE_PATTERN = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
# Task fields whose YAML form is a list but whose runtime form is a tuple.
_TUPLE_TASK_FIELDS = {"args", "email_to"}
# Task fields that must stay plain strings after re-rendering.
_STRING_TASK_FIELDS = {"command", "url", "body", "email_subject", "email_body", "host", "user"}


def _expand_runtime_value(value: object, variables: dict[str, str]) -> object:
    """Resolve remaining config placeholders using variables supplied by an upstream run."""
    if isinstance(value, str):
        return _RUNTIME_VARIABLE_PATTERN.sub(lambda match: variables.get(match.group(1), match.group(0)), value)
    if isinstance(value, Path):
        return Path(_expand_runtime_value(str(value), variables))
    if isinstance(value, tuple):
        return tuple(_expand_runtime_value(item, variables) for item in value)
    if isinstance(value, list):
        return [_expand_runtime_value(item, variables) for item in value]
    if isinstance(value, dict):
        return {key: _expand_runtime_value(item, variables) for key, item in value.items()}
    return value


def _json_safe(value: object) -> object:
    """Return a JSON-serializable copy of a runtime value, dropping what cannot be stored."""
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        if isinstance(value, dict):
            return {str(key): _json_safe(item) for key, item in value.items()}
        if isinstance(value, list | tuple):
            return [_json_safe(item) for item in value]
        return str(value)
    return value


def _format_relative_time(delta_seconds: float) -> str:
    """Format a short relative label like 'in 2 hours'."""
    if delta_seconds < 60:
        return "in less than a minute"
    if delta_seconds < 3600:
        minutes = max(1, round(delta_seconds / 60))
        suffix = "" if minutes == 1 else "s"
        return f"in {minutes} minute{suffix}"
    hours = max(1, round(delta_seconds / 3600))
    suffix = "" if hours == 1 else "s"
    return f"in {hours} hour{suffix}"


class PipelineService:
    """PipelineService coordinates config loading, execution, retries, and UI summaries."""

    def __init__(
        self,
        *,
        config_path: str | Path | None = None,
        database_path: str | Path | None = None,
        engine: BaseEngine | None = None,
        settings: PiplySettings | None = None,
    ) -> None:
        # resolved_config_path = Path(config_path).resolve() if config_path else discover_config()
        if config_path:
            resolved_config_path = Path(config_path).resolve()
        else:
            resolved_config_path = discover_config()

        self.settings = settings or load_settings(resolved_config_path)
        self.config_path = resolved_config_path
        # A PostgreSQL DSN is passed through untouched; anything else is a
        # SQLite file path and is resolved relative to the config directory.
        if database_path is not None and is_postgres_dsn(str(database_path)):
            database_target: str | Path = str(database_path)
        elif database_path is not None:
            database_target = Path(database_path).resolve()
        elif self.settings.database_dsn is not None:
            database_target = self.settings.database_dsn
        else:
            database_target = self.settings.database_path or (self.config_path.parent / ".piply" / "piply.db").resolve()
        self.store = RunStore(database_target)
        # None for a server-backed store; `database_location` is always safe to print.
        self.database_path = self.store.database_path
        self.database_location = self.store.location
        self.engine = engine or LocalEngine(heartbeat_interval_seconds=self.settings.heartbeat_interval_seconds)
        self._engine_accepts_initial_context = self._detect_engine_initial_context_support()
        self._dispatch_context = threading.local()
        self._project: ProjectDefinition | None = None
        self._config_mtime: float | None = None
        self._lock = threading.RLock()
        self.store.set_meta("runtime_accepting_work", "true")
        self._accept_new_work = True
        self._shutdown_reason: str | None = None
        self._last_reconcile_monotonic = 0.0
        #: Failed sign-in tracking, kept in memory alongside the single process.
        self.login_throttle = LoginThrottle()
        #: Hashed once so an unknown username costs the same as a wrong password
        #: without paying for a second key derivation on every failed attempt.
        self._timing_decoy_hash = hash_password(secrets.token_hex(16))
        self.recover_interrupted_executions()
        self.reload_project(force=True)

    def _detect_engine_initial_context_support(self) -> bool:
        """Return whether the configured engine accepts initial runtime context."""
        try:
            signature = inspect.signature(self.engine.dispatch)
        except (TypeError, ValueError):
            return False
        return "initial_context" in signature.parameters or any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()
        )

    def _should_wait_for_pipeline_triggers(self) -> bool:
        """Return whether the current dispatch should finish downstream pipeline triggers inline."""
        return bool(getattr(self._dispatch_context, "wait_for_pipeline_triggers", False))

    @property
    def project(self) -> ProjectDefinition:
        """Return the cached project definition, reloading when needed."""
        return self.reload_project()

    def reload_project(self, *, force: bool = False) -> ProjectDefinition:
        """Reload the config when it changes on disk."""
        with self._lock:
            current_mtime = self.config_path.stat().st_mtime
            if not force and self._project is not None and self._config_mtime == current_mtime:
                return self._project

            self._project = load_project(
                self.config_path,
                default_max_parallel_tasks=self.settings.default_max_parallel_tasks,
            )
            self._config_mtime = current_mtime
            return self._project

    def validate(self) -> ProjectDefinition:
        """Validate and return the current project config."""
        return load_project(
            self.config_path,
            default_max_parallel_tasks=self.settings.default_max_parallel_tasks,
        )

    def reconcile_runtime_health(self, *, force: bool = False) -> list[str]:
        """Reconcile stale queued or running executions before building UI views.

        Reconciliation runs a full table scan, so read paths call it through a
        short cooldown window instead of on every request.
        """
        interval = self.settings.reconcile_interval_seconds
        with self._lock:
            elapsed = time.monotonic() - self._last_reconcile_monotonic
            if not force and interval > 0 and elapsed < interval:
                return []
            self._last_reconcile_monotonic = time.monotonic()
        return self.store.reconcile_stale_runs(self.settings.stale_run_timeout_seconds)

    def reconcile_run_health(self, run_id: str) -> None:
        """Reconcile one run without paying for a full stale-run scan."""
        self.store.reconcile_stale_runs(self.settings.stale_run_timeout_seconds, run_id=run_id)

    def recover_interrupted_executions(self) -> list[str]:
        """Interrupt runs left behind by a crashed or killed Piply process.

        A run is only recovered when the process that owned it is gone, so a
        second service instance inside the same process never disturbs work
        that is still executing.
        """
        recovered: list[str] = []
        for run_id, owner_pid in self.store.list_active_runs_with_owner():
            if is_process_alive(owner_pid):
                continue
            reason = (
                "Run marked interrupted during startup recovery because the process that owned it "
                f"(pid {owner_pid if owner_pid is not None else 'unknown'}) is no longer running."
            )
            if self.store.interrupt_run(
                run_id,
                reason=reason,
                queued_reason="Task never started because the owning Piply process stopped.",
            ):
                recovered.append(run_id)
        self.store.set_meta("runtime_last_recovery_at", datetime.now(timezone.utc).isoformat())
        self.store.set_meta("runtime_last_recovered_runs", str(len(recovered)))
        self.reconcile_runtime_health(force=True)
        return recovered

    @property
    def is_shutting_down(self) -> bool:
        """Return whether the service has stopped accepting new work."""
        with self._lock:
            return not self._accept_new_work

    def prepare_for_shutdown(self, reason: str = "Piply is shutting down.") -> None:
        """Stop accepting new work before the scheduler and engine wind down."""
        with self._lock:
            self._accept_new_work = False
            self._shutdown_reason = reason
        self.store.set_meta("runtime_accepting_work", "false")
        self.store.set_meta("runtime_shutdown_reason", reason)

    def shutdown_runtime(self, reason: str = "Run interrupted because the Piply service shut down.") -> list[str]:
        """Interrupt queued or running executions during graceful shutdown."""
        self.prepare_for_shutdown(reason)
        interrupted: list[str] = []
        for run_id in self.store.list_active_run_ids():
            self.engine.cancel(run_id)
            if self.store.interrupt_run(
                run_id,
                reason=reason,
                queued_reason="Task did not start before the Piply service shut down.",
            ):
                interrupted.append(run_id)
        return interrupted

    def _ensure_accepting_new_work(self) -> None:
        """Raise when a shutdown is in progress and new executions should be rejected."""
        if self.is_shutting_down:
            reason = self._shutdown_reason or "Piply is shutting down and not accepting new executions."
            raise ValueError(reason)

    def _format_next_run_label(
        self,
        next_run_at: datetime | None,
        *,
        now: datetime | None = None,
    ) -> str:
        """Format next-run text using relative labels for same-day schedules."""
        if next_run_at is None:
            return "manual only"

        current = now or datetime.now(timezone.utc)
        local_now = current.astimezone()
        local_next = next_run_at.astimezone()
        if local_now.date() == local_next.date() and local_next >= local_now:
            return _format_relative_time((local_next - local_now).total_seconds())
        return local_next.strftime("%Y-%m-%d %H:%M")

    def upstream_pipeline_map(self) -> dict[str, tuple[str, ...]]:
        """Return, for each pipeline, the pipelines that trigger it on success."""
        upstream: dict[str, list[str]] = {}
        for pipeline in self.project.pipelines.values():
            for target in pipeline.triggers_on_success:
                upstream.setdefault(target, []).append(pipeline.pipeline_id)
        return {key: tuple(sorted(value)) for key, value in upstream.items()}

    def list_pipelines(self) -> list[PipelineSummary]:
        """Return pipeline summaries enriched with scheduling and run metadata.

        Run metadata is loaded with three aggregate queries instead of four
        per-pipeline queries so large projects stay cheap to render.
        """
        self.reconcile_runtime_health()
        project = self.project
        paused_ids = self.store.list_paused_pipeline_ids()
        # One windowed query supplies both the run-history dots and the latest
        # run, so this stays at three queries regardless of pipeline count.
        recent_runs = self.store.recent_runs_by_pipeline(self.settings.pipeline_run_history_count)
        latest_runs = {pipeline_id: runs[0] for pipeline_id, runs in recent_runs.items() if runs}
        active_counts = self.store.active_run_counts_by_pipeline()
        task_states = self.store.task_states_for_runs([run.run_id for run in latest_runs.values()])
        upstream_map = self.upstream_pipeline_map()
        now = datetime.now(timezone.utc)
        summaries: list[PipelineSummary] = []
        for pipeline in project.pipelines.values():
            last_run = latest_runs.get(pipeline.pipeline_id)
            next_run_at = pipeline.schedule.next_after(now) if pipeline.schedule else None
            summaries.append(
                PipelineSummary(
                    pipeline_id=pipeline.pipeline_id,
                    title=pipeline.title,
                    description=pipeline.description,
                    enabled=pipeline.enabled,
                    paused=pipeline.pipeline_id in paused_ids,
                    schedule_text=(pipeline.schedule.describe() if pipeline.schedule else "Manual only"),
                    next_run_at=next_run_at,
                    next_run_label=self._format_next_run_label(next_run_at, now=now),
                    tags=pipeline.tags,
                    primary_entry=pipeline.primary_entry,
                    command_preview=pipeline.command_preview,
                    max_concurrent_runs=pipeline.max_concurrent_runs,
                    execution_mode=pipeline.execution_mode,
                    max_parallel_tasks=pipeline.max_parallel_tasks,
                    task_count=pipeline.task_count,
                    trigger_targets=pipeline.triggers_on_success,
                    timeout_seconds=pipeline.timeout_seconds,
                    triggered_by=upstream_map.get(pipeline.pipeline_id, ()),
                    latest_task_states=(
                        {} if last_run is None else task_states.get(last_run.run_id, {})  # type: ignore[arg-type]
                    ),
                    last_run=last_run,
                    recent_runs=tuple(recent_runs.get(pipeline.pipeline_id, ())),
                    active_runs=active_counts.get(pipeline.pipeline_id, 0),
                    retry_summary=pipeline.retry_policy.summary,
                    template_id=pipeline.template_id,
                    deployment_id=pipeline.deployment_id,
                )
            )
        return sorted(summaries, key=lambda item: item.title.lower())

    def get_pipeline(self, pipeline_id: str) -> PipelineDefinition:
        """Return one pipeline definition by id."""
        project = self.project
        try:
            return project.pipelines[pipeline_id]
        except KeyError as exc:
            raise KeyError(f"Unknown pipeline '{pipeline_id}'") from exc

    def get_pipeline_summary(self, pipeline_id: str) -> PipelineSummary:
        """Return one UI summary for a pipeline."""
        for summary in self.list_pipelines():
            if summary.pipeline_id == pipeline_id:
                return summary
        raise KeyError(f"Unknown pipeline '{pipeline_id}'")

    def get_pipeline_detail(self, pipeline_id: str) -> dict[str, object]:
        """Return the pipeline definition plus recent run details for the UI."""
        pipeline = self.get_pipeline(pipeline_id)
        summary = self.get_pipeline_summary(pipeline_id)
        latest_task_runs = []
        if summary.last_run is not None:
            latest_task_runs = self.store.list_task_runs(summary.last_run.run_id)
        return {
            "pipeline": pipeline,
            "summary": summary,
            "latest_task_runs": latest_task_runs,
            "recent_runs": self.store.list_runs(pipeline_id=pipeline_id, limit=12),
        }

    def list_upcoming_runs(
        self,
        pipeline_id: str,
        *,
        count: int | None = None,
        now: datetime | None = None,
    ) -> list[dict[str, object]]:
        """Preview the next scheduled slots for one pipeline."""
        pipeline = self.get_pipeline(pipeline_id)
        effective_count = count or self.settings.upcoming_run_preview_count
        if pipeline.schedule is None or effective_count < 1:
            return []

        current = now or datetime.now(timezone.utc)
        upcoming: list[dict[str, object]] = []
        cursor = current
        for _ in range(effective_count):
            next_run_at = pipeline.schedule.next_after(cursor)
            if next_run_at is None:
                break
            upcoming.append(
                {
                    "scheduled_for": next_run_at,
                    "label": self._format_next_run_label(next_run_at, now=current),
                }
            )
            cursor = next_run_at + timedelta(seconds=1)
        return upcoming

    def _iter_due_schedule_slots(
        self,
        pipeline: PipelineDefinition,
        *,
        now: datetime,
        limit: int = 256,
    ) -> list[datetime]:
        """Return every due schedule slot that has not yet been materialized."""
        if pipeline.schedule is None:
            return []

        current_slot = pipeline.schedule.current_slot(now)
        if current_slot is None:
            return []

        latest_slot = self.store.get_latest_materialized_slot(pipeline.pipeline_id)
        if latest_slot is None:
            return [current_slot]

        slots: list[datetime] = []
        cursor = latest_slot
        for _ in range(limit):
            next_slot = pipeline.schedule.next_after(cursor)
            if next_slot is None or next_slot > now:
                break
            slots.append(next_slot)
            cursor = next_slot
        return slots

    def _sensor_state_key(self, pipeline_id: str, sensor_id: str) -> str:
        """Build the stable storage key for one sensor state snapshot."""
        return f"{pipeline_id}:{sensor_id}"

    def enqueue_pipeline_trigger(
        self,
        pipeline_id: str,
        *,
        trigger: str,
        available_at: datetime | None = None,
        scheduled_for: datetime | None = None,
        payload: dict[str, object] | None = None,
        source_key: str | None = None,
        dedupe_key: str | None = None,
    ) -> bool:
        """Persist one trigger event in the lightweight internal queue."""
        self.get_pipeline(pipeline_id)
        return self.store.enqueue_trigger(
            pipeline_id,
            trigger,
            available_at=available_at or datetime.now(timezone.utc),
            scheduled_for=scheduled_for,
            source_key=source_key,
            dedupe_key=dedupe_key,
            payload=payload,
        )

    def enqueue_due_schedules(self, *, now: datetime | None = None) -> int:
        """Backfill and enqueue every due scheduled slot that is not yet materialized."""
        if self.is_shutting_down:
            return 0
        current = now or datetime.now(timezone.utc)
        paused_ids = self.store.list_paused_pipeline_ids()
        enqueued = 0
        for pipeline in self.project.pipelines.values():
            if not pipeline.is_schedulable() or pipeline.pipeline_id in paused_ids:
                continue
            for slot in self._iter_due_schedule_slots(pipeline, now=current):
                slot_iso = slot.isoformat()
                if self.enqueue_pipeline_trigger(
                    pipeline.pipeline_id,
                    trigger="schedule",
                    available_at=slot,
                    scheduled_for=slot,
                    payload={"scheduled_for": slot_iso},
                    source_key=slot_iso,
                    dedupe_key=f"schedule:{pipeline.pipeline_id}:{slot_iso}",
                ):
                    enqueued += 1
        return enqueued

    def poll_sensors(self, *, now: datetime | None = None) -> int:
        """Poll configured sensors and enqueue pipeline triggers for new events."""
        if self.is_shutting_down:
            return 0
        current = now or datetime.now(timezone.utc)
        paused_ids = self.store.list_paused_pipeline_ids()
        enqueued = 0
        for pipeline in self.project.pipelines.values():
            if not pipeline.enabled or pipeline.pipeline_id in paused_ids:
                continue
            for sensor in pipeline.sensors.values():
                if not sensor.enabled:
                    continue
                sensor_key = self._sensor_state_key(pipeline.pipeline_id, sensor.sensor_id)
                state = self.store.get_sensor_state(sensor_key)
                try:
                    if sensor.sensor_type == "file_sensor":
                        next_state, event = poll_file_sensor(sensor, state)
                    elif sensor.sensor_type == "sql_sensor":
                        next_state, event = poll_sql_sensor(sensor, state)
                    else:
                        next_state, event = poll_api_sensor(sensor, state)
                except Exception as exc:  # noqa: BLE001 - one bad sensor must not stop the others
                    message = f"{exc.__class__.__name__}: {exc}"
                    self.store.record_sensor_health(
                        sensor_key,
                        pipeline_id=pipeline.pipeline_id,
                        sensor_id=sensor.sensor_id,
                        sensor_type=sensor.sensor_type,
                        succeeded=False,
                        produced_event=False,
                        error=message,
                    )
                    self.store.set_meta(
                        "sensor_last_error",
                        f"Sensor '{pipeline.pipeline_id}:{sensor.sensor_id}' failed to poll: {message}",
                    )
                    continue

                error_text = next_state.get("last_error") if isinstance(next_state, dict) else None
                self.store.record_sensor_health(
                    sensor_key,
                    pipeline_id=pipeline.pipeline_id,
                    sensor_id=sensor.sensor_id,
                    sensor_type=sensor.sensor_type,
                    succeeded=not error_text,
                    produced_event=event is not None,
                    error=None if not error_text else str(error_text),
                )
                self.store.set_sensor_state(sensor_key, next_state)
                if event is None:
                    continue

                payload = dict(event.payload)
                payload["sensor_summary"] = sensor.summary
                if sensor.task_id is not None:
                    payload["task_id"] = sensor.task_id

                if self.enqueue_pipeline_trigger(
                    pipeline.pipeline_id,
                    trigger="sensor",
                    available_at=current,
                    payload=payload,
                    source_key=f"{sensor.sensor_id}:{event.source_key}",
                    dedupe_key=f"sensor:{pipeline.pipeline_id}:{sensor.sensor_id}:{event.source_key}",
                ):
                    enqueued += 1
        return enqueued

    def drain_trigger_queue(
        self,
        *,
        now: datetime | None = None,
        limit: int = 100,
        wait_for_pipeline_triggers: bool = False,
    ) -> list[str]:
        """Dispatch due queue items while keeping per-pipeline order intact."""
        if self.is_shutting_down:
            return []
        self.reconcile_runtime_health()
        effective_limit = max(1, min(limit, self.settings.queue_dispatch_batch_size))
        self.store.requeue_stale_dispatches(self.settings.queue_dispatch_stale_seconds)
        current = now or datetime.now(timezone.utc)
        dispatched_run_ids: list[str] = []
        blocked_pipelines: set[str] = set()

        for item in self.store.list_due_queue(now=current, limit=effective_limit):
            if item.pipeline_id in blocked_pipelines:
                continue
            try:
                pipeline = self.get_pipeline(item.pipeline_id)
            except KeyError as exc:
                self.store.mark_queue_failed(item.queue_id, str(exc))
                continue

            if not pipeline.enabled or self.store.is_pipeline_paused(item.pipeline_id):
                blocked_pipelines.add(item.pipeline_id)
                continue
            if self.store.count_running_runs(item.pipeline_id) > 0:
                blocked_pipelines.add(item.pipeline_id)
                continue

            if not self.store.claim_queue_item(item.queue_id):
                blocked_pipelines.add(item.pipeline_id)
                continue

            payload = item.payload
            try:
                if item.trigger == "retry":
                    retry_of = payload.get("retry_of")
                    if not isinstance(retry_of, str) or not retry_of:
                        raise ValueError("Retry queue item is missing retry_of.")
                    mode = str(payload.get("mode") or "resume")
                    task_id = None if payload.get("task_id") is None else str(payload.get("task_id"))
                    run = self.retry_run(
                        retry_of,
                        mode=mode,  # type: ignore[arg-type]
                        task_id=task_id,
                        wait=False,
                    )
                elif item.trigger == "sensor" and isinstance(payload.get("task_id"), str):
                    run = self.trigger_task(
                        item.pipeline_id,
                        str(payload["task_id"]),
                        trigger="sensor",
                        wait=False,
                    )
                else:
                    parent_run_id = (
                        str(payload["source_run_id"]) if isinstance(payload.get("source_run_id"), str) else None
                    )
                    parent_pipeline_id = (
                        str(payload["source_pipeline_id"])
                        if isinstance(payload.get("source_pipeline_id"), str)
                        else None
                    )
                    tenant_id = str(payload["tenant_id"]) if isinstance(payload.get("tenant_id"), str) else None
                    initial_context: dict[str, object] = {}
                    if isinstance(payload.get("context"), dict):
                        # type: ignore[arg-type]
                        initial_context.update(payload["context"])
                    if isinstance(payload.get("upstream"), dict):
                        initial_context.setdefault("upstream", payload["upstream"])
                    inherited_variables = (
                        {str(key): str(value) for key, value in payload.get("variables", {}).items()}
                        if isinstance(payload.get("variables"), dict)
                        else {}
                    )
                    inherited_env = (
                        {str(key): str(value) for key, value in payload.get("env", {}).items()}
                        if isinstance(payload.get("env"), dict)
                        else {}
                    )
                    if parent_run_id is not None:
                        initial_context.setdefault(
                            "parent",
                            {
                                "run_id": parent_run_id,
                                "pipeline_id": parent_pipeline_id,
                            },
                        )
                    run = self.trigger_pipeline(
                        item.pipeline_id,
                        trigger=item.trigger,
                        scheduled_for=item.scheduled_for,
                        wait=wait_for_pipeline_triggers and item.trigger == "pipeline",
                        parent_run_id=parent_run_id,
                        parent_pipeline_id=parent_pipeline_id,
                        tenant_id=tenant_id,
                        initial_context=initial_context,
                        inherited_variables=inherited_variables,
                        inherited_env=inherited_env,
                    )
                self.store.mark_queue_dispatched(item.queue_id, run.run_id)
                dispatched_run_ids.append(run.run_id)

                if item.trigger == "schedule" and item.scheduled_for is not None:
                    self.store.append_log(
                        run.run_id,
                        f"Scheduled slot {item.scheduled_for.isoformat()} dispatched from the queue.",
                    )
                if item.trigger == "sensor":
                    sensor_id = payload.get("sensor_id") or "sensor"
                    self.store.append_log(
                        run.run_id,
                        f"Triggered by sensor '{sensor_id}'.",
                    )
                    if payload.get("sensor_type") == "file_sensor" and payload.get("new_files"):
                        self.store.append_log(
                            run.run_id,
                            f"Detected new files: {', '.join(str(item) for item in payload['new_files'])}",
                        )
                    if payload.get("sensor_type") == "sql_sensor":
                        self.store.append_log(
                            run.run_id,
                            (
                                f"Detected new rows in {payload.get('table')} "
                                f"from cursor {payload.get('cursor_from')} to {payload.get('cursor_to')}."
                            ),
                        )
                    if payload.get("sensor_type") == "api_sensor":
                        self.store.append_log(
                            run.run_id,
                            (
                                f"Detected API sensor change at {payload.get('url')} "
                                f"from cursor {payload.get('cursor_from')} to {payload.get('cursor_to')}."
                            ),
                        )
                if item.trigger == "pipeline" and isinstance(payload.get("source_run_id"), str):
                    self.store.append_log(
                        run.run_id,
                        f"Triggered from upstream run {payload['source_run_id']}.",
                    )
            except Exception as exc:  # pragma: no cover - defensive path
                self.store.mark_queue_failed(item.queue_id, str(exc))
            blocked_pipelines.add(item.pipeline_id)

        return dispatched_run_ids

    def list_runs(
        self,
        *,
        pipeline_id: str | None = None,
        status: str | None = None,
        tenant_id: str | None = None,
        trigger: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        sort: str = "started_desc",
        limit: int = 50,
    ) -> list[RunRecord]:
        """Return recent runs with optional filters and sort order."""
        self.reconcile_runtime_health()
        return self.store.list_runs(
            pipeline_id=pipeline_id,
            status=status,
            tenant_id=tenant_id,
            trigger=trigger,
            created_after=created_after,
            created_before=created_before,
            sort=sort,
            limit=limit,
        )

    def get_run(self, run_id: str):
        """Return one run, its task runs, and its raw logs."""
        self.reconcile_run_health(run_id)
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(f"Unknown run '{run_id}'")
        task_runs = self.store.list_task_runs(run_id)
        logs = self.store.list_logs(run_id)
        return run, task_runs, logs

    def get_run_detail(self, run_id: str) -> dict[str, object]:
        """Return one run plus task runs, logs, lineage, and upcoming schedule slots."""
        run, task_runs, logs = self.get_run(run_id)
        return {
            "run": run,
            "task_runs": task_runs,
            "logs": logs,
            "upcoming_runs": self.list_upcoming_runs(run.pipeline_id, count=8),
            "downstream": self.downstream_run_links(run),
            "upstream": self.upstream_run_link(run),
            "artifacts": self.store.list_task_artifacts(run_id),
            "has_run_config": self.store.get_run_config(run_id) is not None,
        }

    def downstream_run_links(self, run: RunRecord) -> list[dict[str, object]]:
        """Return every downstream pipeline this run triggers, with its run status.

        Targets that have not been dispatched yet are still returned so the run
        graph always shows the full chain rather than hiding what is pending.
        """
        try:
            pipeline = self.get_pipeline(run.pipeline_id)
        except KeyError:
            return []

        child_runs = {child.pipeline_id: child for child in self.store.list_child_runs(run.run_id)}
        links: list[dict[str, object]] = []
        for target in pipeline.triggers_on_success:
            child = child_runs.pop(target, None)
            target_title = target
            try:
                target_title = self.get_pipeline(target).title
            except KeyError:
                pass
            links.append(
                {
                    "pipeline_id": target,
                    "pipeline_title": target_title,
                    "run_id": None if child is None else child.run_id,
                    "status": child.status if child is not None else ("pending" if run.status == "success" else "-"),
                    "started_at": None if child is None or child.started_at is None else child.started_at.isoformat(),
                    "duration_seconds": None if child is None else child.duration_seconds,
                    "successful_tasks": 0 if child is None else child.successful_tasks,
                    "task_count": 0 if child is None else child.task_count,
                    "error": None if child is None else child.error,
                }
            )

        # Anything triggered through the API rather than triggers_on_success.
        for pipeline_id, child in child_runs.items():
            links.append(
                {
                    "pipeline_id": pipeline_id,
                    "pipeline_title": child.pipeline_title,
                    "run_id": child.run_id,
                    "status": child.status,
                    "started_at": None if child.started_at is None else child.started_at.isoformat(),
                    "duration_seconds": child.duration_seconds,
                    "successful_tasks": child.successful_tasks,
                    "task_count": child.task_count,
                    "error": child.error,
                }
            )
        return links

    #: How far up a trigger chain to walk before giving up. Chains are short in
    #: practice; the cap only guards against a cycle in corrupted data.
    MAX_LINEAGE_DEPTH = 12

    def lineage_for_runs(self, runs: list[RunRecord]) -> dict[str, list[dict[str, object]]]:
        """Return the full ancestor chain for each run, root first.

        Walks one generation at a time across every run at once, so the cost is
        one query per level of depth rather than one per run.
        """
        parents_by_run: dict[str, str] = {
            run.run_id: run.parent_run_id for run in runs if run.parent_run_id is not None
        }
        known: dict[str, dict[str, object]] = {}

        frontier = list(dict.fromkeys(parents_by_run.values()))
        for _ in range(self.MAX_LINEAGE_DEPTH):
            missing = [run_id for run_id in frontier if run_id not in known]
            if not missing:
                break
            fetched = self.store.runs_by_ids(missing)
            known.update(fetched)
            frontier = [
                str(item["parent_run_id"])
                for item in fetched.values()
                if item.get("parent_run_id") and str(item["parent_run_id"]) not in known
            ]
            if not frontier:
                break

        lineage: dict[str, list[dict[str, object]]] = {}
        for run in runs:
            chain: list[dict[str, object]] = []
            cursor = run.parent_run_id
            seen: set[str] = set()
            while cursor and cursor not in seen and len(chain) < self.MAX_LINEAGE_DEPTH:
                seen.add(cursor)
                ancestor = known.get(cursor)
                if ancestor is None:
                    # The parent was pruned; record the reference so the chain
                    # is honest about being incomplete rather than silently short.
                    chain.append(
                        {
                            "run_id": cursor,
                            "pipeline_id": run.parent_pipeline_id,
                            "pipeline_title": run.parent_pipeline_id or cursor,
                            "status": "deleted",
                            "trigger": None,
                            "available": False,
                        }
                    )
                    break
                chain.append({**ancestor, "available": True})
                cursor = ancestor.get("parent_run_id")  # type: ignore[assignment]
            lineage[run.run_id] = list(reversed(chain))
        return lineage

    def upstream_run_link(self, run: RunRecord) -> dict[str, object] | None:
        """Return the parent run that triggered this one, when there is one."""
        if run.parent_run_id is None:
            return None
        parent = self.store.get_run(run.parent_run_id)
        if parent is None:
            return {
                "run_id": run.parent_run_id,
                "pipeline_id": run.parent_pipeline_id,
                "pipeline_title": run.parent_pipeline_id,
                "status": "deleted",
            }
        return {
            "run_id": parent.run_id,
            "pipeline_id": parent.pipeline_id,
            "pipeline_title": parent.pipeline_title,
            "status": parent.status,
        }

    def get_task_detail(self, run_id: str, task_id: str) -> dict[str, object]:
        """Return one task run with logs and output metadata."""
        run, task_runs, _ = self.get_run(run_id)
        task = next((item for item in task_runs if item.task_id == task_id), None)
        if task is None:
            raise KeyError(f"Unknown task '{task_id}' in run '{run_id}'")
        logs = self.store.list_logs(run_id, task_id=task_id)
        return {
            "run": run,
            "task_run": task,
            "logs": logs,
            "output": self.store.get_task_output(run_id, task_id),
        }

    def get_task_output(self, run_id: str, task_id: str):
        """Return one task output metadata record."""
        self.get_task_detail(run_id, task_id)
        output = self.store.get_task_output(run_id, task_id)
        if output is None:
            raise KeyError(f"No output captured for task '{task_id}' in run '{run_id}'")
        return output

    def _clone_pipeline_with_command_overrides(
        self,
        pipeline: PipelineDefinition,
        command_overrides: dict[str, str] | None,
    ) -> PipelineDefinition:
        """Apply manual CLI command overrides for one triggered run."""
        if not command_overrides:
            return pipeline

        updated_tasks: dict[str, object] = {}
        for task_id, task in pipeline.tasks.items():
            override = command_overrides.get(task_id)
            if override is None and task.template_id is not None:
                override = command_overrides.get(task.template_id)
            if override is None:
                updated_tasks[task_id] = task
                continue
            if task.task_type != "cli":
                raise ValueError(f"Task '{task_id}' does not support command overrides.")
            stripped = override.strip()
            if not stripped:
                raise ValueError(f"Task '{task_id}' command override cannot be empty.")
            updated_tasks[task_id] = replace(task, command=stripped, path=None)

        return replace(pipeline, tasks=updated_tasks)

    def _clone_pipeline_with_inherited_variables(
        self,
        pipeline: PipelineDefinition,
        inherited_variables: dict[str, str] | None,
        inherited_env: dict[str, str] | None = None,
    ) -> PipelineDefinition:
        """Resolve placeholders left by the downstream config with upstream deployment values.

        Environment values travel with the variables so a downstream deployment
        sees the same env the upstream deployment ran with. Both layers are
        applied by the parent because the parent deployment is the one that
        knows the tenant/environment this chain belongs to.
        """
        if not inherited_variables and not inherited_env:
            return pipeline
        # Direct/manual downstream runs use their configured variables. Pipeline
        # triggers let the parent deployment override shared keys for that run.
        effective_variables = dict(pipeline.variables)
        effective_variables.update(inherited_variables or {})
        updated_tasks: dict[str, TaskDefinition] = {}
        for task_id, task in pipeline.tasks.items():
            task_variables = effective_variables | task.entity_values
            expanded = {
                field_name: _expand_runtime_value(getattr(task, field_name), task_variables)
                for field_name in task.__dataclass_fields__
                if field_name != "variable_templates"
            }
            for field_name, template_value in task.variable_templates.items():
                if field_name not in task.__dataclass_fields__:
                    continue
                rendered_value = _expand_runtime_value(template_value, task_variables)
                if field_name in _TUPLE_TASK_FIELDS and isinstance(rendered_value, list):
                    rendered_value = tuple(rendered_value)
                if field_name in _STRING_TASK_FIELDS and rendered_value is not None:
                    rendered_value = str(rendered_value)
                expanded[field_name] = rendered_value
            if inherited_env:
                merged_env = dict(expanded.get("env") or {})
                merged_env.update(inherited_env)
                expanded["env"] = merged_env
            updated_tasks[task_id] = replace(task, **expanded)
        return replace(pipeline, tasks=updated_tasks, variables=effective_variables)

    def _pipeline_env(self, pipeline: PipelineDefinition) -> dict[str, str]:
        """Return the env values shared by every task in a pipeline.

        Task-level env is layered on top of pipeline env at load time, so the
        shared subset is the intersection of all task environments.
        """
        task_envs = [task.env for task in pipeline.tasks.values() if task.env]
        if not task_envs:
            return {}
        shared = dict(task_envs[0])
        for env in task_envs[1:]:
            shared = {key: value for key, value in shared.items() if env.get(key) == value}
        return shared

    def _clone_pipeline_for_task(self, pipeline: PipelineDefinition, task_id: str) -> PipelineDefinition:
        """Build a task-focused pipeline that includes the selected task and its dependencies."""
        selected_task_ids = (
            {task_id}
            if task_id in pipeline.tasks
            else {current_id for current_id, task in pipeline.tasks.items() if task.template_id == task_id}
        )
        if not selected_task_ids:
            raise KeyError(f"Unknown task '{task_id}' in pipeline '{pipeline.pipeline_id}'")
        required_ids = upstream_closure(pipeline, selected_task_ids)
        scoped_tasks = {
            current_task_id: current_task
            for current_task_id, current_task in pipeline.tasks.items()
            if current_task_id in required_ids
        }
        return replace(
            pipeline,
            tasks=scoped_tasks,
            schedule=None,
            triggers_on_success=(),
            retry_policy=RetryPolicy(),
        )

    def _build_run_config(
        self,
        pipeline: PipelineDefinition,
        *,
        trigger: str,
        scheduled_for: datetime | None,
        tenant_id: str | None,
        command_overrides: dict[str, str] | None,
        inherited_variables: dict[str, str] | None,
        inherited_env: dict[str, str] | None,
        initial_context: dict[str, object] | None,
        parent_run_id: str | None = None,
        parent_pipeline_id: str | None = None,
        task_id: str | None = None,
    ) -> dict[str, object]:
        """Capture everything needed to reproduce this execution later.

        Retries, task-scoped reruns, and backfills replay this snapshot instead
        of re-deriving values from an upstream pipeline, so a downstream run can
        be repaired without re-running the chain that produced it.
        """
        return {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "pipeline_id": pipeline.pipeline_id,
            "template_id": pipeline.template_id,
            "deployment_id": pipeline.deployment_id,
            "trigger": trigger,
            "scheduled_for": None if scheduled_for is None else scheduled_for.isoformat(),
            "tenant_id": tenant_id,
            "task_id": task_id,
            "parent_run_id": parent_run_id,
            "parent_pipeline_id": parent_pipeline_id,
            "variables": dict(pipeline.variables),
            "inherited_variables": dict(inherited_variables or {}),
            "inherited_env": dict(inherited_env or {}),
            "env": self._pipeline_env(pipeline),
            "command_overrides": dict(command_overrides or {}),
            "context": _json_safe(initial_context or {}),
            "selectors": {
                "entities": sorted(
                    {task.entity_key for task in pipeline.tasks.values() if task.entity_key is not None}
                ),
                "task_ids": list(pipeline.tasks),
            },
            "settings": {
                "max_parallel_tasks": pipeline.max_parallel_tasks,
                "execution_mode": pipeline.execution_mode,
                "timeout_seconds": pipeline.timeout_seconds,
                "retry": {
                    "attempts": pipeline.retry_policy.attempts,
                    "mode": pipeline.retry_policy.mode,
                    "delay_seconds": pipeline.retry_policy.delay_seconds,
                },
            },
        }

    def _replay_arguments(self, run_id: str) -> dict[str, object]:
        """Return the trigger arguments captured for one run, if any."""
        snapshot = self.store.get_run_config(run_id)
        if not snapshot:
            return {}
        return {
            "inherited_variables": {
                str(key): str(value) for key, value in (snapshot.get("inherited_variables") or {}).items()
            },
            "inherited_env": {str(key): str(value) for key, value in (snapshot.get("inherited_env") or {}).items()},
            "command_overrides": {
                str(key): str(value) for key, value in (snapshot.get("command_overrides") or {}).items()
            },
            "context": dict(snapshot.get("context") or {}),
        }

    def _dispatch_engine(
        self,
        pipeline: PipelineDefinition,
        run: RunRecord,
        *,
        wait: bool,
        on_log: Callable[[str], None] | None,
        initial_task_statuses: dict[str, str],
        retry_source_run_id: str | None,
        initial_context: dict[str, object] | None = None,
    ) -> None:
        """Dispatch a run while preserving compatibility with older custom engines."""
        kwargs: dict[str, object] = {
            "wait": wait,
            "on_log": on_log,
            "on_success": self._handle_pipeline_success,
            "on_failure": self._handle_pipeline_failure,
            "initial_task_statuses": initial_task_statuses,
            "retry_source_run_id": retry_source_run_id,
        }
        if self._engine_accepts_initial_context:
            kwargs["initial_context"] = initial_context or {}
        previous_wait = self._should_wait_for_pipeline_triggers()
        self._dispatch_context.wait_for_pipeline_triggers = previous_wait or wait
        try:
            self.engine.dispatch(pipeline, run, self.store, **kwargs)
        finally:
            self._dispatch_context.wait_for_pipeline_triggers = previous_wait

    def trigger_pipeline(
        self,
        pipeline_id: str,
        *,
        trigger: str = "manual",
        scheduled_for: datetime | None = None,
        wait: bool = False,
        on_log: Callable[[str], None] | None = None,
        retry_of: str | None = None,
        retry_mode: RetryMode | None = None,
        retry_task_id: str | None = None,
        initial_task_statuses: dict[str, str] | None = None,
        command_overrides: dict[str, str] | None = None,
        parent_run_id: str | None = None,
        parent_pipeline_id: str | None = None,
        tenant_id: str | None = None,
        initial_context: dict[str, object] | None = None,
        inherited_variables: dict[str, str] | None = None,
        inherited_env: dict[str, str] | None = None,
    ) -> RunRecord:
        """Create and dispatch one new run for a pipeline."""
        self._ensure_accepting_new_work()
        self.reconcile_runtime_health()
        pipeline = self._clone_pipeline_with_command_overrides(
            self._clone_pipeline_with_inherited_variables(
                self.get_pipeline(pipeline_id),
                inherited_variables,
                inherited_env,
            ),
            command_overrides,
        )
        if scheduled_for is not None and self.store.has_run_for_slot(pipeline_id, scheduled_for):
            existing = self.store.get_run_for_slot(pipeline_id, scheduled_for)
            if existing is not None:
                return existing

        try:
            run = self.store.create_run(
                pipeline,
                trigger=trigger,
                scheduled_for=scheduled_for,
                retry_of=retry_of,
                retry_mode=retry_mode,
                retry_task_id=retry_task_id,
                parent_run_id=parent_run_id,
                parent_pipeline_id=parent_pipeline_id,
                tenant_id=tenant_id,
                run_config=self._build_run_config(
                    pipeline,
                    trigger=trigger,
                    scheduled_for=scheduled_for,
                    tenant_id=tenant_id,
                    command_overrides=command_overrides,
                    inherited_variables=inherited_variables,
                    inherited_env=inherited_env,
                    initial_context=initial_context,
                    parent_run_id=parent_run_id,
                    parent_pipeline_id=parent_pipeline_id,
                ),
            )
            dispatch_context = dict(initial_context or {})
            if pipeline.variables:
                dispatch_context.setdefault("variables", dict(pipeline.variables))
                for key, value in pipeline.variables.items():
                    dispatch_context.setdefault(key, value)
            if tenant_id is not None:
                dispatch_context.setdefault("tenant_id", tenant_id)
            self._dispatch_engine(
                pipeline,
                run,
                wait=wait,
                on_log=on_log,
                initial_task_statuses=initial_task_statuses or {},
                retry_source_run_id=retry_of,
                initial_context=dispatch_context,
            )
        except sqlite3.IntegrityError:
            if scheduled_for is not None:
                existing = self.store.get_run_for_slot(pipeline_id, scheduled_for)
                if existing is not None:
                    return existing
            raise
        return self.store.get_run(run.run_id) or run

    def trigger_task(
        self,
        pipeline_id: str,
        task_id: str,
        *,
        trigger: str = "task",
        wait: bool = False,
        on_log: Callable[[str], None] | None = None,
        command_overrides: dict[str, str] | None = None,
        tenant_id: str | None = None,
        initial_context: dict[str, object] | None = None,
        source_run_id: str | None = None,
    ) -> RunRecord:
        """Create and dispatch one run scoped to a selected task and its dependencies.

        ``source_run_id`` replays the configuration of an earlier run so a single
        failed task can be repaired with the variables and environment it was
        originally given.
        """
        self._ensure_accepting_new_work()
        self.reconcile_runtime_health()
        replay = self._replay_arguments(source_run_id) if source_run_id else {}
        inherited_variables = replay.get("inherited_variables") or None
        inherited_env = replay.get("inherited_env") or None
        effective_overrides = command_overrides or replay.get("command_overrides") or None
        pipeline = self._clone_pipeline_for_task(
            self._clone_pipeline_with_inherited_variables(
                self.get_pipeline(pipeline_id),
                inherited_variables,  # type: ignore[arg-type]
                inherited_env,  # type: ignore[arg-type]
            ),
            task_id,
        )
        pipeline = self._clone_pipeline_with_command_overrides(pipeline, effective_overrides)  # type: ignore[arg-type]
        dispatch_context: dict[str, object] = dict(replay.get("context") or {})  # type: ignore[arg-type]
        dispatch_context.update(initial_context or {})
        if pipeline.variables:
            dispatch_context.setdefault("variables", dict(pipeline.variables))
            for key, value in pipeline.variables.items():
                dispatch_context.setdefault(key, value)
        if source_run_id:
            dispatch_context.update(self.store.output_context_for_run(source_run_id))
        run = self.store.create_run(
            pipeline,
            trigger=trigger,
            retry_task_id=task_id,
            tenant_id=tenant_id,
            run_config=self._build_run_config(
                pipeline,
                trigger=trigger,
                scheduled_for=None,
                tenant_id=tenant_id,
                command_overrides=effective_overrides,  # type: ignore[arg-type]
                inherited_variables=inherited_variables,  # type: ignore[arg-type]
                inherited_env=inherited_env,  # type: ignore[arg-type]
                initial_context=dispatch_context,
                task_id=task_id,
            ),
        )
        if tenant_id is not None:
            dispatch_context.setdefault("tenant_id", tenant_id)
        self._dispatch_engine(
            pipeline,
            run,
            wait=wait,
            on_log=on_log,
            initial_task_statuses={},
            retry_source_run_id=None,
            initial_context=dispatch_context,
        )
        return self.store.get_run(run.run_id) or run

    def retry_run(
        self,
        run_id: str,
        *,
        mode: RetryMode,
        task_id: str | None = None,
        wait: bool = False,
        on_log: Callable[[str], None] | None = None,
    ) -> RunRecord:
        """Create a retry run in startover or resume mode."""
        self._ensure_accepting_new_work()
        self.reconcile_runtime_health()
        previous_run, previous_task_runs, _ = self.get_run(run_id)
        if previous_run.status in {"queued", "running"}:
            raise ValueError("Retry is only available after a run has finished.")

        # Replay the configuration the original run was launched with. Without
        # this a downstream run could only be repaired by re-running the whole
        # upstream chain that supplied its variables and environment.
        replay = self._replay_arguments(previous_run.run_id)
        pipeline = self._clone_pipeline_with_command_overrides(
            self._clone_pipeline_with_inherited_variables(
                self.get_pipeline(previous_run.pipeline_id),
                replay.get("inherited_variables"),  # type: ignore[arg-type]
                replay.get("inherited_env"),  # type: ignore[arg-type]
            ),
            replay.get("command_overrides"),  # type: ignore[arg-type]
        )
        retry_plan = build_retry_plan(
            pipeline,
            previous_task_runs,
            mode=mode,
            task_id=task_id,
        )

        reused_task_statuses = {task_id: "success" for task_id in retry_plan.reuse_task_ids}
        retry_run = self.store.create_run(
            pipeline,
            trigger="retry",
            retry_of=previous_run.run_id,
            retry_mode=mode,
            retry_task_id=task_id,
            parent_run_id=previous_run.parent_run_id,
            parent_pipeline_id=previous_run.parent_pipeline_id,
            tenant_id=previous_run.tenant_id,
            run_config=self._build_run_config(
                pipeline,
                trigger="retry",
                scheduled_for=previous_run.scheduled_for,
                tenant_id=previous_run.tenant_id,
                command_overrides=replay.get("command_overrides"),  # type: ignore[arg-type]
                inherited_variables=replay.get("inherited_variables"),  # type: ignore[arg-type]
                inherited_env=replay.get("inherited_env"),  # type: ignore[arg-type]
                initial_context=replay.get("context"),  # type: ignore[arg-type]
                parent_run_id=previous_run.parent_run_id,
                parent_pipeline_id=previous_run.parent_pipeline_id,
                task_id=task_id,
            ),
        )

        if mode == "resume":
            self.store.append_log(
                retry_run.run_id,
                f"Retry created from run {previous_run.run_id} using resume mode.",
            )
            if task_id is not None:
                self.store.append_log(
                    retry_run.run_id,
                    f"Retry was requested from selected task '{task_id}'.",
                )
            for reused_task_id in retry_plan.reuse_task_ids:
                self.store.mark_task_reused(retry_run.run_id, reused_task_id, previous_run.run_id)
        else:
            self.store.append_log(
                retry_run.run_id,
                f"Retry created from run {previous_run.run_id} using startover mode.",
            )

        # Seed with the upstream context the original run received so a retry
        # never depends on the upstream pipeline still being available.
        retry_context: dict[str, object] = dict(replay.get("context") or {})  # type: ignore[arg-type]
        if pipeline.variables:
            retry_context.setdefault("variables", dict(pipeline.variables))
            for key, value in pipeline.variables.items():
                retry_context.setdefault(key, value)
        if previous_run.tenant_id is not None:
            retry_context["tenant_id"] = previous_run.tenant_id
        if mode == "resume":
            retry_context.update(self.store.output_context_for_run(previous_run.run_id))

        self._dispatch_engine(
            pipeline,
            retry_run,
            wait=wait,
            on_log=on_log,
            initial_task_statuses=reused_task_statuses,
            retry_source_run_id=previous_run.run_id,
            initial_context=retry_context,
        )
        return self.store.get_run(retry_run.run_id) or retry_run

    def backfill_run(
        self,
        run_id: str,
        *,
        wait: bool = False,
        on_log: Callable[[str], None] | None = None,
    ) -> RunRecord:
        """Re-execute one historic run with the exact configuration it was launched with."""
        self._ensure_accepting_new_work()
        source_run = self.store.get_run(run_id)
        if source_run is None:
            raise KeyError(f"Unknown run '{run_id}'")
        snapshot = self.store.get_run_config(run_id)
        if not snapshot:
            raise ValueError(
                "This run was created before runtime configuration capture was enabled, so it cannot be backfilled."
            )
        replay = self._replay_arguments(run_id)
        scoped_task_id = snapshot.get("task_id")
        if isinstance(scoped_task_id, str) and scoped_task_id:
            return self.trigger_task(
                source_run.pipeline_id,
                scoped_task_id,
                trigger="manual",
                wait=wait,
                on_log=on_log,
                tenant_id=source_run.tenant_id,
                source_run_id=run_id,
            )
        return self.trigger_pipeline(
            source_run.pipeline_id,
            trigger="manual",
            scheduled_for=None,
            wait=wait,
            on_log=on_log,
            command_overrides=replay.get("command_overrides"),  # type: ignore[arg-type]
            parent_run_id=source_run.parent_run_id,
            parent_pipeline_id=source_run.parent_pipeline_id,
            tenant_id=source_run.tenant_id,
            initial_context=replay.get("context"),  # type: ignore[arg-type]
            inherited_variables=replay.get("inherited_variables"),  # type: ignore[arg-type]
            inherited_env=replay.get("inherited_env"),  # type: ignore[arg-type]
        )

    def backfill_schedule(
        self,
        pipeline_id: str,
        *,
        start: datetime,
        end: datetime,
        limit: int = 200,
    ) -> list[datetime]:
        """Queue one run per scheduled slot in a historic window.

        Each slot is enqueued rather than executed inline so the normal
        per-pipeline concurrency and ordering rules still apply.
        """
        self._ensure_accepting_new_work()
        pipeline = self.get_pipeline(pipeline_id)
        if pipeline.schedule is None:
            raise ValueError(f"Pipeline '{pipeline_id}' has no schedule to backfill.")
        if end < start:
            raise ValueError("Backfill end must not be earlier than start.")

        queued: list[datetime] = []
        cursor = start
        for _ in range(max(1, limit)):
            slot = pipeline.schedule.next_after(cursor)
            if slot is None or slot > end:
                break
            cursor = slot
            slot_iso = slot.isoformat()
            if self.enqueue_pipeline_trigger(
                pipeline_id,
                trigger="schedule",
                available_at=datetime.now(timezone.utc),
                scheduled_for=slot,
                payload={"scheduled_for": slot_iso, "backfill": True},
                source_key=slot_iso,
                dedupe_key=f"schedule:{pipeline_id}:{slot_iso}",
            ):
                queued.append(slot)
        return queued

    def notify_run_outcome(self, pipeline: PipelineDefinition, run: RunRecord) -> None:
        """Email the configured recipients about a finished run.

        Delivery uses the central SMTP settings, so a pipeline only lists who to
        tell, never how to reach the mail server. A delivery failure is logged
        against the run and never changes its status.
        """
        recipients = pipeline.notify_on_success if run.status == "success" else pipeline.notify_on_failure
        if not recipients:
            return

        settings = load_smtp_settings(self.store)
        if not settings.configured:
            self.store.append_log(
                run.run_id,
                "Run notification skipped: no SMTP server is configured under Settings.",
                stream="stderr",
            )
            return

        duration = "unknown" if run.duration_seconds is None else f"{run.duration_seconds:.1f}s"
        body = "\n".join(
            [
                f"Pipeline : {pipeline.title} ({pipeline.pipeline_id})",
                f"Run      : {run.run_id}",
                f"Status   : {run.status}",
                f"Trigger  : {run.trigger}",
                f"Tasks    : {run.successful_tasks}/{run.task_count} succeeded",
                f"Duration : {duration}",
                *([f"Error    : {run.error}"] if run.error else []),
            ]
        )
        try:
            send_message(
                settings,
                build_message(
                    settings,
                    to=list(recipients),
                    subject=f"[Piply] {pipeline.title} {run.status}",
                    body=body,
                ),
            )
            self.store.append_log(run.run_id, f"Run notification sent to {', '.join(recipients)}.")
        except Exception as exc:  # noqa: BLE001 - a mail failure must not fail the run
            self.store.append_log(run.run_id, f"Run notification failed: {exc}", stream="stderr")

    def _handle_pipeline_success(self, pipeline: PipelineDefinition, run: RunRecord) -> None:
        """Trigger downstream pipelines after a successful run completes."""
        self.notify_run_outcome(pipeline, run)
        wait_for_pipeline_triggers = self._should_wait_for_pipeline_triggers()
        if not pipeline.triggers_on_success:
            self.drain_trigger_queue(
                limit=20,
                wait_for_pipeline_triggers=wait_for_pipeline_triggers,
            )
            return
        output_context = self.store.output_context_for_run(run.run_id)
        upstream_env = self._pipeline_env(pipeline)
        for target in pipeline.triggers_on_success:
            self.store.append_log(
                run.run_id,
                f"Triggering downstream pipeline '{target}' after successful completion.",
            )
            self.enqueue_pipeline_trigger(
                target,
                trigger="pipeline",
                payload={
                    "source_run_id": run.run_id,
                    "source_pipeline_id": pipeline.pipeline_id,
                    "tenant_id": run.tenant_id,
                    "context": output_context,
                    "upstream": output_context,
                    "variables": pipeline.variables,
                    "env": upstream_env,
                },
                source_key=run.run_id,
                dedupe_key=f"pipeline:{run.run_id}:{target}",
            )
        self.drain_trigger_queue(
            limit=20,
            wait_for_pipeline_triggers=wait_for_pipeline_triggers,
        )

    def _retry_depth(self, run: RunRecord) -> int:
        """Return how many retry generations exist behind the supplied run."""
        depth = 0
        current = run
        while current.retry_of:
            previous = self.store.get_run(current.retry_of)
            if previous is None:
                break
            depth += 1
            current = previous
        return depth

    def _handle_pipeline_failure(self, pipeline: PipelineDefinition, run: RunRecord) -> None:
        """Schedule an automatic retry when the pipeline retry policy allows it."""
        self.notify_run_outcome(pipeline, run)
        retry_policy = pipeline.retry_policy
        if not retry_policy.enabled or run.status != "failed":
            self.drain_trigger_queue(limit=20)
            return

        retry_depth = self._retry_depth(run)
        if retry_depth >= retry_policy.attempts:
            self.store.append_log(
                run.run_id,
                "Retry policy exhausted. No more automatic retries will be created.",
            )
            self.drain_trigger_queue(limit=20)
            return

        self.store.append_log(
            run.run_id,
            (f"Automatic retry {retry_depth + 1}/{retry_policy.attempts} queued using {retry_policy.mode} mode."),
        )

        self.enqueue_pipeline_trigger(
            run.pipeline_id,
            trigger="retry",
            available_at=datetime.now(timezone.utc) + timedelta(seconds=retry_policy.delay_seconds),
            payload={
                "retry_of": run.run_id,
                "mode": retry_policy.mode,
            },
            source_key=run.run_id,
            dedupe_key=f"retry:{run.run_id}:{retry_depth + 1}",
        )

        if retry_policy.delay_seconds > 0:
            timer = threading.Timer(retry_policy.delay_seconds, lambda: self.drain_trigger_queue(limit=20))
            timer.daemon = True
            timer.start()
            return
        self.drain_trigger_queue(limit=20)

    def set_pipeline_paused(self, pipeline_id: str, paused: bool) -> PipelineSummary:
        """Pause or resume a pipeline schedule."""
        self.get_pipeline(pipeline_id)
        self.store.set_pipeline_paused(pipeline_id, paused)
        if not paused:
            self.drain_trigger_queue(limit=20)
        return self.get_pipeline_summary(pipeline_id)

    def cancel_run(self, run_id: str) -> RunRecord:
        """Request cancellation for one queued or running run."""
        self.reconcile_runtime_health()
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(f"Unknown run '{run_id}'")
        if run.status not in {"queued", "running"}:
            raise ValueError("Only queued or running runs can be cancelled.")

        self.store.append_log(run_id, "Cancellation requested by user.", stream="stderr")
        cancelled = self.engine.cancel(run_id)
        if run.status == "queued" or not cancelled:
            self.store.cancel_run(run_id)
        return self.store.get_run(run_id) or run

    def delete_run(self, run_id: str) -> None:
        """Delete one finished run from the runtime store."""
        self.reconcile_runtime_health()
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(f"Unknown run '{run_id}'")
        if run.status in {"queued", "running"}:
            raise ValueError("Cancel the run before deleting it.")
        self.store.delete_run(run_id)

    def delete_pipeline(self, pipeline_id: str) -> None:
        """Delete one pipeline from the config and remove its stored history."""
        if self.store.count_running_runs(pipeline_id) > 0:
            raise ValueError("Cancel active runs before deleting this pipeline.")

        raw_data = yaml.safe_load(self.config_path.read_text(encoding="utf-8")) or {}
        root_key = "pipelines" if "pipelines" in raw_data else "jobs"
        pipelines = raw_data.get(root_key)
        if not isinstance(pipelines, dict) or pipeline_id not in pipelines:
            raise KeyError(f"Unknown pipeline '{pipeline_id}'")

        pipelines.pop(pipeline_id)
        for candidate in pipelines.values():
            if not isinstance(candidate, dict):
                continue
            triggers = candidate.get("triggers_on_success")
            if isinstance(triggers, list):
                candidate["triggers_on_success"] = [item for item in triggers if str(item) != pipeline_id]

        self.config_path.write_text(
            yaml.safe_dump(raw_data, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )
        self.store.delete_pipeline_runs(pipeline_id)
        self.reload_project(force=True)

    def runtime_metrics(self) -> dict[str, object]:
        """Return queue and local worker metrics for API/UI surfaces."""
        queue_metrics = self.store.queue_metrics()
        worker_metrics = self.store.worker_metrics()
        configured_capacity = sum(
            pipeline.max_parallel_tasks for pipeline in self.project.pipelines.values() if pipeline.enabled
        )
        worker_metrics["configured_task_capacity"] = configured_capacity
        worker_metrics["default_task_capacity"] = self.settings.default_max_parallel_tasks
        return {
            "queue": queue_metrics,
            "workers": worker_metrics,
        }

    def scheduler_snapshot(self) -> dict[str, object]:
        """Return scheduler heartbeat, database metadata, and runtime counters for the UI."""
        metrics = self.runtime_metrics()
        heartbeat = self.store.get_meta("scheduler_heartbeat")
        configured_state = self.store.get_meta("scheduler_state") or (
            "running" if self.store.get_meta("scheduler_running") == "true" else "stopped"
        )
        heartbeat_age_seconds: float | None = None
        if heartbeat:
            try:
                heartbeat_dt = datetime.fromisoformat(heartbeat)
                heartbeat_age_seconds = max(
                    0.0,
                    (datetime.now(timezone.utc) - heartbeat_dt.astimezone(timezone.utc)).total_seconds(),
                )
            except ValueError:
                heartbeat_age_seconds = None

        stale_after_seconds = max(6, self.settings.scheduler_poll_interval_seconds * 3)
        raw_owner_pid = self.store.get_meta("scheduler_owner_pid")
        owner_pid = int(raw_owner_pid) if raw_owner_pid and raw_owner_pid.isdigit() else None
        owner_alive = is_process_alive(owner_pid) if owner_pid is not None else None

        state = configured_state
        if configured_state == "running" and owner_alive is False:
            # The process that claimed the scheduler is gone, so a stale
            # heartbeat means a hard kill rather than a slow tick.
            state = "crashed"
        elif configured_state == "running" and (
            heartbeat_age_seconds is None or heartbeat_age_seconds > stale_after_seconds
        ):
            state = "stale"
        running = state == "running"
        if state == "running":
            label = "scheduler live"
        elif state == "crashed":
            label = "scheduler crashed"
        elif state == "stale":
            label = "scheduler not responding"
        else:
            label = "scheduler offline"
        return {
            "running": running,
            "state": state,
            "label": label,
            "heartbeat": heartbeat,
            "heartbeat_age_seconds": heartbeat_age_seconds,
            "owner_pid": owner_pid,
            "owner_alive": owner_alive,
            "started_at": self.store.get_meta("scheduler_started_at"),
            "last_error": self.store.get_meta("scheduler_last_error"),
            "config_path": str(self.config_path),
            "database_path": self.database_location,
            "queue_depth": self.store.count_queue(),
            "sensor_count": sum(pipeline.sensor_count for pipeline in self.project.pipelines.values()),
            "accepting_work": not self.is_shutting_down,
            "queue_metrics": metrics["queue"],
            "worker_metrics": metrics["workers"],
        }

    def preview_pipeline(
        self,
        pipeline_id: str,
        *,
        params: dict[str, object] | None = None,
        tenant_id: str | None = None,
        command_overrides: dict[str, str] | None = None,
        source_run_id: str | None = None,
    ) -> PipelinePreview:
        """Build a dry-run preview without creating a run or touching the store."""
        replay = self._replay_arguments(source_run_id) if source_run_id else {}
        pipeline = self._clone_pipeline_with_command_overrides(
            self._clone_pipeline_with_inherited_variables(
                self.get_pipeline(pipeline_id),
                replay.get("inherited_variables"),  # type: ignore[arg-type]
                replay.get("inherited_env"),  # type: ignore[arg-type]
            ),
            command_overrides or replay.get("command_overrides"),  # type: ignore[arg-type]
        )
        context: dict[str, object] = dict(replay.get("context") or {})  # type: ignore[arg-type]
        if params:
            context["params"] = params
            context.update(params)
        if tenant_id is not None:
            context["tenant_id"] = tenant_id
        return build_pipeline_preview(pipeline, context=context)

    def preview_project(self) -> list[PipelinePreview]:
        """Build dry-run previews for every configured pipeline."""
        return [build_pipeline_preview(pipeline) for pipeline in self.project.pipelines.values()]

    def list_run_artifacts(self, run_id: str, task_id: str | None = None) -> list[dict[str, object]]:
        """Return artifacts recorded for one run, refreshing size and mtime from disk."""
        if self.store.get_run(run_id) is None:
            raise KeyError(f"Unknown run '{run_id}'")
        artifacts = self.store.list_task_artifacts(run_id, task_id)
        for artifact in artifacts:
            path = Path(str(artifact["path"]))
            artifact["exists"] = path.is_file()
            if artifact["exists"]:
                artifact["size_bytes"] = path.stat().st_size
        return artifacts

    def artifact_roots(self) -> list[Path]:
        """Return the directories artifact downloads are allowed to read from."""
        roots = [self.project.workspace, self.config_path.parent]
        if self.settings.artifacts_dir is not None:
            roots.append(self.settings.artifacts_dir)
        return [root for root in roots if root.exists()]

    def prune(self, *, dry_run: bool = False, vacuum: bool = True, **overrides: int) -> dict[str, object]:
        """Remove history beyond the configured retention window."""
        run_days = int(overrides.get("run_retention_days", self.settings.retention_run_days))
        log_days = int(overrides.get("log_retention_days", self.settings.retention_log_days))
        max_runs = int(overrides.get("max_runs_per_pipeline", self.settings.retention_max_runs_per_pipeline))
        size_before = self.store.database_size_bytes()
        summary = self.store.prune(
            run_retention_days=run_days,
            log_retention_days=log_days,
            max_runs_per_pipeline=max_runs,
            vacuum=vacuum and not dry_run,
            dry_run=dry_run,
        )
        return {
            **summary,
            "dry_run": dry_run,
            "run_retention_days": run_days,
            "log_retention_days": log_days,
            "max_runs_per_pipeline": max_runs,
            "database_bytes_before": size_before,
            "database_bytes_after": self.store.database_size_bytes(),
        }

    def sensor_health(self) -> list[dict[str, object]]:
        """Return the health of every configured sensor, including ones never polled."""
        recorded = {str(item["sensor_key"]): item for item in self.store.list_sensor_health()}
        results: list[dict[str, object]] = []
        for pipeline in self.project.pipelines.values():
            for sensor in pipeline.sensors.values():
                key = self._sensor_state_key(pipeline.pipeline_id, sensor.sensor_id)
                entry = recorded.get(key) or {
                    "sensor_key": key,
                    "pipeline_id": pipeline.pipeline_id,
                    "sensor_id": sensor.sensor_id,
                    "sensor_type": sensor.sensor_type,
                    "status": "idle",
                    "last_polled_at": None,
                    "last_success_at": None,
                    "last_event_at": None,
                    "last_error": None,
                    "consecutive_failures": 0,
                    "poll_count": 0,
                    "event_count": 0,
                }
                entry["title"] = sensor.title
                entry["summary"] = sensor.summary
                entry["enabled"] = sensor.enabled
                results.append(entry)
        return sorted(results, key=lambda item: (item["status"] != "failing", str(item["sensor_key"])))

    def get_smtp_settings(self) -> dict[str, object]:
        """Return the central SMTP configuration, without the password."""
        return load_smtp_settings(self.store).public_dict()

    def save_smtp_settings(self, values: dict[str, object]) -> dict[str, object]:
        """Persist central SMTP configuration and return the safe view of it."""
        return save_smtp_settings(self.store, values).public_dict()

    def send_test_email(self, recipient: str) -> str:
        """Send one test message so an admin can confirm the settings work."""
        settings = load_smtp_settings(self.store)
        if not settings.configured:
            raise ValueError("No SMTP server is configured.")
        send_message(
            settings,
            build_message(
                settings,
                to=[recipient],
                subject="[Piply] SMTP test message",
                body=f"This is a test message from Piply ({self.project.title}).",
            ),
        )
        return f"Test message sent to {recipient} via {settings.host}."

    # --- Users and permissions ---------------------------------------------

    def bootstrap_admin(self) -> tuple[str, str | None] | None:
        """Create the initial admin account when none exists.

        This is how a server install gets its first account without shell
        access. Only runs when authentication has been switched on: an existing
        install that never enabled auth keeps working with no accounts and no
        login page, which is what backward compatibility requires here.

        Returns ``(username, password)`` exactly once, on the run that creates
        the account. ``password`` is None when the operator supplied one, so
        the caller knows not to echo a secret it was given into the logs. A
        generated password is returned so it can be shown once; it is never
        stored in clear text and cannot be retrieved again.
        """
        if not self.settings.auth_enabled or self.store.count_users() > 0:
            return None
        if self.settings.auth_username and self.settings.auth_password:
            # PIPLY_AUTH_USERNAME/PASSWORD already define an administrator, so
            # generating a second one would be surprising and unnecessary.
            return None

        username = normalize_username(os.environ.get("PIPLY_ADMIN_USERNAME") or "admin")
        supplied = read_secret(os.environ, "PIPLY_ADMIN_PASSWORD")
        password = supplied or generate_password()
        self.store.upsert_user(username, password_hash=hash_password(password), role="admin", is_active=True)
        self.store.set_meta("admin_bootstrapped_at", datetime.now(timezone.utc).isoformat())
        return username, (None if supplied else password)

    def get_user(self, username: str) -> User | None:
        """Return one account, or None."""
        record = self.store.get_user_record(normalize_username(username))
        if record is None:
            return None
        return User(
            username=str(record["username"]),
            role=str(record["role"]),
            is_active=bool(record["is_active"]),
            created_at=record["created_at"],  # type: ignore[arg-type]
            last_login_at=record["last_login_at"],  # type: ignore[arg-type]
            permissions=dict(record["permissions"]),  # type: ignore[arg-type]
        )

    def list_users(self) -> list[User]:
        """Return every account."""
        return [
            User(
                username=str(item["username"]),
                role=str(item["role"]),
                is_active=bool(item["is_active"]),
                created_at=item["created_at"],  # type: ignore[arg-type]
                last_login_at=item["last_login_at"],  # type: ignore[arg-type]
                permissions=dict(item["permissions"]),  # type: ignore[arg-type]
            )
            for item in self.store.list_user_records()
        ]

    def authenticate(self, username: str, password: str) -> User | None:
        """Return the account when the credentials are valid and active.

        Repeated failures lock the username out for a few minutes. Verification
        is intentionally slow, so an unthrottled endpoint would be both a
        guessing risk and a way to exhaust CPU.
        """
        try:
            normalized = normalize_username(username)
        except AuthError:
            return None
        if self.login_throttle.retry_after(normalized):
            return None

        record = self.store.get_user_record(normalized)
        if record is None or not record["is_active"]:
            # Still hash, so a missing user and a wrong password take the same
            # time and cannot be told apart by an attacker.
            verify_password(password, self._timing_decoy_hash)
            self.login_throttle.record_failure(normalized)
            return None
        if not verify_password(password, str(record["password_hash"])):
            self.login_throttle.record_failure(normalized)
            return None

        self.login_throttle.record_success(normalized)
        self.store.touch_user_login(normalized)
        return self.get_user(normalized)

    def login_retry_after(self, username: str) -> int:
        """Return the remaining lockout in seconds for a username, else 0."""
        try:
            return self.login_throttle.retry_after(normalize_username(username))
        except AuthError:
            return 0

    def create_user(
        self,
        username: str,
        password: str,
        *,
        role: str = "user",
        permissions: dict[str, object] | None = None,
    ) -> User:
        """Create one account with optional initial grants."""
        normalized = normalize_username(username)
        if role not in ROLES:
            raise AuthError(f"Role must be one of: {', '.join(ROLES)}.")
        if self.store.get_user_record(normalized) is not None:
            raise AuthError(f"User '{normalized}' already exists.")
        self.store.upsert_user(normalized, password_hash=hash_password(password), role=role, is_active=True)
        for pipeline_id, actions in (permissions or {}).items():
            self.grant_permission(normalized, str(pipeline_id), actions)
        user = self.get_user(normalized)
        assert user is not None
        return user

    def update_user(
        self,
        username: str,
        *,
        password: str | None = None,
        role: str | None = None,
        is_active: bool | None = None,
    ) -> User:
        """Update one account's password, role, or active flag."""
        normalized = normalize_username(username)
        if self.store.get_user_record(normalized) is None:
            raise AuthError(f"Unknown user '{normalized}'.")
        if role is not None and role not in ROLES:
            raise AuthError(f"Role must be one of: {', '.join(ROLES)}.")
        if (role and role != "admin") or is_active is False:
            self._ensure_another_admin_remains(normalized)
        self.store.upsert_user(
            normalized,
            password_hash=hash_password(password) if password else None,
            role=role,
            is_active=is_active,
        )
        user = self.get_user(normalized)
        assert user is not None
        return user

    def delete_user(self, username: str) -> None:
        """Delete one account, refusing to remove the last active admin."""
        normalized = normalize_username(username)
        self._ensure_another_admin_remains(normalized)
        if not self.store.delete_user(normalized):
            raise AuthError(f"Unknown user '{normalized}'.")

    def _ensure_another_admin_remains(self, username: str) -> None:
        """Refuse a change that would leave the install with no way in."""
        current = self.get_user(username)
        if current is None or not current.is_admin or not current.is_active:
            return
        other_admins = [
            item for item in self.list_users() if item.is_admin and item.is_active and item.username != username
        ]
        if not other_admins:
            raise AuthError("This is the only active admin. Promote another admin first.")

    def grant_permission(self, username: str, pipeline_id: str, actions: object) -> User:
        """Grant pipeline actions to a user. Use '*' for every pipeline."""
        normalized = normalize_username(username)
        if self.store.get_user_record(normalized) is None:
            raise AuthError(f"Unknown user '{normalized}'.")
        if pipeline_id != ALL_PIPELINES and pipeline_id not in self.project.pipelines:
            raise AuthError(f"Unknown pipeline '{pipeline_id}'.")
        self.store.set_user_permission(normalized, pipeline_id, normalize_permissions(actions))
        user = self.get_user(normalized)
        assert user is not None
        return user

    def revoke_permission(self, username: str, pipeline_id: str) -> User:
        """Remove every grant a user holds on one pipeline."""
        return self.grant_permission(username, pipeline_id, frozenset())

    @property
    def auth_required(self) -> bool:
        """Return whether requests must be authenticated.

        Accounts existing in the database is itself enough to switch auth on,
        so creating the first user secures the install without a second step.
        """
        return bool(self.settings.auth_enabled) or self.store.count_users() > 0

    def diagnostics(self) -> dict[str, object]:
        """Return the full runtime diagnostics payload used by the API and UI."""
        scheduler = self.scheduler_snapshot()
        sensors = self.sensor_health()
        failing_sensors = [item for item in sensors if item["status"] == "failing"]
        running_tasks = self.store.list_running_tasks()
        return {
            "scheduler": scheduler,
            "queue": scheduler["queue_metrics"],
            "workers": scheduler["worker_metrics"],
            "running_tasks": running_tasks,
            "sensors": sensors,
            "sensor_summary": {
                "total": len(sensors),
                "failing": len(failing_sensors),
                "healthy": sum(1 for item in sensors if item["status"] == "healthy"),
                "idle": sum(1 for item in sensors if item["status"] == "idle"),
                "last_error": self.store.get_meta("sensor_last_error"),
            },
            "reconciliation": {
                "last_recovery_at": self.store.get_meta("runtime_last_recovery_at"),
                "last_recovered_runs": int(self.store.get_meta("runtime_last_recovered_runs") or 0),
                "stale_run_timeout_seconds": self.settings.stale_run_timeout_seconds,
                "reconcile_interval_seconds": self.settings.reconcile_interval_seconds,
                "accepting_work": not self.is_shutting_down,
            },
            "database": {
                "path": self.database_location,
                "backend": self.store.dialect.name,
                "size_bytes": self.store.database_size_bytes(),
                "retention_run_days": self.settings.retention_run_days,
                "retention_log_days": self.settings.retention_log_days,
                "retention_max_runs_per_pipeline": self.settings.retention_max_runs_per_pipeline,
            },
            "process": {
                "pid": os.getpid(),
                "config_path": str(self.config_path),
                "workspace": str(self.project.workspace),
            },
        }

    def tail_logs(
        self,
        *,
        run_id: str | None = None,
        pipeline_id: str | None = None,
        task_id: str | None = None,
        after: datetime | None = None,
        after_id: int | None = None,
        limit: int = 500,
    ) -> list[dict[str, object]]:
        """Return log lines after a cursor for CLI and UI follow mode."""
        cursor = after_id
        if cursor is None and after is not None:
            cursor = self.store.log_cursor_at(after)
        return self.store.tail_logs(
            run_id=run_id,
            pipeline_id=pipeline_id,
            task_id=task_id,
            after_id=cursor or 0,
            limit=limit,
        )

    def search_logs(
        self,
        *,
        query: str | None = None,
        pipeline_id: str | None = None,
        pipeline_ids: set[str] | None = None,
        task_id: str | None = None,
        limit: int = 300,
    ):
        """Search recent log messages across runs.

        ``pipeline_ids`` narrows the search to a permitted set of pipelines.
        """
        self.reconcile_runtime_health()
        return self.store.search_logs(
            query=query,
            pipeline_id=pipeline_id,
            pipeline_ids=pipeline_ids,
            task_id=task_id,
            limit=limit,
        )

    def execution_matrix(
        self,
        *,
        pipeline_id: str | None = None,
        tenant_id: str | None = None,
        status: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        limit: int = 24,
    ) -> dict[str, object]:
        """Build an Airflow-style grid of tasks by recent pipeline runs."""
        self.reconcile_runtime_health()
        pipelines = self.list_pipelines()
        selected_pipeline_id = pipeline_id
        if selected_pipeline_id is None:
            selected_pipeline_id = pipelines[0].pipeline_id if pipelines else None
        if selected_pipeline_id is None:
            return {
                "pipelines": [],
                "selected_pipeline_id": None,
                "runs": [],
                "rows": [],
                "trend": [],
            }

        pipeline = self.get_pipeline(selected_pipeline_id)
        runs = self.list_runs(
            pipeline_id=selected_pipeline_id,
            status=status,
            tenant_id=tenant_id,
            created_after=date_from,
            created_before=date_to,
            limit=limit,
        )
        ordered_runs = list(reversed(runs))
        task_runs_by_run = {
            run.run_id: {task.task_id: task for task in self.store.list_task_runs(run.run_id)} for run in ordered_runs
        }
        rows: list[dict[str, object]] = []
        for task in pipeline.tasks.values():
            cells = []
            for run in ordered_runs:
                task_run = task_runs_by_run.get(run.run_id, {}).get(task.task_id)
                cells.append(
                    {
                        "run_id": run.run_id,
                        "task_id": task.task_id,
                        "status": task_run.status if task_run else "queued",
                        "duration_seconds": task_run.duration_seconds if task_run else None,
                        "log_count": task_run.log_count if task_run else 0,
                        "error": task_run.error if task_run else None,
                        "output_preview": task_run.output_preview if task_run else None,
                    }
                )
            rows.append({"task": task, "cells": cells})

        trend = [
            {
                "run_id": run.run_id,
                "status": run.status,
                "duration_seconds": run.duration_seconds or 0,
                "created_at": run.created_at,
            }
            for run in ordered_runs
        ]
        return {
            "pipelines": pipelines,
            "selected_pipeline_id": selected_pipeline_id,
            "runs": ordered_runs,
            "rows": rows,
            "trend": trend,
            "filters": {
                "tenant_id": tenant_id,
                "status": status,
                "date_from": date_from,
                "date_to": date_to,
            },
        }

    def _runtime_trend(self, runs: list[RunRecord]) -> list[dict[str, object]]:
        """Build compact run-duration trend points for dashboard charts."""
        return [
            {
                "run_id": run.run_id,
                "pipeline_id": run.pipeline_id,
                "status": run.status,
                "duration_seconds": run.duration_seconds or 0,
                "created_at": run.created_at,
            }
            for run in reversed(runs[-12:])
        ]

    def dashboard(self) -> dict[str, object]:
        """Return the dashboard payload shared by the API and UI."""
        self.reconcile_runtime_health()
        pipelines = self.list_pipelines()
        recent_runs = self.store.list_runs(limit=10)
        trend_runs = self.store.list_runs(limit=12)
        scheduler = self.scheduler_snapshot()
        recent_failure_like_runs = [
            run for run in self.store.list_runs(limit=20) if run.status in {"failed", "interrupted"}
        ][:5]
        runtime_metrics = {
            "queue": scheduler.get("queue_metrics", {}),
            "workers": scheduler.get("worker_metrics", {}),
        }
        stats = self.store.get_stats(
            scheduled_pipeline_count=sum(1 for pipeline in pipelines if pipeline.schedule_text != "Manual only"),
            total_pipeline_count=len(pipelines),
        )
        return {
            "project": {
                "title": self.project.title,
                "config_path": str(self.project.config_path),
                "workspace": str(self.project.workspace),
            },
            "stats": stats,
            "pipelines": pipelines,
            "recent_runs": recent_runs,
            "recent_failures": recent_failure_like_runs,
            "active_pipelines": [pipeline for pipeline in pipelines if pipeline.active_runs > 0],
            "runtime_trend": self._runtime_trend(trend_runs),
            "scheduler": scheduler,
            "runtime_metrics": runtime_metrics,
            "settings": {
                "auth_enabled": self.settings.auth_enabled,
                "default_max_parallel_tasks": self.settings.default_max_parallel_tasks,
                "stale_run_timeout_seconds": self.settings.stale_run_timeout_seconds,
                "heartbeat_interval_seconds": self.settings.heartbeat_interval_seconds,
                "scheduler_poll_interval_seconds": self.settings.scheduler_poll_interval_seconds,
                "queue_dispatch_batch_size": self.settings.queue_dispatch_batch_size,
                "queue_dispatch_stale_seconds": self.settings.queue_dispatch_stale_seconds,
            },
        }
