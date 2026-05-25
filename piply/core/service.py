"""High-level runtime service used by the CLI, API, and scheduler."""

from __future__ import annotations

import inspect
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from piply.engine.base import BaseEngine
from piply.engine.local_engine import LocalEngine
from piply.settings import PiplySettings, load_settings

from .graph import upstream_closure
from .loader import discover_config, load_project
from .models import PipelineDefinition, PipelineSummary, ProjectDefinition, RetryMode, RetryPolicy, RunRecord
from .retry import build_retry_plan
from .sensors import poll_api_sensor, poll_file_sensor, poll_sql_sensor
from .store import RunStore


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
        resolved_config_path = Path(config_path).resolve() if config_path else discover_config()
        self.settings = settings or load_settings(resolved_config_path)
        self.config_path = resolved_config_path
        self.database_path = (
            Path(database_path).resolve()
            if database_path
            else (self.settings.database_path or (self.config_path.parent / ".piply" / "piply.db").resolve())
        )
        self.store = RunStore(self.database_path)
        self.engine = engine or LocalEngine(heartbeat_interval_seconds=self.settings.heartbeat_interval_seconds)
        self._engine_accepts_initial_context = self._detect_engine_initial_context_support()
        self._dispatch_context = threading.local()
        self._project: ProjectDefinition | None = None
        self._config_mtime: float | None = None
        self._lock = threading.RLock()
        self.reconcile_runtime_health()
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

    def reconcile_runtime_health(self) -> list[str]:
        """Reconcile stale queued or running executions before building UI views."""
        return self.store.reconcile_stale_runs(self.settings.stale_run_timeout_seconds)

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

    def list_pipelines(self) -> list[PipelineSummary]:
        """Return pipeline summaries enriched with scheduling and run metadata."""
        self.reconcile_runtime_health()
        project = self.project
        paused_ids = self.store.list_paused_pipeline_ids()
        now = datetime.now(timezone.utc)
        summaries: list[PipelineSummary] = []
        for pipeline in project.pipelines.values():
            last_run = self.store.get_latest_run_for_pipeline(pipeline.pipeline_id)
            next_run_at = pipeline.schedule.next_after(now) if pipeline.schedule else None
            latest_task_states = self.store.get_latest_task_states_for_pipeline(pipeline.pipeline_id)
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
                    latest_task_states=latest_task_states,
                    last_run=last_run,
                    active_runs=self.store.count_running_runs(pipeline.pipeline_id),
                    retry_summary=pipeline.retry_policy.summary,
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
                if sensor.sensor_type == "file_sensor":
                    next_state, event = poll_file_sensor(sensor, state)
                elif sensor.sensor_type == "sql_sensor":
                    next_state, event = poll_sql_sensor(sensor, state)
                else:
                    next_state, event = poll_api_sensor(sensor, state)
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
                        initial_context.update(payload["context"])  # type: ignore[arg-type]
                    if isinstance(payload.get("upstream"), dict):
                        initial_context.setdefault("upstream", payload["upstream"])
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
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = 50,
    ) -> list[RunRecord]:
        """Return recent runs with optional filters."""
        self.reconcile_runtime_health()
        return self.store.list_runs(
            pipeline_id=pipeline_id,
            status=status,
            tenant_id=tenant_id,
            created_after=created_after,
            created_before=created_before,
            limit=limit,
        )

    def get_run(self, run_id: str):
        """Return one run, its task runs, and its raw logs."""
        self.reconcile_runtime_health()
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(f"Unknown run '{run_id}'")
        task_runs = self.store.list_task_runs(run_id)
        logs = self.store.list_logs(run_id, limit=500)
        return run, task_runs, logs

    def get_run_detail(self, run_id: str) -> dict[str, object]:
        """Return one run plus task runs, logs, and upcoming schedule slots."""
        run, task_runs, logs = self.get_run(run_id)
        return {
            "run": run,
            "task_runs": task_runs,
            "logs": logs,
            "upcoming_runs": self.list_upcoming_runs(run.pipeline_id, count=8),
        }

    def get_task_detail(self, run_id: str, task_id: str) -> dict[str, object]:
        """Return one task run with logs and output metadata."""
        run, task_runs, _ = self.get_run(run_id)
        task = next((item for item in task_runs if item.task_id == task_id), None)
        if task is None:
            raise KeyError(f"Unknown task '{task_id}' in run '{run_id}'")
        logs = self.store.list_logs(run_id, limit=500, task_id=task_id)
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

    def _clone_pipeline_for_task(self, pipeline: PipelineDefinition, task_id: str) -> PipelineDefinition:
        """Build a task-focused pipeline that includes the selected task and its dependencies."""
        if task_id not in pipeline.tasks:
            raise KeyError(f"Unknown task '{task_id}' in pipeline '{pipeline.pipeline_id}'")
        required_ids = upstream_closure(pipeline, {task_id})
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
    ) -> RunRecord:
        """Create and dispatch one new run for a pipeline."""
        self.reconcile_runtime_health()
        pipeline = self._clone_pipeline_with_command_overrides(
            self.get_pipeline(pipeline_id),
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
            )
            dispatch_context = dict(initial_context or {})
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
    ) -> RunRecord:
        """Create and dispatch one run scoped to a selected task and its dependencies."""
        self.reconcile_runtime_health()
        pipeline = self._clone_pipeline_for_task(self.get_pipeline(pipeline_id), task_id)
        pipeline = self._clone_pipeline_with_command_overrides(pipeline, command_overrides)
        run = self.store.create_run(
            pipeline,
            trigger=trigger,
            retry_task_id=task_id,
        )
        self._dispatch_engine(
            pipeline,
            run,
            wait=wait,
            on_log=on_log,
            initial_task_statuses={},
            retry_source_run_id=None,
            initial_context={},
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
        self.reconcile_runtime_health()
        previous_run, previous_task_runs, _ = self.get_run(run_id)
        if previous_run.status in {"queued", "running"}:
            raise ValueError("Retry is only available after a run has finished.")

        pipeline = self.get_pipeline(previous_run.pipeline_id)
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

        retry_context: dict[str, object] = {}
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

    def _handle_pipeline_success(self, pipeline: PipelineDefinition, run: RunRecord) -> None:
        """Trigger downstream pipelines after a successful run completes."""
        wait_for_pipeline_triggers = self._should_wait_for_pipeline_triggers()
        if not pipeline.triggers_on_success:
            self.drain_trigger_queue(
                limit=20,
                wait_for_pipeline_triggers=wait_for_pipeline_triggers,
            )
            return
        output_context = self.store.output_context_for_run(run.run_id)
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
        return {
            "running": self.store.get_meta("scheduler_running") == "true",
            "heartbeat": self.store.get_meta("scheduler_heartbeat"),
            "config_path": str(self.config_path),
            "database_path": str(self.database_path),
            "queue_depth": self.store.count_queue(),
            "sensor_count": sum(pipeline.sensor_count for pipeline in self.project.pipelines.values()),
            "queue_metrics": metrics["queue"],
            "worker_metrics": metrics["workers"],
        }

    def search_logs(
        self,
        *,
        query: str | None = None,
        pipeline_id: str | None = None,
        task_id: str | None = None,
        limit: int = 300,
    ):
        """Search recent log messages across runs."""
        self.reconcile_runtime_health()
        return self.store.search_logs(
            query=query,
            pipeline_id=pipeline_id,
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
            "recent_failures": self.store.list_runs(status="failed", limit=5),
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
