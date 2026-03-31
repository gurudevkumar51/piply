"""High-level runtime service used by the CLI, API, and scheduler."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from piply.engine.base import BaseEngine
from piply.engine.local_engine import LocalEngine
from piply.settings import PiplySettings, load_settings

from .loader import discover_config, load_project
from .models import PipelineDefinition, PipelineSummary, ProjectDefinition, RetryMode, RunRecord
from .retry import build_retry_plan
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
            else (
                self.settings.database_path
                or (self.config_path.parent / ".piply" / "piply.db").resolve()
            )
        )
        self.store = RunStore(self.database_path)
        self.engine = engine or LocalEngine(heartbeat_interval_seconds=self.settings.heartbeat_interval_seconds)
        self._project: ProjectDefinition | None = None
        self._config_mtime: float | None = None
        self._lock = threading.RLock()
        self.reconcile_runtime_health()
        self.reload_project(force=True)

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
                    schedule_text=(
                        pipeline.schedule.describe() if pipeline.schedule else "Manual only"
                    ),
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

    def list_runs(
        self,
        *,
        pipeline_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[RunRecord]:
        """Return recent runs with optional filters."""
        self.reconcile_runtime_health()
        return self.store.list_runs(pipeline_id=pipeline_id, status=status, limit=limit)

    def get_run(self, run_id: str):
        """Return one run, its task runs, and its raw logs."""
        self.reconcile_runtime_health()
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(f"Unknown run '{run_id}'")
        task_runs = self.store.list_task_runs(run_id)
        logs = self.store.list_logs(run_id, limit=500)
        return run, task_runs, logs

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
    ) -> RunRecord:
        """Create and dispatch one new run for a pipeline."""
        self.reconcile_runtime_health()
        pipeline = self.get_pipeline(pipeline_id)
        if scheduled_for is not None and self.store.has_run_for_slot(pipeline_id, scheduled_for):
            latest = self.store.list_runs(pipeline_id=pipeline_id, limit=1)
            if latest:
                return latest[0]

        try:
            run = self.store.create_run(
                pipeline,
                trigger=trigger,
                scheduled_for=scheduled_for,
                retry_of=retry_of,
                retry_mode=retry_mode,
                retry_task_id=retry_task_id,
            )
            self.engine.dispatch(
                pipeline,
                run,
                self.store,
                wait=wait,
                on_log=on_log,
                on_success=self._handle_pipeline_success,
                initial_task_statuses=initial_task_statuses or {},
                retry_source_run_id=retry_of,
            )
        except sqlite3.IntegrityError:
            if scheduled_for is not None:
                latest = self.store.list_runs(pipeline_id=pipeline_id, limit=1)
                if latest:
                    return latest[0]
            raise
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
            trigger="manual",
            retry_of=previous_run.run_id,
            retry_mode=mode,
            retry_task_id=task_id,
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

        self.engine.dispatch(
            pipeline,
            retry_run,
            self.store,
            wait=wait,
            on_log=on_log,
            on_success=self._handle_pipeline_success,
            initial_task_statuses=reused_task_statuses,
            retry_source_run_id=previous_run.run_id,
        )
        return self.store.get_run(retry_run.run_id) or retry_run

    def _handle_pipeline_success(self, pipeline: PipelineDefinition, run: RunRecord) -> None:
        """Trigger downstream pipelines after a successful run completes."""
        if not pipeline.triggers_on_success:
            return
        for target in pipeline.triggers_on_success:
            self.store.append_log(
                run.run_id,
                f"Triggering downstream pipeline '{target}' after successful completion.",
            )
            self.trigger_pipeline(target, trigger="pipeline", wait=False)

    def set_pipeline_paused(self, pipeline_id: str, paused: bool) -> PipelineSummary:
        """Pause or resume a pipeline schedule."""
        self.get_pipeline(pipeline_id)
        self.store.set_pipeline_paused(pipeline_id, paused)
        return self.get_pipeline_summary(pipeline_id)

    def scheduler_snapshot(self) -> dict[str, str | bool | None]:
        """Return scheduler heartbeat and database metadata for the UI."""
        return {
            "running": self.store.get_meta("scheduler_running") == "true",
            "heartbeat": self.store.get_meta("scheduler_heartbeat"),
            "config_path": str(self.config_path),
            "database_path": str(self.database_path),
        }

    def dashboard(self) -> dict[str, object]:
        """Return the dashboard payload shared by the API and UI."""
        self.reconcile_runtime_health()
        pipelines = self.list_pipelines()
        stats = self.store.get_stats(
            scheduled_pipeline_count=sum(
                1 for pipeline in pipelines if pipeline.schedule_text != "Manual only"
            ),
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
            "recent_runs": self.store.list_runs(limit=10),
            "scheduler": self.scheduler_snapshot(),
            "settings": {
                "auth_enabled": self.settings.auth_enabled,
                "default_max_parallel_tasks": self.settings.default_max_parallel_tasks,
                "stale_run_timeout_seconds": self.settings.stale_run_timeout_seconds,
            },
        }
