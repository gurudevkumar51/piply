from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from piply.core.models import PipelineDefinition, RunRecord
from piply.core.scheduler import PipelineScheduler
from piply.core.service import PipelineService
from piply.core.store import RunStore
from piply.engine.base import BaseEngine, CompletionCallback, LogCallback


class ImmediateEngine(BaseEngine):
    """ImmediateEngine completes runs synchronously for scheduler-focused tests."""

    def dispatch(
        self,
        pipeline: PipelineDefinition,
        run: RunRecord,
        store: RunStore,
        *,
        wait: bool = False,
        on_log: LogCallback | None = None,
        on_success: CompletionCallback | None = None,
        on_failure: CompletionCallback | None = None,
        initial_task_statuses: dict[str, str] | None = None,
        retry_source_run_id: str | None = None,
    ) -> None:
        del wait, on_log, on_failure, retry_source_run_id
        store.mark_running(run.run_id)
        for task in pipeline.tasks.values():
            if initial_task_statuses and initial_task_statuses.get(task.task_id) == "success":
                store.mark_task_reused(run.run_id, task.task_id, "seed")
                continue
            store.mark_task_running(run.run_id, task.task_id)
            store.append_log(run.run_id, f"ImmediateEngine executed {task.task_id}", task_id=task.task_id)
            store.finish_task_run(run.run_id, task.task_id, status="success", exit_code=0)
        store.finish_run(run.run_id, status="success", exit_code=0)
        final_run = store.get_run(run.run_id)
        if final_run is not None and on_success is not None:
            on_success(pipeline, final_run)

    def cancel(self, run_id: str) -> bool:
        del run_id
        return False


def test_scheduler_backfills_due_slots_through_internal_queue(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "job.py").write_text("print('job')", encoding="utf-8")

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Queue Scheduler Test",
                "workspace: workspace",
                "pipelines:",
                "  scheduled_flow:",
                "    schedule:",
                "      interval_seconds: 60",
                "    tasks:",
                "      main:",
                "        type: python",
                "        path: job.py",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(
        config_path=config_path,
        database_path=tmp_path / "runs.db",
        engine=ImmediateEngine(),
    )
    scheduler = PipelineScheduler(service)
    first_slot = datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc)

    service.trigger_pipeline("scheduled_flow", trigger="schedule", scheduled_for=first_slot, wait=False)
    scheduler.tick(now=first_slot + timedelta(minutes=3))

    slots = sorted(
        run.scheduled_for
        for run in service.list_runs(pipeline_id="scheduled_flow", limit=10)
        if run.scheduled_for is not None
    )
    assert slots == [
        first_slot,
        first_slot + timedelta(minutes=1),
        first_slot + timedelta(minutes=2),
        first_slot + timedelta(minutes=3),
    ]
    assert service.store.count_queue() == 0


def test_file_sensor_enqueues_pipeline_when_new_file_arrives(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    inbox = workspace / "inbox"
    workspace.mkdir()
    inbox.mkdir()
    (workspace / "job.py").write_text("print('job')", encoding="utf-8")

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: File Sensor Test",
                "workspace: workspace",
                "pipelines:",
                "  watch_flow:",
                "    sensors:",
                "      landing_files:",
                "        type: file_sensor",
                "        path: inbox",
                "        pattern: '*.csv'",
                "    tasks:",
                "      main:",
                "        type: python",
                "        path: job.py",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(
        config_path=config_path,
        database_path=tmp_path / "runs.db",
        engine=ImmediateEngine(),
    )
    scheduler = PipelineScheduler(service)
    scheduler.tick(now=datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc))

    (inbox / "batch-001.csv").write_text("id,value\n1,alpha\n", encoding="utf-8")
    scheduler.tick(now=datetime(2026, 4, 11, 10, 0, 10, tzinfo=timezone.utc))

    runs = service.list_runs(pipeline_id="watch_flow", limit=5)
    assert runs
    assert runs[0].trigger == "sensor"
    _, _, logs = service.get_run(runs[0].run_id)
    assert any("Detected new files" in line.message for line in logs)


def test_sql_sensor_enqueues_pipeline_when_new_rows_arrive(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    database_path = workspace / "sensor.db"
    (workspace / "job.py").write_text("print('job')", encoding="utf-8")

    connection = sqlite3.connect(database_path)
    connection.execute("CREATE TABLE inbound_events (id INTEGER PRIMARY KEY, payload TEXT)")
    connection.commit()
    connection.close()

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: SQL Sensor Test",
                "workspace: workspace",
                "pipelines:",
                "  sql_watch_flow:",
                "    sensors:",
                "      inbound_rows:",
                "        type: sql_sensor",
                "        database: sensor.db",
                "        table: inbound_events",
                "        cursor_column: id",
                "    tasks:",
                "      main:",
                "        type: python",
                "        path: job.py",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(
        config_path=config_path,
        database_path=tmp_path / "runs.db",
        engine=ImmediateEngine(),
    )
    scheduler = PipelineScheduler(service)
    scheduler.tick(now=datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc))

    connection = sqlite3.connect(database_path)
    connection.execute("INSERT INTO inbound_events (payload) VALUES ('alpha')")
    connection.commit()
    connection.close()

    scheduler.tick(now=datetime(2026, 4, 11, 10, 0, 10, tzinfo=timezone.utc))

    runs = service.list_runs(pipeline_id="sql_watch_flow", limit=5)
    assert runs
    assert runs[0].trigger == "sensor"
    _, _, logs = service.get_run(runs[0].run_id)
    assert any("Detected new rows in inbound_events" in line.message for line in logs)
