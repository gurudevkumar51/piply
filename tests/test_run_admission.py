"""Run limits reserve database capacity across every launch path and owner."""

from __future__ import annotations

import multiprocessing
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from piply.api.app import create_app
from piply.cli.main import app
from piply.core.dialects import PostgresDialect
from piply.core.service import PipelineService
from piply.core.store import RunConcurrencyLimitError, RunStore
from piply.engine.base import BaseEngine


class HoldingEngine(BaseEngine):
    def __init__(self):
        self.dispatched = []

    def dispatch(self, pipeline, run, store, **kwargs):
        self.dispatched.append(run.run_id)

    def cancel(self, run_id):
        return False


def _project(tmp_path: Path, limit: int = 1) -> Path:
    config = tmp_path / "piply.yaml"
    config.write_text(
        f"""version: '1'
workspace: .
pipelines:
  flow:
    max_concurrent_runs: {limit}
    tasks:
      first: {{type: cli, command: echo unused}}
      last: {{type: cli, command: echo unused, depends_on: [first]}}
  other:
    tasks:
      only: {{type: cli, command: echo unused}}
""",
        encoding="utf-8",
    )
    return config


@pytest.mark.parametrize("limit", [1, 2])
@pytest.mark.parametrize("path", ["manual", "task", "resume", "startover"])
def test_direct_launch_paths_share_capacity(tmp_path: Path, limit: int, path: str) -> None:
    service = PipelineService(config_path=_project(tmp_path, limit), engine=HoldingEngine())
    source = service.trigger_pipeline("flow", actor="operator")
    for task in service.store.list_task_runs(source.run_id):
        service.store.finish_task_run(source.run_id, task.task_id, status="failed")
    service.store.finish_run(source.run_id, status="failed")

    def launch():
        if path == "manual":
            return service.trigger_pipeline("flow", actor="operator")
        if path == "task":
            return service.trigger_task("flow", "last", actor="operator")
        return service.retry_run(source.run_id, mode=path, actor="operator")

    active = [launch() for _ in range(limit)]
    service.set_run_paused(active[0].run_id, True)
    before = len(service.store.list_audit_events(actor="operator"))
    with pytest.raises(RunConcurrencyLimitError, match=f"max_concurrent_runs is {limit}"):
        launch()
    assert service.store.count_running_runs("flow") == limit
    assert len(service.store.list_runs()) == limit + 1
    assert len(service.store.list_audit_events(actor="operator")) == before
    assert len(service.engine.dispatched) == limit + 1
    service.cancel_run(active[0].run_id)
    assert launch().status == "queued"
    assert service.store.count_running_runs("flow") == limit


@pytest.mark.parametrize("limit", [1, 2])
def test_simultaneous_services_reserve_exactly_the_configured_limit(tmp_path: Path, limit: int) -> None:
    config = _project(tmp_path, limit)
    services = [PipelineService(config_path=config, engine=HoldingEngine()) for _ in range(2)]
    barrier = threading.Barrier(6)

    def launch(index):
        barrier.wait(timeout=10)
        try:
            return services[index % 2].trigger_pipeline("flow").run_id
        except RunConcurrencyLimitError:
            return None

    with ThreadPoolExecutor(max_workers=6) as executor:
        results = list(executor.map(launch, range(6)))
    assert len([result for result in results if result is not None]) == limit
    assert services[0].store.count_running_runs("flow") == limit
    assert len(services[0].store.list_runs()) == limit


def _process_admission(database, pipeline, ready, start, results, release):
    store = RunStore(database)
    ready.put(True)
    if not start.wait(30):
        return
    try:
        result = store.create_run(pipeline, trigger="manual").run_id
        results.put(("accepted", result))
    except RunConcurrencyLimitError:
        results.put(("limited", None))
    except Exception as exc:
        results.put(("error", repr(exc)))
    release.wait(30)


@pytest.mark.parametrize("limit", [1, 2])
def test_run_admission_is_atomic_across_processes(tmp_path: Path, limit: int) -> None:
    service = PipelineService(config_path=_project(tmp_path, limit), engine=HoldingEngine())
    context = multiprocessing.get_context("spawn")
    ready, results = context.Queue(), context.Queue()
    start, release = context.Event(), context.Event()
    processes = [
        context.Process(
            target=_process_admission,
            args=(service.database_path, service.get_pipeline("flow"), ready, start, results, release),
        )
        for _ in range(4)
    ]
    try:
        for process in processes:
            process.start()
        for _ in processes:
            assert ready.get(timeout=30)
        start.set()
        outcomes = [results.get(timeout=30) for _ in processes]
        assert all(status in {"accepted", "limited"} for status, _ in outcomes), outcomes
        assert sum(status == "accepted" for status, _ in outcomes) == limit
        assert service.store.count_running_runs("flow") == limit
    finally:
        start.set()
        release.set()
        for process in processes:
            if process.pid is not None:
                process.join(timeout=10)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=10)
        ready.close()
        results.close()


@pytest.mark.parametrize("limit", [1, 2])
def test_racing_scheduled_slot_is_idempotent_even_at_capacity(tmp_path: Path, monkeypatch, limit: int) -> None:
    config = _project(tmp_path, limit)
    services = [PipelineService(config_path=config, engine=HoldingEngine()) for _ in range(2)]
    for service in services:
        monkeypatch.setattr(service.store, "has_run_for_slot", lambda *args: False)
    barrier = threading.Barrier(2)
    slot = datetime(2026, 10, 6, tzinfo=timezone.utc)

    def launch(service):
        barrier.wait(timeout=10)
        return service.trigger_pipeline("flow", trigger="schedule", scheduled_for=slot).run_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(launch, services))
    assert results[0] == results[1]
    assert len(services[0].store.list_runs()) == 1
    assert sum(len(service.engine.dispatched) for service in services) == 1
    assert len(services[0].store.list_audit_events(action="run.create")) == 1


@pytest.mark.parametrize("trigger", ["schedule", "sensor", "retry", "pipeline"])
def test_queue_uses_all_slots_and_retains_excess_work(tmp_path: Path, trigger: str) -> None:
    service = PipelineService(config_path=_project(tmp_path, 2), engine=HoldingEngine())
    source = service.trigger_pipeline("flow")
    service.store.finish_run(source.run_id, status="failed")
    now = datetime.now(timezone.utc)
    for index in range(3):
        payload = {"task_id": "last"} if trigger == "sensor" else {}
        if trigger == "retry":
            payload.update(retry_of=source.run_id, mode="startover")
        service.enqueue_pipeline_trigger(
            "flow", trigger=trigger, available_at=now, dedupe_key=str(index), payload=payload
        )
    first = service.drain_trigger_queue(now=now)
    assert len(first) == 2
    assert service.store.count_running_runs("flow") == 2
    pending = service.store.pending_queue_item("flow")
    assert pending.dedupe_key == "2" and pending.status == "queued"
    assert "max_concurrent_runs: 2" in pending.error
    service.store.finish_run(first[0], status="success")
    assert len(service.drain_trigger_queue(now=now)) == 1
    assert service.store.count_running_runs("flow") == 2
    assert service.store.count_queue("failed") == 0
    assert service.store.count_queue() == 0


def test_queue_capacity_race_releases_claim_and_preserves_payload(tmp_path: Path, monkeypatch) -> None:
    service = PipelineService(config_path=_project(tmp_path), engine=HoldingEngine())
    payload = {"context": {"source": "must survive"}, "variables": {"batch": "42"}}
    service.enqueue_pipeline_trigger("flow", trigger="pipeline", payload=payload, dedupe_key="held")
    original = service.store.claim_queue_item

    def claim(queue_id):
        claimed = original(queue_id)
        if claimed:
            service.trigger_pipeline("flow")
        return claimed

    monkeypatch.setattr(service.store, "claim_queue_item", claim)
    assert service.drain_trigger_queue() == []
    pending = service.store.pending_queue_item("flow")
    assert pending.payload == payload and pending.error == "a run is already in progress"
    assert service.store.count_queue("dispatching") == service.store.count_queue("failed") == 0
    monkeypatch.setattr(service.store, "claim_queue_item", original)
    service.cancel_run(service.store.list_runs()[0].run_id)
    assert len(service.drain_trigger_queue()) == 1


def test_queue_claims_cannot_overtake_an_earlier_trigger(tmp_path: Path) -> None:
    service = PipelineService(config_path=_project(tmp_path, 2), engine=HoldingEngine())
    now = datetime.now(timezone.utc)
    for key in ("first", "second"):
        service.enqueue_pipeline_trigger("flow", trigger="schedule", available_at=now, dedupe_key=key)
    first, second = service.store.list_due_queue(now=now)
    other = RunStore(service.database_path)
    assert not other.claim_queue_item(second.queue_id)
    assert service.store.claim_queue_item(first.queue_id)
    assert not other.claim_queue_item(second.queue_id)
    assert service.store.release_queue_claim(first.queue_id)
    assert not other.claim_queue_item(second.queue_id)
    assert service.store.claim_queue_item(first.queue_id)
    service.store.mark_queue_dispatched(first.queue_id, "already-dispatched")
    assert other.claim_queue_item(second.queue_id)
    assert not service.store.release_queue_claim(first.queue_id)


def test_simultaneous_dispatchers_preserve_capacity_and_queued_work(tmp_path: Path, monkeypatch) -> None:
    config = _project(tmp_path, 2)
    services = [PipelineService(config_path=config, engine=HoldingEngine()) for _ in range(2)]
    for index in range(3):
        services[0].enqueue_pipeline_trigger("flow", trigger="schedule", dedupe_key=str(index))
    barrier = threading.Barrier(2)
    for service in services:
        original = service.store.list_due_queue

        def due(*args, _original=original, **kwargs):
            items = _original(*args, **kwargs)
            barrier.wait(timeout=10)
            return items

        monkeypatch.setattr(service.store, "list_due_queue", due)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda service: service.drain_trigger_queue(), services))
    assert sum(map(len, results)) == 2
    assert services[0].store.count_running_runs("flow") == 2
    assert services[0].store.pending_queue_item("flow").dedupe_key == "2"
    assert services[0].store.count_queue("failed") == 0


def test_postgres_admission_preserves_overrides_and_locks_pipeline_row() -> None:
    calls = []

    class RecordingConnection:
        def execute(self, sql, parameters=()):
            calls.append((sql, parameters))
            return self

        def fetchone(self):
            return {"pipeline_id": "flow"}

    dialect = object.__new__(PostgresDialect)
    dialect.lock_run_admission(RecordingConnection(), "flow")
    assert calls[0] == ("SET TRANSACTION ISOLATION LEVEL READ COMMITTED", ())
    assert "ON CONFLICT (pipeline_id) DO NOTHING" in calls[1][0]
    assert calls[2][0].endswith("FOR UPDATE")
    assert all(parameters == ("flow",) for _, parameters in calls[1:])


def test_limits_are_per_pipeline_not_per_tenant_or_task(tmp_path: Path) -> None:
    service = PipelineService(config_path=_project(tmp_path), engine=HoldingEngine())
    service.trigger_pipeline("flow", tenant_id="one")
    with pytest.raises(RunConcurrencyLimitError):
        service.trigger_task("flow", "first", tenant_id="two")
    assert service.trigger_pipeline("other").status == "queued"
    assert service.store.count_running_runs() == 2


def test_failed_dispatch_releases_capacity(tmp_path: Path, monkeypatch) -> None:
    service = PipelineService(config_path=_project(tmp_path), engine=HoldingEngine())

    def fail(*args, **kwargs):
        raise RuntimeError("worker startup failed")

    with monkeypatch.context() as patch:
        patch.setattr(service.engine, "dispatch", fail)
        with pytest.raises(RuntimeError, match="worker startup failed"):
            service.trigger_pipeline("flow")
    run = service.store.list_runs()[0]
    assert run.status == "interrupted" and "worker startup failed" in run.error
    assert {task.status for task in service.store.list_task_runs(run.run_id)} == {"cancelled"}
    assert service.trigger_pipeline("flow").status == "queued"


def test_partial_insert_rolls_back_reservation_and_tasks(tmp_path: Path, monkeypatch) -> None:
    service = PipelineService(config_path=_project(tmp_path), engine=HoldingEngine())
    original = service.store._connect

    @contextmanager
    def broken():
        with original() as connection:

            class FailingConnection:
                def execute(self, sql, parameters=()):
                    if "INSERT INTO task_runs" in sql and parameters[1] == "last":
                        raise sqlite3.OperationalError("injected task insert failure")
                    return connection.execute(sql, parameters)

                def __getattr__(self, name):
                    return getattr(connection, name)

            yield FailingConnection()

    with monkeypatch.context() as patch:
        patch.setattr(service.store, "_connect", broken)
        with pytest.raises(sqlite3.OperationalError, match="injected task insert"):
            service.trigger_pipeline("flow")
    assert service.store.count_running_runs("flow") == 0
    assert service.store.list_runs() == []
    assert service.trigger_pipeline("flow").status == "queued"


@pytest.mark.parametrize(
    "path,payload",
    [
        ("/api/pipelines/flow/run", None),
        ("/api/pipelines/flow/tasks/last/run", None),
        ("/api/pipelines/other/chain/flow", None),
        ("/api/runs/{source}/retry", {"mode": "startover"}),
        ("/api/runs/{source}/tasks/last/retry", None),
        ("/api/runs/{source}/backfill", None),
    ],
)
def test_api_returns_conflict_without_creating_a_run(tmp_path: Path, monkeypatch, path: str, payload) -> None:
    monkeypatch.setenv("PIPLY_SCHEDULER_ENABLED", "false")
    with TestClient(create_app(str(_project(tmp_path)))) as client:
        service = client.app.state.service
        service.engine = HoldingEngine()
        source = service.trigger_pipeline("flow")
        service.store.finish_run(source.run_id, status="failed")
        service.trigger_pipeline("flow")
        response = client.post(path.format(source=source.run_id), json=payload)
        assert response.status_code == 409, response.text
        assert "max_concurrent_runs is 1" in response.json()["detail"]
        assert len(service.store.list_runs()) == 2


@pytest.mark.parametrize("args", [["run", "flow"], ["tasks", "run", "flow", "last"]])
def test_cli_reports_capacity_without_a_traceback(tmp_path: Path, args) -> None:
    config = _project(tmp_path)
    service = PipelineService(config_path=config, engine=HoldingEngine())
    service.trigger_pipeline("flow")
    result = CliRunner().invoke(app, args + ["--config", str(config), "--detach"])
    assert result.exit_code == 1, result.output
    assert "max_concurrent_runs is 1" in result.output
    assert "Traceback" not in result.output
    assert len(service.store.list_runs()) == 1


def test_admin_command_override_cannot_bypass_run_capacity(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PIPLY_SCHEDULER_ENABLED", "false")
    with TestClient(create_app(str(_project(tmp_path)))) as client:
        service = client.app.state.service
        service.engine = HoldingEngine()
        service.create_user("root", "admission-test-only", role="admin")
        service.trigger_pipeline("flow")
        response = client.post(
            "/api/pipelines/flow/run",
            json={"command_overrides": {"first": "echo administrator"}},
            auth=("root", "admission-test-only"),
        )
        assert response.status_code == 409
        assert "max_concurrent_runs is 1" in response.json()["detail"]
        assert len(service.store.list_runs()) == 1
