"""Run gates must hold new work without interrupting active work."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from piply.api.app import create_app
from piply.cli.main import app
from piply.core.auth import issue_session, read_session
from piply.core.service import PipelineService
from piply.core.store import RunStore
from piply.engine.task_runner import TaskExecutionResult, TaskRunner


def _project(tmp_path: Path, *, parallel: bool = False) -> Path:
    path = tmp_path / "piply.yaml"
    path.write_text(
        f"""version: '1'
workspace: .
pipelines:
  flow:
    max_parallel_tasks: {2 if parallel else 1}
    tasks:
      first: {{type: cli, command: echo first}}
      second: {{type: cli, command: echo second}}
      last: {{type: cli, command: echo last, depends_on: [first, second]}}
""",
        encoding="utf-8",
    )
    return path


def _wait_until(predicate, seconds: float = 5) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("Runtime did not reach the expected state")


@pytest.mark.parametrize("parallel", [False, True])
def test_pause_finishes_active_tasks_and_holds_pending_work(tmp_path: Path, monkeypatch, parallel: bool) -> None:
    service = PipelineService(config_path=_project(tmp_path, parallel=parallel))
    started = {name: threading.Event() for name in ("first", "second", "last")}
    release = threading.Event()

    def run_task(self, task):
        started[task.task_id].set()
        if task.task_id != "last":
            # Only the test releases work; slow startup must not fail a task.
            # The finally block always releases workers, including on failure.
            release.wait()
        return TaskExecutionResult(status="success", exit_code=0)

    monkeypatch.setattr(TaskRunner, "run", run_task)
    run = service.trigger_pipeline("flow")
    try:
        assert started["first"].wait(5)
        if parallel:
            assert started["second"].wait(5)
        assert service.set_run_paused(run.run_id, True).paused
        # A different store instance (CLI or another request) sees the same gate.
        assert RunStore(service.database_path).get_run(run.run_id).paused
        release.set()
        _wait_until(lambda: service.store.list_task_runs(run.run_id)[0].status == "success")
        assert not started["last"].wait(0.25)
        tasks = {task.task_id: task for task in service.store.list_task_runs(run.run_id)}
        assert tasks["last"].status == "queued"
        if not parallel:
            assert not started["second"].is_set()
        assert service.store.get_run(run.run_id).status == "running"
        assert not service.set_run_paused(run.run_id, False).paused
        _wait_until(lambda: service.store.get_run(run.run_id).status == "success")
        assert started["last"].is_set()
        assert {task.status for task in service.store.list_task_runs(run.run_id)} == {"success"}
        assert len(service.list_runs()) == 1
    finally:
        release.set()
        service.shutdown_runtime()


def test_resume_between_parallel_pause_checks_keeps_pending_tasks(tmp_path: Path, monkeypatch) -> None:
    service = PipelineService(config_path=_project(tmp_path, parallel=True))
    checks = 0
    invoked = []

    def paused_once(store, run_id):
        nonlocal checks
        checks += 1
        # The downstream admission sees pause; any subsequent read sees resume.
        return checks == 3

    def run_task(self, task):
        invoked.append(task.task_id)
        return TaskExecutionResult(status="success", exit_code=0)

    monkeypatch.setattr(service.engine, "_run_paused", paused_once)
    monkeypatch.setattr(TaskRunner, "run", run_task)
    try:
        run = service.trigger_pipeline("flow", wait=True)
        assert run.status == "success"
        assert set(invoked) == {"first", "second", "last"}
        assert {task.status for task in service.store.list_task_runs(run.run_id)} == {"success"}
    finally:
        service.shutdown_runtime()


@pytest.mark.parametrize("parallel", [False, True])
def test_cancel_a_paused_run(tmp_path: Path, monkeypatch, parallel: bool) -> None:
    service = PipelineService(config_path=_project(tmp_path, parallel=parallel))
    run = service.store.create_run(service.get_pipeline("flow"), trigger="manual")
    service.set_run_paused(run.run_id, True)
    invoked = threading.Event()

    def run_task(self, task):
        invoked.set()
        return TaskExecutionResult(status="success", exit_code=0)

    monkeypatch.setattr(TaskRunner, "run", run_task)
    service.engine.dispatch(service.get_pipeline("flow"), run, service.store)
    try:
        _wait_until(lambda: service.store.get_run(run.run_id).status == "running")
        service.cancel_run(run.run_id)
        _wait_until(lambda: service.store.get_run(run.run_id).status == "cancelled")
        assert not invoked.is_set()
        assert {task.status for task in service.store.list_task_runs(run.run_id)} == {"cancelled"}
    finally:
        service.shutdown_runtime()


def test_pause_and_task_claim_share_an_atomic_gate(tmp_path: Path) -> None:
    service = PipelineService(config_path=_project(tmp_path))
    run = service.store.create_run(service.get_pipeline("flow"), trigger="manual")
    service.set_run_paused(run.run_id, True)
    assert not service.store.mark_task_running(run.run_id, "first")
    service.set_run_paused(run.run_id, False)
    assert service.store.mark_task_running(run.run_id, "first")
    assert not service.store.mark_task_running(run.run_id, "first")
    service.store.finish_run(run.run_id, status="success")
    with pytest.raises(ValueError, match="Only queued or running"):
        service.set_run_paused(run.run_id, True)
    with pytest.raises(KeyError):
        service.set_run_paused("missing", True)


@pytest.mark.parametrize("parallel", [False, True])
def test_persisted_cancellation_releases_a_paused_worker(tmp_path: Path, monkeypatch, parallel: bool) -> None:
    service = PipelineService(config_path=_project(tmp_path, parallel=parallel))
    run = service.store.create_run(service.get_pipeline("flow"), trigger="manual")
    service.set_run_paused(run.run_id, True)
    invoked = threading.Event()

    def run_task(self, task):
        invoked.set()
        return TaskExecutionResult(status="success", exit_code=0)

    monkeypatch.setattr(TaskRunner, "run", run_task)
    service.engine.dispatch(service.get_pipeline("flow"), run, service.store)
    worker = service.engine._threads[run.run_id]
    try:
        _wait_until(lambda: service.store.get_run(run.run_id).status == "running")
        RunStore(service.database_path).cancel_run(run.run_id)
        worker.join(5)
        assert not worker.is_alive(), "persisted cancellation must release the pause wait"
        assert not invoked.is_set()
        assert service.store.get_run(run.run_id).status == "cancelled"
    finally:
        service.engine.cancel(run.run_id)
        worker.join(5)


def test_resume_between_claim_and_pause_read_does_not_cancel_task(tmp_path: Path, monkeypatch) -> None:
    service = PipelineService(config_path=_project(tmp_path))
    original = service.store.mark_task_running
    rejected_once = False
    invoked = []

    def claim(run_id, task_id):
        nonlocal rejected_once
        if not rejected_once:
            rejected_once = True
            service.store.set_run_paused(run_id, True)
            service.store.set_run_paused(run_id, False)
            return False
        return original(run_id, task_id)

    def run_task(self, task):
        invoked.append(task.task_id)
        return TaskExecutionResult(status="success", exit_code=0)

    monkeypatch.setattr(service.store, "mark_task_running", claim)
    monkeypatch.setattr(TaskRunner, "run", run_task)
    run = service.trigger_pipeline("flow", wait=True)
    assert run.status == "success"
    assert invoked == ["first", "second", "last"]


@pytest.mark.parametrize("parallel", [False, True])
def test_paused_run_keeps_concurrency_slot_and_pipeline_timeout(tmp_path: Path, parallel: bool) -> None:
    service = PipelineService(config_path=_project(tmp_path, parallel=parallel))
    pipeline = service.get_pipeline("flow")
    pipeline.timeout_seconds = 3
    run = service.store.create_run(pipeline, trigger="manual")
    service.set_run_paused(run.run_id, True)
    service.engine.dispatch(pipeline, run, service.store)
    try:
        _wait_until(lambda: service.store.get_run(run.run_id).status == "running")
        assert service.store.count_running_runs("flow") == 1
        service.enqueue_pipeline_trigger("flow", trigger="manual")
        assert service.drain_trigger_queue() == []
        assert service.store.pending_queue_item("flow").error == "a run is already in progress"
        _wait_until(lambda: service.store.get_run(run.run_id).status == "timed_out")
        assert {task.status for task in service.store.list_task_runs(run.run_id)} == {"timed_out"}
    finally:
        service.shutdown_runtime()


def test_run_pause_api_permissions_and_live_ui(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PIPLY_SCHEDULER_ENABLED", "false")
    with TestClient(create_app(str(_project(tmp_path)))) as client:
        service = client.app.state.service
        service.create_user("root", "password-root", role="admin")
        service.create_user("reader", "password-reader")
        service.grant_permission("reader", "flow", ["view"])
        service.create_user("operator", "password-operator")
        service.grant_permission("operator", "flow", ["view", "run"])
        run = service.store.create_run(service.get_pipeline("flow"), trigger="manual")
        url = f"/api/runs/{run.run_id}"
        assert client.post(url + "/pause", auth=("reader", "password-reader")).status_code == 403
        paused = client.post(url + "/pause", auth=("operator", "password-operator"))
        assert paused.status_code == 200
        assert paused.json()["paused"] is True
        page = client.get(f"/runs/{run.run_id}", auth=("reader", "password-reader"))
        assert page.status_code == 200
        assert 'aria-label="Resume run"' in page.text
        assert client.get("/runs", auth=("reader", "password-reader")).status_code == 200
        assert client.post(url + "/resume", auth=("operator", "password-operator")).json()["paused"] is False
        assert client.post("/api/runs/missing/pause", auth=("root", "password-root")).status_code == 404
        service.store.finish_run(run.run_id, status="success")
        assert client.post(url + "/pause", auth=("root", "password-root")).status_code == 400


def test_cli_run_pause_controls_the_persisted_gate(tmp_path: Path) -> None:
    config = _project(tmp_path)
    service = PipelineService(config_path=config)
    run = service.store.create_run(service.get_pipeline("flow"), trigger="manual")
    runner = CliRunner()
    assert runner.invoke(app, ["pause-run", run.run_id, "--config", str(config)]).exit_code == 0
    assert service.store.get_run(run.run_id).paused
    assert runner.invoke(app, ["resume-run", run.run_id, "--config", str(config)]).exit_code == 0
    assert not service.store.get_run(run.run_id).paused
    service.store.finish_run(run.run_id, status="success")
    assert runner.invoke(app, ["pause-run", run.run_id, "--config", str(config)]).exit_code == 1


@pytest.mark.parametrize("status", ["timed_out", "cancelled"])
def test_resume_retry_includes_stopped_tasks(tmp_path: Path, monkeypatch, status: str) -> None:
    service = PipelineService(config_path=_project(tmp_path))
    run = service.store.create_run(service.get_pipeline("flow"), trigger="manual")
    for task_id in ("first", "second", "last"):
        service.store.finish_task_run(run.run_id, task_id, status="success" if task_id == "first" else status)
    service.store.finish_run(run.run_id, status=status)
    invoked = []

    def run_task(self, task):
        invoked.append(task.task_id)
        return TaskExecutionResult(status="success", exit_code=0)

    monkeypatch.setattr(TaskRunner, "run", run_task)
    retry = service.retry_run(run.run_id, mode="resume", wait=True)
    assert retry.status == "success"
    assert invoked == ["second", "last"]


def test_password_change_revokes_old_sessions(tmp_path: Path) -> None:
    service = PipelineService(config_path=_project(tmp_path))
    service.create_user("alice", "old-password")
    token = issue_session(service.store, "alice")
    assert read_session(service.store, token) == "alice"
    service.update_user("alice", role="admin")
    assert read_session(service.store, token) == "alice"
    service.update_user("alice", password="new-password")
    assert read_session(service.store, token) is None
    assert read_session(service.store, issue_session(service.store, "alice")) == "alice"
    assert read_session(service.store, issue_session(service.store, "alice", ttl_seconds=-1)) is None
    assert read_session(service.store, "ascii.non-ascii-\u00e9") is None


SCHEDULE_CONFIG = """version: '1'
workspace: .
pipelines:
  near:
    tags: [daily]
    schedule: {cron: '10 * * * *'}
    tasks: {t: {type: cli, command: echo near}}
  boundary:
    schedule: {cron: '15 * * * *'}
    tasks: {t: {type: cli, command: echo boundary}}
  far:
    schedule: {cron: '16 * * * *'}
    tasks: {t: {type: cli, command: echo far}}
  held:
    schedule: {cron: '5 * * * *'}
    tasks: {t: {type: cli, command: echo held}}
  disabled:
    enabled: false
    schedule: {cron: '8 * * * *'}
    tasks: {t: {type: cli, command: echo disabled}}
  manual:
    tasks: {t: {type: cli, command: echo manual}}
"""


def test_upcoming_window_boundaries_and_states(tmp_path: Path) -> None:
    config = tmp_path / "piply.yaml"
    config.write_text(SCHEDULE_CONFIG, encoding="utf-8")
    service = PipelineService(config_path=config)
    service.set_pipeline_paused("held", True)
    now = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
    upcoming = service.list_upcoming_pipelines(now=now)
    assert [item["pipeline_id"] for item in upcoming] == ["held", "disabled", "near", "boundary"]
    assert [item["state"] for item in upcoming] == ["paused", "disabled", "scheduled", "scheduled"]
    assert [item["pipeline_id"] for item in service.list_upcoming_pipelines(now=now, pipeline_ids={"near"})] == ["near"]
    assert service.list_upcoming_pipelines(now=now, pipeline_ids=set()) == []


def test_upcoming_ui_respects_visibility_and_filters(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "piply.yaml"
    config.write_text(SCHEDULE_CONFIG, encoding="utf-8")
    monkeypatch.setenv("PIPLY_SCHEDULER_ENABLED", "false")
    original = PipelineService.list_upcoming_pipelines
    monkeypatch.setattr(
        PipelineService,
        "list_upcoming_pipelines",
        lambda self, **kwargs: original(self, now=datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc), **kwargs),
    )
    with TestClient(create_app(str(config))) as client:
        service = client.app.state.service
        service.create_user("root", "root-password", role="admin")
        service.create_user("reader", "reader-password")
        service.grant_permission("reader", "near", ["view"])
        auth = ("reader", "reader-password")
        page = client.get("/runs", auth=auth)
        assert page.status_code == 200
        assert "Scheduled in the next 15 minutes" in page.text
        assert 'href="/pipelines/near"' in page.text
        assert 'href="/pipelines/held"' not in page.text
        assert "No scheduled pipelines" in client.get("/runs?tag=missing", auth=auth).text
        assert "No scheduled pipelines" in client.get("/runs?pipeline_id=held", auth=auth).text
        admin_page = client.get("/runs?pipeline_id=held", auth=("root", "root-password"))
        assert "status-badge paused" not in admin_page.text
        service.set_pipeline_paused("held", True)
        assert "status-badge paused" in client.get("/runs?pipeline_id=held", auth=("root", "root-password")).text
