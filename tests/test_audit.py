"""Durable, secret-free action history across services, requests, and CLI."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from piply.api.app import create_app
from piply.cli.main import app as cli
from piply.core.audit import AUDIT_ACTOR, audit_actor, parse_audit_since
from piply.core.auth import AuthError
from piply.core.service import PipelineService
from piply.core.store import RunStore
from piply.engine.base import BaseEngine

CONFIG = """version: "1"
title: Audit test
workspace: .
notifications:
  teams:
    ops: {type: channel, webhook: "https://example.invalid/private-webhook?sig=webhook-secret"}
pipelines:
  flow:
    schedule: {every: 5m}
    env: {PASSWORD: environment-secret}
    tasks:
      first: {type: cli, command: echo first}
      second: {type: cli, command: echo second, depends_on: [first]}
  other:
    tasks:
      t: {type: cli, command: echo other}
"""


class HoldingEngine(BaseEngine):
    def dispatch(self, pipeline, run, store, **kwargs):
        pass

    def cancel(self, run_id):
        return False


@pytest.fixture
def config(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PIPLY_SCHEDULER_ENABLED", "false")
    path = tmp_path / "piply.yaml"
    path.write_text(CONFIG, encoding="utf-8")
    return path


@pytest.fixture
def service(config: Path) -> PipelineService:
    return PipelineService(config_path=config, engine=HoldingEngine())


@pytest.fixture
def client(config: Path):
    with TestClient(create_app(str(config))) as current:
        current.app.state.service.engine = HoldingEngine()
        current.app.state.service.create_user("root", "root-password", role="admin")
        current.app.state.service.create_user("runner", "runner-password", permissions={"flow": "view,run,edit"})
        yield current


def test_service_run_actions_survive_deletion_and_restart(service):
    with audit_actor("operator"):
        run = service.trigger_pipeline("flow", initial_context={"params": {"token": "parameter-secret"}})
        service.set_run_paused(run.run_id, True)
        service.set_run_paused(run.run_id, False)
        service.cancel_run(run.run_id)
        retry = service.retry_run(run.run_id, mode="resume")
        service.cancel_run(retry.run_id)
        service.delete_run(run.run_id)
    reopened = RunStore(service.database_path)
    events = reopened.list_audit_events(target_id=run.run_id)
    assert {item["action"] for item in events} == {"run.create", "run.pause", "run.resume", "run.cancel", "run.delete"}
    assert {item["actor"] for item in events} == {"operator"}
    retry_event = reopened.list_audit_events(action="run.retry")[0]
    assert retry_event["target_id"] == retry.run_id
    assert retry_event["detail"]["retry_of"] == run.run_id
    assert retry.actor == "operator"
    assert reopened.get_run(run.run_id) is None
    serialized = json.dumps(reopened.list_audit_events(), default=str)
    assert "parameter-secret" not in serialized and "environment-secret" not in serialized


def test_pipeline_pause_resume_and_delete_are_audited(service):
    with audit_actor("operator"):
        service.set_pipeline_paused("other", True)
        service.set_pipeline_paused("other", False)
        service.delete_pipeline("other")
    events = service.store.list_audit_events(target_type="pipeline", target_id="other")
    assert [item["action"] for item in events] == ["pipeline.delete", "pipeline.resume", "pipeline.pause"]
    assert {item["actor"] for item in events} == {"operator"}


def test_task_launch_and_backfill_request_are_audited(service):
    with audit_actor("operator"):
        run = service.trigger_task("flow", "second", command_overrides={"first": "echo command-secret"})
        service.store.finish_run(run.run_id, status="success")
        service.backfill_run(run.run_id)
        start = datetime.now(timezone.utc)
        service.backfill_schedule("flow", start=start, end=start + timedelta(minutes=16))
    assert service.store.list_audit_events(target_id=run.run_id)[0]["detail"]["task_id"] == "second"
    assert service.store.list_audit_events(action="schedule.backfill")[0]["detail"]["slots_queued"] > 0
    assert "command-secret" not in json.dumps(service.store.list_audit_events(), default=str)


@pytest.mark.parametrize(
    "trigger,expected",
    [
        ("schedule", "system:scheduler"),
        ("sensor", "system:sensor"),
        ("pipeline", "system:pipeline"),
    ],
)
def test_queue_runs_use_system_actor_even_when_drained_by_a_user(service, trigger, expected):
    service.enqueue_pipeline_trigger("flow", trigger=trigger)
    with audit_actor("operator"):
        ids = service.drain_trigger_queue()
    assert len(ids) == 1
    entry = service.store.list_audit_events(target_id=ids[0])[0]
    assert entry["actor"] == expected
    assert service.store.get_run(ids[0]).actor == expected


def test_sensor_task_and_automatic_retry_have_system_actors(service):
    service.enqueue_pipeline_trigger("flow", trigger="sensor", payload={"task_id": "second"})
    run_id = service.drain_trigger_queue()[0]
    assert service.store.list_audit_events(target_id=run_id)[0]["actor"] == "system:sensor"
    service.store.finish_run(run_id, status="failed")
    service.enqueue_pipeline_trigger("flow", trigger="retry", payload={"retry_of": run_id, "mode": "startover"})
    with audit_actor("operator"):
        retried_id = service.drain_trigger_queue()[0]
    assert service.store.list_audit_events(target_id=retried_id)[0]["actor"] == "system:retry"


def test_invalid_actions_do_not_claim_success_in_history(service):
    before = len(service.store.list_audit_events())
    with pytest.raises(KeyError):
        service.trigger_pipeline("missing")
    with pytest.raises(AuthError):
        service.grant_permission("missing", "flow", "view")
    with pytest.raises(KeyError):
        service.cancel_run("missing")
    assert len(service.store.list_audit_events()) == before


def test_schedule_slot_deduplication_does_not_duplicate_audit_history(service):
    moment = datetime.now(timezone.utc)
    first = service.trigger_pipeline("flow", trigger="schedule", scheduled_for=moment)
    second = service.trigger_pipeline("flow", trigger="schedule", scheduled_for=moment)
    assert first.run_id == second.run_id
    assert len(service.store.list_audit_events(target_id=first.run_id)) == 1


def test_user_changes_and_permissions_never_store_credentials(service):
    with audit_actor("operator"):
        service.create_user("root", "account-secret", role="admin")
        service.create_user("member", "account-secret")
        service.grant_permission("member", "flow", "run")
        service.revoke_permission("member", "flow")
        service.update_user("member", password="replacement-secret", is_active=False)
        service.delete_user("member")
    entries = service.store.list_audit_events(target_type="user", target_id="member")
    assert {item["action"] for item in entries} == {
        "user.create",
        "permission.grant",
        "permission.revoke",
        "user.update",
        "user.delete",
    }
    assert next(item for item in entries if item["action"] == "user.update")["detail"]["password_changed"]
    serialized = json.dumps(entries, default=str)
    for secret in ("account-secret", "replacement-secret", "pbkdf2$"):
        assert secret not in serialized


def test_smtp_and_notification_tests_record_only_safe_summaries(service, monkeypatch):
    with audit_actor("operator"):
        service.save_smtp_settings({"host": "mail.example.invalid", "password": "smtp-secret"})
        monkeypatch.setattr("piply.core.service.send_message", lambda *args: None)
        service.send_test_email("recipient@example.invalid")
        monkeypatch.setattr("piply.core.service.send_alert", lambda *args: [("ops", True, "webhook-secret")])
        service.send_test_notification("ops")
        monkeypatch.setattr("piply.core.service.send_alert", lambda *args: [("ops", False, "webhook-secret")])
        with pytest.raises(RuntimeError):
            service.send_test_notification("ops")

        def failed_mail(*args):
            raise RuntimeError("smtp-secret")

        monkeypatch.setattr("piply.core.service.send_message", failed_mail)
        with pytest.raises(RuntimeError):
            service.send_test_email("recipient@example.invalid")
    entries = service.store.list_audit_events()
    assert {item["detail"].get("outcome") for item in entries if item["action"] == "notification.test"} == {
        "sent",
        "failed",
    }
    assert {item["detail"].get("outcome") for item in entries if item["action"] == "smtp.test"} == {"sent", "failed"}
    serialized = json.dumps(entries, default=str)
    for secret in ("smtp-secret", "webhook-secret", "recipient@example.invalid", "mail.example.invalid"):
        assert secret not in serialized


def test_audit_api_and_page_are_admin_only(client):
    for path in ("/api/audit", "/audit"):
        denied = client.get(path, auth=("runner", "runner-password"))
        assert denied.status_code == 403
        assert "root-password" not in denied.text
        assert client.get(path, auth=("root", "root-password")).status_code == 200
    assert client.get("/api/audit").status_code == 401
    assert client.get("/audit", follow_redirects=False).status_code == 303
    assert 'href="/audit"' not in client.get("/settings", auth=("runner", "runner-password")).text
    assert 'href="/audit"' in client.get("/settings", auth=("root", "root-password")).text


def test_requests_attribute_actions_to_the_current_account(client):
    response = client.post("/api/pipelines/flow/run", auth=("runner", "runner-password"))
    assert response.status_code == 200
    run_id = response.json()["id"]
    assert client.post(f"/api/runs/{run_id}/pause", auth=("runner", "runner-password")).status_code == 200
    assert client.post(f"/api/runs/{run_id}/resume", auth=("runner", "runner-password")).status_code == 200
    assert client.post(f"/api/runs/{run_id}/cancel", auth=("runner", "runner-password")).status_code == 200
    response = client.post(f"/api/runs/{run_id}/retry", json={"mode": "startover"}, auth=("runner", "runner-password"))
    assert response.status_code == 200
    entries = client.get("/api/audit", params={"actor": "runner"}, auth=("root", "root-password")).json()
    assert {entry["action"] for entry in entries} == {
        "run.create",
        "run.pause",
        "run.resume",
        "run.cancel",
        "run.retry",
    }
    assert {entry["actor"] for entry in entries} == {"runner"}
    assert AUDIT_ACTOR.get() is None


def test_concurrent_request_actors_do_not_leak(client):
    service = client.app.state.service
    service.create_user("second", "second-password", permissions={"flow": "edit"})

    def pause_as(username):
        return client.post("/api/pipelines/flow/pause", auth=(username, f"{username}-password"))

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(pause_as, ["runner", "second"]))
    assert [item.status_code for item in results] == [200, 200]
    assert {item["actor"] for item in service.store.list_audit_events(action="pipeline.pause")} == {"runner", "second"}


def test_admin_account_permission_and_smtp_changes_keep_request_actor(client):
    auth = ("root", "root-password")
    assert (
        client.post("/api/users", json={"username": "member", "password": "new-account-secret"}, auth=auth).status_code
        == 200
    )
    assert (
        client.post(
            "/api/users/member/permissions", json={"pipeline_id": "flow", "actions": ["view", "run"]}, auth=auth
        ).status_code
        == 200
    )
    assert (
        client.patch(
            "/api/users/member", json={"password": "replacement-secret", "is_active": False}, auth=auth
        ).status_code
        == 200
    )
    assert (
        client.put(
            "/api/settings/smtp", json={"host": "mail.example.invalid", "password": "smtp-secret"}, auth=auth
        ).status_code
        == 200
    )
    entries = client.get("/api/audit", params={"actor": "root"}, auth=auth).json()
    assert {entry["action"] for entry in entries} >= {"user.create", "user.update", "permission.grant", "smtp.update"}
    for secret in ("new-account-secret", "replacement-secret", "smtp-secret"):
        assert secret not in json.dumps(entries)


def test_audit_filters_pagination_validation_and_html_escaping(client):
    store = client.app.state.service.store
    for index in range(4):
        store.append_audit_event(actor="operator", action="example", target_type="run", target_id=str(index))
    auth = ("root", "root-password")
    filters = {"actor": "operator", "action": "example", "target_type": "run", "since": "7d", "limit": 2}
    first = client.get("/api/audit", params=filters, auth=auth).json()
    store.append_audit_event(actor="operator", action="example", target_type="run", target_id="new")
    second = client.get("/api/audit", params={**filters, "before_id": first[-1]["id"]}, auth=auth).json()
    assert [item["target_id"] for item in first + second] == ["3", "2", "1", "0"]
    assert client.get("/api/audit", params={"target_id": "2"}, auth=auth).json()[0]["target_id"] == "2"
    assert client.get("/api/audit", params={"actor": "' OR 1=1 --"}, auth=auth).json() == []
    for path in ("/api/audit", "/audit"):
        assert client.get(path, params={"since": "nonsense"}, auth=auth).status_code == 400
        assert client.get(path, params={"limit": 501}, auth=auth).status_code == 422
        assert client.get(path, params={"before_id": 0}, auth=auth).status_code == 422
    invalid_page = client.get("/audit", params={"since": "bad"}, auth=auth)
    assert 'role="alert"' in invalid_page.text and 'action="/audit"' in invalid_page.text
    hostile = "<script>alert('unsafe')</script>"
    store.append_audit_event(
        actor=hostile, action="example", target_type="run", target_id=hostile, detail={"label": hostile}
    )
    page = client.get("/audit", auth=auth).text
    assert hostile not in page and "&lt;script&gt;" in page
    assert client.get("/audit", params={"actor": "missing"}, auth=auth).status_code == 200
    page = client.get("/audit", params={"limit": 2, "actor": "operator"}, auth=auth).text
    assert '<option value="2" selected>' in page
    assert "before_id=" in page and "actor=operator" in page


def test_cli_audit_and_command_actor(config: Path):
    runner = CliRunner()
    response = runner.invoke(cli, ["pause", "flow", "--config", str(config)])
    assert response.exit_code == 0, response.output
    assert AUDIT_ACTOR.get() is None
    response = runner.invoke(cli, ["audit", "--config", str(config), "--since", "7d", "--actor", "cli", "--json"])
    assert response.exit_code == 0, response.output
    assert json.loads(response.output)[0]["action"] == "pipeline.pause"
    assert runner.invoke(cli, ["audit", "--config", str(config), "--since", "bad"]).exit_code != 0


def test_copy_and_retention_preserve_audit_events(service, tmp_path):
    service.record_audit("example", "run", "historic", actor="operator")
    copied_store = RunStore(tmp_path / "copy.db")
    counts = service.store.copy_into(copied_store)
    assert counts["audit_events"] == 1
    copied_store.append_audit_event(actor="new", action="example", target_type="run", target_id="new")
    assert [entry["id"] for entry in copied_store.list_audit_events()] == [2, 1]
    later = datetime.now(timezone.utc) + timedelta(days=40)
    args = {
        "run_retention_days": 30,
        "log_retention_days": 0,
        "max_runs_per_pipeline": 0,
        "vacuum": False,
        "now": later,
    }
    assert copied_store.prune(**args, dry_run=True)["audit_deleted"] == 2
    assert len(copied_store.list_audit_events()) == 2
    assert copied_store.prune(**{**args, "run_retention_days": 0})["audit_deleted"] == 0
    assert copied_store.prune(**args)["audit_deleted"] == 2
    assert copied_store.list_audit_events() == []


def test_database_switch_records_both_stores_without_connection_values(client, monkeypatch):
    service = client.app.state.service
    old_store = service.store
    new_store = RunStore(service.config_path.parent / "switched.db")
    monkeypatch.setattr("piply.api.routes.maintenance.database_is_env_managed", lambda: False)
    monkeypatch.setattr(
        "piply.api.routes.maintenance.validate_database_choice",
        lambda *args, **kwargs: ("postgresql://db-user:dsn-secret@db/runtime", "postgresql://db-user:***@db/runtime"),
    )
    monkeypatch.setattr("piply.api.routes.maintenance.persist_database_setting", lambda *args: None)

    # Keep an independent old service, as the real switch builds a new service.
    def real_switch(request):
        request.app.state.service = PipelineService(
            config_path=service.config_path, database_path=new_store.database_path
        )

    monkeypatch.setattr("piply.api.routes.maintenance.apply_database_setting", real_switch)
    response = client.put(
        "/api/settings/database",
        json={"backend": "postgres", "dsn": "postgresql://db-user:dsn-secret@db/runtime"},
        auth=("root", "root-password"),
    )
    assert response.status_code == 200, response.text
    for store in (old_store, new_store):
        entries = store.list_audit_events(action="database.switch")
        assert len(entries) == 1 and entries[0]["actor"] == "root"
        assert "dsn-secret" not in json.dumps(entries, default=str)


def test_since_normalizes_time_and_rejects_bad_intervals():
    now = datetime.now(timezone.utc)
    assert parse_audit_since("7d", now=now) == now - timedelta(days=7)
    assert parse_audit_since("2026-10-06T12:00:00+05:30") == datetime(2026, 10, 6, 6, 30, tzinfo=timezone.utc)
    assert parse_audit_since("2026-10-06") == datetime(2026, 10, 6, tzinfo=timezone.utc)
    for invalid in ("0d", "-1d", "9999999999999999999999999d", "bad"):
        with pytest.raises(ValueError):
            parse_audit_since(invalid, now=now)
