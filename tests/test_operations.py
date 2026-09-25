"""Queue visibility and actor attribution.

Two related complaints: a queued trigger that never fired gave no reason, and
the run page reported every not-yet-started downstream pipeline as "pending"
even when it was never going to start.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from piply.api.app import create_app
from piply.core.service import PipelineService

CONFIG = "\n".join(
    [
        'version: "1"',
        "title: Operations",
        "workspace: .",
        "pipelines:",
        "  upstream:",
        "    triggers_on_success: [paused_child, disabled_child, normal_child]",
        "    tasks:",
        "      t: {type: cli, command: echo up}",
        "  paused_child:",
        "    tasks:",
        "      t: {type: cli, command: echo a}",
        "  disabled_child:",
        "    enabled: false",
        "    tasks:",
        "      t: {type: cli, command: echo b}",
        "  normal_child:",
        "    tasks:",
        "      t: {type: cli, command: echo c}",
    ]
)


def _project(tmp_path: Path) -> Path:
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(CONFIG, encoding="utf-8")
    return config_path


@pytest.fixture()
def service(tmp_path: Path) -> PipelineService:
    return PipelineService(config_path=_project(tmp_path), database_path=tmp_path / "piply.db")


# --- Trigger queue -----------------------------------------------------------


def test_a_skipped_trigger_records_and_logs_its_reason(service: PipelineService, caplog) -> None:
    """ "Nothing happened" is not a diagnosis, so the reason is kept."""
    service.set_pipeline_paused("paused_child", True)
    service.enqueue_pipeline_trigger("paused_child", trigger="manual")

    with caplog.at_level(logging.INFO, logger="piply.scheduler"):
        assert service.drain_trigger_queue() == []
    assert "Skipping trigger for 'paused_child': pipeline is paused" in caplog.text

    queued = service.store.pending_queue_item("paused_child")
    assert queued is not None
    assert queued.status == "queued", "a skipped trigger stays queued and is retried"
    assert queued.error == "pipeline is paused"


def test_the_reason_is_logged_once_not_every_tick(service: PipelineService, caplog) -> None:
    """The scheduler re-evaluates every ten seconds; the log must not fill up."""
    service.set_pipeline_paused("paused_child", True)
    service.enqueue_pipeline_trigger("paused_child", trigger="manual")

    with caplog.at_level(logging.INFO, logger="piply.scheduler"):
        for _ in range(5):
            service.drain_trigger_queue()
    assert caplog.text.count("Skipping trigger for 'paused_child'") == 1


def test_a_disabled_pipeline_says_so(service: PipelineService) -> None:
    """Disabled and paused are different problems with different fixes."""
    service.enqueue_pipeline_trigger("disabled_child", trigger="manual")
    service.drain_trigger_queue()
    queued = service.store.pending_queue_item("disabled_child")
    assert queued is not None
    assert queued.error == "pipeline is disabled in the config"


def test_resuming_lets_the_held_trigger_through(service: PipelineService) -> None:
    """The point of keeping it queued is that it runs once unblocked."""
    service.set_pipeline_paused("paused_child", True)
    service.enqueue_pipeline_trigger("paused_child", trigger="manual")
    assert service.drain_trigger_queue() == []
    assert service.store.pending_queue_item("paused_child") is not None

    # Resuming drains the queue itself, so the held trigger fires here rather
    # than waiting for the next scheduler tick.
    service.set_pipeline_paused("paused_child", False)
    assert service.store.pending_queue_item("paused_child") is None
    assert [run.pipeline_id for run in service.list_runs(limit=5)] == ["paused_child"]


# --- Downstream status -------------------------------------------------------


def test_downstream_chips_report_the_real_state(service: PipelineService) -> None:
    """Reporting everything as "pending" hid the ones that will never start."""
    service.set_pipeline_paused("paused_child", True)
    run = service.trigger_pipeline("upstream", wait=True)
    time.sleep(1.0)
    service.drain_trigger_queue()

    links = {item["pipeline_id"]: item for item in service.downstream_run_links(service.store.get_run(run.run_id))}

    assert links["paused_child"]["status"] == "paused"
    assert "resumed" in links["paused_child"]["reason"]

    assert links["disabled_child"]["status"] == "disabled"
    assert "disabled" in links["disabled_child"]["reason"]

    # One that actually ran keeps its real run status and needs no explanation.
    assert links["normal_child"]["status"] in {"success", "queued", "running"}
    assert links["normal_child"]["reason"] is None


def test_downstream_waits_while_the_parent_is_unfinished(service: PipelineService) -> None:
    """Before the parent succeeds nothing is wrong yet, and the chip should say so."""
    pending = service.store.create_run(service.get_pipeline("upstream"), trigger="manual")
    links = service.downstream_run_links(service.store.get_run(pending.run_id))
    assert {item["status"] for item in links} == {"waiting"}
    assert all("Waiting" in item["reason"] for item in links)


# --- Actor attribution -------------------------------------------------------


def test_user_actions_record_who_asked(tmp_path: Path, caplog) -> None:
    """History should show who did it, not only what happened."""
    config_path = _project(tmp_path)
    with TestClient(create_app(str(config_path))) as client:
        service = client.app.state.service
        service.create_user("root", "root-password", role="admin")

        with caplog.at_level(logging.INFO, logger="piply.scheduler"):
            started = client.post("/api/pipelines/normal_child/run", json={}, auth=("root", "root-password"))
            client.post("/api/pipelines/normal_child/pause", auth=("root", "root-password"))
            client.post("/api/pipelines/normal_child/resume", auth=("root", "root-password"))

        assert started.status_code == 200
        assert started.json()["actor"] == "root"
        assert service.store.get_run(started.json()["id"]).actor == "root"

        assert "Pipeline 'normal_child' run manual by root" in caplog.text
        assert "Pipeline 'normal_child' paused by root" in caplog.text
        assert "Pipeline 'normal_child' resumed by root" in caplog.text


def test_scheduler_runs_have_no_actor(service: PipelineService) -> None:
    """Only user-triggered actions are attributed; nothing invents an actor."""
    run = service.trigger_pipeline("normal_child", trigger="schedule", wait=True)
    assert service.store.get_run(run.run_id).actor is None
