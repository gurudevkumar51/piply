"""Microsoft Teams notifications.

Delivery is deliberately outside the execution path: a run that succeeded did
succeed, whether or not Teams accepted the card. Every failure mode here is
therefore checked twice — that it is reported, and that it did not change the
run.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from piply.core.loader import ConfigError, load_project
from piply.core.notifications import (
    NotificationError,
    build_alert,
    parse_notifications,
    parse_pipeline_notifications,
)
from piply.core.service import PipelineService


class _Sink(BaseHTTPRequestHandler):
    """A stand-in for a Teams incoming webhook."""

    received: list[dict] = []

    def do_POST(self) -> None:  # noqa: N802 - name fixed by BaseHTTPRequestHandler
        length = int(self.headers.get("content-length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        type(self).received.append({"path": self.path, "body": body})
        self.send_response(500 if self.path == "/broken" else 200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args) -> None:  # noqa: D102 - silence the default logging
        return


@pytest.fixture()
def sink():
    """Run a local webhook receiver and yield it with its base URL."""
    _Sink.received = []
    server = HTTPServer(("127.0.0.1", 0), _Sink)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield _Sink, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


def _project(tmp_path: Path, base_url: str, *, extra_pipelines: str = "") -> Path:
    (tmp_path / "piply.yaml").write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Alerts",
                "workspace: .",
                "include: [piply_alert.yaml]",
                "pipelines:",
                "  ok_pipeline:",
                "    notifications:",
                "      on_success: [data_engineering]",
                "      on_failure: [critical]",
                "    tasks:",
                "      t: {type: cli, command: echo fine}",
                "  bad_pipeline:",
                "    notifications:",
                "      on_failure: [critical]",
                "    tasks:",
                "      t: {type: cli, command: exit 3}",
                extra_pipelines,
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "piply_alert.yaml").write_text(
        "\n".join(
            [
                "notifications:",
                "  teams:",
                "    production_alerts:",
                "      type: channel",
                f"      webhook: {base_url}/prod",
                "    data_engineering:",
                "      type: chat",
                f"      webhook: {base_url}/chat",
                "  groups:",
                "    critical: [production_alerts, data_engineering]",
            ]
        ),
        encoding="utf-8",
    )
    return tmp_path / "piply.yaml"


def test_success_and_failure_route_to_different_destinations(tmp_path: Path, sink) -> None:
    """`on_success` and `on_failure` are independent lists."""
    receiver, base_url = sink
    config = _project(tmp_path, base_url)
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    service.trigger_pipeline("ok_pipeline", wait=True)

    assert [item["path"] for item in receiver.received] == ["/chat"]
    card = receiver.received[0]["body"]
    assert card["@type"] == "MessageCard"
    facts = {fact["name"]: fact["value"] for fact in card["sections"][0]["facts"]}
    assert facts["Status"] == "success"


def test_a_group_fans_out_to_every_destination(tmp_path: Path, sink) -> None:
    """One name in the pipeline, several channels notified."""
    receiver, base_url = sink
    config = _project(tmp_path, base_url)
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    run = service.trigger_pipeline("bad_pipeline", wait=True)

    assert sorted(item["path"] for item in receiver.received) == ["/chat", "/prod"]
    facts = {fact["name"]: fact["value"] for fact in receiver.received[0]["body"]["sections"][0]["facts"]}
    assert facts["Status"] == "failed"
    assert "exited with code 3" in facts["Error"]
    assert run.status == "failed"


def test_a_delivery_failure_never_changes_the_run(tmp_path: Path, sink) -> None:
    """The whole point of keeping notifications out of the execution path."""
    receiver, base_url = sink
    config = _project(tmp_path, base_url)
    (tmp_path / "piply_alert.yaml").write_text(
        "\n".join(
            [
                "notifications:",
                "  teams:",
                "    data_engineering:",
                "      type: channel",
                f"      webhook: {base_url}/broken",
                "  groups:",
                "    critical: [data_engineering]",
            ]
        ),
        encoding="utf-8",
    )
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    run = service.trigger_pipeline("ok_pipeline", wait=True)
    _, _, logs = service.get_run(run.run_id)
    messages = [line.message for line in logs]

    assert run.status == "success"
    assert any("HTTP 500" in message for message in messages)
    # The webhook is the credential, so it must never appear in a log.
    assert not [message for message in messages if base_url in message]


def test_an_unknown_destination_is_reported_against_the_run(tmp_path: Path, sink) -> None:
    """A typo must not block the project from loading, or fail the run."""
    _, base_url = sink
    config = _project(
        tmp_path,
        base_url,
        extra_pipelines="\n".join(
            [
                "  typo_pipeline:",
                "    notifications: {on_success: [nope_typo]}",
                "    tasks:",
                "      t: {type: cli, command: echo fine}",
            ]
        ),
    )
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    run = service.trigger_pipeline("typo_pipeline", wait=True)
    _, _, logs = service.get_run(run.run_id)

    assert run.status == "success"
    assert any("Unknown notification destination 'nope_typo'" in line.message for line in logs)


def test_an_unresolved_webhook_warns_at_load_and_skips_at_send(tmp_path: Path) -> None:
    """A developer without the production secret can still run locally."""
    (tmp_path / "piply.yaml").write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Missing Secret",
                "workspace: .",
                "notifications:",
                "  teams:",
                "    production_alerts:",
                "      type: channel",
                "      webhook: ${TEAMS_WEBHOOK_NOT_SET_ANYWHERE}",
                "pipelines:",
                "  p:",
                "    notifications: {on_success: [production_alerts]}",
                "    tasks:",
                "      t: {type: cli, command: echo fine}",
            ]
        ),
        encoding="utf-8",
    )

    project = load_project(tmp_path / "piply.yaml")
    assert any("did not resolve" in warning for warning in project.warnings)

    service = PipelineService(config_path=tmp_path / "piply.yaml", database_path=tmp_path / "runs.db")
    run = service.trigger_pipeline("p", wait=True)
    _, _, logs = service.get_run(run.run_id)

    assert run.status == "success"
    assert any("is not configured" in line.message for line in logs)


@pytest.mark.parametrize(
    ("block", "message"),
    [
        ({"teams": {"a": {"type": "sms", "webhook": "https://x.invalid"}}}, "type must be one of"),
        ({"teams": {"a": {"type": "channel"}}}, "needs a 'webhook'"),
        ({"teams": {"a": {"webhook": "ftp://x.invalid"}}}, "must be an https URL"),
        (
            {"teams": {"a": {"webhook": "https://x.invalid", "timeout_seconds": 0}}},
            "greater than zero",
        ),
        ({"slack": {}}, "Unsupported notification channel"),
    ],
)
def test_misconfiguration_is_rejected(block, message) -> None:
    """Config errors belong at load time, not at 3am when the alert is needed."""
    with pytest.raises(NotificationError, match=message):
        parse_notifications(block)


def test_an_unknown_name_in_a_group_fails_at_load(tmp_path: Path) -> None:
    """Groups are resolved eagerly so a typo cannot hide until a failure."""
    (tmp_path / "piply.yaml").write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Bad Group",
                "workspace: .",
                "notifications:",
                "  teams:",
                "    real: {type: channel, webhook: 'https://x.invalid/h'}",
                "  groups:",
                "    everyone: [real, ghost]",
                "pipelines: {}",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="Unknown notification destination 'ghost'"):
        load_project(tmp_path / "piply.yaml")


def test_a_bare_list_on_a_pipeline_means_on_failure() -> None:
    """Matches `notify:`, because failure is what people want to hear about."""
    assert parse_pipeline_notifications(["alerts"], "p") == (("alerts",), ())


def test_a_long_error_is_truncated_rather_than_dropped() -> None:
    """Teams rejects an oversized card; a shortened alert beats no alert."""
    card = build_alert(
        title="T",
        pipeline_id="p",
        status="failed",
        run_id="r",
        trigger="manual",
        tasks="0/1",
        duration="1s",
        error="x" * 5000,
    )

    error_fact = [f for f in card["sections"][0]["facts"] if f["name"] == "Error"][0]
    assert len(error_fact["value"]) < 1000
    assert error_fact["value"].endswith("…")


def test_delivery_attempts_are_recorded_for_the_ui(tmp_path: Path, sink) -> None:
    """A run log line is not enough: the panel needs queryable outcomes."""
    receiver, base_url = sink
    config = _project(tmp_path, base_url)
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    service.trigger_pipeline("bad_pipeline", wait=True)
    overview = service.notification_overview()

    outcomes = {(item["destination"], item["outcome"]) for item in overview["deliveries"]}
    assert ("production_alerts", "sent") in outcomes
    assert ("data_engineering", "sent") in outcomes
    # Groups are expanded, or a destination reached only via a group would read
    # as "not used by any pipeline" — the most misleading thing the panel could say.
    assert "bad_pipeline (on_failure)" in overview["used_by"]["production_alerts"]


def test_a_run_with_no_matching_destinations_is_still_recorded(tmp_path: Path, sink) -> None:
    """Silence is the hardest failure to debug, so it gets an explicit row."""
    receiver, base_url = sink
    config = _project(tmp_path, base_url)
    (tmp_path / "piply.yaml").write_text(
        (tmp_path / "piply.yaml").read_text(encoding="utf-8").replace("      on_success: [data_engineering]\n", ""),
        encoding="utf-8",
    )
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    service.trigger_pipeline("ok_pipeline", wait=True)
    deliveries = service.store.list_notification_deliveries(limit=10)

    assert [item["outcome"] for item in deliveries] == ["not_configured"]
    assert "no 'on_success' destinations" in deliveries[0]["detail"]
