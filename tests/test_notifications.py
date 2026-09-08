"""Microsoft Teams notifications.

Delivery is deliberately outside the execution path: a run that succeeded did
succeed, whether or not Teams accepted the card. Every failure mode here is
therefore checked twice — that it is reported, and that it did not change the
run.
"""

from __future__ import annotations

import json
import socket
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
        if self.path.startswith("/unauthorized"):
            # Byte-for-byte what Power Automate returns for a bad signature.
            self.send_response(401)
            self.end_headers()
            self.wfile.write(
                b'{"error":{"code":"AuthorizationFailed","message":"The authentication '
                b'credentials are not valid.","messageTemplate":"AuthorizationFailed"}}'
            )
            return
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


def _facts(payload: dict) -> dict[str, str]:
    """Read the card's fields whichever wire format it used."""
    if payload.get("type") == "message":
        card = payload["attachments"][0]["content"]
        factset = next(item for item in card["body"] if item["type"] == "FactSet")
        return {item["title"]: item["value"] for item in factset["facts"]}
    return {item["name"]: item["value"] for item in payload["sections"][0]["facts"]}


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
    assert _facts(receiver.received[0]["body"])["Status"] == "success"


def test_a_group_fans_out_to_every_destination(tmp_path: Path, sink) -> None:
    """One name in the pipeline, several channels notified."""
    receiver, base_url = sink
    config = _project(tmp_path, base_url)
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    run = service.trigger_pipeline("bad_pipeline", wait=True)

    assert sorted(item["path"] for item in receiver.received) == ["/chat", "/prod"]
    facts = _facts(receiver.received[0]["body"])
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

    error_value = _facts(card)["Error"]
    assert len(error_value) < 1000
    assert error_value.endswith("…")


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


def test_a_broken_ca_path_is_ignored_rather_than_fatal(tmp_path: Path, monkeypatch, sink) -> None:
    """`curl` ignores `SSL_CERT_FILE`; Python does not, and used to die on it.

    A conda environment that was rebuilt leaves the variable pointing at a file
    that is gone, and every HTTPS post then failed before reaching the network —
    so the same webhook worked from a terminal and not from Piply.
    """
    import piply.core.notifications as notifications

    monkeypatch.setattr(notifications, "_ca_fallback_warned", False)
    monkeypatch.setenv("SSL_CERT_FILE", str(tmp_path / "gone" / "cacert.pem"))
    receiver, base_url = sink
    config = _project(tmp_path, base_url)
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    run = service.trigger_pipeline("ok_pipeline", wait=True)

    # Delivered, rather than dying on the missing bundle.
    assert [item["path"] for item in receiver.received] == ["/chat"]
    _, _, logs = service.get_run(run.run_id)
    assert not [line for line in logs if "FileNotFoundError" in line.message]
    assert run.status == "success"


def test_a_valid_ca_path_is_left_alone(monkeypatch) -> None:
    """Only a *missing* path is overridden; a real corporate bundle still wins."""
    import certifi

    import piply.core.notifications as notifications

    monkeypatch.setenv("SSL_CERT_FILE", certifi.where())
    assert notifications._broken_ca_variables() == []
    # True means "httpx's own default", which honours the environment.
    assert notifications._verify_option() is True

    for name in ("SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE"):
        monkeypatch.delenv(name, raising=False)
    assert notifications._verify_option() is True


def test_a_broken_ca_variable_is_still_named_when_delivery_fails(tmp_path: Path, monkeypatch) -> None:
    """The fallback keeps TLS working, but the environment still wants fixing.

    So when delivery fails for some *other* reason, the diagnostic still points
    at the misconfigured variable rather than leaving it to be discovered later.
    """
    monkeypatch.setenv("SSL_CERT_FILE", str(tmp_path / "gone" / "cacert.pem"))
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]

    (tmp_path / "piply.yaml").write_text(
        f"""version: "1"
title: CA
workspace: .
notifications:
  teams:
    ops: {{type: channel, webhook: 'http://127.0.0.1:{closed_port}/hook'}}
pipelines:
  p:
    notifications: {{on_success: [ops]}}
    tasks:
      t: {{type: cli, command: echo hi}}
""",
        encoding="utf-8",
    )

    service = PipelineService(config_path=tmp_path / "piply.yaml")

    with pytest.raises(RuntimeError) as error:
        service.send_test_notification("ops")

    message = str(error.value)
    assert "SSL_CERT_FILE" in message
    assert str(closed_port) not in message


def test_a_transport_failure_names_the_exception_type(tmp_path: Path, monkeypatch) -> None:
    """`request failed` alone does not distinguish DNS from TLS from a proxy.

    Points at a closed loopback port rather than an unresolvable name: real DNS
    would make the exception type depend on how fast the resolver fails.
    """
    for name in ("SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE"):
        monkeypatch.delenv(name, raising=False)

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
    (tmp_path / "piply.yaml").write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Unreachable",
                "workspace: .",
                "notifications:",
                "  teams:",
                f"    ops: {{type: channel, webhook: 'http://127.0.0.1:{closed_port}/hook'}}",
                "pipelines:",
                "  p:",
                "    notifications: {on_success: [ops]}",
                "    tasks:",
                "      t: {type: cli, command: echo hi}",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(config_path=tmp_path / "piply.yaml")

    with pytest.raises(RuntimeError) as error:
        service.send_test_notification("ops")

    message = str(error.value)
    # The class name is what distinguishes DNS from TLS from a refused connection.
    assert "ConnectError" in message or "ConnectTimeout" in message
    assert "Check the host is reachable" in message
    assert str(closed_port) not in message


def _defaults_project(tmp_path: Path, base_url: str, pipelines: str, defaults: str) -> Path:
    """A project whose destinations are shared and whose pipelines vary."""
    (tmp_path / "piply.yaml").write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Global alerts",
                "workspace: .",
                "notifications:",
                "  teams:",
                f"    production_alerts: {{type: channel, webhook: '{base_url}/prod'}}",
                f"    data_engineering: {{type: chat, webhook: '{base_url}/chat'}}",
                "  groups:",
                "    critical: [production_alerts, data_engineering]",
                "  defaults:",
                defaults,
                "pipelines:",
                pipelines,
            ]
        ),
        encoding="utf-8",
    )
    return tmp_path / "piply.yaml"


def test_project_defaults_apply_to_a_pipeline_that_declares_nothing(tmp_path: Path, sink) -> None:
    """The point of the feature: alerting without a block on every pipeline."""
    receiver, base = sink
    config = _defaults_project(
        tmp_path,
        base,
        pipelines="\n".join(
            [
                "  quiet_pipeline:",
                "    tasks:",
                "      t: {type: cli, command: exit 3}",
            ]
        ),
        defaults="    on_failure: [critical]",
    )
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    # Resolved at load time, so every reader of the pipeline agrees.
    pipeline = service.get_pipeline("quiet_pipeline")
    assert pipeline.alert_on_failure == ("critical",)
    assert pipeline.alert_on_success == ()

    service.trigger_pipeline("quiet_pipeline", wait=True)
    assert sorted(item["path"] for item in receiver.received) == ["/chat", "/prod"]


def test_a_pipeline_overrides_one_outcome_and_inherits_the_other(tmp_path: Path, sink) -> None:
    """Naming `on_failure` must not silently drop the inherited `on_success`."""
    _, base = sink
    config = _defaults_project(
        tmp_path,
        base,
        pipelines="\n".join(
            [
                "  narrowed:",
                "    notifications:",
                "      on_failure: [data_engineering]",
                "    tasks:",
                "      t: {type: cli, command: echo hi}",
            ]
        ),
        defaults="    on_failure: [critical]\n    on_success: [production_alerts]",
    )
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    pipeline = service.get_pipeline("narrowed")
    # Replaced, not merged — a pipeline must be able to narrow who is paged.
    assert pipeline.alert_on_failure == ("data_engineering",)
    assert pipeline.alert_on_success == ("production_alerts",)


def test_a_pipeline_can_opt_out_of_the_defaults(tmp_path: Path, sink) -> None:
    """Once a default exists, `false` is the only way to say "tell nobody"."""
    receiver, base = sink
    config = _defaults_project(
        tmp_path,
        base,
        pipelines="\n".join(
            [
                "  silent:",
                "    notifications: false",
                "    tasks:",
                "      t: {type: cli, command: exit 3}",
            ]
        ),
        defaults="    on_failure: [critical]",
    )
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    assert service.get_pipeline("silent").alert_on_failure == ()

    service.trigger_pipeline("silent", wait=True)
    assert receiver.received == []


def test_an_empty_list_silences_one_outcome_without_opting_out(tmp_path: Path, sink) -> None:
    """`on_failure: []` is a deliberate override, distinct from not saying anything."""
    _, base = sink
    config = _defaults_project(
        tmp_path,
        base,
        pipelines="\n".join(
            [
                "  half_silent:",
                "    notifications:",
                "      on_failure: []",
                "    tasks:",
                "      t: {type: cli, command: echo hi}",
            ]
        ),
        defaults="    on_failure: [critical]\n    on_success: [data_engineering]",
    )
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    pipeline = service.get_pipeline("half_silent")
    assert pipeline.alert_on_failure == ()
    assert pipeline.alert_on_success == ("data_engineering",)


def test_a_typo_in_a_default_fails_at_load(tmp_path: Path, sink) -> None:
    """One bad default would otherwise break alerting for every pipeline at once."""
    _, base = sink
    config = _defaults_project(
        tmp_path,
        base,
        pipelines="  p:\n    tasks:\n      t: {type: cli, command: echo hi}",
        defaults="    on_failure: [criticl]",
    )

    with pytest.raises(ConfigError) as error:
        PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    assert "criticl" in str(error.value)
    assert "Known destinations" in str(error.value)


def test_defaults_are_inherited_by_template_deployments(tmp_path: Path, sink) -> None:
    """Deployments are the case with the most repetition, so they must be covered."""
    _, base = sink
    config = _defaults_project(
        tmp_path,
        base,
        pipelines="  placeholder:\n    tasks:\n      t: {type: cli, command: echo hi}",
        defaults="    on_failure: [critical]",
    )
    config.write_text(
        config.read_text(encoding="utf-8")
        + "\n"
        + "\n".join(
            [
                "pipeline_templates:",
                "  extraction:",
                "    tasks:",
                "      t: {type: cli, command: echo extract}",
                "pipeline_deployments:",
                "  ecw_extract: {template: extraction, tenant: ecw}",
                "  athena_extract: {template: extraction, tenant: athena}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    for deployment in ("ecw_extract", "athena_extract"):
        assert service.get_pipeline(deployment).alert_on_failure == ("critical",), deployment


def _unauthorized_project(tmp_path: Path, webhook: str) -> Path:
    """A project whose single destination points at the 401 endpoint."""
    (tmp_path / "piply.yaml").write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Rejected",
                "workspace: .",
                "notifications:",
                "  teams:",
                f"    piply_alerts_technology: {{type: channel, webhook: '{webhook}'}}",
                "pipelines:",
                "  p:",
                "    tasks:",
                "      t: {type: cli, command: echo hi}",
            ]
        ),
        encoding="utf-8",
    )
    return tmp_path / "piply.yaml"


def test_a_rejected_signature_says_what_to_do_about_it(tmp_path: Path, sink) -> None:
    """A bare `HTTP 401: AuthorizationFailed` reads like a Piply bug, and is not one.

    The credential lives in the URL's `sig`, so a 401 means that value is stale
    or truncated — not that the card or the account is wrong. The signature's
    length is the one detail that tells those two apart, and it is safe to
    report; the URL itself still must not appear.
    """
    _, base = sink
    signature = "Ab3-xY_z9QwEr7TyU1oP2sD4fG6hJ8kL0mN5vC1xZ2A"
    config = _unauthorized_project(tmp_path, f"{base}/unauthorized?api-version=2016-06-01&sig={signature}")
    service = PipelineService(config_path=config)

    with pytest.raises(RuntimeError) as error:
        service.send_test_notification("piply_alerts_technology")

    message = str(error.value)
    # The server's own words are kept — they are what the user searches for.
    assert "HTTP 401" in message and "AuthorizationFailed" in message
    # Plus the part that makes it actionable.
    assert f"({len(signature)} characters)" in message
    assert "re-saving a flow issues a new one" in message.lower()
    assert "truncated" in message
    # The URL is the credential and must never be echoed back.
    assert signature not in message


def test_a_webhook_missing_its_signature_is_called_incomplete(tmp_path: Path, sink) -> None:
    """Pasting the URL without its query string is a different mistake, named differently."""
    _, base = sink
    config = _unauthorized_project(tmp_path, f"{base}/unauthorized")
    service = PipelineService(config_path=config)

    with pytest.raises(RuntimeError) as error:
        service.send_test_notification("piply_alerts_technology")

    message = str(error.value)
    assert "no 'sig' parameter" in message
    assert "query string included" in message


def test_a_non_auth_failure_gets_no_signature_advice(tmp_path: Path, sink) -> None:
    """A 500 has nothing to do with the URL; the hint would be a wrong lead."""
    _, base = sink
    config = _unauthorized_project(tmp_path, f"{base}/broken")
    service = PipelineService(config_path=config)

    with pytest.raises(RuntimeError) as error:
        service.send_test_notification("piply_alerts_technology")

    message = str(error.value)
    assert "HTTP 500" in message
    assert "sig" not in message


def test_workflows_endpoints_get_an_adaptive_card(tmp_path: Path, sink) -> None:
    """Office 365 connectors are retired; Workflows rejects a MessageCard.

    The two wire formats are not interchangeable, so the shape is chosen per
    destination rather than shared.
    """
    receiver, base_url = sink
    (tmp_path / "piply.yaml").write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Formats",
                "workspace: .",
                "notifications:",
                "  teams:",
                f"    modern: {{type: channel, webhook: '{base_url}/workflows'}}",
                f"    legacy: {{type: channel, webhook: '{base_url}/legacy', format: messagecard}}",
                "pipelines:",
                "  p:",
                "    notifications: {on_success: [modern, legacy]}",
                "    tasks:",
                "      t: {type: cli, command: echo hi}",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(config_path=tmp_path / "piply.yaml")
    service.trigger_pipeline("p", wait=True)

    by_path = {item["path"]: item["body"] for item in receiver.received}
    modern = by_path["/workflows"]
    # The envelope Power Automate expects, matching what a working curl sends.
    assert modern["type"] == "message"
    attachment = modern["attachments"][0]
    assert attachment["contentType"] == "application/vnd.microsoft.card.adaptive"
    assert attachment["content"]["type"] == "AdaptiveCard"
    facts = {f["title"]: f["value"] for f in attachment["content"]["body"][1]["facts"]}
    assert facts["Status"] == "success"

    # An explicit format still produces the old connector shape.
    assert by_path["/legacy"]["@type"] == "MessageCard"


def test_the_card_format_is_guessed_from_the_host() -> None:
    """A connector URL keeps the old shape; anything else gets the new one."""
    from piply.core.notifications import detect_card_format

    assert detect_card_format("https://contoso.webhook.office.com/webhookb2/a/IncomingWebhook/b") == "messagecard"
    assert detect_card_format("https://prod-12.westus.logic.azure.com:443/workflows/a/triggers/x") == "adaptive"


def test_an_unknown_card_format_is_rejected() -> None:
    """A typo must fail at load, not produce a card Teams silently drops."""
    with pytest.raises(NotificationError, match="format must be one of"):
        parse_notifications({"teams": {"a": {"webhook": "https://x.invalid/h", "format": "slack"}}})


TOLERATED_FAILURE = "\n".join(
    [
        "  tolerant:",
        "    notifications:",
        "      on_failure: [production_alerts]",
        "    tasks:",
        "      optional_sync:",
        "        type: cli",
        "        command: exit 4",
        "        allow_failure: true",
        "        alert_on_failure: true",
        "      main:",
        "        type: cli",
        "        command: echo done",
        "        depends_on: [optional_sync]",
        "        on_upstream_failure: continue",
    ]
)


def test_a_tolerated_task_failure_is_still_reported(tmp_path: Path, sink) -> None:
    """The one gap a pipeline-level alert cannot cover.

    A task allowed to fail leaves the run green, so nothing tells anyone it
    broke. `alert_on_failure: true` on the task closes that.
    """
    receiver, base = sink
    config = _defaults_project(tmp_path, base, pipelines=TOLERATED_FAILURE, defaults="    on_success: []")
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    run = service.trigger_pipeline("tolerant", wait=True)

    assert run.status == "success", "the run must stay green or the test proves nothing"
    assert [item["path"] for item in receiver.received] == ["/prod"]
    facts = _facts(receiver.received[0]["body"])
    assert "optional_sync" in facts["Error"]
    assert "tolerated" in facts["Error"]


def test_a_failed_run_does_not_also_send_a_task_card(tmp_path: Path, sink) -> None:
    """The run's own failure card already says so; a second is noise."""
    receiver, base = sink
    pipelines = TOLERATED_FAILURE.replace("        allow_failure: true\n", "")
    config = _defaults_project(tmp_path, base, pipelines=pipelines, defaults="    on_success: []")
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    run = service.trigger_pipeline("tolerant", wait=True)

    assert run.status == "failed"
    # Exactly one card, from the pipeline-level alert.
    assert len(receiver.received) == 1
    assert "tolerated" not in (_facts(receiver.received[0]["body"]).get("Error") or "")


def test_a_task_without_the_flag_stays_silent(tmp_path: Path, sink) -> None:
    """Opt-in only: tolerated failures are normal and must not start alerting."""
    receiver, base = sink
    pipelines = TOLERATED_FAILURE.replace("        alert_on_failure: true\n", "")
    config = _defaults_project(tmp_path, base, pipelines=pipelines, defaults="    on_success: []")
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")

    run = service.trigger_pipeline("tolerant", wait=True)

    assert run.status == "success"
    assert receiver.received == []
