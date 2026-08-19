"""Security regressions.

Each test here corresponds to a hole that was found and closed. They assert
what a restricted account must *not* be able to reach, because that is the
direction an authorization bug fails in: a missing check is invisible until
someone exercises it from an account that should have been refused.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from piply.api.app import create_app
from piply.core.auth import read_session
from piply.core.secrets import mask_env_values

CONFIG = "\n".join(
    [
        'version: "1"',
        "title: Security Test",
        "workspace: workspace",
        "pipelines:",
        "  secretflow:",
        "    tasks:",
        "      main:",
        "        type: cli",
        "        command: echo secretflow-ran",
        "        env:",
        '          DB_PASSWORD: "sup3rs3cret"',
        '          TENANT: "BENNETT"',
        "  other:",
        "    tasks:",
        "      main: {type: cli, command: echo other-ran}",
    ]
)


@pytest.fixture()
def restricted(tmp_path: Path):
    """Yield a client, an admin, and an account granted rights on `other` only."""
    (tmp_path / "workspace").mkdir(exist_ok=True)
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(CONFIG, encoding="utf-8")
    with TestClient(create_app(str(config_path))) as client:
        service = client.app.state.service
        service.create_user("root", "root-password", role="admin")
        service.create_user("mallory", "mallory-password", permissions={"other": "view,run"})
        run = service.trigger_pipeline("secretflow", wait=True)
        yield client, service, run, ("mallory", "mallory-password"), ("root", "root-password")


def test_run_config_is_guarded_and_masked(restricted) -> None:
    """The run-config snapshot holds the resolved environment, so it is guarded twice."""
    client, _service, run, mallory, admin = restricted

    denied = client.get(f"/api/runs/{run.run_id}/config", auth=mallory)
    assert denied.status_code == 403
    assert "sup3rs3cret" not in denied.text

    allowed = client.get(f"/api/runs/{run.run_id}/config", auth=admin)
    assert allowed.status_code == 200
    # Even an admin sees credentials masked. The stored snapshot keeps the real
    # value so replaying the run still works.
    assert "sup3rs3cret" not in allowed.text
    assert allowed.json()["env"]["DB_PASSWORD"] == "***"
    # A non-credential value stays readable, which is what makes the view useful.
    assert allowed.json()["env"]["TENANT"] == "BENNETT"


def test_masking_covers_credential_names_only() -> None:
    """Masking is name-based, so it must be broad without hiding everything."""
    masked = mask_env_values(
        {
            "DB_PASSWORD": "x",
            "API_KEY": "x",
            "AWS_SECRET_ACCESS_KEY": "x",
            "SNOWFLAKE_DSN": "x",
            "AUTH_TOKEN": "x",
            "TENANT": "BENNETT",
            "DBT_TARGET": "prod",
            "THREADS": "8",
        }
    )
    assert masked["DB_PASSWORD"] == "***"
    assert masked["API_KEY"] == "***"
    assert masked["AWS_SECRET_ACCESS_KEY"] == "***"
    assert masked["SNOWFLAKE_DSN"] == "***"
    assert masked["AUTH_TOKEN"] == "***"
    assert masked["TENANT"] == "BENNETT"
    assert masked["DBT_TARGET"] == "prod"
    assert masked["THREADS"] == "8"
    # An empty value has nothing to hide and would only be confusing as "***".
    assert mask_env_values({"DB_PASSWORD": ""})["DB_PASSWORD"] == ""


def test_operations_endpoints_are_authorized(restricted) -> None:
    """Preview, artifacts, backfill, and prune all check permissions."""
    client, _service, run, mallory, admin = restricted

    assert client.get(f"/api/runs/{run.run_id}/artifacts", auth=mallory).status_code == 403
    assert client.get("/api/pipelines/secretflow/preview", auth=mallory).status_code == 403
    assert client.post(f"/api/runs/{run.run_id}/backfill", auth=mallory).status_code == 403

    # Retention is installation-wide and irreversible, so it is never delegated.
    assert client.post("/api/maintenance/prune", json={"dry_run": True}, auth=mallory).status_code == 403
    assert client.post("/api/maintenance/prune", json={"dry_run": True}, auth=admin).status_code == 200


def test_diagnostics_are_admin_only(restricted) -> None:
    """Diagnostics name filesystem paths, the config location, and the store."""
    client, _service, _run, mallory, admin = restricted

    assert client.get("/api/diagnostics", auth=mallory).status_code == 403
    assert client.get("/diagnostics", auth=mallory).status_code == 403
    assert client.get("/api/diagnostics", auth=admin).status_code == 200


def test_command_overrides_require_an_admin(restricted) -> None:
    """A `run` grant must not escalate into arbitrary command execution."""
    client, _service, _run, mallory, admin = restricted
    override = {"command_overrides": {"main": "echo pwned-by-override"}}

    assert client.post("/api/pipelines/other/run", json=override, auth=mallory).status_code == 403
    assert client.post("/api/pipelines/other/tasks/main/run", json=override, auth=mallory).status_code == 403
    assert client.post("/api/pipelines/other/preview", json=override, auth=mallory).status_code == 403

    # Running the pipeline as configured is still allowed.
    assert client.post("/api/pipelines/other/run", json={}, auth=mallory).status_code == 200
    # And an admin keeps the debugging affordance.
    assert client.post("/api/pipelines/other/run", json=override, auth=admin).status_code == 200


def test_cross_pipeline_listings_are_filtered(restricted) -> None:
    """Dashboard, matrix, and log search must not reveal invisible pipelines."""
    client, _service, run, mallory, _admin = restricted

    dashboard = client.get("/api/dashboard", auth=mallory).json()
    assert all(item["pipeline_id"] == "other" for item in dashboard["pipelines"])
    assert all(item["pipeline_id"] == "other" for item in dashboard["recent_runs"])

    matrix = client.get("/api/execution-matrix", auth=mallory).json()
    assert all(item["pipeline_id"] == "other" for item in matrix["pipelines"])

    logs = client.get("/api/logs", auth=mallory).json()
    assert all(item["run_id"] != run.run_id for item in logs)
    assert not any("secretflow-ran" in item["message"] for item in logs)

    streamed = client.get("/api/logs/stream", auth=mallory).json()
    assert all(item["pipeline_id"] == "other" for item in streamed)
    assert client.get(f"/api/logs/stream?run_id={run.run_id}", auth=mallory).status_code == 403


def test_admin_still_sees_everything(restricted) -> None:
    """The filtering must not accidentally restrict administrators."""
    client, _service, run, _mallory, admin = restricted

    dashboard = client.get("/api/dashboard", auth=admin).json()
    assert {item["pipeline_id"] for item in dashboard["pipelines"]} == {"secretflow", "other"}
    logs = client.get("/api/logs", auth=admin).json()
    assert any("secretflow-ran" in item["message"] for item in logs)
    assert client.get(f"/api/runs/{run.run_id}/artifacts", auth=admin).status_code == 200


def test_malformed_credentials_are_rejected_not_crashed(restricted) -> None:
    """Hostile input on the auth path must produce 401, never a 500."""
    client, service, _run, _mallory, _admin = restricted

    # hmac.compare_digest raises TypeError on non-ASCII str; a failed login must
    # not turn that into a server error.
    assert client.get("/api/pipelines", auth=("h\xe9llo", "x")).status_code == 401
    submitted = client.post("/login", data={"username": "h\xe9llo", "password": "x"}, follow_redirects=False)
    assert submitted.status_code == 303

    # A cookie is attacker-controlled, so every malformed shape returns None.
    for token in ["h\xe9llo.sig", "not-a-token", "", ".", "a.b.c", "!!!.###"]:
        assert read_session(service.store, token) is None


def test_login_redirect_cannot_leave_the_site(restricted) -> None:
    """`next` must not bounce a signed-in user to another host."""
    client, _service, _run, _mallory, admin = restricted
    signed_in = client.post(
        "/login",
        data={"username": admin[0], "password": admin[1], "next": "/"},
        follow_redirects=False,
    )
    cookie = signed_in.cookies.get("piply_session")

    for hostile in ["//evil.example.com/", "https://evil.example.com", "/\\evil.example.com"]:
        response = client.get(
            f"/login?next={hostile}",
            cookies={"piply_session": cookie},
            follow_redirects=False,
        )
        assert response.headers["location"] == "/", hostile

    # A genuine same-site path still works.
    response = client.get("/login?next=/runs", cookies={"piply_session": cookie}, follow_redirects=False)
    assert response.headers["location"] == "/runs"


def test_repeated_failures_lock_an_account_out(restricted) -> None:
    """Password verification is expensive, so guessing has to be throttled."""
    client, service, _run, _mallory, _admin = restricted

    for _ in range(10):
        client.post("/login", data={"username": "root", "password": "wrong"}, follow_redirects=False)

    assert service.login_retry_after("root") > 0
    # Even the correct password is refused while the lockout stands.
    assert service.authenticate("root", "root-password") is None

    service.login_throttle.record_success("root")
    assert service.authenticate("root", "root-password") is not None


def test_responses_carry_hardening_headers(restricted) -> None:
    """Clickjacking and MIME-sniffing defences apply to every response."""
    client, _service, _run, _mallory, admin = restricted

    page = client.get("/pipelines", auth=admin)
    assert page.headers["x-frame-options"] == "DENY"
    assert page.headers["x-content-type-options"] == "nosniff"
    assert "frame-ancestors 'none'" in page.headers["content-security-policy"]

    # Including on the challenge the middleware returns before any route runs.
    assert client.get("/api/pipelines").headers["x-frame-options"] == "DENY"
