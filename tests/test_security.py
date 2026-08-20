"""Security regressions.

Each test here corresponds to a hole that was found and closed. They assert
what a restricted account must *not* be able to reach, because that is the
direction an authorization bug fails in: a missing check is invisible until
someone exercises it from an account that should have been refused.
"""

from __future__ import annotations

import re
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
    # Signing in leaves the session cookie on the client, so later requests are
    # made as an authenticated user.
    client.post("/login", data={"username": admin[0], "password": admin[1], "next": "/"}, follow_redirects=False)

    for hostile in ["//evil.example.com/", "https://evil.example.com", "/\\evil.example.com"]:
        response = client.get(f"/login?next={hostile}", follow_redirects=False)
        assert response.headers["location"] == "/", hostile

    # A genuine same-site path still works.
    assert client.get("/login?next=/runs", follow_redirects=False).headers["location"] == "/runs"


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


def test_csp_allows_every_origin_the_ui_actually_loads(restricted) -> None:
    """A policy that blocks the app's own assets is worse than no policy.

    Scans every template and script rather than a known list, because the way
    this breaks is someone adding a CDN reference to one page and nobody
    noticing the DAG or the fonts stopped rendering. Checked against the real
    response header, not the constant.
    """
    client, _service, _run, _mallory, admin = restricted
    policy = client.get("/pipelines", auth=admin).headers["content-security-policy"]

    ui = Path("piply/ui")
    sources = list(ui.glob("templates/*.html")) + list(ui.glob("static/*.js"))
    referenced = set()
    for source in sources:
        referenced.update(re.findall(r"https://[a-zA-Z0-9.-]+", source.read_text(encoding="utf-8")))

    assert referenced, "expected the UI to reference at least one remote origin"
    for origin in sorted(referenced):
        assert origin in policy, f"the UI loads {origin} but the CSP would block it"

    # The parts that make the policy worth having.
    assert "default-src 'self'" in policy
    assert "frame-ancestors 'none'" in policy
    assert "form-action 'self'" in policy
    assert "object-src" not in policy or "'none'" in policy


# --- Server bootstrap --------------------------------------------------------


def test_first_admin_is_created_on_a_server_without_shell_access(tmp_path: Path, monkeypatch) -> None:
    """A server install must be able to produce its first login from config alone."""
    (tmp_path / "workspace").mkdir(exist_ok=True)
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(CONFIG, encoding="utf-8")

    monkeypatch.setenv("PIPLY_AUTH_ENABLED", "true")
    monkeypatch.setenv("PIPLY_DATABASE", str(tmp_path / "piply.db"))
    monkeypatch.delenv("PIPLY_ADMIN_PASSWORD", raising=False)
    monkeypatch.delenv("PIPLY_ADMIN_PASSWORD_FILE", raising=False)

    with TestClient(create_app(str(config_path))) as client:
        service = client.app.state.service
        # The account exists and the generated password was returned once so the
        # startup banner could show it.
        assert service.get_user("admin") is not None
        assert service.get_user("admin").is_admin
        # Bootstrapping again is a no-op, so a restart does not reset the account.
        assert service.bootstrap_admin() is None
        assert client.get("/api/pipelines").status_code == 401


def test_admin_password_can_come_from_a_mounted_secret_file(tmp_path: Path, monkeypatch) -> None:
    """Docker and Kubernetes mount secrets as files, not environment variables."""
    (tmp_path / "workspace").mkdir(exist_ok=True)
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(CONFIG, encoding="utf-8")
    secret_file = tmp_path / "admin_password"
    secret_file.write_text("mounted-secret-value\n", encoding="utf-8")

    monkeypatch.setenv("PIPLY_AUTH_ENABLED", "true")
    monkeypatch.setenv("PIPLY_DATABASE", str(tmp_path / "piply.db"))
    monkeypatch.setenv("PIPLY_ADMIN_USERNAME", "ops")
    monkeypatch.setenv("PIPLY_ADMIN_PASSWORD_FILE", str(secret_file))
    monkeypatch.delenv("PIPLY_ADMIN_PASSWORD", raising=False)

    with TestClient(create_app(str(config_path))) as client:
        service = client.app.state.service
        # The trailing newline a file mount usually carries must be stripped.
        assert service.authenticate("ops", "mounted-secret-value") is not None
        assert client.get("/api/pipelines", auth=("ops", "mounted-secret-value")).status_code == 200


def test_a_supplied_admin_password_is_not_echoed(tmp_path: Path, monkeypatch) -> None:
    """Only a generated password is returned for printing, never one we were given."""
    from piply.core.service import PipelineService

    (tmp_path / "workspace").mkdir(exist_ok=True)
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(CONFIG, encoding="utf-8")

    monkeypatch.setenv("PIPLY_AUTH_ENABLED", "true")
    monkeypatch.setenv("PIPLY_ADMIN_PASSWORD", "operator-chosen-password")
    service = PipelineService(config_path=config_path, database_path=tmp_path / "piply.db")

    username, password = service.bootstrap_admin()
    assert username == "admin"
    # None means "do not print"; the operator already knows this value and it
    # should not be copied into the container's log stream.
    assert password is None
    assert service.authenticate("admin", "operator-chosen-password") is not None


def test_secret_files_apply_to_the_legacy_credentials_too(tmp_path: Path) -> None:
    """PIPLY_AUTH_PASSWORD and PIPLY_API_TOKEN accept the same _FILE convention."""
    from piply.settings import read_secret

    secret_file = tmp_path / "token"
    secret_file.write_text("  tok_abc123  \n", encoding="utf-8")

    assert read_secret({"PIPLY_API_TOKEN_FILE": str(secret_file)}, "PIPLY_API_TOKEN") == "tok_abc123"
    # The file wins over the variable, because it is the more secure source.
    env = {"PIPLY_API_TOKEN": "from-env", "PIPLY_API_TOKEN_FILE": str(secret_file)}
    assert read_secret(env, "PIPLY_API_TOKEN") == "tok_abc123"
    # Falling back to the variable keeps existing deployments working.
    assert read_secret({"PIPLY_API_TOKEN": "from-env"}, "PIPLY_API_TOKEN") == "from-env"
    assert read_secret({}, "PIPLY_API_TOKEN") is None

    # An unreadable path is a deployment mistake and must not fail silently.
    with pytest.raises(RuntimeError) as excinfo:
        read_secret({"PIPLY_API_TOKEN_FILE": str(tmp_path / "missing")}, "PIPLY_API_TOKEN")
    assert "PIPLY_API_TOKEN_FILE" in str(excinfo.value)


def test_health_is_public_and_reveals_nothing_sensitive(restricted) -> None:
    """A load balancer must reach /health without credentials, and learn little."""
    client, _service, _run, _mallory, _admin = restricted

    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert set(payload) == {"status", "version", "scheduler", "accepting_work"}

    # No pipeline names, paths, or store location leak through the public probe.
    body = response.text
    for secret in ["secretflow", "sup3rs3cret", "piply.db", "workspace"]:
        assert secret not in body

    # Making it public must not have opened anything else.
    assert client.get("/api/pipelines").status_code == 401
    assert client.get("/api/diagnostics").status_code == 401
