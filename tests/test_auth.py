"""Authentication, authorization, and centralised SMTP.

Permission checks are the one place where a bug means a user sees or runs
something they should not, so the assertions here are about what is *denied*
as much as what is allowed.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from piply.api.app import create_app
from piply.core.auth import (
    AuthError,
    User,
    generate_password,
    hash_password,
    normalize_permissions,
    normalize_username,
    verify_password,
)
from piply.core.service import PipelineService

CONFIG = "\n".join(
    [
        'version: "1"',
        "title: Auth Test",
        "workspace: workspace",
        "pipelines:",
        "  alpha:",
        "    tasks:",
        "      main: {type: cli, command: echo alpha}",
        "  beta:",
        "    tasks:",
        "      main: {type: cli, command: echo beta}",
        "  gamma:",
        "    tasks:",
        "      main: {type: cli, command: echo gamma}",
    ]
)


def _project(tmp_path: Path) -> Path:
    """Write a config with three pipelines."""
    (tmp_path / "workspace").mkdir(exist_ok=True)
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(CONFIG, encoding="utf-8")
    return config_path


# --- Password and permission primitives --------------------------------------


def test_passwords_are_salted_hashed_and_verified() -> None:
    """A stored password is never recoverable and never repeats its salt."""
    first = hash_password("correct horse battery")
    second = hash_password("correct horse battery")

    assert first != second, "identical passwords must not produce identical hashes"
    assert "correct horse battery" not in first
    assert first.startswith("pbkdf2$")
    assert verify_password("correct horse battery", first) is True
    assert verify_password("wrong", first) is False
    assert verify_password("", first) is False
    assert verify_password("anything", None) is False
    assert verify_password("anything", "not-a-hash") is False


def test_username_and_permission_validation() -> None:
    """Input is normalised, and nonsense is rejected rather than stored."""
    assert normalize_username("  Admin ") == "admin"
    assert normalize_username("ops.team@example.com") == "ops.team@example.com"
    for bad in ("", "   ", "has space", "semi;colon"):
        with pytest.raises(AuthError):
            normalize_username(bad)

    # Acting on a pipeline implies being able to see it.
    assert normalize_permissions("run") == frozenset({"run", "view"})
    assert normalize_permissions(["edit"]) == frozenset({"edit", "view"})
    assert normalize_permissions("all") == frozenset({"view", "edit", "run"})
    assert normalize_permissions("view") == frozenset({"view"})
    assert normalize_permissions(None) == frozenset()
    with pytest.raises(AuthError):
        normalize_permissions("delete")


def test_user_can_matrix() -> None:
    """A user's grants decide exactly what they may do, per pipeline."""
    viewer = User(username="v", role="user", permissions={"alpha": frozenset({"view"})})
    assert viewer.can("view", "alpha") is True
    assert viewer.can("run", "alpha") is False
    assert viewer.can("edit", "alpha") is False
    assert viewer.can("view", "beta") is False

    runner = User(username="r", role="user", permissions={"alpha": frozenset({"view", "run"})})
    assert runner.can("run", "alpha") is True
    assert runner.can("edit", "alpha") is False
    assert runner.can("run", "beta") is False

    wildcard = User(username="w", role="user", permissions={"*": frozenset({"view", "run"})})
    assert wildcard.can("run", "anything") is True
    assert wildcard.can("edit", "anything") is False

    admin = User(username="a", role="admin")
    assert admin.can("edit", "anything") is True

    disabled = User(username="d", role="admin", is_active=False)
    assert disabled.can("view", "alpha") is False


# --- Service-level account management ----------------------------------------


def test_account_lifecycle_and_last_admin_guard(tmp_path: Path) -> None:
    """Accounts can be created, changed, and removed, but never all the admins."""
    service = PipelineService(config_path=_project(tmp_path))

    admin = service.create_user("root", generate_password(), role="admin")
    assert admin.is_admin

    member = service.create_user("ops", "hunter2-long", role="user", permissions={"alpha": ["run"]})
    assert member.permissions["alpha"] == frozenset({"run", "view"})

    assert service.authenticate("ops", "hunter2-long") is not None
    assert service.authenticate("ops", "wrong") is None
    assert service.authenticate("nobody", "hunter2-long") is None

    # Usernames are matched case-insensitively after normalisation.
    assert service.authenticate("OPS", "hunter2-long") is not None

    with pytest.raises(AuthError):
        service.create_user("ops", "another-password")

    # Removing the only active admin would lock the install out.
    with pytest.raises(AuthError):
        service.delete_user("root")
    with pytest.raises(AuthError):
        service.update_user("root", role="user")
    with pytest.raises(AuthError):
        service.update_user("root", is_active=False)

    # With a second admin it is allowed.
    service.create_user("root2", generate_password(), role="admin")
    service.delete_user("root")
    assert {item.username for item in service.list_users()} == {"ops", "root2"}

    # A disabled account cannot sign in.
    service.update_user("ops", is_active=False)
    assert service.authenticate("ops", "hunter2-long") is None


def test_grants_are_validated_against_real_pipelines(tmp_path: Path) -> None:
    """A grant must name a pipeline that exists, or the wildcard."""
    service = PipelineService(config_path=_project(tmp_path))
    service.create_user("ops", "hunter2-long")

    service.grant_permission("ops", "alpha", "view")
    service.grant_permission("ops", "*", ["view"])
    with pytest.raises(AuthError):
        service.grant_permission("ops", "does-not-exist", "view")
    with pytest.raises(AuthError):
        service.grant_permission("ghost", "alpha", "view")

    user = service.revoke_permission("ops", "alpha")
    assert "alpha" not in user.permissions


def test_auth_is_off_until_the_first_account_exists(tmp_path: Path) -> None:
    """An install with no accounts keeps working with no login at all."""
    service = PipelineService(config_path=_project(tmp_path))
    assert service.auth_required is False
    assert service.bootstrap_admin() is None, "no bootstrap unless auth is enabled"

    service.create_user("root", generate_password(), role="admin")
    assert service.auth_required is True


# --- End-to-end authorization through the API --------------------------------


def _sign_in(client: TestClient, username: str, password: str) -> bool:
    """Sign in through the login form; return whether it worked."""
    response = client.post(
        "/login",
        data={"username": username, "password": password, "next": "/"},
        follow_redirects=False,
    )
    return "piply_session" in response.cookies or bool(client.cookies.get("piply_session"))


def test_permissions_are_enforced_across_the_api(tmp_path: Path) -> None:
    """A restricted user sees and does only what they were granted."""
    config_path = _project(tmp_path)
    service = PipelineService(config_path=config_path)
    service.create_user("root", "admin-password-1", role="admin")
    service.create_user("viewer", "viewer-password-1", permissions={"alpha": ["view"]})
    service.create_user("runner", "runner-password-1", permissions={"beta": ["run"]})

    with TestClient(create_app(str(config_path))) as client:
        # Anonymous requests are refused once accounts exist.
        assert client.get("/api/pipelines").status_code == 401
        assert client.get("/", follow_redirects=False).status_code == 303

        assert _sign_in(client, "viewer", "viewer-password-1")
        listed = client.get("/api/pipelines").json()
        assert [item["pipeline_id"] for item in listed] == ["alpha"], "viewer must not see beta or gamma"

        # Visible pipeline, but no run permission.
        assert client.post("/api/pipelines/alpha/run", json={}).status_code == 403
        assert client.get("/api/pipelines/alpha").status_code == 200
        # Invisible pipeline is refused outright.
        assert client.get("/api/pipelines/beta").status_code == 403
        assert client.post("/api/pipelines/beta/run", json={}).status_code == 403
        assert client.post("/api/pipelines/alpha/pause", json={}).status_code == 403
        assert client.delete("/api/pipelines/alpha").status_code == 403
        # Only admins manage users.
        assert client.get("/api/users").status_code == 403
        assert client.get("/api/settings/smtp").status_code == 403

    with TestClient(create_app(str(config_path))) as client:
        assert _sign_in(client, "runner", "runner-password-1")
        assert [item["pipeline_id"] for item in client.get("/api/pipelines").json()] == ["beta"]
        assert client.post("/api/pipelines/beta/run", json={}).status_code == 200
        assert client.post("/api/pipelines/alpha/run", json={}).status_code == 403
        # run does not imply edit
        assert client.post("/api/pipelines/beta/pause", json={}).status_code == 403

    with TestClient(create_app(str(config_path))) as client:
        assert _sign_in(client, "root", "admin-password-1")
        assert len(client.get("/api/pipelines").json()) == 3
        assert client.post("/api/pipelines/gamma/run", json={}).status_code == 200
        assert client.get("/api/users").status_code == 200
        assert client.get("/api/settings/smtp").status_code == 200


def test_run_visibility_follows_pipeline_permission(tmp_path: Path) -> None:
    """A run is only reachable by someone who may see its pipeline."""
    config_path = _project(tmp_path)
    service = PipelineService(config_path=config_path)
    service.create_user("root", "admin-password-1", role="admin")
    service.create_user("viewer", "viewer-password-1", permissions={"alpha": ["view"]})

    alpha_run = service.trigger_pipeline("alpha", wait=True)
    beta_run = service.trigger_pipeline("beta", wait=True)

    with TestClient(create_app(str(config_path))) as client:
        assert _sign_in(client, "viewer", "viewer-password-1")

        assert client.get(f"/api/runs/{alpha_run.run_id}").status_code == 200
        assert client.get(f"/api/runs/{beta_run.run_id}").status_code == 403
        assert client.get(f"/api/runs/{beta_run.run_id}/logs").status_code == 403
        assert client.post(f"/api/runs/{beta_run.run_id}/cancel", json={}).status_code == 403
        # Viewing is not running: a retry on an allowed pipeline is still denied.
        assert client.post(f"/api/runs/{alpha_run.run_id}/retry", json={"mode": "startover"}).status_code == 403

        visible = {item["id"] for item in client.get("/api/runs").json()}
        assert alpha_run.run_id in visible
        assert beta_run.run_id not in visible

        # The rendered pages agree with the API.
        assert client.get(f"/runs/{alpha_run.run_id}", follow_redirects=False).status_code == 200
        assert client.get(f"/runs/{beta_run.run_id}", follow_redirects=False).status_code == 403
        body = client.get("/runs").text
        assert alpha_run.run_id in body
        assert beta_run.run_id not in body


def test_sessions_and_logout(tmp_path: Path) -> None:
    """A session grants access until it is cleared."""
    config_path = _project(tmp_path)
    service = PipelineService(config_path=config_path)
    service.create_user("root", "admin-password-1", role="admin")

    with TestClient(create_app(str(config_path))) as client:
        assert client.get("/api/me").json()["authenticated"] is False

        assert _sign_in(client, "root", "admin-password-1")
        me = client.get("/api/me").json()
        assert me["authenticated"] is True
        assert me["role"] == "admin"

        client.get("/logout", follow_redirects=False)
        assert client.get("/api/me").json()["authenticated"] is False

        # A tampered cookie is rejected rather than trusted.
        client.cookies.set("piply_session", "bogus.signature")
        assert client.get("/api/me").json()["authenticated"] is False


def test_login_rejects_offsite_redirects(tmp_path: Path) -> None:
    """`next` cannot bounce a signed-in user to another host."""
    config_path = _project(tmp_path)
    service = PipelineService(config_path=config_path)
    service.create_user("root", "admin-password-1", role="admin")

    with TestClient(create_app(str(config_path))) as client:
        response = client.post(
            "/login",
            data={"username": "root", "password": "admin-password-1", "next": "//evil.example.com/"},
            follow_redirects=False,
        )
        assert response.headers["location"] == "/"


# --- Centralised SMTP --------------------------------------------------------


def test_smtp_settings_round_trip_without_exposing_the_password(tmp_path: Path) -> None:
    """Settings persist, and the password never comes back out."""
    service = PipelineService(config_path=_project(tmp_path))

    assert service.get_smtp_settings()["configured"] is False

    saved = service.save_smtp_settings(
        {
            "host": "smtp.example.com",
            "port": 2525,
            "username": "mailer",
            "password": "super-secret",
            "from_address": "piply@example.com",
            "use_tls": True,
        }
    )
    assert saved["host"] == "smtp.example.com"
    assert saved["port"] == 2525
    assert saved["configured"] is True
    assert saved["password_set"] is True
    assert "super-secret" not in str(saved)
    assert "password" not in saved

    # Editing another field with no password keeps the stored one.
    updated = service.save_smtp_settings({"host": "smtp2.example.com"})
    assert updated["host"] == "smtp2.example.com"
    assert updated["password_set"] is True


def test_email_task_falls_back_to_central_smtp(tmp_path: Path) -> None:
    """An email task with no SMTP block uses the central configuration."""
    from piply.core.mailer import load_smtp_settings, resolve_for_task
    from piply.core.models import TaskDefinition

    service = PipelineService(config_path=_project(tmp_path))
    service.save_smtp_settings({"host": "central.example.com", "port": 587, "username": "central"})
    central = load_smtp_settings(service.store)

    plain = TaskDefinition(task_id="notify", title="Notify", task_type="email")
    resolved = resolve_for_task(central, plain)
    assert resolved.host == "central.example.com"
    assert resolved.username == "central"

    # An inline host still wins, so existing per-pipeline SMTP keeps working.
    override = TaskDefinition(
        task_id="notify",
        title="Notify",
        task_type="email",
        smtp_host="own.example.com",
        smtp_port=1025,
    )
    resolved_override = resolve_for_task(central, override)
    assert resolved_override.host == "own.example.com"
    assert resolved_override.port == 1025


def test_email_task_without_any_smtp_fails_clearly(tmp_path: Path) -> None:
    """With nothing configured, the task fails with an actionable message."""
    config_path = tmp_path / "piply.yaml"
    (tmp_path / "workspace").mkdir(exist_ok=True)
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Mail Test",
                "workspace: workspace",
                "pipelines:",
                "  notify_flow:",
                "    tasks:",
                "      notify:",
                "        type: email",
                "        to: [ops@example.com]",
                "        subject: Hello",
            ]
        ),
        encoding="utf-8",
    )
    service = PipelineService(config_path=config_path)
    run = service.trigger_pipeline("notify_flow", wait=True)
    record, _, logs = service.get_run(run.run_id)

    assert record.status == "failed"
    assert any("No SMTP server is configured" in line.message for line in logs)


def test_pipeline_notify_block_is_parsed(tmp_path: Path) -> None:
    """`notify:` accepts a bare list and an explicit mapping."""
    from piply.core.loader import ConfigError, load_project

    (tmp_path / "workspace").mkdir(exist_ok=True)
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Notify Test",
                "workspace: workspace",
                "pipelines:",
                "  shorthand:",
                "    notify: [oncall@example.com]",
                "    tasks:",
                "      main: {type: cli, command: echo hi}",
                "  explicit:",
                "    notify:",
                "      on_failure: [oncall@example.com]",
                "      on_success: [team@example.com]",
                "    tasks:",
                "      main: {type: cli, command: echo hi}",
            ]
        ),
        encoding="utf-8",
    )
    project = load_project(config_path)

    # A bare list means "tell me when it breaks", which is what people want.
    assert project.pipelines["shorthand"].notify_on_failure == ("oncall@example.com",)
    assert project.pipelines["shorthand"].notify_on_success == ()
    assert project.pipelines["explicit"].notify_on_failure == ("oncall@example.com",)
    assert project.pipelines["explicit"].notify_on_success == ("team@example.com",)

    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Bad Notify",
                "workspace: workspace",
                "pipelines:",
                "  broken:",
                "    notify: [not-an-address]",
                "    tasks:",
                "      main: {type: cli, command: echo hi}",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError) as excinfo:
        load_project(config_path)
    assert "invalid email address" in str(excinfo.value)
