"""First-run database setup.

Without `PIPLY_DATABASE` Piply used to fall back silently to a file under the
config directory. In a container that is the writable layer, so the choice was
being made by default and only discovered when a redeploy wiped the history.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from piply.api.app import create_app
from piply.api.routes.setup import validate_database_choice

CONFIG = "\n".join(
    [
        'version: "1"',
        "title: Setup Test",
        "workspace: .",
        "pipelines:",
        "  demo:",
        "    tasks:",
        "      t: {type: cli, command: echo hi}",
    ]
)


@pytest.fixture()
def unconfigured(tmp_path: Path, monkeypatch):
    """Yield a client for an install that has not chosen a database yet."""
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(CONFIG, encoding="utf-8")
    monkeypatch.delenv("PIPLY_DATABASE", raising=False)
    monkeypatch.delenv("PIPLY_AUTH_ENABLED", raising=False)
    monkeypatch.chdir(tmp_path)
    with TestClient(create_app(str(config_path))) as client:
        yield client, tmp_path


def test_an_unconfigured_install_lands_on_setup(unconfigured) -> None:
    """Rendered pages redirect until a database is chosen."""
    client, _tmp = unconfigured
    assert client.app.state.settings.database_configured is False

    for path in ["/", "/pipelines", "/runs", "/settings"]:
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 303, path
        assert response.headers["location"] == "/setup"

    page = client.get("/setup")
    assert page.status_code == 200
    assert "SQLite file" in page.text
    assert "PostgreSQL" in page.text


def test_api_callers_are_not_redirected_to_a_html_page(unconfigured) -> None:
    """A script deserves a status code, not a login form or a setup page."""
    client, _tmp = unconfigured
    response = client.get("/api/pipelines", follow_redirects=False)
    assert response.status_code != 303
    # /health must keep answering so a container probe does not fail during setup.
    assert client.get("/health").status_code == 200


def test_invalid_choices_are_refused_before_anything_is_written(unconfigured) -> None:
    """Validation opens the database, so a bad value cannot be saved."""
    client, tmp_path = unconfigured

    cases = [
        ({"backend": "postgres", "dsn": ""}, "Enter a PostgreSQL connection URL"),
        ({"backend": "postgres", "dsn": "mysql://user:pw@host/db"}, "not a PostgreSQL URL"),
        ({"backend": "sqlite", "sqlite_path": "postgresql://x"}, "file path for SQLite"),
        ({"backend": "nonsense"}, "Choose SQLite or PostgreSQL"),
    ]
    for payload, expected in cases:
        response = client.post("/setup", data=payload, follow_redirects=False)
        assert response.status_code == 400, payload
        assert expected in response.text, payload

    assert not (tmp_path / ".env").exists(), "a rejected choice must not be persisted"
    assert client.app.state.settings.database_configured is False


def test_an_unreachable_database_is_rejected(tmp_path: Path) -> None:
    """A DSN that parses but cannot be opened is caught now, not at next restart."""
    with pytest.raises(ValueError, match="Could not open that database"):
        validate_database_choice(
            "postgres",
            "",
            # Port 1 is reserved and refuses instantly.
            "postgresql://piply:piply@127.0.0.1:1/piply",
            base_dir=tmp_path,
        )


def test_choosing_sqlite_persists_and_continues_into_piply(unconfigured) -> None:
    """The happy path: validate, save, swap the runtime, offer the first admin."""
    client, tmp_path = unconfigured

    response = client.post(
        "/setup",
        data={"backend": "sqlite", "sqlite_path": "data/piply.db"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    # An install with no accounts is offered the optional first-admin step.
    assert response.headers["location"] == "/setup/admin"

    env_file = tmp_path / ".env"
    assert env_file.exists()
    assert "PIPLY_DATABASE=" in env_file.read_text(encoding="utf-8")
    assert (tmp_path / "data" / "piply.db").exists()

    # The running process now uses it, without a restart.
    assert client.app.state.settings.database_configured is True
    assert "data" in client.app.state.service.database_location
    assert client.get("/", follow_redirects=False).status_code == 200
    assert client.get("/pipelines", follow_redirects=False).status_code == 200
    # And the runtime it swapped in actually works.
    assert client.post("/api/pipelines/demo/run", json={}).status_code == 200


def test_setup_cannot_repoint_a_configured_install(unconfigured) -> None:
    """Otherwise anyone who can reach the page could swap a live database."""
    client, tmp_path = unconfigured
    client.post("/setup", data={"backend": "sqlite", "sqlite_path": "data/piply.db"}, follow_redirects=False)
    original = (tmp_path / ".env").read_text(encoding="utf-8")

    assert client.get("/setup", follow_redirects=False).status_code == 303
    hijack = client.post(
        "/setup",
        data={"backend": "sqlite", "sqlite_path": "elsewhere.db"},
        follow_redirects=False,
    )
    assert hijack.status_code == 303
    assert hijack.headers["location"] == "/"
    assert (tmp_path / ".env").read_text(encoding="utf-8") == original
    assert not (tmp_path / "elsewhere.db").exists()


def test_setup_form_rejects_wrong_content_types(unconfigured) -> None:
    """The form parser is hand-rolled, so its guards are pinned."""
    client, _tmp = unconfigured
    assert client.post("/setup", json={"backend": "sqlite"}, follow_redirects=False).status_code == 415
    oversized = client.post(
        "/setup",
        content=b"backend=sqlite&sqlite_path=" + b"a" * 9000,
        headers={"content-type": "application/x-www-form-urlencoded"},
        follow_redirects=False,
    )
    assert oversized.status_code == 413


SCHEDULED_CONFIG = "\n".join(
    [
        'version: "1"',
        "title: Scheduled Setup Test",
        "workspace: .",
        "pipelines:",
        "  demo:",
        "    schedule:",
        "      every: 15m",
        "    tasks:",
        "      t: {type: cli, command: echo hi}",
    ]
)


def test_a_scheduled_pipeline_does_not_defeat_setup(tmp_path: Path, monkeypatch) -> None:
    """`piply init` generates scheduled pipelines, which used to hide the setup page.

    The scheduler fired within seconds of boot, created a run, and the database
    stopped looking empty — so a genuinely fresh install went straight to the
    dashboard. The decision is now made once at startup, before the scheduler.
    """
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(SCHEDULED_CONFIG, encoding="utf-8")
    monkeypatch.delenv("PIPLY_DATABASE", raising=False)
    monkeypatch.chdir(tmp_path)

    with TestClient(create_app(str(config_path))) as client:
        assert client.app.state.setup_required is True

        # The scheduler is deliberately not started: writing run history into a
        # database the operator is about to replace would be wasted, and it is
        # what made the install stop looking fresh.
        time.sleep(2)
        assert client.app.state.service.store.row_counts()["runs"] == 0
        assert client.get("/", follow_redirects=False).status_code == 303

        # Finishing setup releases the scheduler.
        done = client.post(
            "/setup",
            data={"backend": "sqlite", "sqlite_path": "data/piply.db"},
            follow_redirects=False,
        )
        assert done.status_code == 303
        assert client.app.state.setup_required is False
        assert client.app.state.scheduler.is_running is True


def test_setup_continues_into_the_first_admin_step(unconfigured) -> None:
    """After choosing a database, offer to create the first account."""
    client, tmp_path = unconfigured

    response = client.post(
        "/setup",
        data={"backend": "sqlite", "sqlite_path": "data/piply.db"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/setup/admin"
    page = client.get("/setup/admin")
    assert page.status_code == 200
    assert "Create the first admin" in page.text
    # The step is optional, so there has to be a visible way past it.
    assert "Skip for now" in page.text


def test_the_first_admin_is_created_and_signed_in(unconfigured) -> None:
    """Creating the first account must not lock out the page that created it."""
    client, _ = unconfigured
    client.post("/setup", data={"backend": "sqlite", "sqlite_path": "data/piply.db"})

    response = client.post(
        "/setup/admin",
        data={"username": "root", "password": "correct-horse", "confirm": "correct-horse"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/"
    service = client.app.state.service
    created = service.list_users()
    assert [user.username for user in created] == ["root"]
    assert created[0].role == "admin"
    # Authentication is now on, and this session is already signed in as the
    # account it just created — no credentials on the request.
    assert service.auth_required is True
    assert client.get("/api/pipelines").status_code == 200


@pytest.mark.parametrize(
    ("form", "message"),
    [
        ({"username": "", "password": "correct-horse", "confirm": "correct-horse"}, "Enter a username."),
        ({"username": "root", "password": "short", "confirm": "short"}, "at least 8 characters"),
        ({"username": "root", "password": "correct-horse", "confirm": "different-one"}, "do not match"),
    ],
)
def test_first_admin_input_is_validated(unconfigured, form, message) -> None:
    """A rejected form re-renders with the reason and creates nothing."""
    client, _ = unconfigured
    client.post("/setup", data={"backend": "sqlite", "sqlite_path": "data/piply.db"})

    response = client.post("/setup/admin", data=form, follow_redirects=False)

    assert response.status_code == 400
    assert message in response.text
    assert client.app.state.service.list_users() == []


def test_the_first_admin_step_closes_once_an_account_exists(unconfigured) -> None:
    """The page is a bootstrap, not a standing way to mint admins."""
    client, _ = unconfigured
    client.post("/setup", data={"backend": "sqlite", "sqlite_path": "data/piply.db"})
    client.post(
        "/setup/admin",
        data={"username": "root", "password": "correct-horse", "confirm": "correct-horse"},
    )
    client.cookies.clear()

    assert client.get("/setup/admin", follow_redirects=False).status_code == 303
    hijack = client.post(
        "/setup/admin",
        data={"username": "intruder", "password": "correct-horse", "confirm": "correct-horse"},
        follow_redirects=False,
    )

    assert hijack.status_code == 303
    assert [user.username for user in client.app.state.service.list_users()] == ["root"]
