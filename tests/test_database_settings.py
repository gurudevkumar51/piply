"""Admin-only database management from the settings page.

Moving the metadata store used to mean editing `.env` and restarting. Doing it
from the UI is only safe because the target is opened before anything is saved
and because the cases that cannot work are refused rather than half-applied.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from piply.api.app import create_app

CONFIG = "\n".join(
    [
        'version: "1"',
        "title: Database Settings",
        "workspace: .",
        "pipelines:",
        "  quick:",
        "    tasks:",
        "      t: {type: cli, command: echo hi}",
        "  slow:",
        "    tasks:",
        "      t:",
        "        type: python",
        "        path: slow.py",
        "        function: work",
    ]
)

SLOW_TASK = "\n".join(
    [
        "import time",
        "",
        "",
        "def work():",
        "    time.sleep(6)",
    ]
)

ADMIN = ("root", "root-password")
MEMBER = ("mallory", "mallory-password")


def _wait_until_idle(client, timeout: float = 15.0) -> None:
    """Block until no run is in flight, so a switch is allowed."""
    store = client.app.state.service.store
    deadline = time.monotonic() + timeout
    while store.count_running_runs() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert store.count_running_runs() == 0, "runs did not finish in time"


@pytest.fixture()
def configured(tmp_path: Path, monkeypatch):
    """An install whose database came from `.env`, so it can be changed here.

    The shared conftest fixture sets `PIPLY_DATABASE` as a real environment
    variable, which deliberately locks the panel — the process environment wins
    over `.env`, so saving could not take effect. These tests need the ordinary
    case instead.
    """
    (tmp_path / "piply.yaml").write_text(CONFIG, encoding="utf-8")
    (tmp_path / "slow.py").write_text(SLOW_TASK, encoding="utf-8")
    (tmp_path / ".env").write_text(f"PIPLY_DATABASE={tmp_path / 'live.db'}\n", encoding="utf-8")
    monkeypatch.delenv("PIPLY_DATABASE", raising=False)
    monkeypatch.chdir(tmp_path)

    with TestClient(create_app(str(tmp_path / "piply.yaml"))) as client:
        service = client.app.state.service
        service.create_user("root", "root-password", role="admin")
        service.create_user("mallory", "mallory-password", permissions={"quick": "view,run"})
        yield client, tmp_path


def test_only_admins_can_see_or_change_the_database(configured) -> None:
    """The store holds every run and account, so this is admin territory."""
    client, _ = configured
    body = {"backend": "sqlite", "sqlite_path": "elsewhere.db"}

    assert client.get("/api/settings/database").status_code == 401
    assert client.put("/api/settings/database", json=body).status_code == 401
    assert client.get("/api/settings/database", auth=MEMBER).status_code == 403

    refused = client.put("/api/settings/database", json=body, auth=MEMBER)
    assert refused.status_code == 403
    assert "administrators" in refused.json()["detail"]

    assert client.get("/api/settings/database", auth=ADMIN).status_code == 200


def test_the_database_panel_is_admin_only_on_the_settings_page(configured) -> None:
    """A non-admin must not even be offered the form."""
    client, _ = configured

    assert "Test and switch" in client.get("/settings", auth=ADMIN).text
    assert "Test and switch" not in client.get("/settings", auth=MEMBER).text


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({"backend": "postgres", "dsn": "not-a-url"}, "not a PostgreSQL URL"),
        ({"backend": "postgres", "dsn": ""}, "Enter a PostgreSQL connection URL"),
        ({"backend": "sqlite", "sqlite_path": "https://example.com/db"}, "not a URL"),
        ({"backend": "mysql"}, "Choose SQLite or PostgreSQL"),
    ],
)
def test_a_bad_choice_is_refused_before_anything_is_saved(configured, body, message) -> None:
    """Validation opens the database, so mistakes surface here, not at restart."""
    client, tmp_path = configured
    before = (tmp_path / ".env").read_text(encoding="utf-8")

    response = client.put("/api/settings/database", json=body, auth=ADMIN)

    assert response.status_code == 400
    assert message in response.json()["detail"]
    assert (tmp_path / ".env").read_text(encoding="utf-8") == before


def test_switching_to_the_current_database_is_refused(configured) -> None:
    """A no-op switch would copy nothing and look like it worked."""
    client, tmp_path = configured

    response = client.put(
        "/api/settings/database",
        json={"backend": "sqlite", "sqlite_path": str(tmp_path / "live.db")},
        auth=ADMIN,
    )

    assert response.status_code == 400
    assert "already the configured database" in response.json()["detail"]


def test_switching_moves_the_history_and_takes_effect_immediately(configured) -> None:
    """The whole point: no file editing, no restart, nothing left behind."""
    client, tmp_path = configured
    client.post("/api/pipelines/quick/run", json={}, auth=ADMIN)
    _wait_until_idle(client)

    response = client.put(
        "/api/settings/database",
        json={"backend": "sqlite", "sqlite_path": "moved.db", "migrate": True},
        auth=ADMIN,
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["migrated"]["runs"] == 1
    assert payload["migrated"]["users"] == 2

    # Applied to the running process, and persisted for the next start.
    service = client.app.state.service
    assert Path(service.database_location).name == "moved.db"
    assert "moved.db" in (tmp_path / ".env").read_text(encoding="utf-8")
    assert client.app.state.scheduler.is_running

    # The accounts came too, so the admin can still get in.
    assert client.get("/api/pipelines", auth=ADMIN).status_code == 200
    assert len(service.list_runs(limit=5)) == 1
    # The old database is left alone, so it stays a rollback.
    assert (tmp_path / "live.db").exists()


def test_switching_is_refused_while_a_run_is_in_flight(configured) -> None:
    """An in-flight run holds the old store and would be stranded by a switch.

    It would finish writing to the database being left behind, while the new one
    kept the half-copied row — a run stuck at `running` for ever, and the real
    result in a file nothing reads.
    """
    client, tmp_path = configured
    client.post("/api/pipelines/slow/run", json={}, auth=ADMIN)

    store = client.app.state.service.store
    deadline = time.monotonic() + 10
    while store.count_running_runs() == 0 and time.monotonic() < deadline:
        time.sleep(0.05)
    assert store.count_running_runs() > 0, "the slow run never started"

    response = client.put(
        "/api/settings/database",
        json={"backend": "sqlite", "sqlite_path": "moved.db", "migrate": True},
        auth=ADMIN,
    )

    assert response.status_code == 409
    assert "still in progress" in response.json()["detail"]
    # Refused before validation, so the target was not even created.
    assert not (tmp_path / "moved.db").exists()


def test_the_panel_is_locked_when_the_database_comes_from_the_environment(tmp_path: Path, monkeypatch) -> None:
    """`PIPLY_DATABASE` in the process environment overrides `.env`.

    Saving would write a file nothing reads, so Piply refuses rather than
    appearing to succeed.
    """
    (tmp_path / "piply.yaml").write_text(CONFIG, encoding="utf-8")
    monkeypatch.setenv("PIPLY_DATABASE", str(tmp_path / "from-env.db"))
    monkeypatch.chdir(tmp_path)

    with TestClient(create_app(str(tmp_path / "piply.yaml"))) as client:
        client.app.state.service.create_user("root", "root-password", role="admin")

        assert client.get("/api/settings/database", auth=ADMIN).json()["env_managed"] is True
        response = client.put(
            "/api/settings/database",
            json={"backend": "sqlite", "sqlite_path": "moved.db"},
            auth=ADMIN,
        )

        assert response.status_code == 409
        assert "PIPLY_DATABASE is set" in response.json()["detail"]
        # And the form is not offered in the first place.
        assert "Test and switch" not in client.get("/settings", auth=ADMIN).text
