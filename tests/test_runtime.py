from __future__ import annotations

import base64
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from piply.api.app import create_app
from piply.core.service import PipelineService


def _basic_auth_header(username: str, password: str) -> dict[str, str]:
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def test_service_reconciles_stale_running_runs(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "job.py").write_text("print('job')", encoding="utf-8")

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Stale Run Test",
                "workspace: workspace",
                "pipelines:",
                "  job_flow:",
                "    tasks:",
                "      main:",
                "        type: python",
                "        path: job.py",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    pipeline = service.get_pipeline("job_flow")
    run = service.store.create_run(pipeline, trigger="manual")
    service.store.mark_running(run.run_id)
    service.store.mark_task_running(run.run_id, "main")

    stale_at = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
    connection = sqlite3.connect(service.database_path)
    connection.execute(
        "UPDATE runs SET heartbeat_at = ?, started_at = ? WHERE id = ?",
        (stale_at, stale_at, run.run_id),
    )
    connection.commit()
    connection.close()

    record, task_runs, logs = service.get_run(run.run_id)

    assert record.status == "failed"
    assert task_runs[0].status == "failed"
    assert any("heartbeat timeout" in line.message for line in logs)


def test_auth_middleware_supports_basic_for_ui_and_bearer_for_api(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "job.py").write_text("print('job')", encoding="utf-8")

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Auth Test",
                "workspace: workspace",
                "pipelines:",
                "  job_flow:",
                "    tasks:",
                "      main:",
                "        type: python",
                "        path: job.py",
            ]
        ),
        encoding="utf-8",
    )

    previous_config = os.environ.get("PIPLY_CONFIG")
    previous_database = os.environ.get("PIPLY_DATABASE")
    previous_auth_enabled = os.environ.get("PIPLY_AUTH_ENABLED")
    previous_auth_username = os.environ.get("PIPLY_AUTH_USERNAME")
    previous_auth_password = os.environ.get("PIPLY_AUTH_PASSWORD")
    previous_api_token = os.environ.get("PIPLY_API_TOKEN")

    os.environ["PIPLY_CONFIG"] = str(config_path)
    os.environ["PIPLY_DATABASE"] = str(tmp_path / "auth.db")
    os.environ["PIPLY_AUTH_ENABLED"] = "true"
    os.environ["PIPLY_AUTH_USERNAME"] = "demo"
    os.environ["PIPLY_AUTH_PASSWORD"] = "secret"
    os.environ["PIPLY_API_TOKEN"] = "token-123"

    try:
        app = create_app(str(config_path))
        with TestClient(app) as client:
            ui_unauthorized = client.get("/")
            ui_authorized = client.get("/", headers=_basic_auth_header("demo", "secret"))
            api_bearer = client.get(
                "/api/dashboard",
                headers={"Authorization": "Bearer token-123"},
            )
    finally:
        for key, value in [
            ("PIPLY_CONFIG", previous_config),
            ("PIPLY_DATABASE", previous_database),
            ("PIPLY_AUTH_ENABLED", previous_auth_enabled),
            ("PIPLY_AUTH_USERNAME", previous_auth_username),
            ("PIPLY_AUTH_PASSWORD", previous_auth_password),
            ("PIPLY_API_TOKEN", previous_api_token),
        ]:
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    assert ui_unauthorized.status_code == 401
    assert ui_authorized.status_code == 200
    assert api_bearer.status_code == 200
