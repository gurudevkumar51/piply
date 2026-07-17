from __future__ import annotations

import base64
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from piply.api.app import create_app
from piply.core.service import PipelineService


def _basic_auth_header(username: str, password: str) -> dict[str, str]:
    token = base64.b64encode(f"{username}:{password}".encode()).decode("ascii")
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

    service = PipelineService(config_path=config_path,
                              database_path=tmp_path / "runs.db")
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

    assert record.status == "interrupted"
    assert task_runs[0].status == "interrupted"
    assert any("heartbeat timeout recovery" in line.message for line in logs)


def test_app_shutdown_interrupts_active_runs(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "slow.py").write_text(
        "\n".join(
            [
                "import time",
                "for index in range(20):",
                "    print(f'tick-{index}')",
                "    time.sleep(0.2)",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Shutdown Test",
                "workspace: workspace",
                "pipelines:",
                "  slow_flow:",
                "    tasks:",
                "      main:",
                "        type: python",
                "        path: slow.py",
            ]
        ),
        encoding="utf-8",
    )

    database_path = tmp_path / "shutdown.db"
    previous_config = os.environ.get("PIPLY_CONFIG")
    previous_database = os.environ.get("PIPLY_DATABASE")
    os.environ["PIPLY_CONFIG"] = str(config_path)
    os.environ["PIPLY_DATABASE"] = str(database_path)

    try:
        app = create_app(str(config_path))
        with TestClient(app) as client:
            run_response = client.post("/api/pipelines/slow_flow/run", json={})
            assert run_response.status_code == 200
            run_id = run_response.json()["id"]

            detail = None
            for _ in range(30):
                detail = client.get(f"/api/runs/{run_id}")
                payload = detail.json()
                status = payload["run"]["status"]
                task_statuses = [item["status"]
                                 for item in payload["task_runs"]]
                if status == "running" and "running" in task_statuses:
                    break
                time.sleep(0.1)
            assert detail is not None
            payload = detail.json()
            assert payload["run"]["status"] == "running"
            assert "running" in [item["status"]
                                 for item in payload["task_runs"]]
    finally:
        if previous_config is None:
            os.environ.pop("PIPLY_CONFIG", None)
        else:
            os.environ["PIPLY_CONFIG"] = previous_config
        if previous_database is None:
            os.environ.pop("PIPLY_DATABASE", None)
        else:
            os.environ["PIPLY_DATABASE"] = previous_database

    service = PipelineService(config_path=config_path,
                              database_path=database_path)
    record, task_runs, logs = service.get_run(run_id)

    assert record.status == "interrupted"
    assert task_runs[0].status == "interrupted"
    assert any("Piply service shut down" in line.message for line in logs)


def test_auth_middleware_supports_basic_for_ui_and_bearer_for_api(
    tmp_path: Path,
) -> None:
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
            ui_authorized = client.get(
                "/", headers=_basic_auth_header("demo", "secret"))
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


def test_run_api_includes_upcoming_runs_and_pipeline_run_overrides(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: API Override Test",
                "workspace: workspace",
                "pipelines:",
                "  cli_flow:",
                "    schedule:",
                "      every: 5m",
                "    tasks:",
                "      command_task:",
                "        type: cli",
                "        command: python -c \"print('original')\"",
            ]
        ),
        encoding="utf-8",
    )

    previous_config = os.environ.get("PIPLY_CONFIG")
    previous_database = os.environ.get("PIPLY_DATABASE")
    os.environ["PIPLY_CONFIG"] = str(config_path)
    os.environ["PIPLY_DATABASE"] = str(tmp_path / "api.db")
    try:
        app = create_app(str(config_path))
        with TestClient(app) as client:
            run_response = client.post(
                "/api/pipelines/cli_flow/run",
                json={"command_overrides": {
                    "command_task": "python -c \"print('override-from-api')\""}},
            )
            assert run_response.status_code == 200
            run_id = run_response.json()["id"]

            for _ in range(30):
                detail = client.get(f"/api/runs/{run_id}")
                if detail.json()["run"]["status"] == "success":
                    break
            payload = detail.json()
            payload = wait_for_run_completion(client, run_id)
    finally:
        if previous_config is None:
            os.environ.pop("PIPLY_CONFIG", None)
        else:
            os.environ["PIPLY_CONFIG"] = previous_config
        if previous_database is None:
            os.environ.pop("PIPLY_DATABASE", None)
        else:
            os.environ["PIPLY_DATABASE"] = previous_database

    assert payload["run"]["status"] == "success"
    assert payload["upcoming_runs"]
    assert any("override-from-api" in line["message"]
               for line in payload["logs"])


def test_pipeline_run_api_still_accepts_empty_body(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: API Empty Body Test",
                "workspace: workspace",
                "pipelines:",
                "  cli_flow:",
                "    tasks:",
                "      command_task:",
                "        type: cli",
                "        command: python -c \"print('no-body-trigger')\"",
            ]
        ),
        encoding="utf-8",
    )

    previous_config = os.environ.get("PIPLY_CONFIG")
    previous_database = os.environ.get("PIPLY_DATABASE")
    os.environ["PIPLY_CONFIG"] = str(config_path)
    os.environ["PIPLY_DATABASE"] = str(tmp_path / "empty-body.db")
    try:
        app = create_app(str(config_path))
        with TestClient(app) as client:
            run_response = client.post("/api/pipelines/cli_flow/run")
            assert run_response.status_code == 200
            run_id = run_response.json()["id"]

            for _ in range(30):
                detail = client.get(f"/api/runs/{run_id}")
                if detail.json()["run"]["status"] == "success":
                    break
            payload = detail.json()
            payload = wait_for_run_completion(client, run_id)
    finally:
        if previous_config is None:
            os.environ.pop("PIPLY_CONFIG", None)
        else:
            os.environ["PIPLY_CONFIG"] = previous_config
        if previous_database is None:
            os.environ.pop("PIPLY_DATABASE", None)
        else:
            os.environ["PIPLY_DATABASE"] = previous_database
    # payload = wait_for_run_completion(client, run_id)
    assert payload["run"]["status"] == "success"
    assert any("no-body-trigger" in line["message"]
               for line in payload["logs"])


def test_pipeline_task_run_api_executes_selected_task_scope(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "extract.py").write_text("print('extract-ok')", encoding="utf-8")
    (workspace / "publish.py").write_text("print('publish-ok')", encoding="utf-8")

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Task Run API Test",
                "workspace: workspace",
                "pipelines:",
                "  task_flow:",
                "    tasks:",
                "      extract:",
                "        type: python",
                "        path: extract.py",
                "      publish:",
                "        type: python",
                "        path: publish.py",
                "        depends_on: [extract]",
            ]
        ),
        encoding="utf-8",
    )

    previous_config = os.environ.get("PIPLY_CONFIG")
    previous_database = os.environ.get("PIPLY_DATABASE")
    os.environ["PIPLY_CONFIG"] = str(config_path)
    os.environ["PIPLY_DATABASE"] = str(tmp_path / "task-scope.db")
    try:
        app = create_app(str(config_path))
        with TestClient(app) as client:
            run_response = client.post(
                "/api/pipelines/task_flow/tasks/publish/run", json={})
            assert run_response.status_code == 200
            run_id = run_response.json()["id"]

            for _ in range(30):
                detail = client.get(f"/api/runs/{run_id}")
                if detail.json()["run"]["status"] == "success":
                    break
            payload = detail.json()
            payload = wait_for_run_completion(client, run_id)
    finally:
        if previous_config is None:
            os.environ.pop("PIPLY_CONFIG", None)
        else:
            os.environ["PIPLY_CONFIG"] = previous_config
        if previous_database is None:
            os.environ.pop("PIPLY_DATABASE", None)
        else:
            os.environ["PIPLY_DATABASE"] = previous_database

    task_ids = [task["task_id"] for task in payload["task_runs"]]

    # payload = wait_for_run_completion(client, run_id)

    assert payload["run"]["status"] == "success"
    assert task_ids == ["extract", "publish"]
    assert any("publish-ok" in line["message"] for line in payload["logs"])


def wait_for_run_completion(client, run_id, timeout=5.0):
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        response = client.get(f"/api/runs/{run_id}")
        payload = response.json()

        if payload["run"]["status"] in {
            "success",
            "failed",
            "cancelled",
            "interrupted",
        }:
            return payload

        time.sleep(0.1)

    raise AssertionError(
        f"Run {run_id} did not reach a terminal state within {timeout} seconds.")
