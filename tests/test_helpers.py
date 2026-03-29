from __future__ import annotations

import os
import re
import sqlite3
import time
from pathlib import Path

from fastapi.testclient import TestClient

from piply.api.app import create_app
from piply.core.service import PipelineService


def test_multitask_run_triggers_downstream_pipeline(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "extract.py").write_text("print('extract complete')", encoding="utf-8")
    (workspace / "report.py").write_text("print('report complete')", encoding="utf-8")

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Runtime Test",
                "workspace: workspace",
                "pipelines:",
                "  extract_flow:",
                "    triggers_on_success:",
                "      - report_flow",
                "    tasks:",
                "      extract:",
                "        type: python",
                "        path: extract.py",
                "      validate:",
                "        type: cli",
                "        command: python -c \"print('validated')\"",
                "        depends_on: [extract]",
                "  report_flow:",
                "    tasks:",
                "      build_report:",
                "        type: python",
                "        path: report.py",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    root_run = service.trigger_pipeline("extract_flow", wait=True)
    stored_run, _, root_logs = service.get_run(root_run.run_id)

    assert stored_run.status == "success"
    assert any("extract complete" in line.message for line in root_logs)

    downstream_runs = []
    for _ in range(20):
        downstream_runs = service.list_runs(pipeline_id="report_flow")
        if downstream_runs:
            break
        time.sleep(0.2)

    assert downstream_runs
    assert downstream_runs[0].pipeline_id == "report_flow"


def test_run_api_returns_newest_logs_first_with_time_label(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "job.py").write_text(
        "\n".join(
            [
                "import time",
                "print('first line')",
                "time.sleep(0.05)",
                "print('second line')",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: API Test",
                "workspace: workspace",
                "pipelines:",
                "  job:",
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
    os.environ["PIPLY_CONFIG"] = str(config_path)
    os.environ["PIPLY_DATABASE"] = str(tmp_path / "api.db")
    try:
        service = PipelineService(config_path=config_path, database_path=tmp_path / "api.db")
        run = service.trigger_pipeline("job", wait=True)

        app = create_app(str(config_path))
        with TestClient(app) as client:
            response = client.get(f"/api/runs/{run.run_id}")
    finally:
        if previous_config is None:
            os.environ.pop("PIPLY_CONFIG", None)
        else:
            os.environ["PIPLY_CONFIG"] = previous_config
        if previous_database is None:
            os.environ.pop("PIPLY_DATABASE", None)
        else:
            os.environ["PIPLY_DATABASE"] = previous_database

    assert response.status_code == 200
    payload = response.json()
    assert payload["logs"][0]["message"] == "Run completed successfully." or payload["logs"][0]["message"] == "Pipeline completed successfully."
    assert re.match(r"^\d{2}:\d{2}:\d{2}\.\d{3}$", payload["logs"][0]["time_label"])


def test_retry_run_resume_reuses_successful_tasks(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "extract.py").write_text("print('extract complete')", encoding="utf-8")
    (workspace / "flaky.py").write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "",
                "flag = Path('flaky.ok')",
                "if flag.exists():",
                "    print('flaky recovered')",
                "else:",
                "    flag.write_text('ok', encoding='utf-8')",
                "    print('flaky failed once')",
                "    raise SystemExit(1)",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Retry Test",
                "workspace: workspace",
                "pipelines:",
                "  retry_flow:",
                "    tasks:",
                "      extract:",
                "        type: python",
                "        path: extract.py",
                "      flaky_step:",
                "        type: python",
                "        path: flaky.py",
                "        depends_on: [extract]",
                "      publish_manifest:",
                "        type: cli",
                "        command: python -c \"print('published')\"",
                "        depends_on: [flaky_step]",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    failed_run = service.trigger_pipeline("retry_flow", wait=True)
    failed_record, failed_tasks, _ = service.get_run(failed_run.run_id)

    assert failed_record.status == "failed"
    assert {task.task_id: task.status for task in failed_tasks} == {
        "extract": "success",
        "flaky_step": "failed",
        "publish_manifest": "skipped",
    }

    resumed_run = service.retry_run(failed_run.run_id, mode="resume", wait=True)
    resumed_record, resumed_tasks, resumed_logs = service.get_run(resumed_run.run_id)

    assert resumed_record.status == "success"
    assert resumed_record.retry_of == failed_run.run_id
    assert resumed_record.retry_mode == "resume"
    assert all(task.status == "success" for task in resumed_tasks)
    assert any("Reused successful result" in line.message for line in resumed_logs)
    assert any("flaky recovered" in line.message for line in resumed_logs)


def test_manual_run_creates_new_ids_against_legacy_schema(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "job.py").write_text("print('job complete')", encoding="utf-8")

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Legacy Schema Test",
                "workspace: workspace",
                "pipelines:",
                "  job:",
                "    tasks:",
                "      main:",
                "        type: python",
                "        path: job.py",
            ]
        ),
        encoding="utf-8",
    )

    database_path = tmp_path / "legacy.db"
    connection = sqlite3.connect(database_path)
    connection.executescript(
        """
        CREATE TABLE runs (
            id TEXT PRIMARY KEY,
            pipeline_id TEXT NOT NULL,
            pipeline_title TEXT NOT NULL,
            status TEXT NOT NULL,
            trigger TEXT NOT NULL,
            command TEXT NOT NULL,
            script_path TEXT NOT NULL,
            working_dir TEXT NOT NULL,
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            scheduled_for TEXT,
            exit_code INTEGER,
            error TEXT
        );
        CREATE TABLE task_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            title TEXT NOT NULL,
            task_type TEXT NOT NULL,
            status TEXT NOT NULL,
            position INTEGER NOT NULL,
            command_preview TEXT NOT NULL,
            depends_on TEXT,
            started_at TEXT,
            finished_at TEXT,
            exit_code INTEGER,
            error TEXT,
            UNIQUE(run_id, task_id)
        );
        CREATE TABLE logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            stream TEXT NOT NULL,
            message TEXT NOT NULL
        );
        CREATE TABLE pipeline_overrides (
            pipeline_id TEXT PRIMARY KEY,
            paused INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )
    connection.commit()
    connection.close()

    service = PipelineService(config_path=config_path, database_path=database_path)
    first_run = service.trigger_pipeline("job", wait=False)
    second_run = service.trigger_pipeline("job", wait=False)

    assert first_run.run_id != second_run.run_id


def test_same_day_schedule_uses_relative_next_run_label(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "job.py").write_text("print('job')", encoding="utf-8")

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Schedule Label Test",
                "workspace: workspace",
                "pipelines:",
                "  job:",
                "    schedule:",
                "      every: 5m",
                "    tasks:",
                "      main:",
                "        type: python",
                "        path: job.py",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    summary = service.list_pipelines()[0]

    assert summary.next_run_label.startswith("in ")
