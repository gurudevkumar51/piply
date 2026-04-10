from __future__ import annotations

from pathlib import Path

from piply.core.loader import load_project


def test_load_project_parses_multitask_pipeline_and_trigger(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "extract.py").write_text("print('extract')", encoding="utf-8")
    (workspace / "report.py").write_text("print('report')", encoding="utf-8")

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Test Workspace",
                "workspace: workspace",
                "pipelines:",
                "  extract_flow:",
                "    schedule:",
                "      every: 5m",
                "    triggers_on_success:",
                "      - report_flow",
                "    tasks:",
                "      extract:",
                "        type: python",
                "        path: extract.py",
                "      validate:",
                "        type: cli",
                "        command: python -c \"print('validate')\"",
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

    project = load_project(config_path)
    pipeline = project.pipelines["extract_flow"]

    assert pipeline.task_count == 2
    assert pipeline.execution_mode == "sequential"
    assert pipeline.triggers_on_success == ("report_flow",)
    assert pipeline.tasks["validate"].depends_on == ("extract",)
    assert pipeline.tasks["extract"].command_preview.endswith("extract.py")


def test_load_project_detects_parallelizable_graph_and_limit(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "job.py").write_text("print('job')", encoding="utf-8")
    (workspace / "fanout.py").write_text("print('fanout')", encoding="utf-8")

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Parallel Workspace",
                "workspace: workspace",
                "pipelines:",
                "  job_flow:",
                "    execution:",
                "      mode: parallel",
                "      max_parallel_tasks: 3",
                "    tasks:",
                "      main:",
                "        type: python",
                "        path: job.py",
                "      validate:",
                "        type: python",
                "        path: fanout.py",
                "        depends_on: [main]",
                "      publish:",
                "        type: python",
                "        path: fanout.py",
                "        depends_on: [main]",
            ]
        ),
        encoding="utf-8",
    )

    project = load_project(config_path)
    pipeline = project.pipelines["job_flow"]

    assert pipeline.execution_mode == "parallel"
    assert pipeline.parallelizable is True
    assert pipeline.max_parallel_tasks == 3


def test_load_project_parses_retry_policy_and_cli_path(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "job.cmd").write_text("@echo off\r\necho hello\r\n", encoding="utf-8")

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Retry Workspace",
                "workspace: workspace",
                "pipelines:",
                "  job_flow:",
                "    retry:",
                "      attempts: 2",
                "      mode: resume",
                "      delay_seconds: 3",
                "    tasks:",
                "      main:",
                "        type: cli",
                "        path: job.cmd",
            ]
        ),
        encoding="utf-8",
    )

    project = load_project(config_path)
    pipeline = project.pipelines["job_flow"]

    assert pipeline.retry_policy.attempts == 2
    assert pipeline.retry_policy.mode == "resume"
    assert pipeline.retry_policy.delay_seconds == 3
    assert pipeline.tasks["main"].path is not None
    assert pipeline.tasks["main"].command_preview.endswith("job.cmd")


def test_load_project_parses_file_and_sql_sensors(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "job.py").write_text("print('job')", encoding="utf-8")

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Sensor Workspace",
                "workspace: workspace",
                "pipelines:",
                "  watch_flow:",
                "    sensors:",
                "      landing_files:",
                "        type: file_sensor",
                "        path: inbox",
                "        pattern: '*.csv'",
                "        task_id: main",
                "      inbound_rows:",
                "        type: sql_sensor",
                "        database: sensor.db",
                "        table: inbound_events",
                "        cursor_column: id",
                "    tasks:",
                "      main:",
                "        type: python",
                "        path: job.py",
            ]
        ),
        encoding="utf-8",
    )

    project = load_project(config_path)
    pipeline = project.pipelines["watch_flow"]

    assert pipeline.sensor_count == 2
    assert pipeline.sensors["landing_files"].sensor_type == "file_sensor"
    assert pipeline.sensors["landing_files"].task_id == "main"
    assert pipeline.sensors["inbound_rows"].sensor_type == "sql_sensor"
    assert pipeline.sensors["inbound_rows"].table == "inbound_events"


def test_load_project_expands_dotenv_for_sql_connection_and_sftp_sensor(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "job.py").write_text("print('job')", encoding="utf-8")
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "PIPLY_SENSOR_DB_URL=sqlite:///sensor.db",
                "PIPLY_SENSOR_SFTP=sftp://demo@example.com:2222/inbox",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Env Sensor Workspace",
                "workspace: workspace",
                "pipelines:",
                "  watch_flow:",
                "    sensors:",
                "      landing_files:",
                "        type: file_sensor",
                "        path: ${PIPLY_SENSOR_SFTP}",
                "      inbound_rows:",
                "        type: sql_sensor",
                "        connection: ${PIPLY_SENSOR_DB_URL}",
                "        table: inbound_events",
                "    tasks:",
                "      main:",
                "        type: python",
                "        path: job.py",
            ]
        ),
        encoding="utf-8",
    )

    project = load_project(config_path)
    pipeline = project.pipelines["watch_flow"]

    assert pipeline.sensors["landing_files"].is_remote is True
    assert pipeline.sensors["landing_files"].ssh_host == "example.com"
    assert pipeline.sensors["landing_files"].ssh_user == "demo"
    assert pipeline.sensors["landing_files"].remote_path == "/inbox"
    assert pipeline.sensors["inbound_rows"].connection == "sqlite:///sensor.db"
