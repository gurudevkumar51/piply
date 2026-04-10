from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from piply.cli.main import app
from piply.core.loader import load_project
from piply.core.service import PipelineService


def test_init_scaffolds_multitask_project(tmp_path: Path) -> None:
    runner = CliRunner()
    project_dir = tmp_path / "starter"

    result = runner.invoke(app, ["init", str(project_dir)])

    assert result.exit_code == 0
    assert (project_dir / "piply.yaml").exists()
    assert (project_dir / "pipelines" / "extract.py").exists()
    assert (project_dir / "pipelines" / "report.py").exists()

    project = load_project(project_dir / "piply.yaml")

    assert tuple(project.pipelines) == ("extract_flow", "report_flow")
    assert project.pipelines["extract_flow"].task_count == 3
    assert project.pipelines["extract_flow"].triggers_on_success == ("report_flow",)
    assert project.pipelines["extract_flow"].execution_mode == "parallel"
    assert project.pipelines["extract_flow"].retry_policy.attempts == 2
    assert project.pipelines["extract_flow"].retry_policy.mode == "resume"
    assert project.pipelines["report_flow"].task_count == 1
    assert project.pipelines["report_flow"].tasks["build_report"].task_type == "python"
    assert project.pipelines["report_flow"].tasks["build_report"].call is not None


def test_tasks_retry_cli_retries_from_selected_failed_task(tmp_path: Path) -> None:
    runner = CliRunner()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "extract.py").write_text("print('extract complete')", encoding="utf-8")
    (workspace / "flaky.py").write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "flag = Path('flaky.ok')",
                "if flag.exists():",
                "    print('recovered')",
                "else:",
                "    flag.write_text('ok', encoding='utf-8')",
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
                "title: Retry CLI Test",
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
            ]
        ),
        encoding="utf-8",
    )

    database_path = tmp_path / "runs.db"
    service = PipelineService(config_path=config_path, database_path=database_path)
    failed_run = service.trigger_pipeline("retry_flow", wait=True)

    result = runner.invoke(
        app,
        [
            "tasks",
            "retry",
            failed_run.run_id,
            "flaky_step",
            "--config",
            str(config_path),
        ],
        env={"PIPLY_DATABASE": str(database_path)},
    )

    assert result.exit_code == 0
    assert "Finished with status: success" in result.stdout
