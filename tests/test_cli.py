from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from piply.cli.main import app
from piply.core.loader import load_project


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
    assert project.pipelines["report_flow"].task_count == 1
