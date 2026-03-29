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


def test_load_project_parses_parallel_execution_mode(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "job.py").write_text("print('job')", encoding="utf-8")

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
            ]
        ),
        encoding="utf-8",
    )

    project = load_project(config_path)
    pipeline = project.pipelines["job_flow"]

    assert pipeline.execution_mode == "parallel"
    assert pipeline.max_parallel_tasks == 3
