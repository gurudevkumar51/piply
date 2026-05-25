from __future__ import annotations

import time
from pathlib import Path

from piply.core.loader import load_project
from piply.core.service import PipelineService


def test_python_task_outputs_are_passed_through_context_and_persisted(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "ops.py").write_text(
        "\n".join(
            [
                "from __future__ import annotations",
                "",
                "def extract() -> dict[str, int]:",
                "    return {'records': 41}",
                "",
                "def transform(context):",
                "    return {'records': context['extract']['records'] + 1}",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Context Test",
                "workspace: workspace",
                "pipelines:",
                "  context_flow:",
                "    tasks:",
                "      extract:",
                "        type: python",
                "        path: ops.py",
                "        function: extract",
                "      transform:",
                "        type: python",
                "        path: ops.py",
                "        function: transform",
                "        depends_on: [extract]",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    run = service.trigger_pipeline("context_flow", wait=True)
    stored_run, task_runs, _ = service.get_run(run.run_id)
    transform_output = service.get_task_output(run.run_id, "transform")

    assert stored_run.status == "success"
    assert [task.status for task in task_runs] == ["success", "success"]
    assert transform_output.json_value == '{"records": 42}'


def test_task_upstream_failure_behavior_can_fail_or_continue(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Failure Behavior Test",
                "workspace: workspace",
                "pipelines:",
                "  failure_flow:",
                "    tasks:",
                "      extract:",
                "        type: cli",
                '        command: python -c "raise SystemExit(1)"',
                "      transform:",
                "        type: cli",
                "        command: python -c \"print('still-runs')\"",
                "        depends_on: [extract]",
                "        on_upstream_failure: continue",
                "      load:",
                "        type: cli",
                "        command: python -c \"print('should-not-run')\"",
                "        depends_on: [extract]",
                "        on_upstream_failure: fail",
            ]
        ),
        encoding="utf-8",
    )

    project = load_project(config_path)
    assert project.pipelines["failure_flow"].tasks["transform"].on_upstream_failure == "continue"
    assert project.pipelines["failure_flow"].tasks["load"].on_upstream_failure == "fail"

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    run = service.trigger_pipeline("failure_flow", wait=True)
    stored_run, task_runs, logs = service.get_run(run.run_id)
    status_by_task = {task.task_id: task.status for task in task_runs}

    assert stored_run.status == "failed"
    assert status_by_task == {
        "extract": "failed",
        "transform": "success",
        "load": "failed",
    }
    assert any("still-runs" in line.message for line in logs)


def test_downstream_pipeline_receives_parent_output_context(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "ops.py").write_text(
        "\n".join(
            [
                "from __future__ import annotations",
                "",
                "def source():",
                "    return {'value': 'passed'}",
                "",
                "def sink(context):",
                "    print(context['source']['value'])",
                "    return context['upstream']['source']['value']",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Pipeline Context Test",
                "workspace: workspace",
                "pipelines:",
                "  pipeline_a:",
                "    triggers_on_success: [pipeline_b]",
                "    tasks:",
                "      source:",
                "        type: python",
                "        path: ops.py",
                "        function: source",
                "  pipeline_b:",
                "    tasks:",
                "      sink:",
                "        type: python",
                "        path: ops.py",
                "        function: sink",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    source_run = service.trigger_pipeline("pipeline_a", wait=True)

    downstream_run = None
    for _ in range(50):
        runs = service.list_runs(pipeline_id="pipeline_b", limit=1)
        if runs and runs[0].status == "success":
            downstream_run = runs[0]
            break
        time.sleep(0.05)

    assert downstream_run is not None
    assert downstream_run.parent_run_id == source_run.run_id
    output = service.get_task_output(downstream_run.run_id, "sink")
    assert output.json_value == '"passed"'
