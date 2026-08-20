"""Tests for priority, timeouts, conditions, artifacts, preview, backfill, and retention."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from piply.api.app import create_app
from piply.core.loader import ConfigError, load_project
from piply.core.models import SensorDefinition
from piply.core.service import PipelineService
from piply.settings import SettingsError, load_settings


def _write(tmp_path: Path, body: str, *, files: dict[str, str] | None = None) -> Path:
    """Write a workspace and config, returning the config path."""
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    for name, content in (files or {}).items():
        (workspace / name).write_text(content, encoding="utf-8")
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(body, encoding="utf-8")
    return config_path


def test_priority_orders_independent_tasks(tmp_path: Path) -> None:
    """Higher-priority tasks run first when nothing forces an order."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Priority Test",
                "workspace: workspace",
                "pipelines:",
                "  flow:",
                "    tasks:",
                "      low:",
                "        type: cli",
                "        priority: low",
                "        command: python -c \"print('low')\"",
                "      urgent:",
                "        type: cli",
                '        priority: "***"',
                "        command: python -c \"print('urgent')\"",
                "      normal:",
                "        type: cli",
                "        command: python -c \"print('normal')\"",
            ]
        ),
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    pipeline = service.get_pipeline("flow")
    assert pipeline.tasks["urgent"].priority == 3
    assert pipeline.tasks["low"].priority == -1
    assert pipeline.tasks["normal"].priority == 0

    run = service.trigger_pipeline("flow", wait=True)
    _, _, logs = service.get_run(run.run_id)
    ordered = [line.task_id for line in reversed(logs) if line.task_id and "Running task" in line.message]
    assert ordered == ["urgent", "normal", "low"]


def test_star_suffix_task_ids_set_priority(tmp_path: Path) -> None:
    """`extract***` is shorthand for a task named `extract` with priority 3."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Star Priority Test",
                "workspace: workspace",
                "pipelines:",
                "  flow:",
                "    tasks:",
                "      extract***:",
                "        type: cli",
                "        command: python -c \"print('extract')\"",
                "      transform**:",
                "        type: cli",
                "        command: python -c \"print('transform')\"",
                "      validate*:",
                "        type: cli",
                "        depends_on: [extract]",
                "        command: python -c \"print('validate')\"",
            ]
        ),
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    pipeline = service.get_pipeline("flow")

    # Ids are normalized, so dependencies reference the clean name.
    assert sorted(pipeline.tasks) == ["extract", "transform", "validate"]
    assert pipeline.tasks["extract"].priority == 3
    assert pipeline.tasks["transform"].priority == 2
    assert pipeline.tasks["validate"].priority == 1
    assert pipeline.tasks["validate"].depends_on == ("extract",)

    run = service.trigger_pipeline("flow", wait=True)
    record, _, logs = service.get_run(run.run_id)
    ordered = [line.task_id for line in reversed(logs) if line.task_id and "Running task" in line.message]

    assert record.status == "success"
    assert ordered == ["extract", "transform", "validate"]


def test_dependency_order_beats_priority(tmp_path: Path) -> None:
    """A high-priority task still waits for its dependency."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Priority Dependency Test",
                "workspace: workspace",
                "pipelines:",
                "  flow:",
                "    tasks:",
                "      first:",
                "        type: cli",
                "        priority: -5",
                "        command: python -c \"print('first')\"",
                "      second:",
                "        type: cli",
                "        priority: 99",
                "        depends_on: [first]",
                "        command: python -c \"print('second')\"",
            ]
        ),
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    run = service.trigger_pipeline("flow", wait=True)
    record, _, logs = service.get_run(run.run_id)

    assert record.status == "success"
    ordered = [line.task_id for line in reversed(logs) if line.task_id and "Running task" in line.message]
    assert ordered == ["first", "second"]


def test_task_timeout_marks_run_timed_out(tmp_path: Path) -> None:
    """A task that overruns its timeout is killed and reported as timed_out."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Timeout Test",
                "workspace: workspace",
                "pipelines:",
                "  flow:",
                "    tasks:",
                "      slow:",
                "        type: python",
                "        path: slow.py",
                "        timeout: 2s",
                "        kill_grace_period: 1",
            ]
        ),
        files={"slow.py": "import time\nprint('started', flush=True)\ntime.sleep(60)\n"},
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    started = time.monotonic()
    run = service.trigger_pipeline("flow", wait=True)
    elapsed = time.monotonic() - started

    record, task_runs, logs = service.get_run(run.run_id)
    assert record.status == "timed_out"
    assert task_runs[0].status == "timed_out"
    assert task_runs[0].timeout_seconds == 2
    assert elapsed < 30, "the task should have been terminated near its timeout"
    assert any("timed out after 2 seconds" in line.message for line in logs)


def test_pipeline_timeout_stops_remaining_tasks(tmp_path: Path) -> None:
    """A pipeline-level timeout ends the run even when each task is under its own limit."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Pipeline Timeout Test",
                "workspace: workspace",
                "pipelines:",
                "  flow:",
                "    timeout: 2s",
                "    tasks:",
                "      slow:",
                "        type: python",
                "        path: slow.py",
            ]
        ),
        files={"slow.py": "import time\nprint('started', flush=True)\ntime.sleep(60)\n"},
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    run = service.trigger_pipeline("flow", wait=True)
    record, task_runs, logs = service.get_run(run.run_id)

    assert record.status == "timed_out"
    assert task_runs[0].status == "timed_out"
    assert any("Pipeline timed out after 2 seconds" in line.message for line in logs)


def test_invalid_priority_is_rejected(tmp_path: Path) -> None:
    """A misspelled priority fails config validation instead of defaulting silently."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Bad Priority",
                "workspace: workspace",
                "pipelines:",
                "  flow:",
                "    tasks:",
                "      task:",
                "        type: cli",
                "        priority: urgentish",
                "        command: echo hi",
            ]
        ),
    )

    try:
        load_project(config_path)
    except ConfigError as exc:
        assert "priority" in str(exc)
    else:  # pragma: no cover - the loader must reject this
        raise AssertionError("an invalid priority should raise ConfigError")


def test_run_if_skips_task_without_failing_the_run(tmp_path: Path) -> None:
    """A false run_if skips the task and leaves the run successful."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Conditional Test",
                "workspace: workspace",
                "variables:",
                "  report: payment",
                "pipelines:",
                "  flow:",
                "    tasks:",
                "      payment_only:",
                "        type: cli",
                "        run_if: \"{report} == 'payment'\"",
                "        command: python -c \"print('payment ran')\"",
                "      refund_only:",
                "        type: cli",
                "        run_if: \"{report} == 'refund'\"",
                "        command: python -c \"print('refund ran')\"",
            ]
        ),
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    run = service.trigger_pipeline("flow", wait=True)
    record, task_runs, logs = service.get_run(run.run_id)
    statuses = {task.task_id: task.status for task in task_runs}

    assert record.status == "success"
    assert statuses == {"payment_only": "success", "refund_only": "skipped"}
    assert any("payment ran" in line.message for line in logs)
    assert not any("refund ran" in line.message for line in logs)


def test_artifacts_are_recorded_and_listed(tmp_path: Path) -> None:
    """Declared artifact globs are recorded against the task that produced them."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Artifact Test",
                "workspace: workspace",
                "pipelines:",
                "  flow:",
                "    tasks:",
                "      build:",
                "        type: python",
                "        path: build.py",
                "        cwd: .",
                "        artifacts:",
                "          - 'out/*.txt'",
            ]
        ),
        files={
            "build.py": "\n".join(
                [
                    "from pathlib import Path",
                    "out = Path('out')",
                    "out.mkdir(exist_ok=True)",
                    "(out / 'report.txt').write_text('hello', encoding='utf-8')",
                    "(out / 'summary.txt').write_text('world', encoding='utf-8')",
                    "print('built')",
                ]
            )
        },
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    run = service.trigger_pipeline("flow", wait=True)
    artifacts = service.list_run_artifacts(run.run_id)
    names = sorted(str(item["name"]) for item in artifacts)

    assert names == ["report.txt", "summary.txt"]
    assert all(item["exists"] for item in artifacts)
    assert all(int(item["size_bytes"]) == 5 for item in artifacts)


def test_preview_reports_order_variables_and_conditions(tmp_path: Path) -> None:
    """The dry run resolves variables, entities, order, and conditional skips."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Preview Test",
                "workspace: workspace",
                "variables:",
                "  report: payment",
                "pipelines:",
                "  flow:",
                "    entities:",
                "      region:",
                "        - eu",
                "        - us",
                "    tasks:",
                "      load:",
                "        type: cli",
                "        priority: 5",
                "        command: python -c \"print('load {region}')\"",
                "      publish:",
                "        type: cli",
                "        depends_on: [load]",
                "        run_if: \"{report} == 'refund'\"",
                "        command: python -c \"print('publish')\"",
            ]
        ),
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    preview = service.preview_pipeline("flow")
    payload = preview.as_dict()

    assert payload["variables"]["report"] == "payment"
    assert sorted(payload["entities"]["region"]) == ["eu", "us"]
    assert len(payload["stages"]) == 2
    assert set(payload["stages"][0]) == {"eu.load", "us.load"}

    publish = next(item for item in payload["tasks"] if item["task_id"].endswith("publish"))
    assert publish["will_run"] is False
    assert "run_if evaluated false" in publish["skip_reason"]

    load_task = next(item for item in payload["tasks"] if item["task_id"] == "eu.load")
    assert load_task["priority"] == 5
    assert "load eu" in load_task["command"]


def test_downstream_pipeline_inherits_upstream_env(tmp_path: Path) -> None:
    """Env values from the upstream deployment reach the downstream pipeline."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Env Inheritance Test",
                "workspace: workspace",
                "pipelines:",
                "  upstream:",
                "    env:",
                "      TENANT_CODE: acme-42",
                "    triggers_on_success:",
                "      - downstream",
                "    tasks:",
                "      emit:",
                "        type: cli",
                "        command: python -c \"print('emitted')\"",
                "  downstream:",
                "    tasks:",
                "      consume:",
                "        type: python",
                "        path: consume.py",
            ]
        ),
        files={"consume.py": "import os\nprint('TENANT_CODE=' + os.environ.get('TENANT_CODE', 'missing'))\n"},
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    service.trigger_pipeline("upstream", wait=True)

    downstream_runs: list = []
    for _ in range(60):
        downstream_runs = service.list_runs(pipeline_id="downstream")
        if downstream_runs and downstream_runs[0].status in {"success", "failed"}:
            break
        time.sleep(0.2)

    assert downstream_runs
    _, _, logs = service.get_run(downstream_runs[0].run_id)
    assert any("TENANT_CODE=acme-42" in line.message for line in logs)


def test_defaults_env_resolves_against_each_pipelines_variables(tmp_path: Path) -> None:
    """A `{placeholder}` in defaults.env renders per pipeline, not once at the root.

    Declaring `DBT_CLIENT: "{practice}"` once under defaults is only useful if
    each deployment renders it with its own practice; resolving it against root
    variables would silently give every tenant the same value.
    """
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Defaults Env Test",
                "workspace: workspace",
                "variables:",
                "  practice: UNSET",
                "defaults:",
                "  env:",
                '    DBT_CLIENT: "{practice}"',
                "    STATIC: fixed",
                "pipeline_templates:",
                "  tenant_flow:",
                "    tasks:",
                "      run_dbt:",
                "        type: cli",
                "        command: dbt run",
                "pipelines:",
                "  shared_downstream:",
                "    tasks:",
                "      run_dbt:",
                "        type: cli",
                "        command: dbt run",
                "pipeline_deployments:",
                "  acme_flow:",
                "    template: tenant_flow",
                "    variables:",
                "      practice: ACME",
                "  globex_flow:",
                "    template: tenant_flow",
                "    variables:",
                "      practice: GLOBEX",
            ]
        ),
    )

    project = load_project(config_path)
    env_of = {pid: pipeline.tasks["run_dbt"].env for pid, pipeline in project.pipelines.items()}

    assert env_of["acme_flow"]["DBT_CLIENT"] == "ACME"
    assert env_of["globex_flow"]["DBT_CLIENT"] == "GLOBEX"
    # A pipeline with no practice of its own falls back to the root default,
    # which makes an accidental manual run obvious instead of silent.
    assert env_of["shared_downstream"]["DBT_CLIENT"] == "UNSET"
    # Values without placeholders are unaffected.
    assert all(env["STATIC"] == "fixed" for env in env_of.values())


def test_deployment_env_reaches_every_hop_of_a_trigger_chain(tmp_path: Path) -> None:
    """A deployment's env and variables survive a multi-hop downstream chain.

    Deployment -> silver -> gold -> semantic is the shape a per-tenant dbt
    project uses: only the first pipeline knows the tenant, so every hop after
    it must inherit that value or the wrong tenant's data gets written.
    """
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Chain Inheritance Test",
                "workspace: workspace",
                "pipeline_templates:",
                "  extract_template:",
                "    env_file: .env",
                "    env:",
                '      DBT_CLIENT: "{practice}"',
                "    tasks:",
                "      pre_flight:",
                "        type: cli",
                "        cwd: .",
                "        command: python show_env.py extract",
                "pipelines:",
                "  silver:",
                "    triggers_on_success: [gold]",
                "    env:",
                '      DBT_CLIENT: "{practice}"',
                "    tasks:",
                "      load_silver:",
                "        type: cli",
                "        cwd: .",
                "        command: python show_env.py silver",
                "  gold:",
                "    triggers_on_success: [semantic]",
                "    env:",
                '      DBT_CLIENT: "{practice}"',
                "    tasks:",
                "      load_gold:",
                "        type: cli",
                "        cwd: .",
                "        command: python show_env.py gold",
                "  semantic:",
                "    env:",
                '      DBT_CLIENT: "{practice}"',
                "    tasks:",
                "      load_semantic:",
                "        type: cli",
                "        cwd: .",
                "        command: python show_env.py semantic",
                "pipeline_deployments:",
                "  TENANT_ETL:",
                "    template: extract_template",
                "    variables:",
                "      practice: TENANT_A",
                "    triggers_on_success: [silver]",
            ]
        ),
        files={
            ".env": "SHARED_SECRET=from-env-file\n",
            "show_env.py": "\n".join(
                [
                    "import os, sys",
                    "client = os.environ.get('DBT_CLIENT', 'MISSING')",
                    "shared = os.environ.get('SHARED_SECRET', 'MISSING')",
                    "print(f'{sys.argv[1]} DBT_CLIENT={client} SHARED={shared}')",
                ]
            ),
        },
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    service.trigger_pipeline("TENANT_ETL", wait=True)

    for _ in range(100):
        semantic_runs = service.list_runs(pipeline_id="semantic")
        if semantic_runs and semantic_runs[0].status in {"success", "failed"}:
            break
        time.sleep(0.2)

    seen: dict[str, str] = {}
    for pipeline_id in ("TENANT_ETL", "silver", "gold", "semantic"):
        runs = service.list_runs(pipeline_id=pipeline_id)
        assert runs, f"{pipeline_id} never ran"
        _, _, logs = service.get_run(runs[0].run_id)
        for line in logs:
            if "DBT_CLIENT=" in line.message and "Output captured" not in line.message:
                seen[pipeline_id] = line.message
                break

    assert set(seen) == {"TENANT_ETL", "silver", "gold", "semantic"}
    for pipeline_id, message in seen.items():
        assert "DBT_CLIENT=TENANT_A" in message, f"{pipeline_id} lost the tenant: {message}"
        assert "SHARED=from-env-file" in message, f"{pipeline_id} lost the env_file value: {message}"

    # A mid-chain run keeps everything it needs to be re-run on its own.
    gold_run = service.list_runs(pipeline_id="gold")[0]
    snapshot = service.store.get_run_config(gold_run.run_id)
    assert snapshot["inherited_variables"]["practice"] == "TENANT_A"
    assert snapshot["inherited_env"]["DBT_CLIENT"] == "TENANT_A"

    replay = service.backfill_run(gold_run.run_id, wait=True)
    _, _, replay_logs = service.get_run(replay.run_id)
    assert any("DBT_CLIENT=TENANT_A" in line.message for line in replay_logs)


def test_downstream_run_can_be_retried_without_the_upstream_pipeline(tmp_path: Path) -> None:
    """A failed downstream run replays the variables and env it was launched with."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Downstream Retry Test",
                "workspace: workspace",
                "pipelines:",
                "  upstream:",
                "    variables:",
                "      batch: batch-77",
                "    env:",
                "      BATCH_ENV: env-77",
                "    triggers_on_success:",
                "      - downstream",
                "    tasks:",
                "      emit:",
                "        type: cli",
                "        command: python -c \"print('emitted')\"",
                "  downstream:",
                "    variables:",
                "      batch: unset",
                "    tasks:",
                "      consume:",
                "        type: python",
                "        path: consume.py",
                "        args: ['{batch}']",
            ]
        ),
        files={
            "consume.py": "\n".join(
                [
                    "import os, sys",
                    "print('batch=' + sys.argv[1])",
                    "print('BATCH_ENV=' + os.environ.get('BATCH_ENV', 'missing'))",
                ]
            )
        },
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    service.trigger_pipeline("upstream", wait=True)

    downstream_runs: list = []
    for _ in range(60):
        downstream_runs = service.list_runs(pipeline_id="downstream")
        if downstream_runs and downstream_runs[0].status in {"success", "failed"}:
            break
        time.sleep(0.2)
    assert downstream_runs
    original = downstream_runs[0]

    _, _, first_logs = service.get_run(original.run_id)
    assert any("batch=batch-77" in line.message for line in first_logs)

    # Retrying the downstream run alone must reuse the upstream values.
    retried = service.retry_run(original.run_id, mode="startover", wait=True)
    record, _, retry_logs = service.get_run(retried.run_id)

    assert record.status == "success"
    assert any("batch=batch-77" in line.message for line in retry_logs)
    assert any("BATCH_ENV=env-77" in line.message for line in retry_logs)


def test_backfill_replays_the_original_run_configuration(tmp_path: Path) -> None:
    """Backfilling a run reuses its captured configuration."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Backfill Test",
                "workspace: workspace",
                "pipelines:",
                "  flow:",
                "    tasks:",
                "      report:",
                "        type: cli",
                "        command: python -c \"print('reported')\"",
            ]
        ),
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    original = service.trigger_pipeline(
        "flow",
        wait=True,
        tenant_id="tenant-a",
        inherited_variables={"season": "q3"},
    )

    snapshot = service.store.get_run_config(original.run_id)
    assert snapshot is not None
    assert snapshot["tenant_id"] == "tenant-a"
    assert snapshot["inherited_variables"] == {"season": "q3"}

    replay = service.backfill_run(original.run_id, wait=True)
    replay_snapshot = service.store.get_run_config(replay.run_id)

    assert service.store.get_run(replay.run_id).status == "success"
    assert replay.tenant_id == "tenant-a"
    assert replay_snapshot["inherited_variables"] == {"season": "q3"}


def test_backfill_schedule_queues_historic_slots(tmp_path: Path) -> None:
    """A schedule window queues one trigger per missed slot."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Schedule Backfill Test",
                "workspace: workspace",
                "pipelines:",
                "  flow:",
                "    schedule:",
                "      interval_seconds: 3600",
                "    tasks:",
                "      report:",
                "        type: cli",
                "        command: python -c \"print('reported')\"",
            ]
        ),
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    slots = service.backfill_schedule("flow", start=end - timedelta(hours=4), end=end, limit=10)

    assert len(slots) == 4
    assert service.store.count_queue() == 4


def test_prune_removes_old_runs_and_reclaims_space(tmp_path: Path) -> None:
    """Retention pruning deletes history and vacuums the database."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Prune Test",
                "workspace: workspace",
                "pipelines:",
                "  flow:",
                "    tasks:",
                "      report:",
                "        type: cli",
                "        command: python -c \"print('reported')\"",
            ]
        ),
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    keep = service.trigger_pipeline("flow", wait=True)
    for _ in range(3):
        service.trigger_pipeline("flow", wait=True)

    planned = service.prune(dry_run=True, max_runs_per_pipeline=1, run_retention_days=0, log_retention_days=0)
    assert planned["runs_deleted"] == 3
    assert service.store.get_run(keep.run_id) is not None

    summary = service.prune(max_runs_per_pipeline=1, run_retention_days=0, log_retention_days=0)
    assert summary["runs_deleted"] == 3
    assert len(service.list_runs(pipeline_id="flow")) == 1


def test_metrics_and_diagnostics_endpoints(tmp_path: Path) -> None:
    """The Prometheus and diagnostics endpoints expose live runtime state."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Observability Test",
                "workspace: workspace",
                "pipelines:",
                "  flow:",
                "    tasks:",
                "      report:",
                "        type: cli",
                "        command: python -c \"print('reported')\"",
            ]
        ),
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "obs.db")
    service.trigger_pipeline("flow", wait=True)

    with TestClient(create_app(str(config_path))) as client:
        metrics = client.get("/metrics")
        diagnostics = client.get("/api/diagnostics")

    assert metrics.status_code == 200
    assert "text/plain" in metrics.headers["content-type"]
    body = metrics.text
    assert "# TYPE piply_runs_total gauge" in body
    assert "piply_scheduler_up" in body
    assert 'piply_queue_size{status="queued"}' in body
    assert "piply_run_duration_seconds_count" in body

    payload = diagnostics.json()
    assert diagnostics.status_code == 200
    assert payload["scheduler"]["state"] in {"running", "stopped", "stale", "crashed"}
    assert "running_tasks" in payload
    assert payload["database"]["size_bytes"] > 0


def test_artifact_download_rejects_paths_outside_the_run(tmp_path: Path) -> None:
    """A download request for a path the run never recorded is refused."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Artifact Guard Test",
                "workspace: workspace",
                "pipelines:",
                "  flow:",
                "    tasks:",
                "      report:",
                "        type: cli",
                "        command: python -c \"print('reported')\"",
            ]
        ),
    )
    secret = tmp_path / "secret.txt"
    secret.write_text("do not leak", encoding="utf-8")

    service = PipelineService(config_path=config_path, database_path=tmp_path / "guard.db")
    run = service.trigger_pipeline("flow", wait=True)

    with TestClient(create_app(str(config_path))) as client:
        response = client.get(f"/api/runs/{run.run_id}/artifacts/download", params={"path": str(secret)})

    assert response.status_code == 404


def test_runtime_database_setting_accepts_paths_and_postgres_only(tmp_path: Path) -> None:
    """PIPLY_DATABASE is a SQLite path or a PostgreSQL DSN, and says so otherwise.

    Treating an unrecognised URL as a path either raises a confusing filesystem
    error or, on POSIX, silently creates a directory named `mysql:` and starts
    an empty runtime against it.
    """
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Database URL Test",
                "workspace: workspace",
                "pipelines:",
                "  flow:",
                "    tasks:",
                "      main:",
                "        type: cli",
                "        command: echo hi",
            ]
        ),
    )

    # Supported: a PostgreSQL DSN selects the external metadata store and is
    # passed through verbatim rather than being resolved as a path.
    for dsn in (
        "postgresql://user:pw@db.internal:5432/piply",
        "postgres://user:pw@db.internal:5432/piply",
        "postgresql+psycopg://user:pw@db.internal:5432/piply",
    ):
        settings = load_settings(config_path, environ={"PIPLY_DATABASE": dsn})
        assert settings.database_dsn == dsn
        assert settings.database_path is None

    # Unsupported server URLs are refused with the reason.
    for url in ("mysql+pymysql://user@host/piply", "mssql+pyodbc://host/piply"):
        try:
            load_settings(config_path, environ={"PIPLY_DATABASE": url})
        except SettingsError as exc:
            assert "does not support" in str(exc)
            assert "sql_sensor" in str(exc)
        else:  # pragma: no cover - the guard must reject these
            raise AssertionError(f"{url} should have been rejected")

    # A sqlite:// URL gets its own message pointing at the plain path.
    try:
        load_settings(config_path, environ={"PIPLY_DATABASE": "sqlite:///runs.db"})
    except SettingsError as exc:
        assert "plain file path" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("sqlite:// URL should have been rejected")

    # Ordinary paths keep working, relative to the config directory, and remain
    # the default when nothing is configured.
    settings = load_settings(config_path, environ={"PIPLY_DATABASE": "state/runs.db"})
    assert settings.database_path == (tmp_path / "state" / "runs.db").resolve()
    assert settings.database_dsn is None

    default = load_settings(config_path, environ={})
    assert default.database_path is None
    assert default.database_dsn is None


def test_sensor_summaries_redact_credentials() -> None:
    """A password in a connection string or URL never reaches the UI or API.

    `summary` is rendered on the Diagnostics page, returned by GET /api/sensors,
    and written into run logs when a sensor fires.
    """
    leaky = [
        SensorDefinition(
            sensor_id="wh",
            sensor_type="sql_sensor",
            title="Warehouse",
            connection="postgresql://piply:s3cret@db.internal:5432/warehouse",
            table="events",
        ),
        SensorDefinition(
            sensor_id="feed",
            sensor_type="api_sensor",
            title="Feed",
            url="https://svc:s3cret@api.example.com/feed",
        ),
    ]
    for sensor in leaky:
        assert "s3cret" not in sensor.summary, sensor.summary
        assert ":***@" in sensor.summary, sensor.summary

    # Sensors without credentials are rendered unchanged.
    plain = SensorDefinition(
        sensor_id="local",
        sensor_type="sql_sensor",
        title="Local",
        connection="sqlite:///app.db",
        table="events",
    )
    assert plain.summary == "sql_sensor sqlite:///app.db:events"


def test_sensor_failures_are_recorded_not_raised(tmp_path: Path) -> None:
    """One broken sensor records its error and leaves the others polling."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Sensor Health Test",
                "workspace: workspace",
                "connections:",
                "  legacy: oracle://user:pw@host/db",
                "pipelines:",
                "  bad_scheme:",
                "    sensors:",
                "      old:",
                "        type: sql_sensor",
                "        connection_ref: legacy",
                "        table: events",
                "        cursor_column: id",
                "    tasks:",
                "      t: {type: cli, command: echo hi}",
                "  missing_dir:",
                "    sensors:",
                "      gone:",
                "        type: file_sensor",
                "        path: no_such_dir",
                "        pattern: '*.csv'",
                "    tasks:",
                "      t: {type: cli, command: echo hi}",
                "  dead_api:",
                "    sensors:",
                "      down:",
                "        type: api_sensor",
                "        url: http://127.0.0.1:9/nope",
                "        expected_status: [200]",
                "    tasks:",
                "      t: {type: cli, command: echo hi}",
            ]
        ),
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    # No exception escapes, and nothing is enqueued from a failed poll.
    assert service.poll_sensors() == 0

    health = {item["sensor_id"]: item for item in service.sensor_health()}
    assert set(health) == {"old", "gone", "down"}
    assert all(item["status"] == "failing" for item in health.values())
    assert "Unsupported sql_sensor connection scheme 'oracle'" in health["old"]["last_error"]
    assert "does not exist" in health["gone"]["last_error"]
    assert health["down"]["last_error"]
    # The failing sensor's credentials are still redacted.
    assert "pw" not in health["old"]["summary"].replace("piply", "")

    diagnostics = service.diagnostics()
    assert diagnostics["sensor_summary"]["failing"] == 3
    assert diagnostics["sensor_summary"]["healthy"] == 0


def test_backup_snapshots_a_live_database(tmp_path: Path) -> None:
    """A backup taken while the runtime is open restores the same history.

    This is the recovery path for a deployment that loses its storage, so the
    snapshot has to be consistent without stopping the server first.
    """
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Backup Test",
                "workspace: workspace",
                "pipelines:",
                "  demo:",
                "    tasks:",
                "      main:",
                "        type: cli",
                "        command: python -c \"print('done')\"",
            ]
        ),
    )
    database_path = tmp_path / "runs.db"
    service = PipelineService(config_path=config_path, database_path=database_path)
    first = service.trigger_pipeline("demo", wait=True)
    second = service.trigger_pipeline("demo", wait=True)

    # Passing a directory produces a timestamped file inside it, creating the
    # directory if needed.
    written = service.store.backup_to(tmp_path / "backups")
    assert written.parent == (tmp_path / "backups").resolve()
    assert written.suffix == ".db"
    assert written.stat().st_size > 0

    # Runs created after the snapshot must not appear in it.
    third = service.trigger_pipeline("demo", wait=True)

    restored = PipelineService(config_path=config_path, database_path=written)
    ids = {run.run_id for run in restored.list_runs(limit=50)}
    assert {first.run_id, second.run_id} <= ids
    assert third.run_id not in ids

    # Task runs and logs travel with the snapshot, not just the run rows.
    record, task_runs, logs = restored.get_run(first.run_id)
    assert record.status == "success"
    assert [task.task_id for task in task_runs] == ["main"]
    assert any("done" in line.message for line in logs)


def test_backup_to_an_explicit_file_path(tmp_path: Path) -> None:
    """A destination with a file extension is used verbatim."""
    config_path = _write(
        tmp_path,
        "\n".join(
            [
                'version: "1"',
                "title: Backup Path Test",
                "workspace: workspace",
                "pipelines:",
                "  demo:",
                "    tasks:",
                "      main: {type: cli, command: echo hi}",
            ]
        ),
    )
    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    target = tmp_path / "nested" / "snapshot.db"

    written = service.store.backup_to(target)

    assert written == target.resolve()
    assert written.is_file()


def test_missing_env_file_warns_instead_of_failing_silently(tmp_path: Path) -> None:
    """A declared env_file that does not resolve must be visible, not silent.

    `env_file` resolves against `workspace:`, not the config file. When those
    differ the file loads nothing and the pipeline runs with the variables
    simply absent, which surfaces much later as "my credentials aren't set".
    """
    (tmp_path / "workspace").mkdir(exist_ok=True)
    (tmp_path / ".env").write_text("SHARED=from_env_file\n", encoding="utf-8")
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Env File Warning",
                "workspace: workspace",
                "pipelines:",
                "  demo:",
                "    env_file: .env",
                "    tasks:",
                "      main: {type: cli, command: echo hi}",
            ]
        ),
        encoding="utf-8",
    )

    project = load_project(config_path)
    assert len(project.warnings) == 1
    warning = project.warnings[0]
    assert "env_file '.env' was not found" in warning
    # The message has to name the path it actually looked at, or it does not help.
    assert str(tmp_path / "workspace") in warning
    # Loading still succeeds: an absent env file is legitimate in some environments.
    assert "demo" in project.pipelines

    # Once the file is where the workspace expects it, the warning clears and
    # the values load.
    (tmp_path / "workspace" / ".env").write_text("SHARED=from_env_file\n", encoding="utf-8")
    project = load_project(config_path)
    assert project.warnings == ()
    assert project.pipelines["demo"].tasks["main"].env["SHARED"] == "from_env_file"


def test_env_file_overrides_inline_pipeline_env(tmp_path: Path) -> None:
    """Documented precedence: env_file wins over inline `env:` at pipeline level.

    This surprises people, so it is pinned here: if it ever changes, it is a
    behaviour change that needs a note, not a silent fix.
    """
    (tmp_path / "workspace").mkdir(exist_ok=True)
    (tmp_path / "workspace" / ".env").write_text("SHARED=from_env_file\n", encoding="utf-8")
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Env Precedence",
                "workspace: workspace",
                "defaults:",
                "  env:",
                "    SHARED: from_defaults",
                "pipelines:",
                "  demo:",
                "    env_file: .env",
                "    env:",
                "      SHARED: from_pipeline_env",
                "    tasks:",
                "      plain: {type: cli, command: echo hi}",
                "      overridden:",
                "        type: cli",
                "        command: echo hi",
                "        env:",
                "          SHARED: from_task_env",
            ]
        ),
        encoding="utf-8",
    )

    tasks = load_project(config_path).pipelines["demo"].tasks
    assert tasks["plain"].env["SHARED"] == "from_env_file"
    # Task-level env is the last word, which is how you win against an env file.
    assert tasks["overridden"].env["SHARED"] == "from_task_env"
