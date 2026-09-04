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
                task_statuses = [item["status"] for item in payload["task_runs"]]
                if status == "running" and "running" in task_statuses:
                    break
                time.sleep(0.1)
            assert detail is not None
            payload = detail.json()
            assert payload["run"]["status"] == "running"
            assert "running" in [item["status"] for item in payload["task_runs"]]
    finally:
        if previous_config is None:
            os.environ.pop("PIPLY_CONFIG", None)
        else:
            os.environ["PIPLY_CONFIG"] = previous_config
        if previous_database is None:
            os.environ.pop("PIPLY_DATABASE", None)
        else:
            os.environ["PIPLY_DATABASE"] = previous_database

    service = PipelineService(config_path=config_path, database_path=database_path)
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
                json={"command_overrides": {"command_task": "python -c \"print('override-from-api')\""}},
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
    assert any("override-from-api" in line["message"] for line in payload["logs"])


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
    assert any("no-body-trigger" in line["message"] for line in payload["logs"])


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
            run_response = client.post("/api/pipelines/task_flow/tasks/publish/run", json={})
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

    raise AssertionError(f"Run {run_id} did not reach a terminal state within {timeout} seconds.")


def test_parallel_python_tasks_keep_their_output_separate(tmp_path: Path) -> None:
    """Two Python tasks running at once must not land in each other's log.

    Capture used to swap the process-global `sys.stdout`, so with
    `max_parallel_tasks` above one the enter/exit order interleaved: most of one
    task's output was recorded against the other, and the real stream was never
    put back.
    """
    import sys

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "tasks.py").write_text(
        "\n".join(
            [
                "import time",
                "",
                "",
                "def alpha():",
                "    for index in range(30):",
                "        print(f'ALPHA-{index}')",
                "        time.sleep(0.01)",
                "",
                "",
                "def beta():",
                "    for index in range(30):",
                "        print(f'BETA-{index}')",
                "        time.sleep(0.01)",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Parallel Capture",
                "workspace: workspace",
                "pipelines:",
                "  race:",
                "    max_parallel_tasks: 2",
                "    tasks:",
                "      a:",
                "        type: python",
                "        path: tasks.py",
                "        function: alpha",
                "      b:",
                "        type: python",
                "        path: tasks.py",
                "        function: beta",
            ]
        ),
        encoding="utf-8",
    )

    real_stdout, real_stderr = sys.stdout, sys.stderr
    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    run = service.trigger_pipeline("race", wait=True)
    stored_run, _, logs = service.get_run(run.run_id)

    assert stored_run.status == "success"

    by_task: dict[str, set[str]] = {}
    for line in logs:
        marker = line.message.split("-")[0]
        if marker in {"ALPHA", "BETA"}:
            by_task.setdefault(line.task_id, set()).add(marker)

    assert by_task == {"a": {"ALPHA"}, "b": {"BETA"}}
    # Every line is accounted for, so nothing was dropped to fix the mixing.
    assert sum(1 for line in logs if line.message.startswith("ALPHA")) == 30
    assert sum(1 for line in logs if line.message.startswith("BETA")) == 30

    # The process keeps the streams it started with.
    assert sys.stdout is real_stdout
    assert sys.stderr is real_stderr


def test_a_timed_out_python_task_releases_the_process_streams(tmp_path: Path) -> None:
    """A runaway task cannot be killed, but it must not keep stdout either."""
    import sys

    from piply.engine import task_runner

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "tasks.py").write_text(
        "\n".join(
            [
                "import time",
                "",
                "",
                "def slow():",
                "    print('SLOW-start')",
                "    time.sleep(30)",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Timeout Capture",
                "workspace: workspace",
                "pipelines:",
                "  slow_flow:",
                "    tasks:",
                "      slow:",
                "        type: python",
                "        path: tasks.py",
                "        function: slow",
                "        timeout: 2s",
            ]
        ),
        encoding="utf-8",
    )

    real_stdout, real_stderr = sys.stdout, sys.stderr
    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    run = service.trigger_pipeline("slow_flow", wait=True)
    stored_run, _, logs = service.get_run(run.run_id)

    assert stored_run.status == "timed_out"
    # Whatever it printed before the deadline is still recorded.
    assert any(line.message == "SLOW-start" for line in logs)
    assert any("timed out" in line.message for line in logs)

    assert sys.stdout is real_stdout
    assert sys.stderr is real_stderr
    assert task_runner._capture_users == 0


def test_python_callable_output_streams_while_the_task_runs(tmp_path: Path) -> None:
    """A long task must show progress, not go silent until it finishes.

    Output used to be buffered and flushed only when the callable returned, so a
    slow extraction looked identical to a hung one. Subprocess tasks always
    streamed; callables now do too.
    """
    import threading

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "tasks.py").write_text(
        "\n".join(
            [
                "import time",
                "",
                "",
                "def slow_steps():",
                "    for index in range(6):",
                "        print(f'STEP-{index}')",
                "        time.sleep(0.4)",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Streaming",
                "workspace: workspace",
                "pipelines:",
                "  stream:",
                "    tasks:",
                "      t:",
                "        type: python",
                "        path: tasks.py",
                "        function: slow_steps",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    finished = threading.Event()

    def _run() -> None:
        service.trigger_pipeline("stream", wait=True)
        finished.set()

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()

    # Watch the log grow while the task is still going.
    seen_midway = 0
    deadline = time.monotonic() + 20
    while not finished.is_set() and time.monotonic() < deadline:
        runs = service.list_runs(pipeline_id="stream", limit=1)
        if runs:
            _, _, logs = service.get_run(runs[0].run_id)
            steps = sum(1 for line in logs if line.message.startswith("STEP-"))
            if 0 < steps < 6:
                seen_midway = steps
                break
        time.sleep(0.1)

    assert seen_midway > 0, "no output was visible until the task had finished"

    worker.join(timeout=20)
    assert finished.is_set()
    _, _, logs = service.get_run(service.list_runs(pipeline_id="stream", limit=1)[0].run_id)
    # Streaming must not lose or duplicate anything.
    assert sorted(line.message for line in logs if line.message.startswith("STEP-")) == [
        f"STEP-{index}" for index in range(6)
    ]


def test_streamed_output_survives_failure_and_keeps_stream_order(tmp_path: Path) -> None:
    """Partial output before an exception is kept, and stderr interleaves."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "tasks.py").write_text(
        "\n".join(
            [
                "import sys",
                "",
                "",
                "def mixed():",
                "    print('OUT-1')",
                "    print('ERR-1', file=sys.stderr)",
                "    sys.stdout.write('NO-NEWLINE-AT-END')",
                "    raise RuntimeError('kaboom')",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Streaming Failure",
                "workspace: workspace",
                "pipelines:",
                "  boom:",
                "    tasks:",
                "      t:",
                "        type: python",
                "        path: tasks.py",
                "        function: mixed",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    run = service.trigger_pipeline("boom", wait=True)
    stored_run, _, logs = service.get_run(run.run_id)
    messages = [line.message for line in logs]

    assert stored_run.status == "failed"
    assert "OUT-1" in messages
    assert "ERR-1" in messages
    # A trailing write with no newline is still flushed, exactly once.
    assert messages.count("NO-NEWLINE-AT-END") == 1
    assert "kaboom" in messages


def test_logging_output_reaches_the_run_log(tmp_path: Path) -> None:
    """Real code logs, it does not print.

    A `StreamHandler` binds `sys.stderr` when it is constructed, so a module that
    calls `logging.basicConfig()` at import time wrote straight past the stream
    proxy and its output never appeared in the run log at all.
    """
    import logging
    import sys

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "tasks.py").write_text(
        "\n".join(
            [
                "import logging",
                "",
                "# A handler built at import time binds sys.stderr *now*, which is",
                "# exactly the case that used to bypass capture entirely.",
                "handler = logging.StreamHandler()",
                "handler.setFormatter(logging.Formatter('%(levelname)s %(message)s'))",
                "log = logging.getLogger('extract')",
                "log.addHandler(handler)",
                "log.setLevel(logging.INFO)",
                "log.propagate = False",
                "",
                "",
                "def job():",
                "    log.info('ROWS-EXTRACTED')",
                "    try:",
                "        1 / 0",
                "    except ZeroDivisionError:",
                "        log.exception('CALCULATION-FAILED')",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Logging",
                "workspace: workspace",
                "pipelines:",
                "  logged:",
                "    tasks:",
                "      t: {type: python, path: tasks.py, function: job}",
            ]
        ),
        encoding="utf-8",
    )

    real_stderr = sys.stderr
    root_streams_before = [getattr(h, "stream", None) for h in logging.getLogger().handlers]

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    run = service.trigger_pipeline("logged", wait=True)
    _, _, logs = service.get_run(run.run_id)
    messages = [line.message for line in logs]

    assert any("ROWS-EXTRACTED" in message for message in messages)
    assert any("CALCULATION-FAILED" in message for message in messages)
    # The traceback comes through as its own lines, not one blob.
    assert any(message.startswith("ZeroDivisionError") for message in messages)

    # Borrowed handlers are handed back exactly as they were found.
    assert sys.stderr is real_stderr
    assert [getattr(h, "stream", None) for h in logging.getLogger().handlers] == root_streams_before


def test_piply_own_logging_never_lands_in_a_run_log(tmp_path: Path) -> None:
    """Retargeting handlers is process-wide, so it must stay thread-scoped."""
    import logging
    import threading

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "tasks.py").write_text(
        "\n".join(["import time", "", "", "def job():", "    time.sleep(1.5)"]),
        encoding="utf-8",
    )

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: No Leak",
                "workspace: workspace",
                "pipelines:",
                "  quiet:",
                "    tasks:",
                "      t: {type: python, path: tasks.py, function: job}",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    done = threading.Event()

    def _run() -> None:
        service.trigger_pipeline("quiet", wait=True)
        done.set()

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()

    scheduler_log = logging.getLogger("piply.scheduler")
    while not done.wait(timeout=0.1):
        scheduler_log.warning("SCHEDULER-NOISE")

    worker.join(timeout=15)
    runs = service.list_runs(pipeline_id="quiet", limit=1)
    _, _, logs = service.get_run(runs[0].run_id)

    assert not [line for line in logs if "SCHEDULER-NOISE" in line.message]
