"""Startup and shutdown recovery tests.

Each test drives one of the ways a Piply process can stop without finishing its
work: Ctrl+C, a scheduler restart, a crashed scheduler thread, and an
unexpected process kill. In every case no run may be left in a RUNNING state.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from piply.core.processes import is_process_alive
from piply.core.scheduler import PipelineScheduler
from piply.core.service import PipelineService

SLOW_PIPELINE = "\n".join(
    [
        'version: "1"',
        "title: Recovery Test",
        "workspace: workspace",
        "pipelines:",
        "  slow_flow:",
        "    tasks:",
        "      main:",
        "        type: python",
        "        path: slow.py",
    ]
)


def _write_project(tmp_path: Path, config_text: str = SLOW_PIPELINE) -> Path:
    """Create a workspace with one long-running task."""
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    (workspace / "slow.py").write_text(
        "\n".join(
            [
                "import time",
                "print('started', flush=True)",
                "time.sleep(30)",
                "print('finished', flush=True)",
            ]
        ),
        encoding="utf-8",
    )
    (workspace / "job.py").write_text("print('job complete')", encoding="utf-8")
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(config_text, encoding="utf-8")
    return config_path


def _active_run_count(database_path: Path) -> int:
    """Count rows still marked queued or running."""
    connection = sqlite3.connect(database_path)
    try:
        return int(connection.execute("SELECT COUNT(*) FROM runs WHERE status IN ('queued', 'running')").fetchone()[0])
    finally:
        connection.close()


def test_ctrl_c_marks_active_runs_interrupted(tmp_path: Path) -> None:
    """Ctrl+C during a foreground run leaves no orphaned RUNNING record."""
    config_path = _write_project(tmp_path)
    database_path = tmp_path / "runs.db"
    service = PipelineService(config_path=config_path, database_path=database_path)

    run = service.trigger_pipeline("slow_flow", wait=False)
    for _ in range(50):
        if service.store.get_run(run.run_id).status == "running":
            break
        time.sleep(0.1)

    # shutdown_runtime is exactly what the CLI Ctrl+C handler and the API
    # lifespan shutdown call.
    interrupted = service.shutdown_runtime("Run interrupted by Ctrl+C.")

    assert run.run_id in interrupted
    record, task_runs, logs = service.get_run(run.run_id)
    assert record.status == "interrupted"
    assert all(task.status in {"interrupted", "cancelled"} for task in task_runs)
    assert any("Ctrl+C" in line.message for line in logs)
    assert _active_run_count(database_path) == 0
    assert service.is_shutting_down is True


def test_shutdown_rejects_new_work(tmp_path: Path) -> None:
    """After shutdown starts, new executions are refused instead of orphaned."""
    config_path = _write_project(tmp_path)
    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    service.prepare_for_shutdown("Piply is shutting down.")

    try:
        service.trigger_pipeline("slow_flow", wait=False)
    except ValueError as exc:
        assert "shutting down" in str(exc)
    else:  # pragma: no cover - the guard must reject the call
        raise AssertionError("trigger_pipeline should refuse work during shutdown")

    assert service.store.get_meta("runtime_accepting_work") == "false"


def test_startup_recovers_runs_owned_by_a_dead_process(tmp_path: Path) -> None:
    """A run whose owning process is gone is interrupted when the runtime restarts."""
    config_path = _write_project(tmp_path)
    database_path = tmp_path / "runs.db"

    service = PipelineService(config_path=config_path, database_path=database_path)
    pipeline = service.get_pipeline("slow_flow")
    run = service.store.create_run(pipeline, trigger="manual")
    service.store.mark_running(run.run_id)
    service.store.mark_task_running(run.run_id, "main")

    # Point the run at a pid that cannot be running, simulating a hard kill.
    dead_pid = _find_dead_pid()
    connection = sqlite3.connect(database_path)
    connection.execute("UPDATE runs SET owner_pid = ? WHERE id = ?", (dead_pid, run.run_id))
    connection.commit()
    connection.close()

    recovered = PipelineService(config_path=config_path, database_path=database_path)
    record, task_runs, _ = recovered.get_run(run.run_id)

    assert record.status == "interrupted"
    assert task_runs[0].status == "interrupted"
    assert _active_run_count(database_path) == 0


def test_startup_leaves_runs_owned_by_a_live_process_alone(tmp_path: Path) -> None:
    """Recovery must not disturb a run that another live process still owns."""
    config_path = _write_project(tmp_path)
    database_path = tmp_path / "runs.db"

    service = PipelineService(config_path=config_path, database_path=database_path)
    pipeline = service.get_pipeline("slow_flow")
    run = service.store.create_run(pipeline, trigger="manual")
    service.store.mark_running(run.run_id)

    # create_run stamped the current pid, which is obviously still alive.
    assert is_process_alive(os.getpid()) is True

    second = PipelineService(config_path=config_path, database_path=database_path)
    assert second.store.get_run(run.run_id).status == "running"


def test_scheduler_restart_reconciles_previous_owner(tmp_path: Path) -> None:
    """Starting a scheduler claims ownership and recovers the old owner's work."""
    config_path = _write_project(tmp_path)
    database_path = tmp_path / "runs.db"
    service = PipelineService(config_path=config_path, database_path=database_path)

    pipeline = service.get_pipeline("slow_flow")
    orphan = service.store.create_run(pipeline, trigger="schedule")
    service.store.mark_running(orphan.run_id)
    connection = sqlite3.connect(database_path)
    connection.execute("UPDATE runs SET owner_pid = ? WHERE id = ?", (_find_dead_pid(), orphan.run_id))
    connection.commit()
    connection.close()

    scheduler = PipelineScheduler(service, poll_interval=2)
    scheduler.start()
    try:
        assert service.store.get_meta("scheduler_owner_pid") == str(os.getpid())
        assert service.store.get_run(orphan.run_id).status == "interrupted"
        assert int(service.store.get_meta("scheduler_recovered_runs")) >= 1

        snapshot = service.scheduler_snapshot()
        assert snapshot["state"] == "running"
        assert snapshot["owner_alive"] is True
    finally:
        scheduler.stop()

    assert service.scheduler_snapshot()["state"] == "stopped"


def test_scheduler_health_reports_crashed_when_owner_process_is_gone(tmp_path: Path) -> None:
    """A scheduler killed mid-flight is reported as crashed, not merely stale."""
    config_path = _write_project(tmp_path)
    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")

    stale_heartbeat = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    service.store.set_meta_many(
        {
            "scheduler_running": "true",
            "scheduler_state": "running",
            "scheduler_heartbeat": stale_heartbeat,
            "scheduler_owner_pid": str(_find_dead_pid()),
        }
    )

    snapshot = service.scheduler_snapshot()
    assert snapshot["state"] == "crashed"
    assert snapshot["running"] is False
    assert snapshot["owner_alive"] is False
    assert snapshot["label"] == "scheduler crashed"


def test_unexpected_termination_is_recovered_on_next_start(tmp_path: Path) -> None:
    """Killing the owning process outright still yields a clean state on restart."""
    config_path = _write_project(tmp_path)
    database_path = tmp_path / "runs.db"
    PipelineService(config_path=config_path, database_path=database_path)

    # Launch a real child process that starts a run and then never finishes.
    launcher = tmp_path / "launcher.py"
    launcher.write_text(
        "\n".join(
            [
                "import sys, time",
                "from piply.core.service import PipelineService",
                "service = PipelineService(config_path=sys.argv[1], database_path=sys.argv[2])",
                "run = service.trigger_pipeline('slow_flow', wait=False)",
                "print(run.run_id, flush=True)",
                "time.sleep(120)",
            ]
        ),
        encoding="utf-8",
    )

    process = subprocess.Popen(
        [sys.executable, str(launcher), str(config_path), str(database_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=str(Path(__file__).resolve().parent.parent),
    )
    try:
        assert process.stdout is not None
        run_id = process.stdout.readline().strip()
        assert run_id, "child process did not report a run id"

        deadline = time.time() + 15
        while time.time() < deadline:
            connection = sqlite3.connect(database_path)
            status = connection.execute("SELECT status FROM runs WHERE id = ?", (run_id,)).fetchone()
            connection.close()
            if status and status[0] == "running":
                break
            time.sleep(0.2)
    finally:
        process.kill()
        process.wait(timeout=15)

    assert _active_run_count(database_path) >= 1, "the killed process should have left an active run behind"

    recovered = PipelineService(config_path=config_path, database_path=database_path)
    assert _active_run_count(database_path) == 0
    assert recovered.store.get_run(run_id).status == "interrupted"


def _find_dead_pid() -> int:
    """Return a pid that is not currently running."""
    for candidate in range(999_000, 1_000_000):
        if not is_process_alive(candidate):
            return candidate
    raise AssertionError("could not find an unused pid for the test")
