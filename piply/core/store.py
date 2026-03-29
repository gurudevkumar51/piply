"""SQLite-backed storage for runs, task runs, logs, and scheduler state."""

from __future__ import annotations

import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .models import (
    DashboardStats,
    LogRecord,
    PipelineDefinition,
    RetryMode,
    RunRecord,
    TaskRunRecord,
)


def _to_iso(value: datetime | None) -> str | None:
    """Serialize a datetime value to UTC ISO text."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _from_iso(value: str | None) -> datetime | None:
    """Parse an ISO timestamp stored in SQLite."""
    if value is None:
        return None
    return datetime.fromisoformat(value)


class RunStore:
    """RunStore persists pipeline state and shields the runtime from SQL details."""

    def __init__(self, database_path: str | Path):
        self.database_path = Path(database_path).resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._run_columns: set[str] = set()
        self._log_columns: set[str] = set()
        self._initialize()

    @contextmanager
    def _connect(self):
        """Open a thread-friendly SQLite connection."""
        connection = sqlite3.connect(self.database_path, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()

    def _refresh_schema_info(self, connection: sqlite3.Connection) -> None:
        """Cache schema metadata used for compatibility-aware inserts."""
        self._run_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(runs)").fetchall()
        }
        self._log_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(logs)").fetchall()
        }

    def _initialize(self) -> None:
        """Create or migrate the runtime schema in place."""
        with self._connect() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    pipeline_id TEXT NOT NULL,
                    pipeline_title TEXT NOT NULL,
                    status TEXT NOT NULL,
                    trigger TEXT NOT NULL,
                    command TEXT NOT NULL,
                    primary_entry TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    scheduled_for TEXT,
                    exit_code INTEGER,
                    error TEXT,
                    retry_of TEXT,
                    retry_mode TEXT,
                    retry_task_id TEXT
                );

                CREATE TABLE IF NOT EXISTS task_runs (
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
                    FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
                    UNIQUE(run_id, task_id)
                );

                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    task_id TEXT,
                    created_at TEXT NOT NULL,
                    stream TEXT NOT NULL,
                    message TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS pipeline_overrides (
                    pipeline_id TEXT PRIMARY KEY,
                    paused INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_runs_pipeline_id ON runs(pipeline_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
                CREATE INDEX IF NOT EXISTS idx_task_runs_run_id ON task_runs(run_id, position);
                CREATE INDEX IF NOT EXISTS idx_logs_run_id ON logs(run_id, id DESC);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_unique_schedule_slot
                    ON runs(pipeline_id, scheduled_for)
                    WHERE scheduled_for IS NOT NULL;
                """
            )

            self._refresh_schema_info(connection)

            if "primary_entry" not in self._run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN primary_entry TEXT")
                connection.execute(
                    "UPDATE runs SET primary_entry = COALESCE(script_path, working_dir, command, '')"
                )

            if "retry_of" not in self._run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN retry_of TEXT")
            if "retry_mode" not in self._run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN retry_mode TEXT")
            if "retry_task_id" not in self._run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN retry_task_id TEXT")

            if "task_id" not in self._log_columns:
                connection.execute("ALTER TABLE logs ADD COLUMN task_id TEXT")

            connection.commit()
            self._refresh_schema_info(connection)

    def create_run(
        self,
        pipeline: PipelineDefinition,
        trigger: str,
        scheduled_for: datetime | None = None,
        *,
        retry_of: str | None = None,
        retry_mode: RetryMode | None = None,
        retry_task_id: str | None = None,
    ) -> RunRecord:
        """Insert one new run and its queued task records."""
        with self._lock, self._connect() as connection:
            run_id = uuid.uuid4().hex[:12]
            created_at = datetime.now(timezone.utc)
            first_task = pipeline.first_task
            working_directory = ""
            if first_task is not None and first_task.working_directory is not None:
                working_directory = str(first_task.working_directory)

            run_values: dict[str, object | None] = {
                "id": run_id,
                "pipeline_id": pipeline.pipeline_id,
                "pipeline_title": pipeline.title,
                "status": "queued",
                "trigger": trigger,
                "command": pipeline.command_preview,
                "primary_entry": pipeline.primary_entry,
                "created_at": _to_iso(created_at),
                "scheduled_for": _to_iso(scheduled_for),
                "retry_of": retry_of,
                "retry_mode": retry_mode,
                "retry_task_id": retry_task_id,
            }

            if "script_path" in self._run_columns:
                run_values["script_path"] = pipeline.primary_entry
            if "working_dir" in self._run_columns:
                run_values["working_dir"] = working_directory

            columns_sql = ", ".join(run_values.keys())
            placeholders_sql = ", ".join("?" for _ in run_values)
            connection.execute(
                f"INSERT INTO runs ({columns_sql}) VALUES ({placeholders_sql})",
                tuple(run_values.values()),
            )

            for position, task in enumerate(pipeline.tasks.values()):
                connection.execute(
                    """
                    INSERT INTO task_runs (
                        run_id, task_id, title, task_type, status, position,
                        command_preview, depends_on
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        task.task_id,
                        task.title,
                        task.task_type,
                        "queued",
                        position,
                        task.command_preview,
                        ",".join(task.depends_on),
                    ),
                )

            connection.commit()
        return self.get_run(run_id)

    def mark_running(self, run_id: str) -> None:
        """Mark a run as actively executing."""
        with self._lock, self._connect() as connection:
            connection.execute(
                "UPDATE runs SET status = ?, started_at = ? WHERE id = ?",
                ("running", _to_iso(datetime.now(timezone.utc)), run_id),
            )
            connection.commit()

    def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        exit_code: int | None = None,
        error: str | None = None,
    ) -> None:
        """Persist the final pipeline-level outcome for one run."""
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE runs
                SET status = ?, finished_at = ?, exit_code = ?, error = ?
                WHERE id = ?
                """,
                (
                    status,
                    _to_iso(datetime.now(timezone.utc)),
                    exit_code,
                    error,
                    run_id,
                ),
            )
            connection.commit()

    def mark_task_running(self, run_id: str, task_id: str) -> None:
        """Mark one task as actively executing."""
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE task_runs
                SET status = ?, started_at = ?
                WHERE run_id = ? AND task_id = ?
                """,
                ("running", _to_iso(datetime.now(timezone.utc)), run_id, task_id),
            )
            connection.commit()

    def finish_task_run(
        self,
        run_id: str,
        task_id: str,
        *,
        status: str,
        exit_code: int | None = None,
        error: str | None = None,
    ) -> None:
        """Persist the final outcome for one task run."""
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE task_runs
                SET status = ?, finished_at = ?, exit_code = ?, error = ?
                WHERE run_id = ? AND task_id = ?
                """,
                (
                    status,
                    _to_iso(datetime.now(timezone.utc)),
                    exit_code,
                    error,
                    run_id,
                    task_id,
                ),
            )
            connection.commit()

    def mark_task_reused(self, run_id: str, task_id: str, source_run_id: str) -> None:
        """Mark a task as reused from a previous successful retry source."""
        now = _to_iso(datetime.now(timezone.utc))
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE task_runs
                SET status = 'success', started_at = ?, finished_at = ?, exit_code = 0, error = NULL
                WHERE run_id = ? AND task_id = ?
                """,
                (now, now, run_id, task_id),
            )
            connection.commit()
        self.append_log(
            run_id,
            f"Reused successful result from run {source_run_id}.",
            task_id=task_id,
        )

    def append_log(
        self,
        run_id: str,
        message: str,
        stream: str = "stdout",
        task_id: str | None = None,
    ) -> None:
        """Append one raw log line to the run log stream."""
        message = message.rstrip()
        if not message:
            return
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO logs (run_id, task_id, created_at, stream, message)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    task_id,
                    _to_iso(datetime.now(timezone.utc)),
                    stream,
                    message,
                ),
            )
            connection.commit()

    def get_run(self, run_id: str) -> RunRecord | None:
        """Load one run with aggregate task and log counters."""
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    runs.*,
                    (SELECT COUNT(*) FROM logs WHERE logs.run_id = runs.id) AS log_count,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id) AS task_count,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'success') AS successful_tasks,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'failed') AS failed_tasks,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'skipped') AS skipped_tasks
                FROM runs
                WHERE runs.id = ?
                """,
                (run_id,),
            ).fetchone()
        return self._row_to_run(row) if row else None

    def list_runs(
        self,
        *,
        pipeline_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[RunRecord]:
        """List recent runs with optional pipeline and status filters."""
        conditions: list[str] = []
        params: list[object] = []
        if pipeline_id:
            conditions.append("pipeline_id = ?")
            params.append(pipeline_id)
        if status:
            conditions.append("status = ?")
            params.append(status)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT
                runs.*,
                (SELECT COUNT(*) FROM logs WHERE logs.run_id = runs.id) AS log_count,
                (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id) AS task_count,
                (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'success') AS successful_tasks,
                (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'failed') AS failed_tasks,
                (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'skipped') AS skipped_tasks
            FROM runs
            {where_clause}
            ORDER BY COALESCE(runs.started_at, runs.created_at) DESC
            LIMIT ?
        """
        params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._row_to_run(row) for row in rows]

    def list_task_runs(self, run_id: str) -> list[TaskRunRecord]:
        """List task runs for one pipeline run in declared order."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    task_runs.*,
                    (SELECT COUNT(*) FROM logs WHERE logs.run_id = task_runs.run_id AND logs.task_id = task_runs.task_id) AS log_count
                FROM task_runs
                WHERE run_id = ?
                ORDER BY position ASC
                """,
                (run_id,),
            ).fetchall()
        return [self._row_to_task_run(row) for row in rows]

    def list_logs(self, run_id: str, limit: int | None = None):
        """List raw logs newest first for one run."""
        query = """
            SELECT run_id, task_id, created_at, stream, message
            FROM logs
            WHERE run_id = ?
            ORDER BY id DESC
        """
        params: list[object] = [run_id]
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [
            LogRecord(
                run_id=row["run_id"],
                task_id=row["task_id"],
                created_at=_from_iso(row["created_at"]) or datetime.now(timezone.utc),
                stream=row["stream"],
                message=row["message"],
            )
            for row in rows
        ]

    def get_latest_run_for_pipeline(self, pipeline_id: str) -> RunRecord | None:
        """Return the most recent run for one pipeline."""
        runs = self.list_runs(pipeline_id=pipeline_id, limit=1)
        return runs[0] if runs else None

    def get_latest_task_states_for_pipeline(self, pipeline_id: str) -> dict[str, str]:
        """Return the latest known task status map for one pipeline."""
        latest_run = self.get_latest_run_for_pipeline(pipeline_id)
        if latest_run is None:
            return {}
        return {task.task_id: task.status for task in self.list_task_runs(latest_run.run_id)}

    def count_running_runs(self, pipeline_id: str | None = None) -> int:
        """Count active pipeline runs globally or per pipeline."""
        conditions = ["status = 'running'"]
        params: list[object] = []
        if pipeline_id:
            conditions.append("pipeline_id = ?")
            params.append(pipeline_id)
        where_clause = " AND ".join(conditions)
        with self._connect() as connection:
            row = connection.execute(
                f"SELECT COUNT(*) AS count FROM runs WHERE {where_clause}",
                params,
            ).fetchone()
        return int(row["count"])

    def has_run_for_slot(self, pipeline_id: str, scheduled_for: datetime) -> bool:
        """Return whether a scheduled slot has already been materialized."""
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM runs
                WHERE pipeline_id = ? AND scheduled_for = ?
                LIMIT 1
                """,
                (pipeline_id, _to_iso(scheduled_for)),
            ).fetchone()
        return row is not None

    def get_stats(
        self,
        scheduled_pipeline_count: int,
        total_pipeline_count: int,
    ) -> DashboardStats:
        """Compute dashboard counters from the run table."""
        with self._connect() as connection:
            totals = connection.execute(
                """
                SELECT
                    COUNT(*) AS total_runs,
                    SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_runs,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_runs,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs
                FROM runs
                """
            ).fetchone()
        return DashboardStats(
            total_pipelines=total_pipeline_count,
            scheduled_pipelines=scheduled_pipeline_count,
            total_runs=int(totals["total_runs"] or 0),
            running_runs=int(totals["running_runs"] or 0),
            successful_runs=int(totals["successful_runs"] or 0),
            failed_runs=int(totals["failed_runs"] or 0),
        )

    def set_pipeline_paused(self, pipeline_id: str, paused: bool) -> None:
        """Persist a schedule pause override for one pipeline."""
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO pipeline_overrides (pipeline_id, paused)
                VALUES (?, ?)
                ON CONFLICT(pipeline_id)
                DO UPDATE SET paused = excluded.paused
                """,
                (pipeline_id, 1 if paused else 0),
            )
            connection.commit()

    def is_pipeline_paused(self, pipeline_id: str) -> bool:
        """Return whether a pipeline is paused in the override table."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT paused FROM pipeline_overrides WHERE pipeline_id = ?",
                (pipeline_id,),
            ).fetchone()
        return bool(row["paused"]) if row else False

    def list_paused_pipeline_ids(self) -> set[str]:
        """Return the set of paused pipeline ids."""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT pipeline_id FROM pipeline_overrides WHERE paused = 1"
            ).fetchall()
        return {row["pipeline_id"] for row in rows}

    def set_meta(self, key: str, value: str) -> None:
        """Persist one metadata key used by the scheduler."""
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO meta (key, value)
                VALUES (?, ?)
                ON CONFLICT(key)
                DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )
            connection.commit()

    def get_meta(self, key: str) -> str | None:
        """Load one metadata value used by the scheduler."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT value FROM meta WHERE key = ?",
                (key,),
            ).fetchone()
        return row["value"] if row else None

    def _row_to_run(self, row: sqlite3.Row) -> RunRecord:
        """Convert one run row into a RunRecord."""
        return RunRecord(
            run_id=row["id"],
            pipeline_id=row["pipeline_id"],
            pipeline_title=row["pipeline_title"],
            status=row["status"],
            trigger=row["trigger"],
            command=row["command"],
            primary_entry=row["primary_entry"] or row["command"],
            created_at=_from_iso(row["created_at"]) or datetime.now(timezone.utc),
            started_at=_from_iso(row["started_at"]),
            finished_at=_from_iso(row["finished_at"]),
            scheduled_for=_from_iso(row["scheduled_for"]),
            exit_code=row["exit_code"],
            error=row["error"],
            log_count=int(row["log_count"] or 0),
            task_count=int(row["task_count"] or 0),
            successful_tasks=int(row["successful_tasks"] or 0),
            failed_tasks=int(row["failed_tasks"] or 0),
            skipped_tasks=int(row["skipped_tasks"] or 0),
            retry_of=row["retry_of"] if "retry_of" in row.keys() else None,
            retry_mode=row["retry_mode"] if "retry_mode" in row.keys() else None,
            retry_task_id=row["retry_task_id"] if "retry_task_id" in row.keys() else None,
        )

    def _row_to_task_run(self, row: sqlite3.Row) -> TaskRunRecord:
        """Convert one task run row into a TaskRunRecord."""
        depends_on = tuple(item for item in str(row["depends_on"] or "").split(",") if item)
        return TaskRunRecord(
            run_id=row["run_id"],
            task_id=row["task_id"],
            title=row["title"],
            task_type=row["task_type"],
            status=row["status"],
            position=int(row["position"]),
            command_preview=row["command_preview"],
            started_at=_from_iso(row["started_at"]),
            finished_at=_from_iso(row["finished_at"]),
            exit_code=row["exit_code"],
            error=row["error"],
            depends_on=depends_on,
            log_count=int(row["log_count"] or 0),
        )
