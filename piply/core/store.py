"""SQLite-backed storage for runs, task runs, logs, and scheduler state."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .dialects import SqliteDialect, build_dialect
from .models import (
    DashboardStats,
    LogRecord,
    PipelineDefinition,
    RetryMode,
    RunRecord,
    TaskOutputRecord,
    TaskRunRecord,
    TriggerQueueRecord,
)
from .outputs import load_json_output, serialize_task_output


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
        self.dialect = build_dialect(database_path)
        # Only a SQLite store has a file. Callers that report or back up the
        # location use `location` instead, which is a plain string and is
        # already credential-free for a DSN.
        self.database_path = self.dialect.database_path if isinstance(self.dialect, SqliteDialect) else None
        self.location = self.dialect.describe()
        self._lock = threading.Lock()
        self._run_columns: set[str] = set()
        self._task_run_columns: set[str] = set()
        self._log_columns: set[str] = set()
        self._initialize()

    @property
    def is_sqlite(self) -> bool:
        """Return whether runtime state lives in a local SQLite file."""
        return isinstance(self.dialect, SqliteDialect)

    def describe_location(self) -> str:
        """Return a human-readable, credential-free description of the store."""
        return self.dialect.describe()

    @contextmanager
    def _connect(self):
        """Open a connection to the configured metadata store."""
        connection = self.dialect.connect()
        try:
            self.dialect.prepare(connection)
            yield connection
        finally:
            connection.close()

    def _refresh_schema_info(self, connection) -> None:
        """Cache schema metadata used for compatibility-aware inserts."""
        self._run_columns = self.dialect.existing_columns(connection, "runs")
        self._task_run_columns = self.dialect.existing_columns(connection, "task_runs")
        self._log_columns = self.dialect.existing_columns(connection, "logs")

    def _initialize(self) -> None:
        """Create or migrate the runtime schema in place."""
        autoincrement_pk = self.dialect.autoincrement_pk
        with self._connect() as connection:
            connection.executescript(
                f"""
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
                    heartbeat_at TEXT,
                    retry_of TEXT,
                    retry_mode TEXT,
                    retry_task_id TEXT,
                    parent_run_id TEXT,
                    parent_pipeline_id TEXT,
                    tenant_id TEXT,
                    owner_pid INTEGER,
                    run_config TEXT
                );

                CREATE TABLE IF NOT EXISTS task_runs (
                    id {autoincrement_pk},
                    run_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    command_preview TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0,
                    timeout_seconds INTEGER,
                    run_if TEXT,
                    depends_on TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    exit_code INTEGER,
                    error TEXT,
                    FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
                    UNIQUE(run_id, task_id)
                );

                CREATE TABLE IF NOT EXISTS logs (
                    id {autoincrement_pk},
                    run_id TEXT NOT NULL,
                    task_id TEXT,
                    created_at TEXT NOT NULL,
                    stream TEXT NOT NULL,
                    message TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS task_outputs (
                    run_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    output_type TEXT NOT NULL,
                    preview TEXT NOT NULL,
                    is_json INTEGER NOT NULL DEFAULT 0,
                    json_value TEXT,
                    metadata_json TEXT,
                    size_bytes INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
                    UNIQUE(run_id, task_id)
                );

                CREATE TABLE IF NOT EXISTS task_artifacts (
                    id {autoincrement_pk},
                    run_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    path TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL DEFAULT 0,
                    content_type TEXT,
                    modified_at TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
                    UNIQUE(run_id, task_id, path)
                );

                CREATE TABLE IF NOT EXISTS sensor_health (
                    sensor_key TEXT PRIMARY KEY,
                    pipeline_id TEXT NOT NULL,
                    sensor_id TEXT NOT NULL,
                    sensor_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_polled_at TEXT,
                    last_success_at TEXT,
                    last_event_at TEXT,
                    last_error TEXT,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    poll_count INTEGER NOT NULL DEFAULT 0,
                    event_count INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'user',
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    last_login_at TEXT
                );

                CREATE TABLE IF NOT EXISTS user_permissions (
                    username TEXT NOT NULL,
                    pipeline_id TEXT NOT NULL,
                    actions TEXT NOT NULL,
                    PRIMARY KEY (username, pipeline_id)
                );

                CREATE TABLE IF NOT EXISTS pipeline_overrides (
                    pipeline_id TEXT PRIMARY KEY,
                    paused INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS trigger_queue (
                    id {autoincrement_pk},
                    pipeline_id TEXT NOT NULL,
                    trigger TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    available_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    scheduled_for TEXT,
                    source_key TEXT,
                    dedupe_key TEXT,
                    payload_json TEXT,
                    dispatched_at TEXT,
                    dispatched_run_id TEXT,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS sensor_state (
                    sensor_key TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_runs_pipeline_id ON runs(pipeline_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
                CREATE INDEX IF NOT EXISTS idx_task_runs_run_id ON task_runs(run_id, position);
                CREATE INDEX IF NOT EXISTS idx_task_outputs_run_id ON task_outputs(run_id);
                CREATE INDEX IF NOT EXISTS idx_task_artifacts_run_id ON task_artifacts(run_id, task_id);
                CREATE INDEX IF NOT EXISTS idx_logs_created_at ON logs(created_at);
                CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at);
                CREATE INDEX IF NOT EXISTS idx_logs_run_id ON logs(run_id, id DESC);
                CREATE INDEX IF NOT EXISTS idx_trigger_queue_status_available
                    ON trigger_queue(status, available_at, id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_unique_schedule_slot
                    ON runs(pipeline_id, scheduled_for)
                    WHERE scheduled_for IS NOT NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS idx_trigger_queue_dedupe
                    ON trigger_queue(dedupe_key)
                    WHERE dedupe_key IS NOT NULL;
                """
            )

            self._refresh_schema_info(connection)

            if "primary_entry" not in self._run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN primary_entry TEXT")
                # script_path/working_dir only exist in pre-1.0 SQLite databases.
                legacy = self._run_columns & {"script_path", "working_dir"}
                fallback = ", ".join([*sorted(legacy), "command", "''"])
                connection.execute(f"UPDATE runs SET primary_entry = COALESCE({fallback})")

            if "retry_of" not in self._run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN retry_of TEXT")
            if "retry_mode" not in self._run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN retry_mode TEXT")
            if "retry_task_id" not in self._run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN retry_task_id TEXT")
            if "heartbeat_at" not in self._run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN heartbeat_at TEXT")
                connection.execute(
                    """
                    UPDATE runs
                    SET heartbeat_at = COALESCE(finished_at, started_at, created_at)
                    WHERE heartbeat_at IS NULL
                    """
                )
            if "parent_run_id" not in self._run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN parent_run_id TEXT")
            if "parent_pipeline_id" not in self._run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN parent_pipeline_id TEXT")
            if "tenant_id" not in self._run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN tenant_id TEXT")
            if "owner_pid" not in self._run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN owner_pid INTEGER")
            if "run_config" not in self._run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN run_config TEXT")

            if "priority" not in self._task_run_columns:
                connection.execute("ALTER TABLE task_runs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0")
            if "timeout_seconds" not in self._task_run_columns:
                connection.execute("ALTER TABLE task_runs ADD COLUMN timeout_seconds INTEGER")
            if "run_if" not in self._task_run_columns:
                connection.execute("ALTER TABLE task_runs ADD COLUMN run_if TEXT")

            connection.execute("CREATE INDEX IF NOT EXISTS idx_runs_tenant ON runs(tenant_id)")

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
        parent_run_id: str | None = None,
        parent_pipeline_id: str | None = None,
        tenant_id: str | None = None,
        run_config: dict[str, object] | None = None,
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
                "heartbeat_at": _to_iso(created_at),
                "retry_of": retry_of,
                "retry_mode": retry_mode,
                "retry_task_id": retry_task_id,
                "parent_run_id": parent_run_id,
                "parent_pipeline_id": parent_pipeline_id,
                "tenant_id": tenant_id,
                "owner_pid": os.getpid(),
                "run_config": None if run_config is None else json.dumps(run_config, default=str, sort_keys=True),
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
                        command_preview, priority, timeout_seconds, run_if, depends_on
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        task.task_id,
                        task.title,
                        task.task_type,
                        "queued",
                        position,
                        task.command_preview,
                        task.priority,
                        task.timeout_seconds,
                        task.run_if,
                        ",".join(task.depends_on),
                    ),
                )

            connection.commit()
        return self.get_run(run_id)

    def mark_running(self, run_id: str) -> None:
        """Mark a run as actively executing."""
        now = _to_iso(datetime.now(timezone.utc))
        with self._lock, self._connect() as connection:
            connection.execute(
                "UPDATE runs SET status = ?, started_at = ?, heartbeat_at = ? WHERE id = ? AND status = 'queued'",
                ("running", now, now, run_id),
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
        now = _to_iso(datetime.now(timezone.utc))
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE runs
                SET status = ?, finished_at = ?, exit_code = ?, error = ?, heartbeat_at = ?
                WHERE id = ? AND status IN ('queued', 'running')
                """,
                (
                    status,
                    now,
                    exit_code,
                    error,
                    now,
                    run_id,
                ),
            )
            connection.commit()

    def touch_run(self, run_id: str) -> None:
        """Refresh the heartbeat timestamp for one run."""
        with self._lock, self._connect() as connection:
            connection.execute(
                "UPDATE runs SET heartbeat_at = ? WHERE id = ? AND status IN ('queued', 'running')",
                (_to_iso(datetime.now(timezone.utc)), run_id),
            )
            connection.commit()

    def mark_task_running(self, run_id: str, task_id: str) -> None:
        """Mark one task as actively executing."""
        now = _to_iso(datetime.now(timezone.utc))
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE task_runs
                SET status = ?, started_at = ?
                WHERE run_id = ? AND task_id = ?
                  AND status = 'queued'
                  AND EXISTS (
                      SELECT 1
                      FROM runs
                      WHERE id = ? AND status IN ('queued', 'running')
                  )
                """,
                ("running", now, run_id, task_id, run_id),
            )
            connection.execute(
                "UPDATE runs SET heartbeat_at = ? WHERE id = ? AND status IN ('queued', 'running')",
                (now, run_id),
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
        now = _to_iso(datetime.now(timezone.utc))
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE task_runs
                SET status = ?, finished_at = ?, exit_code = ?, error = ?
                WHERE run_id = ? AND task_id = ? AND status IN ('queued', 'running')
                """,
                (
                    status,
                    now,
                    exit_code,
                    error,
                    run_id,
                    task_id,
                ),
            )
            connection.execute(
                "UPDATE runs SET heartbeat_at = ? WHERE id = ?",
                (now, run_id),
            )
            connection.commit()

    def record_task_output(self, run_id: str, task_id: str, output: object) -> TaskOutputRecord:
        """Persist bounded metadata and JSON value for one successful task output."""
        serialized = serialize_task_output(output)
        now = datetime.now(timezone.utc)
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO task_outputs (
                    run_id, task_id, output_type, preview, is_json, json_value,
                    metadata_json, size_bytes, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, task_id)
                DO UPDATE SET
                    output_type = excluded.output_type,
                    preview = excluded.preview,
                    is_json = excluded.is_json,
                    json_value = excluded.json_value,
                    metadata_json = excluded.metadata_json,
                    size_bytes = excluded.size_bytes,
                    created_at = excluded.created_at
                """,
                (
                    run_id,
                    task_id,
                    serialized.output_type,
                    serialized.preview,
                    1 if serialized.is_json else 0,
                    serialized.json_value,
                    json.dumps(serialized.metadata, sort_keys=True),
                    serialized.size_bytes,
                    _to_iso(now),
                ),
            )
            connection.execute(
                "UPDATE runs SET heartbeat_at = ? WHERE id = ?",
                (_to_iso(now), run_id),
            )
            connection.commit()
        record = self.get_task_output(run_id, task_id)
        assert record is not None
        return record

    def get_task_output(self, run_id: str, task_id: str) -> TaskOutputRecord | None:
        """Return persisted output metadata for one task."""
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM task_outputs
                WHERE run_id = ? AND task_id = ?
                """,
                (run_id, task_id),
            ).fetchone()
        return self._row_to_task_output(row) if row else None

    def list_task_outputs(self, run_id: str) -> list[TaskOutputRecord]:
        """Return all task outputs captured for one run in task order."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT task_outputs.*
                FROM task_outputs
                LEFT JOIN task_runs
                  ON task_runs.run_id = task_outputs.run_id
                 AND task_runs.task_id = task_outputs.task_id
                WHERE task_outputs.run_id = ?
                ORDER BY COALESCE(task_runs.position, 999999), task_outputs.created_at ASC
                """,
                (run_id,),
            ).fetchall()
        return [self._row_to_task_output(row) for row in rows]

    def output_context_for_run(self, run_id: str) -> dict[str, object]:
        """Return JSON-restorable outputs for use as downstream pipeline context."""
        context: dict[str, object] = {}
        for output in self.list_task_outputs(run_id):
            if output.is_json and output.json_value is not None:
                context[output.task_id] = load_json_output(output.json_value)
        return context

    def record_task_artifacts(self, run_id: str, task_id: str, artifacts: list[dict[str, object]]) -> None:
        """Persist the files one task declared and produced."""
        if not artifacts:
            return
        now = _to_iso(datetime.now(timezone.utc))
        with self._lock, self._connect() as connection:
            for artifact in artifacts:
                connection.execute(
                    """
                    INSERT INTO task_artifacts (
                        run_id, task_id, name, path, size_bytes, content_type, modified_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, task_id, path)
                    DO UPDATE SET
                        size_bytes = excluded.size_bytes,
                        content_type = excluded.content_type,
                        modified_at = excluded.modified_at
                    """,
                    (
                        run_id,
                        task_id,
                        artifact.get("name"),
                        artifact.get("path"),
                        int(artifact.get("size_bytes") or 0),
                        artifact.get("content_type"),
                        artifact.get("modified_at"),
                        now,
                    ),
                )
            connection.commit()

    def list_task_artifacts(self, run_id: str, task_id: str | None = None) -> list[dict[str, object]]:
        """Return recorded artifacts for one run, optionally narrowed to one task."""
        conditions = ["run_id = ?"]
        params: list[object] = [run_id]
        if task_id is not None:
            conditions.append("task_id = ?")
            params.append(task_id)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT task_id, name, path, size_bytes, content_type, modified_at, created_at
                FROM task_artifacts
                WHERE {" AND ".join(conditions)}
                ORDER BY task_id ASC, name ASC
                """,
                params,
            ).fetchall()
        return [
            {
                "task_id": row["task_id"],
                "name": row["name"],
                "path": row["path"],
                "size_bytes": int(row["size_bytes"] or 0),
                "content_type": row["content_type"],
                "modified_at": row["modified_at"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def record_sensor_health(
        self,
        sensor_key: str,
        *,
        pipeline_id: str,
        sensor_id: str,
        sensor_type: str,
        succeeded: bool,
        produced_event: bool,
        error: str | None = None,
    ) -> None:
        """Record the outcome of one sensor poll so failures stay visible in the UI."""
        now = _to_iso(datetime.now(timezone.utc))
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO sensor_health (
                    sensor_key, pipeline_id, sensor_id, sensor_type, status,
                    last_polled_at, last_success_at, last_event_at, last_error,
                    consecutive_failures, poll_count, event_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                ON CONFLICT(sensor_key) DO UPDATE SET
                    pipeline_id = excluded.pipeline_id,
                    sensor_id = excluded.sensor_id,
                    sensor_type = excluded.sensor_type,
                    status = excluded.status,
                    last_polled_at = excluded.last_polled_at,
                    last_success_at = COALESCE(excluded.last_success_at, sensor_health.last_success_at),
                    last_event_at = COALESCE(excluded.last_event_at, sensor_health.last_event_at),
                    last_error = excluded.last_error,
                    consecutive_failures = CASE
                        WHEN excluded.status = 'healthy' THEN 0
                        ELSE sensor_health.consecutive_failures + 1
                    END,
                    poll_count = sensor_health.poll_count + 1,
                    event_count = sensor_health.event_count + excluded.event_count
                """,
                (
                    sensor_key,
                    pipeline_id,
                    sensor_id,
                    sensor_type,
                    "healthy" if succeeded else "failing",
                    now,
                    now if succeeded else None,
                    now if produced_event else None,
                    error,
                    0 if succeeded else 1,
                    1 if produced_event else 0,
                ),
            )
            connection.commit()

    def list_sensor_health(self) -> list[dict[str, object]]:
        """Return the recorded health of every polled sensor."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM sensor_health
                ORDER BY status DESC, pipeline_id ASC, sensor_id ASC
                """
            ).fetchall()
        return [
            {
                "sensor_key": row["sensor_key"],
                "pipeline_id": row["pipeline_id"],
                "sensor_id": row["sensor_id"],
                "sensor_type": row["sensor_type"],
                "status": row["status"],
                "last_polled_at": row["last_polled_at"],
                "last_success_at": row["last_success_at"],
                "last_event_at": row["last_event_at"],
                "last_error": row["last_error"],
                "consecutive_failures": int(row["consecutive_failures"] or 0),
                "poll_count": int(row["poll_count"] or 0),
                "event_count": int(row["event_count"] or 0),
            }
            for row in rows
        ]

    def cancel_run(self, run_id: str, reason: str = "Run cancelled by user.") -> None:
        """Mark one queued or running run as cancelled."""
        now = _to_iso(datetime.now(timezone.utc))
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE runs
                SET status = 'cancelled',
                    finished_at = ?,
                    exit_code = NULL,
                    error = COALESCE(error, ?),
                    heartbeat_at = ?
                WHERE id = ? AND status IN ('queued', 'running')
                """,
                (now, reason, now, run_id),
            )
            connection.execute(
                """
                UPDATE task_runs
                SET status = 'cancelled',
                    finished_at = ?,
                    exit_code = NULL,
                    error = COALESCE(error, ?)
                WHERE run_id = ? AND status IN ('queued', 'running')
                """,
                (now, reason, run_id),
            )
            connection.execute(
                """
                INSERT INTO logs (run_id, task_id, created_at, stream, message)
                VALUES (?, NULL, ?, 'stderr', ?)
                """,
                (run_id, now, reason),
            )
            connection.commit()

    def interrupt_run(
        self,
        run_id: str,
        *,
        reason: str = "Run interrupted during shutdown.",
        queued_reason: str = "Task did not start before the run was interrupted.",
    ) -> bool:
        """Mark one queued or running run as interrupted during shutdown or recovery."""
        now = _to_iso(datetime.now(timezone.utc))
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE runs
                SET status = 'interrupted',
                    finished_at = ?,
                    exit_code = NULL,
                    error = COALESCE(error, ?),
                    heartbeat_at = ?
                WHERE id = ? AND status IN ('queued', 'running')
                """,
                (now, reason, now, run_id),
            )
            if cursor.rowcount <= 0:
                connection.commit()
                return False
            connection.execute(
                """
                UPDATE task_runs
                SET status = 'interrupted',
                    finished_at = ?,
                    exit_code = NULL,
                    error = COALESCE(error, ?)
                WHERE run_id = ? AND status = 'running'
                """,
                (now, reason, run_id),
            )
            connection.execute(
                """
                UPDATE task_runs
                SET status = 'cancelled',
                    finished_at = ?,
                    exit_code = NULL,
                    error = COALESCE(error, ?)
                WHERE run_id = ? AND status = 'queued'
                """,
                (now, queued_reason, run_id),
            )
            connection.execute(
                """
                INSERT INTO logs (run_id, task_id, created_at, stream, message)
                VALUES (?, NULL, ?, 'stderr', ?)
                """,
                (run_id, now, reason),
            )
            connection.commit()
        return True

    def mark_unfinished_tasks_timed_out(self, run_id: str, reason: str) -> None:
        """Flip every task that never completed to timed_out after a pipeline timeout."""
        now = _to_iso(datetime.now(timezone.utc))
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE task_runs
                SET status = 'timed_out',
                    finished_at = COALESCE(finished_at, ?),
                    error = ?
                WHERE run_id = ? AND status IN ('queued', 'running', 'cancelled')
                """,
                (now, reason, run_id),
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
            connection.execute(
                "UPDATE runs SET heartbeat_at = ? WHERE id = ?",
                (_to_iso(datetime.now(timezone.utc)), run_id),
            )
            connection.commit()

    def reconcile_stale_runs(self, stale_after_seconds: int, *, run_id: str | None = None) -> list[str]:
        """Mark long-silent queued or running runs as interrupted.

        Passing ``run_id`` narrows the scan to a single indexed row so read
        paths for one run stay accurate without a full table sweep.
        """
        with self._lock, self._connect() as connection:
            now = datetime.now(timezone.utc)
            cutoff_dt = _to_iso(now - timedelta(seconds=stale_after_seconds))
            conditions = ["status IN ('queued', 'running')", "COALESCE(heartbeat_at, started_at, created_at) < ?"]
            params: list[object] = [cutoff_dt]
            if run_id is not None:
                conditions.append("id = ?")
                params.append(run_id)
            rows = connection.execute(
                f"SELECT id FROM runs WHERE {' AND '.join(conditions)}",
                params,
            ).fetchall()

            if not rows:
                return []

            stale_ids = [row["id"] for row in rows]
            now_iso = _to_iso(now)
            for run_id in stale_ids:
                connection.execute(
                    """
                    UPDATE runs
                    SET status = 'interrupted',
                        finished_at = ?,
                        exit_code = NULL,
                        error = COALESCE(error, 'Run marked interrupted after heartbeat timeout recovery.'),
                        heartbeat_at = ?
                    WHERE id = ? AND status IN ('queued', 'running')
                    """,
                    (now_iso, now_iso, run_id),
                )
                connection.execute(
                    """
                    UPDATE task_runs
                    SET status = 'interrupted',
                        finished_at = ?,
                        exit_code = NULL,
                        error = COALESCE(error, 'Task interrupted after heartbeat timeout recovery.')
                    WHERE run_id = ? AND status = 'running'
                    """,
                    (now_iso, run_id),
                )
                connection.execute(
                    """
                    UPDATE task_runs
                    SET status = 'cancelled',
                        finished_at = ?,
                        exit_code = NULL,
                        error = COALESCE(error, 'Task did not start before heartbeat timeout recovery.')
                    WHERE run_id = ? AND status = 'queued'
                    """,
                    (now_iso, run_id),
                )
                connection.execute(
                    """
                    INSERT INTO logs (run_id, task_id, created_at, stream, message)
                    VALUES (?, NULL, ?, 'stderr', 'Run marked interrupted after heartbeat timeout recovery.')
                    """,
                    (run_id, now_iso),
                )
            connection.commit()
        return stale_ids

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
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status IN ('failed', 'timed_out')) AS failed_tasks,
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
        tenant_id: str | None = None,
        trigger: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        sort: str = "started_desc",
        limit: int = 50,
    ) -> list[RunRecord]:
        """List recent runs with optional filters and an explicit sort order.

        ``status`` and ``trigger`` accept a comma-separated list so the UI can
        offer multi-select without a second round trip.
        """
        conditions: list[str] = []
        params: list[object] = []
        if pipeline_id:
            conditions.append("pipeline_id = ?")
            params.append(pipeline_id)
        if status:
            values = [item.strip() for item in str(status).split(",") if item.strip()]
            if values:
                conditions.append(f"status IN ({', '.join('?' for _ in values)})")
                params.extend(values)
        if trigger:
            values = [item.strip() for item in str(trigger).split(",") if item.strip()]
            if values:
                conditions.append(f'"trigger" IN ({", ".join("?" for _ in values)})')
                params.extend(values)
        if tenant_id:
            conditions.append("tenant_id = ?")
            params.append(tenant_id)
        if created_after is not None:
            conditions.append("created_at >= ?")
            params.append(_to_iso(created_after))
        if created_before is not None:
            conditions.append("created_at <= ?")
            params.append(_to_iso(created_before))

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT
                runs.*,
                (SELECT COUNT(*) FROM logs WHERE logs.run_id = runs.id) AS log_count,
                (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id) AS task_count,
                (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'success') AS successful_tasks,
                (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status IN ('failed', 'timed_out')) AS failed_tasks,
                (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'skipped') AS skipped_tasks
            FROM runs
            {where_clause}
            ORDER BY {self._run_sort_clause(sort)}
            LIMIT ?
        """
        params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._row_to_run(row) for row in rows]

    #: Sort keys the runs page offers, mapped to their ORDER BY clause.
    RUN_SORTS = {
        "started_desc": "COALESCE(runs.started_at, runs.created_at) DESC, runs.id DESC",
        "started_asc": "COALESCE(runs.started_at, runs.created_at) ASC, runs.id ASC",
        "pipeline": "runs.pipeline_title ASC, COALESCE(runs.started_at, runs.created_at) DESC",
        "status": "runs.status ASC, COALESCE(runs.started_at, runs.created_at) DESC",
        "trigger": '"trigger" ASC, COALESCE(runs.started_at, runs.created_at) DESC',
    }

    def _run_sort_clause(self, sort: str) -> str:
        """Return a validated ORDER BY clause.

        The value comes from a query string, so it is looked up rather than
        interpolated, and an unknown key falls back to the default.
        """
        if sort in {"duration_desc", "duration_asc"}:
            direction = "DESC" if sort == "duration_desc" else "ASC"
            duration = self.dialect.epoch_diff("runs.finished_at", "runs.started_at")
            # Unfinished runs have no duration; keep them out of the way.
            return f"CASE WHEN runs.finished_at IS NULL THEN 1 ELSE 0 END, {duration} {direction}"
        return self.RUN_SORTS.get(sort, self.RUN_SORTS["started_desc"])

    def runs_by_ids(self, run_ids: list[str]) -> dict[str, dict[str, object]]:
        """Return compact run records by id, for building lineage chains."""
        if not run_ids:
            return {}
        unique = list(dict.fromkeys(run_ids))
        placeholders = ", ".join("?" for _ in unique)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT id, pipeline_id, pipeline_title, status, "trigger", parent_run_id,
                       parent_pipeline_id, started_at, created_at
                FROM runs
                WHERE id IN ({placeholders})
                """,
                unique,
            ).fetchall()
        return {
            str(row["id"]): {
                "run_id": row["id"],
                "pipeline_id": row["pipeline_id"],
                "pipeline_title": row["pipeline_title"],
                "status": row["status"],
                "trigger": row["trigger"],
                "parent_run_id": row["parent_run_id"],
                "parent_pipeline_id": row["parent_pipeline_id"],
                "started_at": row["started_at"] or row["created_at"],
            }
            for row in rows
        }

    def list_task_runs(self, run_id: str) -> list[TaskRunRecord]:
        """List task runs for one pipeline run in declared order."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    task_runs.*,
                    task_outputs.output_type,
                    task_outputs.preview AS output_preview,
                    task_outputs.is_json AS output_is_json,
                    (SELECT COUNT(*) FROM logs WHERE logs.run_id = task_runs.run_id AND logs.task_id = task_runs.task_id) AS log_count
                FROM task_runs
                LEFT JOIN task_outputs
                  ON task_outputs.run_id = task_runs.run_id
                 AND task_outputs.task_id = task_runs.task_id
                WHERE task_runs.run_id = ?
                ORDER BY position ASC
                """,
                (run_id,),
            ).fetchall()
        return [self._row_to_task_run(row) for row in rows]

    def list_logs(
        self,
        run_id: str,
        limit: int | None = None,
        offset: int = 0,
        *,
        task_id: str | None = None,
    ):
        """List raw logs newest first for one run."""
        conditions = ["run_id = ?"]
        params: list[object] = [run_id]
        if task_id is not None:
            conditions.append("task_id = ?")
            params.append(task_id)

        query = """
            SELECT run_id, task_id, created_at, stream, message
            FROM logs
            WHERE {where_clause}
            ORDER BY id DESC
        """.format(where_clause=" AND ".join(conditions))
        if limit is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])

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

    def search_logs(
        self,
        *,
        query: str | None = None,
        pipeline_id: str | None = None,
        pipeline_ids: set[str] | None = None,
        task_id: str | None = None,
        limit: int = 300,
    ) -> list[LogRecord]:
        """Search recent logs across runs with lightweight SQLite filters.

        ``pipeline_ids`` restricts the search to a set of pipelines, which is
        how the API limits results to what the caller is allowed to read. An
        empty set matches nothing rather than everything.
        """
        conditions: list[str] = []
        params: list[object] = []
        if query:
            conditions.append("logs.message LIKE ?")
            params.append(f"%{query}%")
        if pipeline_id:
            conditions.append("runs.pipeline_id = ?")
            params.append(pipeline_id)
        if pipeline_ids is not None:
            if not pipeline_ids:
                return []
            ordered = sorted(pipeline_ids)
            conditions.append(f"runs.pipeline_id IN ({', '.join('?' for _ in ordered)})")
            params.extend(ordered)
        if task_id:
            conditions.append("logs.task_id = ?")
            params.append(task_id)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT logs.run_id, logs.task_id, logs.created_at, logs.stream, logs.message
                FROM logs
                JOIN runs ON runs.id = logs.run_id
                {where_clause}
                ORDER BY logs.id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
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

    def tail_logs(
        self,
        *,
        run_id: str | None = None,
        pipeline_id: str | None = None,
        task_id: str | None = None,
        after_id: int = 0,
        limit: int = 500,
    ) -> list[dict[str, object]]:
        """Return log lines after a cursor, oldest first, for follow-mode readers.

        The monotonic rowid is used as the cursor instead of a timestamp so
        lines written inside the same millisecond are never skipped.
        """
        conditions = ["logs.id > ?"]
        params: list[object] = [after_id]
        if run_id:
            conditions.append("logs.run_id = ?")
            params.append(run_id)
        if pipeline_id:
            conditions.append("runs.pipeline_id = ?")
            params.append(pipeline_id)
        if task_id:
            conditions.append("logs.task_id = ?")
            params.append(task_id)
        params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT
                    logs.id, logs.run_id, logs.task_id, logs.created_at, logs.stream, logs.message,
                    runs.pipeline_id, runs.status AS run_status,
                    task_runs.title AS task_title
                FROM logs
                JOIN runs ON runs.id = logs.run_id
                LEFT JOIN task_runs
                       ON task_runs.run_id = logs.run_id
                      AND task_runs.task_id = logs.task_id
                WHERE {" AND ".join(conditions)}
                ORDER BY logs.id ASC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [
            {
                "id": int(row["id"]),
                "run_id": row["run_id"],
                "pipeline_id": row["pipeline_id"],
                "run_status": row["run_status"],
                "task_id": row["task_id"],
                "task_title": row["task_title"],
                "created_at": row["created_at"],
                "stream": row["stream"],
                "message": row["message"],
            }
            for row in rows
        ]

    def list_child_runs(self, parent_run_id: str) -> list[RunRecord]:
        """Return the downstream runs a successful run triggered."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    runs.*,
                    (SELECT COUNT(*) FROM logs WHERE logs.run_id = runs.id) AS log_count,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id) AS task_count,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'success') AS successful_tasks,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status IN ('failed', 'timed_out')) AS failed_tasks,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'skipped') AS skipped_tasks
                FROM runs
                WHERE parent_run_id = ?
                ORDER BY COALESCE(started_at, created_at) ASC
                """,
                (parent_run_id,),
            ).fetchall()
        return [self._row_to_run(row) for row in rows]

    def recent_logs(
        self,
        *,
        run_id: str | None = None,
        pipeline_id: str | None = None,
        task_id: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, object]]:
        """Return the newest log lines for a scope, oldest first.

        Selecting in reverse and re-sorting keeps the tail cheap: without this a
        caller wanting the last 200 lines would have to read the whole scope.
        """
        conditions: list[str] = ["1 = 1"]
        params: list[object] = []
        if run_id:
            conditions.append("logs.run_id = ?")
            params.append(run_id)
        if pipeline_id:
            conditions.append("runs.pipeline_id = ?")
            params.append(pipeline_id)
        if task_id:
            conditions.append("logs.task_id = ?")
            params.append(task_id)
        params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT
                    logs.id, logs.run_id, logs.task_id, logs.created_at, logs.stream, logs.message,
                    runs.pipeline_id, runs.status AS run_status,
                    task_runs.title AS task_title
                FROM logs
                JOIN runs ON runs.id = logs.run_id
                LEFT JOIN task_runs
                       ON task_runs.run_id = logs.run_id
                      AND task_runs.task_id = logs.task_id
                WHERE {" AND ".join(conditions)}
                ORDER BY logs.id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [
            {
                "id": int(row["id"]),
                "run_id": row["run_id"],
                "pipeline_id": row["pipeline_id"],
                "run_status": row["run_status"],
                "task_id": row["task_id"],
                "task_title": row["task_title"],
                "created_at": row["created_at"],
                "stream": row["stream"],
                "message": row["message"],
            }
            for row in reversed(rows)
        ]

    def log_cursor_at(self, moment: datetime) -> int:
        """Return the newest log id written at or before a timestamp."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(id), 0) AS cursor FROM logs WHERE created_at <= ?",
                (_to_iso(moment),),
            ).fetchone()
        return int(row["cursor"] or 0)

    def latest_log_id(self) -> int:
        """Return the id of the most recent log line."""
        with self._connect() as connection:
            row = connection.execute("SELECT COALESCE(MAX(id), 0) AS cursor FROM logs").fetchone()
        return int(row["cursor"] or 0)

    def get_latest_run_for_pipeline(self, pipeline_id: str) -> RunRecord | None:
        """Return the most recent run for one pipeline."""
        runs = self.list_runs(pipeline_id=pipeline_id, limit=1)
        return runs[0] if runs else None

    def latest_runs_by_pipeline(self) -> dict[str, RunRecord]:
        """Return the newest run for every pipeline using a single scan."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    runs.*,
                    (SELECT COUNT(*) FROM logs WHERE logs.run_id = runs.id) AS log_count,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id) AS task_count,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'success') AS successful_tasks,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status IN ('failed', 'timed_out')) AS failed_tasks,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'skipped') AS skipped_tasks
                FROM runs
                WHERE runs.id = (
                    SELECT newest.id
                    FROM runs AS newest
                    WHERE newest.pipeline_id = runs.pipeline_id
                    ORDER BY COALESCE(newest.started_at, newest.created_at) DESC, newest.id DESC
                    LIMIT 1
                )
                """
            ).fetchall()
        return {str(row["pipeline_id"]): self._row_to_run(row) for row in rows}

    def recent_runs_by_pipeline(self, limit: int = 5) -> dict[str, list[RunRecord]]:
        """Return the newest ``limit`` runs for every pipeline, newest first.

        One windowed query rather than one per pipeline, so the listing page
        cost does not grow with the number of pipelines.
        """
        capped = max(1, limit)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM (
                    SELECT
                        runs.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY runs.pipeline_id
                            ORDER BY COALESCE(runs.started_at, runs.created_at) DESC, runs.id DESC
                        ) AS recency,
                        (SELECT COUNT(*) FROM logs WHERE logs.run_id = runs.id) AS log_count,
                        (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id) AS task_count,
                        (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'success') AS successful_tasks,
                        (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status IN ('failed', 'timed_out')) AS failed_tasks,
                        (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'skipped') AS skipped_tasks
                    FROM runs
                ) AS ranked
                WHERE ranked.recency <= ?
                ORDER BY ranked.pipeline_id ASC, ranked.recency ASC
                """,
                (capped,),
            ).fetchall()

        grouped: dict[str, list[RunRecord]] = {}
        for row in rows:
            grouped.setdefault(str(row["pipeline_id"]), []).append(self._row_to_run(row))
        return grouped

    def task_states_for_runs(self, run_ids: list[str]) -> dict[str, dict[str, str]]:
        """Return {run_id: {task_id: status}} for the supplied runs in one query."""
        if not run_ids:
            return {}
        placeholders = ", ".join("?" for _ in run_ids)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT run_id, task_id, status FROM task_runs WHERE run_id IN ({placeholders})",
                run_ids,
            ).fetchall()
        states: dict[str, dict[str, str]] = {run_id: {} for run_id in run_ids}
        for row in rows:
            states.setdefault(str(row["run_id"]), {})[str(row["task_id"])] = str(row["status"])
        return states

    def active_run_counts_by_pipeline(self) -> dict[str, int]:
        """Return the number of queued or running runs per pipeline in one query."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT pipeline_id, COUNT(*) AS count
                FROM runs
                WHERE status IN ('queued', 'running')
                GROUP BY pipeline_id
                """
            ).fetchall()
        return {str(row["pipeline_id"]): int(row["count"] or 0) for row in rows}

    def get_run_for_slot(self, pipeline_id: str, scheduled_for: datetime) -> RunRecord | None:
        """Return the run materialized for one scheduled slot when it exists."""
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    runs.*,
                    (SELECT COUNT(*) FROM logs WHERE logs.run_id = runs.id) AS log_count,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id) AS task_count,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'success') AS successful_tasks,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status IN ('failed', 'timed_out')) AS failed_tasks,
                    (SELECT COUNT(*) FROM task_runs WHERE task_runs.run_id = runs.id AND task_runs.status = 'skipped') AS skipped_tasks
                FROM runs
                WHERE pipeline_id = ? AND scheduled_for = ?
                LIMIT 1
                """,
                (pipeline_id, _to_iso(scheduled_for)),
            ).fetchone()
        return self._row_to_run(row) if row else None

    def get_latest_task_states_for_pipeline(self, pipeline_id: str) -> dict[str, str]:
        """Return the latest known task status map for one pipeline."""
        latest_run = self.get_latest_run_for_pipeline(pipeline_id)
        if latest_run is None:
            return {}
        return {task.task_id: task.status for task in self.list_task_runs(latest_run.run_id)}

    def count_running_runs(self, pipeline_id: str | None = None) -> int:
        """Count active pipeline runs globally or per pipeline."""
        conditions = ["status IN ('queued', 'running')"]
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

    def list_active_runs_with_owner(self) -> list[tuple[str, int | None]]:
        """Return (run_id, owner_pid) for every queued or running run."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, owner_pid
                FROM runs
                WHERE status IN ('queued', 'running')
                ORDER BY COALESCE(started_at, created_at) ASC
                """
            ).fetchall()
        return [(str(row["id"]), None if row["owner_pid"] is None else int(row["owner_pid"])) for row in rows]

    def get_run_config(self, run_id: str) -> dict[str, object] | None:
        """Return the runtime configuration snapshot captured when a run was created."""
        with self._connect() as connection:
            row = connection.execute("SELECT run_config FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None or not row["run_config"]:
            return None
        value = json.loads(row["run_config"])
        return value if isinstance(value, dict) else None

    def list_active_run_ids(self) -> list[str]:
        """Return queued or running run ids for shutdown and recovery workflows."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id
                FROM runs
                WHERE status IN ('queued', 'running')
                ORDER BY COALESCE(started_at, created_at) ASC
                """
            ).fetchall()
        return [str(row["id"]) for row in rows]

    def delete_run(self, run_id: str) -> None:
        """Delete one finished run and all related task runs and logs."""
        with self._lock, self._connect() as connection:
            connection.execute("DELETE FROM logs WHERE run_id = ?", (run_id,))
            connection.execute("DELETE FROM task_outputs WHERE run_id = ?", (run_id,))
            connection.execute("DELETE FROM task_runs WHERE run_id = ?", (run_id,))
            connection.execute("DELETE FROM runs WHERE id = ?", (run_id,))
            connection.commit()

    def delete_pipeline_runs(self, pipeline_id: str) -> None:
        """Delete every persisted run and override state for one pipeline."""
        with self._lock, self._connect() as connection:
            run_ids = [
                row["id"]
                for row in connection.execute(
                    "SELECT id FROM runs WHERE pipeline_id = ?",
                    (pipeline_id,),
                ).fetchall()
            ]
            if run_ids:
                placeholders = ", ".join("?" for _ in run_ids)
                connection.execute(
                    f"DELETE FROM logs WHERE run_id IN ({placeholders})",
                    run_ids,
                )
                connection.execute(
                    f"DELETE FROM task_outputs WHERE run_id IN ({placeholders})",
                    run_ids,
                )
                connection.execute(
                    f"DELETE FROM task_runs WHERE run_id IN ({placeholders})",
                    run_ids,
                )
            connection.execute("DELETE FROM runs WHERE pipeline_id = ?", (pipeline_id,))
            connection.execute("DELETE FROM pipeline_overrides WHERE pipeline_id = ?", (pipeline_id,))
            connection.execute("DELETE FROM trigger_queue WHERE pipeline_id = ?", (pipeline_id,))
            connection.execute(
                "DELETE FROM sensor_state WHERE sensor_key LIKE ?",
                (f"{pipeline_id}:%",),
            )
            connection.commit()

    def prune(
        self,
        *,
        run_retention_days: int,
        log_retention_days: int,
        max_runs_per_pipeline: int,
        vacuum: bool = True,
        dry_run: bool = False,
        now: datetime | None = None,
    ) -> dict[str, int]:
        """Delete history beyond the configured retention window.

        Active runs are never removed. A zero value disables that particular
        retention rule so operators can prune by age, by count, or by both.
        """
        current = now or datetime.now(timezone.utc)
        summary = {"runs_deleted": 0, "logs_deleted": 0, "artifacts_deleted": 0, "outputs_deleted": 0}
        expired_run_ids: set[str] = set()

        with self._connect() as connection:
            if run_retention_days > 0:
                cutoff = _to_iso(current - timedelta(days=run_retention_days))
                expired_run_ids.update(
                    str(row["id"])
                    for row in connection.execute(
                        """
                        SELECT id FROM runs
                        WHERE status NOT IN ('queued', 'running') AND created_at < ?
                        """,
                        (cutoff,),
                    ).fetchall()
                )

            if max_runs_per_pipeline > 0:
                for pipeline_row in connection.execute("SELECT DISTINCT pipeline_id FROM runs").fetchall():
                    expired_run_ids.update(
                        str(row["id"])
                        for row in connection.execute(
                            f"""
                            SELECT id FROM runs
                            WHERE pipeline_id = ? AND status NOT IN ('queued', 'running')
                            ORDER BY COALESCE(started_at, created_at) DESC
                            {self.dialect.offset_without_limit}
                            """,
                            (pipeline_row["pipeline_id"], max_runs_per_pipeline),
                        ).fetchall()
                    )

            log_cutoff = _to_iso(current - timedelta(days=log_retention_days)) if log_retention_days > 0 else None
            if dry_run:
                summary["runs_deleted"] = len(expired_run_ids)
                if log_cutoff is not None:
                    summary["logs_deleted"] = int(
                        connection.execute(
                            "SELECT COUNT(*) AS count FROM logs WHERE created_at < ?",
                            (log_cutoff,),
                        ).fetchone()["count"]
                        or 0
                    )
                return summary

        with self._lock, self._connect() as connection:
            if expired_run_ids:
                run_ids = list(expired_run_ids)
                for index in range(0, len(run_ids), 400):
                    chunk = run_ids[index : index + 400]
                    placeholders = ", ".join("?" for _ in chunk)
                    summary["logs_deleted"] += connection.execute(
                        f"DELETE FROM logs WHERE run_id IN ({placeholders})", chunk
                    ).rowcount
                    summary["artifacts_deleted"] += connection.execute(
                        f"DELETE FROM task_artifacts WHERE run_id IN ({placeholders})", chunk
                    ).rowcount
                    summary["outputs_deleted"] += connection.execute(
                        f"DELETE FROM task_outputs WHERE run_id IN ({placeholders})", chunk
                    ).rowcount
                    connection.execute(f"DELETE FROM task_runs WHERE run_id IN ({placeholders})", chunk)
                    summary["runs_deleted"] += connection.execute(
                        f"DELETE FROM runs WHERE id IN ({placeholders})", chunk
                    ).rowcount

            if log_cutoff is not None:
                summary["logs_deleted"] += connection.execute(
                    "DELETE FROM logs WHERE created_at < ?", (log_cutoff,)
                ).rowcount

            connection.execute(
                "DELETE FROM trigger_queue WHERE status IN ('dispatched', 'failed') AND created_at < ?",
                (_to_iso(current - timedelta(days=max(1, log_retention_days))),),
            )
            connection.commit()

        if vacuum:
            with self._connect() as connection:
                self.dialect.vacuum(connection)
        return summary

    def backup_to(self, destination: str | Path) -> Path:
        """Copy the runtime database to ``destination`` while it is in use.

        Uses SQLite's online backup API rather than a file copy, so the result is
        a consistent snapshot even if runs are executing and the WAL has
        uncheckpointed pages.
        """
        if not self.is_sqlite:
            raise RuntimeError(
                "piply backup only snapshots a SQLite store. This runtime uses "
                f"{self.dialect.name}; back it up with your database's own tooling, for example "
                "'pg_dump'."
            )

        raw = str(destination)
        target = Path(raw).resolve()
        # A destination is a directory if it already is one, or if it looks like
        # one: a trailing separator, or no file extension. Without the suffix
        # check, `piply backup ./backups` would create a file named "backups".
        looks_like_directory = target.is_dir() or raw.endswith(("/", "\\")) or not target.suffix
        if looks_like_directory:
            target.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            target = target / f"piply-{stamp}.db"
        target.parent.mkdir(parents=True, exist_ok=True)

        with self._lock, self._connect() as source, sqlite3.connect(target) as destination_connection:
            source.raw.backup(destination_connection)
        return target

    def database_size_bytes(self) -> int:
        """Return the on-disk size of the runtime database.

        Only meaningful for SQLite; a server-side store reports 0 because its
        size is not something Piply owns or can cheaply measure.
        """
        if not self.is_sqlite or self.database_path is None:
            return 0
        try:
            return self.database_path.stat().st_size
        except OSError:
            return 0

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

    def get_latest_materialized_slot(self, pipeline_id: str) -> datetime | None:
        """Return the newest scheduled slot present in either runs or the queue."""
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT MAX(slot_value) AS latest_slot
                FROM (
                    SELECT scheduled_for AS slot_value
                    FROM runs
                    WHERE pipeline_id = ? AND scheduled_for IS NOT NULL
                    UNION ALL
                    SELECT scheduled_for AS slot_value
                    FROM trigger_queue
                    WHERE pipeline_id = ? AND scheduled_for IS NOT NULL
                )
                """,
                (pipeline_id, pipeline_id),
            ).fetchone()
        return _from_iso(row["latest_slot"]) if row and row["latest_slot"] else None

    def enqueue_trigger(
        self,
        pipeline_id: str,
        trigger: str,
        *,
        available_at: datetime,
        scheduled_for: datetime | None = None,
        source_key: str | None = None,
        dedupe_key: str | None = None,
        payload: dict[str, object] | None = None,
    ) -> bool:
        """Persist one queued trigger event for later dispatch."""
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                f"""
                {self.dialect.insert_or_ignore} trigger_queue (
                    pipeline_id, "trigger", status, available_at, created_at,
                    scheduled_for, source_key, dedupe_key, payload_json
                ) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, ?)
                {self.dialect.on_conflict_do_nothing}
                """,
                (
                    pipeline_id,
                    trigger,
                    _to_iso(available_at),
                    _to_iso(datetime.now(timezone.utc)),
                    _to_iso(scheduled_for),
                    source_key,
                    dedupe_key,
                    json.dumps(payload or {}, sort_keys=True),
                ),
            )
            connection.commit()
        return cursor.rowcount > 0

    def list_due_queue(
        self,
        *,
        now: datetime | None = None,
        limit: int = 200,
    ) -> list[TriggerQueueRecord]:
        """Return queued trigger events that are ready to be dispatched."""
        current = _to_iso(now or datetime.now(timezone.utc))
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM trigger_queue
                WHERE status = 'queued' AND available_at <= ?
                ORDER BY available_at ASC, id ASC
                LIMIT ?
                """,
                (current, limit),
            ).fetchall()
        return [self._row_to_queue_record(row) for row in rows]

    def mark_queue_dispatched(self, queue_id: int, run_id: str) -> None:
        """Mark one trigger event as successfully dispatched to a run."""
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE trigger_queue
                SET status = 'dispatched',
                    dispatched_at = ?,
                    dispatched_run_id = ?,
                    error = NULL
                WHERE id = ?
                """,
                (_to_iso(datetime.now(timezone.utc)), run_id, queue_id),
            )
            connection.commit()

    def claim_queue_item(self, queue_id: int) -> bool:
        """Move one queued trigger event into a short-lived dispatching state."""
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE trigger_queue
                SET status = 'dispatching',
                    dispatched_at = ?,
                    error = NULL
                WHERE id = ? AND status = 'queued'
                """,
                (_to_iso(datetime.now(timezone.utc)), queue_id),
            )
            connection.commit()
        return cursor.rowcount > 0

    def mark_queue_failed(self, queue_id: int, error: str) -> None:
        """Mark one trigger event as failed after an unrecoverable dispatch error."""
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE trigger_queue
                SET status = 'failed',
                    dispatched_at = ?,
                    error = ?
                WHERE id = ?
                """,
                (_to_iso(datetime.now(timezone.utc)), error, queue_id),
            )
            connection.commit()

    def requeue_stale_dispatches(self, max_age_seconds: int = 300) -> int:
        """Return abandoned dispatching queue items back to queued state."""
        cutoff = _to_iso(datetime.now(timezone.utc) - timedelta(seconds=max_age_seconds))
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE trigger_queue
                SET status = 'queued',
                    dispatched_at = NULL,
                    error = NULL
                WHERE status = 'dispatching'
                  AND dispatched_run_id IS NULL
                  AND COALESCE(dispatched_at, created_at) <= ?
                """,
                (cutoff,),
            )
            connection.commit()
        return int(cursor.rowcount or 0)

    def count_queue(self, status: str = "queued") -> int:
        """Return the number of queued trigger items in one status bucket."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM trigger_queue WHERE status = ?",
                (status,),
            ).fetchone()
        return int(row["count"] or 0)

    def queue_metrics(self, *, now: datetime | None = None) -> dict[str, int | float | None]:
        """Return queue status counts and lightweight latency metrics."""
        current = now or datetime.now(timezone.utc)
        current_iso = _to_iso(current)
        metrics: dict[str, int | float | None] = {
            "queued": 0,
            "due": 0,
            "dispatching": 0,
            "dispatched": 0,
            "failed": 0,
            "oldest_queued_age_seconds": None,
        }
        with self._connect() as connection:
            for row in connection.execute(
                "SELECT status, COUNT(*) AS count FROM trigger_queue GROUP BY status"
            ).fetchall():
                metrics[str(row["status"])] = int(row["count"] or 0)
            due_row = connection.execute(
                """
                SELECT COUNT(*) AS count, MIN(available_at) AS oldest_available_at
                FROM trigger_queue
                WHERE status = 'queued' AND available_at <= ?
                """,
                (current_iso,),
            ).fetchone()

        metrics["due"] = int(due_row["count"] or 0)
        oldest = _from_iso(due_row["oldest_available_at"])
        if oldest is not None:
            metrics["oldest_queued_age_seconds"] = max(
                0.0,
                (current - oldest).total_seconds(),
            )
        return metrics

    def worker_metrics(self) -> dict[str, int]:
        """Return active run/task counts for the local worker engine."""
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM runs WHERE status = 'running') AS running_runs,
                    (SELECT COUNT(*) FROM runs WHERE status = 'queued') AS queued_runs,
                    (SELECT COUNT(*) FROM task_runs WHERE status = 'running') AS running_tasks,
                    (SELECT COUNT(*) FROM task_runs WHERE status = 'queued') AS queued_tasks
                """
            ).fetchone()
        return {
            "running_runs": int(row["running_runs"] or 0),
            "queued_runs": int(row["queued_runs"] or 0),
            "running_tasks": int(row["running_tasks"] or 0),
            "queued_tasks": int(row["queued_tasks"] or 0),
        }

    def list_running_tasks(self) -> list[dict[str, object]]:
        """Return every task currently executing, with its owning run and pipeline."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    task_runs.run_id,
                    task_runs.task_id,
                    task_runs.title,
                    task_runs.task_type,
                    task_runs.priority,
                    task_runs.timeout_seconds,
                    task_runs.started_at,
                    runs.pipeline_id,
                    runs.pipeline_title,
                    runs.owner_pid
                FROM task_runs
                JOIN runs ON runs.id = task_runs.run_id
                WHERE task_runs.status = 'running'
                ORDER BY task_runs.started_at ASC
                """
            ).fetchall()
        now = datetime.now(timezone.utc)
        results: list[dict[str, object]] = []
        for row in rows:
            started_at = _from_iso(row["started_at"])
            results.append(
                {
                    "run_id": row["run_id"],
                    "task_id": row["task_id"],
                    "title": row["title"],
                    "task_type": row["task_type"],
                    "priority": int(row["priority"] or 0),
                    "timeout_seconds": row["timeout_seconds"],
                    "pipeline_id": row["pipeline_id"],
                    "pipeline_title": row["pipeline_title"],
                    "owner_pid": row["owner_pid"],
                    "started_at": None if started_at is None else started_at.isoformat(),
                    "running_seconds": None if started_at is None else max(0.0, (now - started_at).total_seconds()),
                }
            )
        return results

    def status_counts(self) -> dict[str, dict[str, int]]:
        """Return run and task counts grouped by status for metrics endpoints."""
        with self._connect() as connection:
            run_rows = connection.execute("SELECT status, COUNT(*) AS count FROM runs GROUP BY status").fetchall()
            task_rows = connection.execute("SELECT status, COUNT(*) AS count FROM task_runs GROUP BY status").fetchall()
            trigger_rows = connection.execute("SELECT trigger, COUNT(*) AS count FROM runs GROUP BY trigger").fetchall()
        return {
            "runs": {str(row["status"]): int(row["count"] or 0) for row in run_rows},
            "tasks": {str(row["status"]): int(row["count"] or 0) for row in task_rows},
            "triggers": {str(row["trigger"]): int(row["count"] or 0) for row in trigger_rows},
        }

    def duration_metrics(self) -> dict[str, float]:
        """Return aggregate run-duration metrics for the Prometheus histogram summary."""
        with self._connect() as connection:
            duration = self.dialect.epoch_diff("finished_at", "started_at")
            row = connection.execute(
                f"""
                SELECT
                    COUNT(*) AS completed,
                    COALESCE(SUM({duration}), 0) AS total_seconds,
                    COALESCE(MAX({duration}), 0) AS max_seconds
                FROM runs
                WHERE started_at IS NOT NULL AND finished_at IS NOT NULL
                """
            ).fetchone()
        completed = int(row["completed"] or 0)
        total = float(row["total_seconds"] or 0.0)
        return {
            "completed_runs": float(completed),
            "total_seconds": total,
            "max_seconds": float(row["max_seconds"] or 0.0),
            "average_seconds": (total / completed) if completed else 0.0,
        }

    def get_sensor_state(self, sensor_key: str) -> dict[str, object] | None:
        """Load one persisted sensor cursor or snapshot state."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT state_json FROM sensor_state WHERE sensor_key = ?",
                (sensor_key,),
            ).fetchone()
        if row is None:
            return None
        value = json.loads(row["state_json"])
        return value if isinstance(value, dict) else None

    def set_sensor_state(self, sensor_key: str, state: dict[str, object]) -> None:
        """Persist one sensor cursor or snapshot state."""
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO sensor_state (sensor_key, state_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(sensor_key)
                DO UPDATE SET state_json = excluded.state_json, updated_at = excluded.updated_at
                """,
                (
                    sensor_key,
                    json.dumps(state, sort_keys=True),
                    _to_iso(datetime.now(timezone.utc)),
                ),
            )
            connection.commit()

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
                    SUM(CASE WHEN status IN ('failed', 'interrupted', 'timed_out') THEN 1 ELSE 0 END) AS failed_runs
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
            rows = connection.execute("SELECT pipeline_id FROM pipeline_overrides WHERE paused = 1").fetchall()
        return {row["pipeline_id"] for row in rows}

    # --- Users and permissions ---------------------------------------------

    def count_users(self) -> int:
        """Return how many accounts exist."""
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM users").fetchone()
        return int(row["count"] or 0)

    def upsert_user(
        self,
        username: str,
        *,
        password_hash: str | None = None,
        role: str | None = None,
        is_active: bool | None = None,
    ) -> None:
        """Create or update one account, leaving omitted fields untouched."""
        now = _to_iso(datetime.now(timezone.utc))
        with self._lock, self._connect() as connection:
            existing = connection.execute("SELECT username FROM users WHERE username = ?", (username,)).fetchone()
            if existing is None:
                if password_hash is None:
                    raise ValueError("A new user needs a password.")
                connection.execute(
                    """
                    INSERT INTO users (username, password_hash, role, is_active, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (username, password_hash, role or "user", 1 if is_active is None else int(is_active), now),
                )
            else:
                assignments: list[str] = []
                params: list[object] = []
                if password_hash is not None:
                    assignments.append("password_hash = ?")
                    params.append(password_hash)
                if role is not None:
                    assignments.append("role = ?")
                    params.append(role)
                if is_active is not None:
                    assignments.append("is_active = ?")
                    params.append(int(is_active))
                if assignments:
                    params.append(username)
                    connection.execute(
                        f"UPDATE users SET {', '.join(assignments)} WHERE username = ?",
                        params,
                    )
            connection.commit()

    def get_user_record(self, username: str) -> dict[str, object] | None:
        """Return one account row including its password hash."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT username, password_hash, role, is_active, created_at, last_login_at "
                "FROM users WHERE username = ?",
                (username,),
            ).fetchone()
            if row is None:
                return None
            grants = connection.execute(
                "SELECT pipeline_id, actions FROM user_permissions WHERE username = ?",
                (username,),
            ).fetchall()
        return {
            "username": row["username"],
            "password_hash": row["password_hash"],
            "role": row["role"],
            "is_active": bool(row["is_active"]),
            "created_at": row["created_at"],
            "last_login_at": row["last_login_at"],
            "permissions": {str(grant["pipeline_id"]): frozenset(str(grant["actions"]).split(",")) for grant in grants},
        }

    def list_user_records(self) -> list[dict[str, object]]:
        """Return every account with its grants, without password hashes."""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT username, role, is_active, created_at, last_login_at FROM users ORDER BY username ASC"
            ).fetchall()
            grants = connection.execute("SELECT username, pipeline_id, actions FROM user_permissions").fetchall()

        by_user: dict[str, dict[str, frozenset[str]]] = {}
        for grant in grants:
            by_user.setdefault(str(grant["username"]), {})[str(grant["pipeline_id"])] = frozenset(
                str(grant["actions"]).split(",")
            )
        return [
            {
                "username": row["username"],
                "role": row["role"],
                "is_active": bool(row["is_active"]),
                "created_at": row["created_at"],
                "last_login_at": row["last_login_at"],
                "permissions": by_user.get(str(row["username"]), {}),
            }
            for row in rows
        ]

    def delete_user(self, username: str) -> bool:
        """Remove one account and every grant it held."""
        with self._lock, self._connect() as connection:
            connection.execute("DELETE FROM user_permissions WHERE username = ?", (username,))
            cursor = connection.execute("DELETE FROM users WHERE username = ?", (username,))
            connection.commit()
        return cursor.rowcount > 0

    def touch_user_login(self, username: str) -> None:
        """Record a successful sign-in."""
        with self._lock, self._connect() as connection:
            connection.execute(
                "UPDATE users SET last_login_at = ? WHERE username = ?",
                (_to_iso(datetime.now(timezone.utc)), username),
            )
            connection.commit()

    def set_user_permission(self, username: str, pipeline_id: str, actions: frozenset[str]) -> None:
        """Grant or clear one pipeline permission for a user."""
        with self._lock, self._connect() as connection:
            if not actions:
                connection.execute(
                    "DELETE FROM user_permissions WHERE username = ? AND pipeline_id = ?",
                    (username, pipeline_id),
                )
            else:
                connection.execute(
                    """
                    INSERT INTO user_permissions (username, pipeline_id, actions)
                    VALUES (?, ?, ?)
                    ON CONFLICT(username, pipeline_id)
                    DO UPDATE SET actions = excluded.actions
                    """,
                    (username, pipeline_id, ",".join(sorted(actions))),
                )
            connection.commit()

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

    def set_meta_many(self, values: dict[str, str]) -> None:
        """Persist several metadata keys in one transaction.

        Scheduler state and its error message must land together, otherwise a
        reader can observe a crashed scheduler with no reason attached.
        """
        if not values:
            return
        with self._lock, self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO meta (key, value)
                VALUES (?, ?)
                ON CONFLICT(key)
                DO UPDATE SET value = excluded.value
                """,
                list(values.items()),
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
            parent_run_id=row["parent_run_id"] if "parent_run_id" in row.keys() else None,
            parent_pipeline_id=row["parent_pipeline_id"] if "parent_pipeline_id" in row.keys() else None,
            tenant_id=row["tenant_id"] if "tenant_id" in row.keys() else None,
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
            priority=int(row["priority"] or 0) if "priority" in row.keys() else 0,
            timeout_seconds=row["timeout_seconds"] if "timeout_seconds" in row.keys() else None,
            run_if=row["run_if"] if "run_if" in row.keys() else None,
            started_at=_from_iso(row["started_at"]),
            finished_at=_from_iso(row["finished_at"]),
            exit_code=row["exit_code"],
            error=row["error"],
            depends_on=depends_on,
            log_count=int(row["log_count"] or 0),
            output_type=row["output_type"] if "output_type" in row.keys() else None,
            output_preview=row["output_preview"] if "output_preview" in row.keys() else None,
            output_is_json=bool(row["output_is_json"]) if "output_is_json" in row.keys() else False,
        )

    def _row_to_task_output(self, row: sqlite3.Row) -> TaskOutputRecord:
        """Convert one task output row into a TaskOutputRecord."""
        metadata = json.loads(row["metadata_json"]) if row["metadata_json"] else {}
        if not isinstance(metadata, dict):
            metadata = {}
        return TaskOutputRecord(
            run_id=row["run_id"],
            task_id=row["task_id"],
            output_type=row["output_type"],
            preview=row["preview"],
            is_json=bool(row["is_json"]),
            json_value=row["json_value"],
            metadata=metadata,
            size_bytes=int(row["size_bytes"] or 0),
            created_at=_from_iso(row["created_at"]),
        )

    def _row_to_queue_record(self, row: sqlite3.Row) -> TriggerQueueRecord:
        """Convert one trigger_queue row into a TriggerQueueRecord."""
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        if not isinstance(payload, dict):
            payload = {}
        return TriggerQueueRecord(
            queue_id=int(row["id"]),
            pipeline_id=row["pipeline_id"],
            trigger=row["trigger"],
            status=row["status"],
            available_at=_from_iso(row["available_at"]) or datetime.now(timezone.utc),
            created_at=_from_iso(row["created_at"]) or datetime.now(timezone.utc),
            scheduled_for=_from_iso(row["scheduled_for"]),
            source_key=row["source_key"],
            dedupe_key=row["dedupe_key"],
            payload=payload,
            dispatched_at=_from_iso(row["dispatched_at"]),
            dispatched_run_id=row["dispatched_run_id"],
            error=row["error"],
        )
