"""Lightweight sensor helpers for local files, SFTP paths, and SQL cursors."""

from __future__ import annotations

import hashlib
import importlib
import shlex
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from .models import SensorDefinition


@dataclass(slots=True)
class SensorEvent:
    """A sensor event describes one enqueueable change detected by polling."""

    sensor_id: str
    source_key: str
    payload: dict[str, Any]


def _hash_parts(parts: list[str]) -> str:
    """Return a short deterministic digest for queue dedupe keys."""
    digest = hashlib.sha1()
    digest.update("\n".join(parts).encode("utf-8"))
    return digest.hexdigest()


def _mask_connection_secret(value: str | None) -> str | None:
    """Redact passwords from connection strings before surfacing them in logs."""
    if not value:
        return value
    parsed = urlparse(value)
    if not parsed.scheme or parsed.password is None:
        return value
    username = parsed.username or ""
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    auth = f"{username}:***@" if username else ""
    return parsed._replace(netloc=f"{auth}{host}{port}").geturl()


def _iter_local_sensor_files(sensor: SensorDefinition) -> list[str]:
    """Return a stable local file snapshot for one file sensor."""
    if sensor.path is None:
        return []

    target = sensor.path
    pattern = sensor.pattern or "*"
    if any(token in str(target) for token in ("*", "?", "[")):
        matches = sorted(path for path in target.parent.glob(target.name) if path.is_file())
        return [str(path) for path in matches]

    if target.is_file():
        return [str(target)]
    if not target.exists() or not target.is_dir():
        return []

    iterator = target.rglob(pattern) if sensor.recursive else target.glob(pattern)
    return [str(path) for path in sorted(iterator) if path.is_file()]


def _iter_remote_sensor_files(sensor: SensorDefinition) -> list[str]:
    """Return a remote SFTP file snapshot by polling over SSH."""
    if not sensor.is_remote or sensor.remote_path is None or sensor.ssh_host is None:
        return []

    max_depth = "" if sensor.recursive else "-maxdepth 1"
    pattern = shlex.quote(sensor.pattern or "*")
    remote_path = shlex.quote(sensor.remote_path)
    remote_command = (
        f"find {remote_path} {max_depth} -type f -name {pattern} -print 2>/dev/null || true"
    ).strip()
    target = "@".join(part for part in [sensor.ssh_user, sensor.ssh_host] if part) or sensor.ssh_host

    command = [
        sensor.ssh_binary,
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={sensor.connect_timeout}",
        "-p",
        str(sensor.ssh_port),
    ]
    if sensor.ssh_key_file is not None:
        command.extend(["-i", str(sensor.ssh_key_file)])
    command.extend([target, remote_command])

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=max(sensor.connect_timeout + 2, 5),
            check=False,
        )
    except OSError:
        return []

    if result.returncode not in {0, None}:
        return []

    return sorted(line.strip() for line in result.stdout.splitlines() if line.strip())


def _iter_sensor_files(sensor: SensorDefinition) -> list[str]:
    """Return the current file snapshot for one local or remote file sensor."""
    return _iter_remote_sensor_files(sensor) if sensor.is_remote else _iter_local_sensor_files(sensor)


def poll_file_sensor(
    sensor: SensorDefinition,
    state: dict[str, Any] | None,
) -> tuple[dict[str, Any], SensorEvent | None]:
    """Compare the current file snapshot to the stored state and return a new event when needed."""
    current_files = _iter_sensor_files(sensor)
    known_files = set(str(item) for item in (state or {}).get("known_files", []))
    current_set = set(current_files)

    if not state:
        next_state = {"known_files": current_files}
        if sensor.ignore_existing:
            return next_state, None
        new_files = current_files
    else:
        new_files = sorted(current_set - known_files)
        next_state = {"known_files": current_files}

    if not new_files:
        return next_state, None

    return (
        next_state,
        SensorEvent(
            sensor_id=sensor.sensor_id,
            source_key=_hash_parts(new_files),
            payload={
                "sensor_id": sensor.sensor_id,
                "sensor_type": sensor.sensor_type,
                "sensor_summary": sensor.summary,
                "new_files": new_files,
                "file_count": len(new_files),
            },
        ),
    )


def _sqlite_path_from_connection(connection: str) -> Path | None:
    """Resolve a sqlite:/// connection string into a filesystem path."""
    parsed = urlparse(connection)
    if parsed.scheme.lower() not in {"sqlite", "sqlite3", "sqlite+pysqlite"}:
        return None
    raw_path = unquote(parsed.path or "")
    if parsed.netloc and not raw_path.startswith("/"):
        raw_path = f"/{raw_path}"
    if raw_path.startswith("/") and len(raw_path) > 2 and raw_path[2] == ":":
        raw_path = raw_path[1:]
    if not raw_path:
        return None
    return Path(raw_path)


def _connect_sql_sensor(sensor: SensorDefinition):
    """Open the lightest available DB-API connection for a SQL sensor."""
    if sensor.connection:
        parsed = urlparse(sensor.connection)
        scheme = parsed.scheme.lower()
        if scheme in {"sqlite", "sqlite3", "sqlite+pysqlite"}:
            sqlite_path = _sqlite_path_from_connection(sensor.connection)
            if sqlite_path is None:
                raise RuntimeError("Invalid sqlite connection string")
            return sqlite3.connect(sqlite_path)
        if scheme in {"postgres", "postgresql"}:
            try:
                psycopg = importlib.import_module("psycopg")
                return psycopg.connect(sensor.connection)
            except ImportError:
                psycopg2 = importlib.import_module("psycopg2")
                return psycopg2.connect(sensor.connection)
        if scheme in {"mysql", "mysql+pymysql", "mariadb"}:
            pymysql = importlib.import_module("pymysql")
            return pymysql.connect(
                host=parsed.hostname or "localhost",
                port=parsed.port or 3306,
                user=parsed.username,
                password=parsed.password,
                database=parsed.path.lstrip("/") or None,
            )
        if scheme in {"mssql", "sqlserver"}:
            pyodbc = importlib.import_module("pyodbc")
            return pyodbc.connect(sensor.connection)
        raise RuntimeError(f"Unsupported sql_sensor connection scheme '{scheme or '<none>'}'")

    if sensor.database is None or not sensor.database.exists():
        return None
    return sqlite3.connect(sensor.database)


def _read_sql_cursor(sensor: SensorDefinition) -> tuple[int, int]:
    """Read the current SQL cursor and row count from a supported database."""
    if sensor.table is None:
        return 0, 0

    connection = _connect_sql_sensor(sensor)
    if connection is None:
        return 0, 0

    query = f"SELECT COALESCE(MAX({sensor.cursor_column}), 0) AS max_cursor, COUNT(*) AS row_count FROM {sensor.table}"
    if sensor.where:
        query += f" WHERE {sensor.where}"

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
    finally:
        connection.close()

    if row is None:
        return 0, 0
    return int(row[0] or 0), int(row[1] or 0)


def poll_sql_sensor(
    sensor: SensorDefinition,
    state: dict[str, Any] | None,
) -> tuple[dict[str, Any], SensorEvent | None]:
    """Compare a SQL cursor to the stored state and emit an event for new inserts."""
    try:
        current_cursor, row_count = _read_sql_cursor(sensor)
    except Exception:
        return state or {"cursor": 0, "row_count": 0}, None

    previous_cursor = int((state or {}).get("cursor", 0))
    previous_state = {"cursor": current_cursor, "row_count": row_count}
    if not state and sensor.ignore_existing:
        return previous_state, None
    if current_cursor <= previous_cursor:
        return previous_state, None

    database_label = (
        _mask_connection_secret(sensor.connection)
        or (str(sensor.database) if sensor.database is not None else None)
    )
    return (
        previous_state,
        SensorEvent(
            sensor_id=sensor.sensor_id,
            source_key=str(current_cursor),
            payload={
                "sensor_id": sensor.sensor_id,
                "sensor_type": sensor.sensor_type,
                "table": sensor.table,
                "database": database_label,
                "cursor_column": sensor.cursor_column,
                "cursor_from": previous_cursor,
                "cursor_to": current_cursor,
                "row_count": row_count,
            },
        ),
    )
