"""Optional SQL adapter helpers for sensor polling."""

from __future__ import annotations

import importlib
import sqlite3
from pathlib import Path
from urllib.parse import unquote, urlparse

SUPPORTED_SQL_SCHEMES = {
    "sqlite",
    "sqlite3",
    "sqlite+pysqlite",
    "postgres",
    "postgresql",
    "postgresql+psycopg",
    "postgresql+psycopg2",
    "mysql",
    "mysql+pymysql",
    "mysql+mysqlconnector",
    "mariadb",
    "mariadb+pymysql",
    "mssql",
    "mssql+pyodbc",
    "sqlserver",
    "odbc",
}


def mask_connection_secret(value: str | None) -> str | None:
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


def sqlite_path_from_connection(connection: str) -> Path | None:
    """Resolve a sqlite:/// connection string into a filesystem path."""
    parsed = urlparse(connection)
    if parsed.scheme.lower() not in {"sqlite", "sqlite3", "sqlite+pysqlite"}:
        return None
    raw_path = unquote(parsed.path or "")
    if raw_path in {":memory:", "/:memory:"}:
        return Path(":memory:")
    if parsed.netloc and not raw_path.startswith("/"):
        raw_path = f"/{raw_path}"
    if raw_path.startswith("/") and len(raw_path) > 2 and raw_path[2] == ":":
        raw_path = raw_path[1:]
    if not raw_path:
        return None
    return Path(raw_path)


def connect_sql(connection: str | None = None, *, database: Path | None = None):
    """Open the lightest available DB-API connection for a SQL sensor."""
    if connection:
        parsed = urlparse(connection)
        scheme = parsed.scheme.lower()
        if scheme in {"sqlite", "sqlite3", "sqlite+pysqlite"}:
            sqlite_path = sqlite_path_from_connection(connection)
            if sqlite_path is None:
                raise RuntimeError("Invalid sqlite connection string")
            if str(sqlite_path) == ":memory:":
                return sqlite3.connect(":memory:")
            return sqlite3.connect(sqlite_path)
        if scheme in {"postgres", "postgresql", "postgresql+psycopg"}:
            try:
                psycopg = importlib.import_module("psycopg")
            except ImportError:
                psycopg = importlib.import_module("psycopg2")
            return psycopg.connect(connection)
        if scheme == "postgresql+psycopg2":
            psycopg2 = importlib.import_module("psycopg2")
            return psycopg2.connect(connection)
        if scheme in {"mysql", "mysql+pymysql", "mariadb", "mariadb+pymysql"}:
            pymysql = importlib.import_module("pymysql")
            return pymysql.connect(
                host=parsed.hostname or "localhost",
                port=parsed.port or 3306,
                user=parsed.username,
                password=parsed.password,
                database=parsed.path.lstrip("/") or None,
            )
        if scheme == "mysql+mysqlconnector":
            mysql_connector = importlib.import_module("mysql.connector")
            return mysql_connector.connect(
                host=parsed.hostname or "localhost",
                port=parsed.port or 3306,
                user=parsed.username,
                password=parsed.password,
                database=parsed.path.lstrip("/") or None,
            )
        if scheme in {"mssql", "mssql+pyodbc", "sqlserver", "odbc"}:
            pyodbc = importlib.import_module("pyodbc")
            return pyodbc.connect(connection)
        raise RuntimeError(f"Unsupported sql_sensor connection scheme '{scheme or '<none>'}'")

    if database is None or not database.exists():
        return None
    return sqlite3.connect(database)


def supported_sql_adapters() -> tuple[str, ...]:
    """Return the documented connection schemes supported by built-in sensors."""
    return tuple(sorted(SUPPORTED_SQL_SCHEMES))
