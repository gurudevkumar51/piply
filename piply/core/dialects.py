"""Database dialects for Piply's own metadata store.

SQLite is the default and needs no configuration. PostgreSQL is opt-in, for
deployments that already have a managed database and want the runtime state to
live there rather than on a local volume.

The store is written once, against the SQLite spelling, and this module adapts
it: `?` placeholders are rewritten, `connection.execute()` is provided on top of
psycopg's cursor API, and the handful of genuinely dialect-specific fragments
(identity columns, upsert-or-ignore, date arithmetic) are named here instead of
being inlined as raw SQL.
"""

from __future__ import annotations

import importlib
import re
import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

POSTGRES_SCHEMES = {
    "postgres",
    "postgresql",
    "postgresql+psycopg",
    "postgresql+psycopg2",
    "postgres+psycopg",
}

# A string literal may legitimately contain a '?', so placeholders are only
# rewritten outside of quoted sections.
_QUOTED_OR_PLACEHOLDER = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|\?")


def is_postgres_dsn(value: str) -> bool:
    """Return whether a configured database value is a PostgreSQL connection URL."""
    if "://" not in value:
        return False
    scheme = urlparse(value).scheme.lower()
    return scheme in POSTGRES_SCHEMES


def normalize_postgres_dsn(value: str) -> str:
    """Return a DSN psycopg accepts, dropping any SQLAlchemy-style driver suffix."""
    scheme, _, remainder = value.partition("://")
    base = scheme.split("+", 1)[0].lower()
    if base == "postgres":
        base = "postgresql"
    return f"{base}://{remainder}"


def translate_placeholders(sql: str) -> str:
    """Rewrite SQLite's ``?`` placeholders as psycopg's ``%s``.

    Existing ``%`` characters are escaped first: psycopg treats ``%`` as the
    start of a placeholder, so a bare ``LIKE '%x%'`` would otherwise raise.
    """
    escaped = sql.replace("%", "%%")

    def _replace(match: re.Match[str]) -> str:
        text = match.group(0)
        return "%s" if text == "?" else text

    return _QUOTED_OR_PLACEHOLDER.sub(_replace, escaped)


class Cursor:
    """The subset of the DB-API cursor surface the store relies on."""

    __slots__ = ("_cursor", "_rowcount")

    def __init__(self, cursor: Any, rowcount: int | None = None) -> None:
        self._cursor = cursor
        self._rowcount = rowcount

    def fetchone(self) -> Any:
        """Return the next row, or None."""
        return self._cursor.fetchone()

    def fetchall(self) -> list[Any]:
        """Return every remaining row."""
        return self._cursor.fetchall()

    def __iter__(self):
        """Iterate the remaining rows."""
        return iter(self._cursor)

    @property
    def rowcount(self) -> int:
        """Return how many rows the statement affected."""
        return self._cursor.rowcount if self._rowcount is None else self._rowcount


class Connection(ABC):
    """A dialect-neutral connection exposing the calls the store makes."""

    @abstractmethod
    def execute(self, sql: str, parameters: Any = ()) -> Cursor:
        """Run one statement and return its cursor."""

    @abstractmethod
    def executemany(self, sql: str, seq_of_parameters: Any) -> Cursor:
        """Run one statement once per parameter set."""

    @abstractmethod
    def executescript(self, script: str) -> None:
        """Run a multi-statement DDL script."""

    @abstractmethod
    def commit(self) -> None:
        """Commit the open transaction."""

    @abstractmethod
    def close(self) -> None:
        """Release the connection."""


class SqliteConnection(Connection):
    """Thin pass-through: sqlite3 already provides this surface."""

    __slots__ = ("raw",)

    def __init__(self, raw: sqlite3.Connection) -> None:
        self.raw = raw

    def execute(self, sql: str, parameters: Any = ()) -> Cursor:
        """Run one statement and return its cursor."""
        return Cursor(self.raw.execute(sql, parameters))

    def executemany(self, sql: str, seq_of_parameters: Any) -> Cursor:
        """Run one statement once per parameter set."""
        return Cursor(self.raw.executemany(sql, seq_of_parameters))

    def executescript(self, script: str) -> None:
        """Run a multi-statement DDL script."""
        self.raw.executescript(script)

    def commit(self) -> None:
        """Commit the open transaction."""
        self.raw.commit()

    def close(self) -> None:
        """Release the connection."""
        self.raw.close()


class PostgresConnection(Connection):
    """Adapt psycopg to the sqlite3-shaped surface the store expects."""

    __slots__ = ("raw", "_row_factory")

    def __init__(self, raw: Any, row_factory: Any) -> None:
        self.raw = raw
        self._row_factory = row_factory

    def execute(self, sql: str, parameters: Any = ()) -> Cursor:
        """Run one statement, translating placeholders first."""
        cursor = self.raw.cursor(row_factory=self._row_factory)
        cursor.execute(translate_placeholders(sql), tuple(parameters or ()))
        return Cursor(cursor)

    def executemany(self, sql: str, seq_of_parameters: Any) -> Cursor:
        """Run one statement once per parameter set."""
        rows = [tuple(item) for item in seq_of_parameters]
        cursor = self.raw.cursor(row_factory=self._row_factory)
        cursor.executemany(translate_placeholders(sql), rows)
        return Cursor(cursor, rowcount=len(rows))

    def executescript(self, script: str) -> None:
        """Run a multi-statement DDL script."""
        with self.raw.cursor() as cursor:
            cursor.execute(script)

    def commit(self) -> None:
        """Commit the open transaction."""
        self.raw.commit()

    def close(self) -> None:
        """Release the connection."""
        self.raw.close()


class Dialect(ABC):
    """Everything about the store that differs between backends."""

    name: str
    #: Whether the backend supports reclaiming space with VACUUM in autocommit.
    supports_vacuum: bool = True

    @abstractmethod
    def connect(self) -> Connection:
        """Open a new connection."""

    @abstractmethod
    def describe(self) -> str:
        """Return a human-readable, credential-free location."""

    # --- SQL fragments -----------------------------------------------------

    @property
    @abstractmethod
    def autoincrement_pk(self) -> str:
        """Column definition for an auto-incrementing integer primary key."""

    @property
    @abstractmethod
    def insert_or_ignore(self) -> str:
        """Statement prefix that skips rows violating a unique constraint."""

    @property
    def on_conflict_do_nothing(self) -> str:
        """Trailing clause pairing with :attr:`insert_or_ignore`."""
        return ""

    @abstractmethod
    def epoch_diff(self, later: str, earlier: str) -> str:
        """Return SQL for the difference between two ISO timestamp columns, in seconds."""

    @property
    def offset_without_limit(self) -> str:
        """Return a LIMIT clause that keeps every row past an OFFSET."""
        return "LIMIT -1 OFFSET ?"

    @abstractmethod
    def existing_columns(self, connection: Connection, table: str) -> set[str]:
        """Return the column names currently present on a table."""

    def prepare(self, connection: Connection) -> None:
        """Apply per-connection pragmas or session settings.

        Optional: most backends need nothing here.
        """
        return None

    def vacuum(self, connection: Connection) -> None:
        """Reclaim free space."""
        connection.execute("VACUUM")

    def resync_identity(self, connection: Connection, table: str, column: str = "id") -> None:
        """Realign a generated-id counter after rows were inserted with explicit ids.

        Only meaningful where the backend keeps a separate sequence object.
        SQLite derives the next rowid from the table itself, so there is
        nothing to fix.
        """
        return


class SqliteDialect(Dialect):
    """The default backend: one local file, no server."""

    name = "sqlite"

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path).resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> Connection:
        """Open a thread-friendly SQLite connection."""
        raw = sqlite3.connect(self.database_path, check_same_thread=False)
        raw.row_factory = sqlite3.Row
        return SqliteConnection(raw)

    def describe(self) -> str:
        """Return the database file path."""
        return str(self.database_path)

    @property
    def autoincrement_pk(self) -> str:
        """Column definition for an auto-incrementing integer primary key."""
        return "INTEGER PRIMARY KEY AUTOINCREMENT"

    @property
    def insert_or_ignore(self) -> str:
        """Statement prefix that skips rows violating a unique constraint."""
        return "INSERT OR IGNORE INTO"

    def epoch_diff(self, later: str, earlier: str) -> str:
        """Return the difference between two ISO timestamp columns, in seconds."""
        return f"((julianday({later}) - julianday({earlier})) * 86400.0)"

    def existing_columns(self, connection: Connection, table: str) -> set[str]:
        """Return the column names currently present on a table."""
        return {row["name"] for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}

    def prepare(self, connection: Connection) -> None:
        """Enable write-ahead logging for better read concurrency."""
        connection.execute("PRAGMA journal_mode=WAL")


class PostgresDialect(Dialect):
    """Opt-in backend for deployments with a managed database."""

    name = "postgres"

    def __init__(self, dsn: str) -> None:
        self.dsn = normalize_postgres_dsn(dsn)
        self._psycopg, self._row_factory, self._version = _load_psycopg()

    def connect(self) -> Connection:
        """Open a new PostgreSQL connection returning mapping-style rows."""
        raw = self._psycopg.connect(self.dsn)
        if self._version == 2:
            return PostgresConnection(_Psycopg2Adapter(raw, self._row_factory), self._row_factory)
        return PostgresConnection(raw, self._row_factory)

    def describe(self) -> str:
        """Return the DSN with any password redacted."""
        from .sql_adapters import mask_connection_secret

        return mask_connection_secret(self.dsn) or self.dsn

    @property
    def autoincrement_pk(self) -> str:
        """Column definition for an auto-incrementing integer primary key."""
        return "BIGSERIAL PRIMARY KEY"

    @property
    def insert_or_ignore(self) -> str:
        """Statement prefix that skips rows violating a unique constraint."""
        return "INSERT INTO"

    @property
    def on_conflict_do_nothing(self) -> str:
        """Trailing clause pairing with :attr:`insert_or_ignore`."""
        return "ON CONFLICT DO NOTHING"

    def epoch_diff(self, later: str, earlier: str) -> str:
        """Return the difference between two ISO timestamp columns, in seconds."""
        return f"EXTRACT(EPOCH FROM ({later}::timestamptz - {earlier}::timestamptz))"

    @property
    def offset_without_limit(self) -> str:
        """Return a LIMIT clause that keeps every row past an OFFSET."""
        return "LIMIT ALL OFFSET ?"

    def existing_columns(self, connection: Connection, table: str) -> set[str]:
        """Return the column names currently present on a table."""
        rows = connection.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = ?",
            (table,),
        ).fetchall()
        return {row["column_name"] for row in rows}

    def vacuum(self, connection: Connection) -> None:
        """Reclaim free space.

        PostgreSQL autovacuum already handles this, and VACUUM cannot run inside
        a transaction block, so this is a no-op rather than a forced full
        rewrite of the tables.
        """
        return

    def resync_identity(self, connection: Connection, table: str, column: str = "id") -> None:
        """Advance a BIGSERIAL sequence past the largest id already present.

        Copying rows in with explicit ids leaves the sequence at its old value,
        so the next natural insert would collide with an existing primary key.
        """
        connection.execute(
            f"""
            SELECT setval(
                pg_get_serial_sequence('{table}', '{column}'),
                COALESCE((SELECT MAX({column}) FROM {table}), 0) + 1,
                false
            )
            """
        )


class _Psycopg2Adapter:
    """Give psycopg2 the `cursor(row_factory=...)` signature psycopg 3 uses."""

    __slots__ = ("_raw", "_row_factory")

    def __init__(self, raw: Any, row_factory: Any) -> None:
        self._raw = raw
        self._row_factory = row_factory

    def cursor(self, row_factory: Any = None) -> Any:
        """Return a dict-style cursor."""
        del row_factory
        return self._raw.cursor(cursor_factory=self._row_factory)

    def commit(self) -> None:
        """Commit the open transaction."""
        self._raw.commit()

    def close(self) -> None:
        """Release the connection."""
        self._raw.close()


def _load_psycopg() -> tuple[Any, Any, int]:
    """Import psycopg 3, falling back to psycopg2, and return its dict row factory."""
    try:
        psycopg = importlib.import_module("psycopg")
        rows = importlib.import_module("psycopg.rows")
        return psycopg, rows.dict_row, 3
    except ImportError:
        pass
    try:
        psycopg2 = importlib.import_module("psycopg2")
        extras = importlib.import_module("psycopg2.extras")
        return psycopg2, extras.RealDictCursor, 2
    except ImportError as exc:
        raise RuntimeError(
            "A PostgreSQL metadata store needs a driver that is not installed. "
            "Install one with 'pip install psycopg' (or 'pip install mr-piply[postgres]')."
        ) from exc


def build_dialect(database: str | Path) -> Dialect:
    """Return the dialect for a configured database location.

    A PostgreSQL URL selects the Postgres backend; anything else is treated as a
    SQLite file path, which keeps the zero-configuration default intact.
    """
    text = str(database)
    if is_postgres_dsn(text):
        return PostgresDialect(text)
    return SqliteDialect(text)
