"""Application settings loaded from environment variables and optional .env files."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _parse_bool(value: str | None, default: bool = False) -> bool:
    """Parse a permissive boolean value from environment text."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_int(value: str | None, default: int) -> int:
    """Parse an integer environment value with a safe fallback."""
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _parse_env_file(path: Path) -> dict[str, str]:
    """Read simple KEY=VALUE pairs from one .env file."""
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


class SettingsError(ValueError):
    """Raised when an environment setting cannot be used as configured."""


# Server URLs that are not a supported metadata store, mapped to the reason.
_UNSUPPORTED_DATABASE_SCHEMES = (
    "mysql",
    "mariadb",
    "mssql",
    "sqlserver",
    "odbc",
    "oracle",
    "mongodb",
    "cockroachdb",
)


def _validate_database_setting(value: str) -> None:
    """Reject a PIPLY_DATABASE value that is neither a file path nor a supported DSN.

    Treating an unrecognised URL as a path produces a confusing filesystem
    error, or worse, silently creates a directory named ``mysql:`` and starts an
    empty runtime.
    """
    if "://" not in value:
        return

    scheme = value.split("://", 1)[0].strip().lower()
    base_scheme = scheme.split("+", 1)[0]

    if base_scheme in {"postgres", "postgresql"}:
        return  # Supported: the optional PostgreSQL metadata store.

    if base_scheme in _UNSUPPORTED_DATABASE_SCHEMES:
        raise SettingsError(
            f"PIPLY_DATABASE does not support '{scheme}'. The metadata store is either a SQLite "
            "file path (the default) or a PostgreSQL URL. Other databases are reached from "
            "sql_sensor and from tasks instead. "
            "See docs/YAML_SPECIFICATION.md section 'Runtime storage and external databases'."
        )
    if base_scheme in {"sqlite", "sqlite3"}:
        raise SettingsError(
            "PIPLY_DATABASE must be a plain file path, not a sqlite:// URL. "
            f"Use the path directly, for example '{value.split('://', 1)[1].lstrip('/') or 'piply.db'}'."
        )
    raise SettingsError(
        f"PIPLY_DATABASE has an unrecognised scheme '{scheme}'. Use a SQLite file path or a "
        "PostgreSQL URL such as 'postgresql://user:password@host:5432/piply'."
    )


def _resolve_optional_path(value: str | None, base_dir: Path | None = None) -> Path | None:
    """Resolve a possibly relative path against the supplied base directory."""
    if value is None or not value.strip():
        return None
    candidate = Path(value.strip())
    if candidate.is_absolute():
        return candidate.resolve()
    root = base_dir or Path.cwd()
    return (root / candidate).resolve()


@dataclass(slots=True, frozen=True)
class PiplySettings:
    """Runtime settings shared by the CLI, API, loader, and engine."""

    config_path: Path | None
    database_path: Path | None
    #: Set instead of database_path when the metadata store is PostgreSQL.
    database_dsn: str | None
    default_max_parallel_tasks: int
    stale_run_timeout_seconds: int
    heartbeat_interval_seconds: int
    scheduler_poll_interval_seconds: int
    queue_dispatch_batch_size: int
    queue_dispatch_stale_seconds: int
    upcoming_run_preview_count: int
    pipeline_run_history_count: int
    reconcile_interval_seconds: int
    retention_run_days: int
    retention_log_days: int
    retention_max_runs_per_pipeline: int
    artifacts_dir: Path | None
    metrics_enabled: bool
    auth_enabled: bool
    auth_username: str | None
    auth_password: str | None
    api_token: str | None
    env_values: dict[str, str]


def _candidate_env_files(config_path: Path | None) -> list[Path]:
    """Return the .env files to load in precedence order."""
    candidates = [Path.cwd() / ".env"]
    if config_path is not None:
        config_dir = config_path.resolve().parent
        config_env = config_dir / ".env"
        if config_env not in candidates:
            candidates.append(config_env)
    return candidates


def load_settings(
    config_path: str | Path | None = None,
    *,
    environ: dict[str, str] | None = None,
) -> PiplySettings:
    """Load settings from .env files and environment variables."""
    env_source = dict(environ or os.environ)
    resolved_config = Path(config_path).resolve() if config_path else None

    merged_env: dict[str, str] = {}
    for env_file in _candidate_env_files(resolved_config):
        merged_env.update(_parse_env_file(env_file))
    merged_env.update(env_source)

    config_value = merged_env.get("PIPLY_CONFIG")
    if resolved_config is None:
        resolved_config = _resolve_optional_path(config_value)

    base_dir = resolved_config.parent if resolved_config is not None else Path.cwd()
    raw_database = (merged_env.get("PIPLY_DATABASE") or "").strip()
    database_dsn: str | None = None
    resolved_database: Path | None = None
    if raw_database:
        _validate_database_setting(raw_database)
        if "://" in raw_database:
            database_dsn = raw_database
        else:
            resolved_database = _resolve_optional_path(raw_database, base_dir)
    default_max_parallel_tasks = max(
        1,
        _parse_int(merged_env.get("PIPLY_DEFAULT_MAX_PARALLEL_TASKS"), 4),
    )
    stale_run_timeout_seconds = max(
        60,
        _parse_int(merged_env.get("PIPLY_STALE_RUN_TIMEOUT_SECONDS"), 60 * 60),
    )
    heartbeat_interval_seconds = max(
        2,
        _parse_int(merged_env.get("PIPLY_HEARTBEAT_INTERVAL_SECONDS"), 10),
    )
    scheduler_poll_interval_seconds = max(
        2,
        _parse_int(merged_env.get("PIPLY_SCHEDULER_POLL_INTERVAL_SECONDS"), 10),
    )
    queue_dispatch_batch_size = max(
        1,
        _parse_int(merged_env.get("PIPLY_QUEUE_DISPATCH_BATCH_SIZE"), 100),
    )
    queue_dispatch_stale_seconds = max(
        30,
        _parse_int(merged_env.get("PIPLY_QUEUE_DISPATCH_STALE_SECONDS"), 300),
    )
    upcoming_run_preview_count = max(
        1,
        _parse_int(merged_env.get("PIPLY_UPCOMING_RUN_PREVIEW_COUNT"), 8),
    )
    pipeline_run_history_count = max(
        1,
        min(20, _parse_int(merged_env.get("PIPLY_PIPELINE_RUN_HISTORY_COUNT"), 5)),
    )
    reconcile_interval_seconds = max(
        0,
        _parse_int(merged_env.get("PIPLY_RECONCILE_INTERVAL_SECONDS"), 15),
    )
    retention_run_days = max(0, _parse_int(merged_env.get("PIPLY_RETENTION_RUN_DAYS"), 30))
    retention_log_days = max(0, _parse_int(merged_env.get("PIPLY_RETENTION_LOG_DAYS"), 14))
    retention_max_runs_per_pipeline = max(
        0,
        _parse_int(merged_env.get("PIPLY_RETENTION_MAX_RUNS_PER_PIPELINE"), 200),
    )
    artifacts_dir = _resolve_optional_path(merged_env.get("PIPLY_ARTIFACTS_DIR"), base_dir)
    metrics_enabled = _parse_bool(merged_env.get("PIPLY_METRICS_ENABLED"), True)

    auth_username = merged_env.get("PIPLY_AUTH_USERNAME")
    auth_password = merged_env.get("PIPLY_AUTH_PASSWORD")
    api_token = merged_env.get("PIPLY_API_TOKEN")
    auth_enabled = _parse_bool(merged_env.get("PIPLY_AUTH_ENABLED")) or any(
        [auth_username and auth_password, api_token]
    )

    return PiplySettings(
        config_path=resolved_config,
        database_path=resolved_database,
        database_dsn=database_dsn,
        default_max_parallel_tasks=default_max_parallel_tasks,
        stale_run_timeout_seconds=stale_run_timeout_seconds,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        scheduler_poll_interval_seconds=scheduler_poll_interval_seconds,
        queue_dispatch_batch_size=queue_dispatch_batch_size,
        queue_dispatch_stale_seconds=queue_dispatch_stale_seconds,
        upcoming_run_preview_count=upcoming_run_preview_count,
        pipeline_run_history_count=pipeline_run_history_count,
        reconcile_interval_seconds=reconcile_interval_seconds,
        retention_run_days=retention_run_days,
        retention_log_days=retention_log_days,
        retention_max_runs_per_pipeline=retention_max_runs_per_pipeline,
        artifacts_dir=artifacts_dir,
        metrics_enabled=metrics_enabled,
        auth_enabled=auth_enabled,
        auth_username=auth_username,
        auth_password=auth_password,
        api_token=api_token,
        env_values=merged_env,
    )
