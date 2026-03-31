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
    default_max_parallel_tasks: int
    stale_run_timeout_seconds: int
    heartbeat_interval_seconds: int
    auth_enabled: bool
    auth_username: str | None
    auth_password: str | None
    api_token: str | None


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
    resolved_database = _resolve_optional_path(merged_env.get("PIPLY_DATABASE"), base_dir)
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

    auth_username = merged_env.get("PIPLY_AUTH_USERNAME")
    auth_password = merged_env.get("PIPLY_AUTH_PASSWORD")
    api_token = merged_env.get("PIPLY_API_TOKEN")
    auth_enabled = _parse_bool(merged_env.get("PIPLY_AUTH_ENABLED")) or any(
        [auth_username and auth_password, api_token]
    )

    return PiplySettings(
        config_path=resolved_config,
        database_path=resolved_database,
        default_max_parallel_tasks=default_max_parallel_tasks,
        stale_run_timeout_seconds=stale_run_timeout_seconds,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        auth_enabled=auth_enabled,
        auth_username=auth_username,
        auth_password=auth_password,
        api_token=api_token,
    )
