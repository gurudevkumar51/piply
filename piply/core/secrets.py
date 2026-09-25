"""Small secret resolution helpers used while loading Piply YAML."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import yaml

ENV_TOKEN_PATTERN = re.compile(r"\$(\w+)|\$\{([^}]+)\}|%([^%]+)%")


def _expand_env(value: str, env_values: dict[str, str]) -> str:
    """Expand only environment-style references while loading secret backends."""
    merged_env = dict(os.environ)
    merged_env.update(env_values)

    def replace(match: re.Match[str]) -> str:
        name = match.group(1) or match.group(2) or match.group(3) or ""
        return merged_env.get(name, match.group(0))

    return ENV_TOKEN_PATTERN.sub(replace, os.path.expanduser(value))


def _resolve_path(value: str, workspace: Path, env_values: dict[str, str]) -> Path:
    """Resolve a secret file path relative to the workspace."""
    expanded = Path(_expand_env(value, env_values))
    if expanded.is_absolute():
        return expanded.resolve()
    return (workspace / expanded).resolve()


def _load_secret_file(path: Path) -> dict[str, str]:
    """Load secrets from .env, JSON, or YAML files."""
    if not path.exists():
        return {}
    suffix = path.suffix.lower()
    if suffix == ".json":
        raw_json = json.loads(path.read_text(encoding="utf-8") or "{}")
        if not isinstance(raw_json, dict):
            return {}
        return {str(key): str(value) for key, value in raw_json.items() if value is not None}
    if suffix in {".yaml", ".yml"}:
        raw_yaml = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(raw_yaml, dict):
            return {}
        return {str(key): str(value) for key, value in raw_yaml.items() if value is not None}

    values: dict[str, str] = {}
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


def _apply_direct_values(
    target: dict[str, str],
    raw_values: Any,
    env_values: dict[str, str],
) -> None:
    """Add direct secret values from a mapping."""
    if not isinstance(raw_values, dict):
        return
    for key, value in raw_values.items():
        if value is not None:
            target[str(key)] = _expand_env(str(value), env_values)


def _apply_env_backend(
    target: dict[str, str],
    raw_backend: dict[str, Any],
    env_values: dict[str, str],
) -> None:
    """Add secrets from environment variables using keys or a prefix."""
    merged_env = dict(os.environ)
    merged_env.update(env_values)
    prefix = raw_backend.get("prefix")
    if isinstance(prefix, str) and prefix:
        for key, value in merged_env.items():
            if key.startswith(prefix):
                target[key.removeprefix(prefix)] = value

    keys = raw_backend.get("keys")
    if isinstance(keys, list):
        for key in keys:
            env_key = str(key)
            if env_key in merged_env:
                target[env_key] = merged_env[env_key]
    elif isinstance(keys, dict):
        for secret_name, env_key in keys.items():
            env_name = str(env_key)
            if env_name in merged_env:
                target[str(secret_name)] = merged_env[env_name]


def _apply_file_backend(
    target: dict[str, str],
    raw_backend: dict[str, Any],
    workspace: Path,
    env_values: dict[str, str],
) -> None:
    """Add secrets from a configured file backend."""
    path_value = raw_backend.get("path") or raw_backend.get("file")
    if path_value is None:
        return
    target.update(_load_secret_file(_resolve_path(str(path_value), workspace, env_values)))


def _iter_secret_backends(raw_value: Any) -> list[dict[str, Any]]:
    """Normalize supported secret config shapes into backend mappings."""
    if raw_value in (None, "", False):
        return []
    if isinstance(raw_value, list):
        return [item for item in raw_value if isinstance(item, dict)]
    if not isinstance(raw_value, dict):
        return []
    backend_keys = {"backend", "type", "path", "file", "prefix", "keys", "values"}
    if any(key in raw_value for key in backend_keys):
        return [raw_value]
    return [{"values": raw_value}]


def load_secret_values(
    raw_value: Any,
    *,
    workspace: Path,
    env_values: dict[str, str],
) -> dict[str, str]:
    """Load configured secrets without adding heavyweight secret-manager dependencies."""
    secrets: dict[str, str] = {}
    for backend in _iter_secret_backends(raw_value):
        backend_type = str(backend.get("backend") or backend.get("type") or "").strip().lower()
        if backend_type in {"", "inline", "values"}:
            _apply_direct_values(secrets, backend.get("values", backend), env_values)
        if backend_type in {"env", "environment"} or "prefix" in backend or "keys" in backend:
            _apply_env_backend(secrets, backend, env_values)
        if backend_type in {"file", "dotenv", "json", "yaml", "yml"} or "path" in backend or "file" in backend:
            _apply_file_backend(secrets, backend, workspace, env_values)
        if "values" in backend and backend_type not in {"", "inline", "values"}:
            _apply_direct_values(secrets, backend["values"], env_values)
    return secrets


#: Substrings that mark an environment variable as carrying a credential.
_SENSITIVE_NAME_PARTS = (
    "password",
    "passwd",
    "secret",
    "token",
    "apikey",
    "api_key",
    "accesskey",
    "access_key",
    "private",
    "credential",
    "auth",
    "dsn",
    "connection_string",
    "conn_str",
    "sasl",
    "session_key",
    "signing",
)

#: What a masked value is replaced with, so the UI can show that a value exists.
MASKED = "***"


def is_sensitive_name(name: str) -> bool:
    """Return whether an environment variable name looks like a credential.

    Name-based rather than value-based because a value cannot be recognised as
    secret on its own. Deliberately broad: a false positive only hides a value
    from a debugging view, while a false negative leaks a credential.
    """
    lowered = str(name).lower()
    return any(part in lowered for part in _SENSITIVE_NAME_PARTS)


def mask_env_values(values: dict[str, str] | None) -> dict[str, str]:
    """Return env values with credential-looking entries replaced by ``***``.

    Used when a stored environment is shown back to a user. Non-secret values
    such as a tenant name stay visible, which is what makes the view useful.
    """
    if not values:
        return {}
    return {key: (MASKED if is_sensitive_name(key) and value else value) for key, value in values.items()}


def secret_env_aliases(secret_values: dict[str, str]) -> dict[str, str]:
    """Expose secret names through explicit expansion tokens such as ${secret:NAME}."""
    aliases: dict[str, str] = {}
    for key, value in secret_values.items():
        aliases[f"secret:{key}"] = value
        aliases[f"secret.{key}"] = value
    return aliases
