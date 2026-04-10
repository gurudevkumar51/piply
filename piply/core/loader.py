"""Project loader for Piply YAML configuration files."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

from piply.settings import load_settings

from .models import PipelineDefinition, ProjectDefinition, RetryPolicy, SensorDefinition, TaskDefinition
from .scheduling import CronSchedule, IntervalSchedule, ScheduleError, parse_interval


class ConfigError(ValueError):
    """Raised when the Piply YAML configuration is invalid."""


TASK_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
ENV_TOKEN_PATTERN = re.compile(r"\$(\w+)|\$\{([^}]+)\}|%([^%]+)%")


def discover_config(start_dir: Path | None = None) -> Path:
    """Discover the nearest Piply config file from the current workspace."""
    search_root = (start_dir or Path.cwd()).resolve()
    candidates = [
        search_root / "piply.yaml",
        search_root / "piply.yml",
        search_root / "piply-demo" / "piply.yaml",
        search_root / "piply-demo" / "piply.yml",
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate

    raise ConfigError(
        "Could not find a Piply config file. Looked for piply.yaml, piply.yml, and piply-demo/piply.yaml."
    )


def _expand_string(value: str, env_values: dict[str, str] | None = None) -> str:
    """Expand environment variables and user-home markers in strings."""
    merged_env = dict(os.environ)
    merged_env.update(env_values or {})

    def replace(match: re.Match[str]) -> str:
        name = match.group(1) or match.group(2) or match.group(3) or ""
        return merged_env.get(name, match.group(0))

    return ENV_TOKEN_PATTERN.sub(replace, os.path.expanduser(value))


def _expand_value(value: Any, env_values: dict[str, str] | None = None) -> Any:
    """Expand env-backed strings inside nested config values."""
    if isinstance(value, str):
        return _expand_string(value, env_values)
    if isinstance(value, list):
        return [_expand_value(item, env_values) for item in value]
    if isinstance(value, dict):
        return {str(key): _expand_value(item, env_values) for key, item in value.items()}
    return value


def _ensure_mapping(value: Any, label: str) -> dict[str, Any]:
    """Validate that a config field is a mapping."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"{label} must be a mapping")
    return value


def _ensure_list(value: Any, label: str) -> list[Any]:
    """Validate that a config field is a list."""
    if value is None:
        return []
    if not isinstance(value, list):
        raise ConfigError(f"{label} must be a list")
    return value


def _resolve_path(
    value: str | None,
    base_dir: Path,
    env_values: dict[str, str] | None = None,
) -> Path | None:
    """Resolve a possibly relative path against the project workspace."""
    if value is None:
        return None
    expanded = Path(_expand_string(value, env_values))
    if expanded.is_absolute():
        return expanded.resolve()
    return (base_dir / expanded).resolve()


def _parse_schedule(raw_value: Any, timezone_name: str):
    """Parse one schedule configuration block."""
    if raw_value in (None, "", False):
        return None
    if isinstance(raw_value, str):
        return CronSchedule(raw_value, timezone_name=timezone_name)
    if not isinstance(raw_value, dict):
        raise ConfigError("schedule must be either a cron string or a mapping")

    if "cron" in raw_value:
        return CronSchedule(
            str(raw_value["cron"]).strip(),
            timezone_name=str(raw_value.get("timezone", timezone_name)),
        )
    if "every" in raw_value:
        return IntervalSchedule(
            seconds=parse_interval(str(raw_value["every"])),
            timezone_name=str(raw_value.get("timezone", timezone_name)),
        )
    if "interval_seconds" in raw_value:
        return IntervalSchedule(
            seconds=int(raw_value["interval_seconds"]),
            timezone_name=str(raw_value.get("timezone", timezone_name)),
        )

    raise ConfigError("schedule mappings must define one of: cron, every, interval_seconds")


def _parse_execution(
    raw_value: Any,
    pipeline_id: str,
    *,
    default_max_parallel_tasks: int,
    explicit_max_parallel_tasks: Any = None,
) -> int:
    """Parse concurrency settings while keeping legacy execution.mode support."""
    raw_value = explicit_max_parallel_tasks if explicit_max_parallel_tasks is not None else raw_value
    if raw_value in (None, "", False):
        return default_max_parallel_tasks

    if isinstance(raw_value, int):
        max_parallel_tasks = raw_value
        mode = None
    elif isinstance(raw_value, str):
        stripped = raw_value.strip().lower()
        if stripped.isdigit():
            max_parallel_tasks = int(stripped)
            mode = None
        elif stripped in {"sequential", "parallel"}:
            mode = stripped
            max_parallel_tasks = default_max_parallel_tasks
        else:
            raise ConfigError(
                f"Pipeline '{pipeline_id}' execution must be a worker count, 'sequential', 'parallel', or a mapping"
            )
    elif isinstance(raw_value, dict):
        mode = None if raw_value.get("mode") is None else str(raw_value.get("mode")).strip().lower()
        max_parallel_tasks = int(
            raw_value.get("max_parallel_tasks", raw_value.get("workers", default_max_parallel_tasks))
        )
    else:
        raise ConfigError(
            f"Pipeline '{pipeline_id}' execution must be an int, string, or mapping"
        )

    if mode not in {None, "sequential", "parallel"}:
        raise ConfigError(
            f"Pipeline '{pipeline_id}' execution mode must be 'sequential' or 'parallel'"
        )
    if max_parallel_tasks < 1:
        raise ConfigError(
            f"Pipeline '{pipeline_id}' max_parallel_tasks must be greater than zero"
        )
    if mode == "sequential":
        return 1
    return max_parallel_tasks


def _normalize_expected_status(raw_value: Any, label: str) -> tuple[int, ...]:
    """Normalize an expected HTTP status configuration value."""
    if raw_value is None:
        return (200, 201, 202, 204)
    if isinstance(raw_value, int):
        return (raw_value,)
    if isinstance(raw_value, list) and all(isinstance(item, int) for item in raw_value):
        return tuple(raw_value)
    raise ConfigError(f"{label} must be an int or a list of ints")


def _parse_retry_policy(raw_value: Any, pipeline_id: str) -> RetryPolicy:
    """Parse automatic retry settings for one pipeline."""
    if raw_value in (None, "", False):
        return RetryPolicy()
    if not isinstance(raw_value, dict):
        raise ConfigError(f"Pipeline '{pipeline_id}' retry must be a mapping")

    attempts = int(raw_value.get("attempts", raw_value.get("count", 0)))
    mode = str(raw_value.get("mode", "startover")).strip().lower()
    delay_seconds = int(raw_value.get("delay_seconds", raw_value.get("delay", 0)))
    if attempts < 0:
        raise ConfigError(f"Pipeline '{pipeline_id}' retry attempts must be zero or greater")
    if mode not in {"resume", "startover"}:
        raise ConfigError(f"Pipeline '{pipeline_id}' retry mode must be 'resume' or 'startover'")
    if delay_seconds < 0:
        raise ConfigError(f"Pipeline '{pipeline_id}' retry delay_seconds must be zero or greater")
    return RetryPolicy(attempts=attempts, mode=mode, delay_seconds=delay_seconds)


def _parse_depends_on(raw_value: Any, label: str) -> tuple[str, ...]:
    """Parse and validate task dependency ids."""
    depends_on = tuple(str(item) for item in _ensure_list(raw_value, label))
    for dependency in depends_on:
        if not TASK_ID_PATTERN.match(dependency):
            raise ConfigError(f"{label} contains an invalid task id '{dependency}'")
    return depends_on


def _is_sftp_path(value: str) -> bool:
    """Return whether a sensor path uses the supported sftp:// URI form."""
    return value.strip().lower().startswith("sftp://")


def _parse_sftp_location(value: str) -> dict[str, object]:
    """Parse an SFTP URI into lightweight SSH polling parameters."""
    parsed = urlparse(value)
    if parsed.scheme.lower() != "sftp":
        raise ConfigError(f"Unsupported remote file sensor path '{value}'")
    if not parsed.hostname or not parsed.path:
        raise ConfigError(f"SFTP sensor path '{value}' must include both host and remote path")
    return {
        "ssh_host": parsed.hostname,
        "ssh_user": parsed.username,
        "ssh_port": parsed.port or 22,
        "remote_path": parsed.path,
    }


def _parse_sensors(
    raw_value: Any,
    pipeline_id: str,
    *,
    workspace: Path,
    task_ids: set[str],
    env_values: dict[str, str],
) -> dict[str, SensorDefinition]:
    """Parse file and SQL sensor definitions for one pipeline."""
    if raw_value in (None, "", False):
        return {}

    raw_sensors: dict[str, Any]
    if isinstance(raw_value, dict):
        raw_sensors = raw_value
    elif isinstance(raw_value, list):
        raw_sensors = {}
        for index, item in enumerate(raw_value, start=1):
            if not isinstance(item, dict):
                raise ConfigError(f"Pipeline '{pipeline_id}' sensors must contain mappings")
            sensor_id = str(item.get("id") or item.get("name") or f"sensor_{index}")
            raw_sensors[sensor_id] = item
    else:
        raise ConfigError(f"Pipeline '{pipeline_id}' sensors must be a mapping or list")

    sensors: dict[str, SensorDefinition] = {}
    for sensor_id, raw_sensor in raw_sensors.items():
        if not isinstance(raw_sensor, dict):
            raise ConfigError(f"Pipeline '{pipeline_id}' sensor '{sensor_id}' must be a mapping")
        if not TASK_ID_PATTERN.match(sensor_id):
            raise ConfigError(
                f"Pipeline '{pipeline_id}' contains invalid sensor id '{sensor_id}'. Use letters, numbers, _ or -."
            )

        sensor_type = str(raw_sensor.get("type") or "").strip().lower()
        if sensor_type not in {"file_sensor", "sql_sensor"}:
            raise ConfigError(
                f"Pipeline '{pipeline_id}' sensor '{sensor_id}' uses unsupported type '{sensor_type}'"
            )

        task_id = raw_sensor.get("task_id")
        if task_id is not None and str(task_id) not in task_ids:
            raise ConfigError(
                f"Pipeline '{pipeline_id}' sensor '{sensor_id}' points to unknown task '{task_id}'"
            )

        title = str(raw_sensor.get("title") or raw_sensor.get("name") or sensor_id.replace("_", " ").title())
        enabled = bool(raw_sensor.get("enabled", True))
        ignore_existing = bool(raw_sensor.get("ignore_existing", True))

        if sensor_type == "file_sensor":
            path_value = raw_sensor.get("path")
            if not path_value:
                raise ConfigError(
                    f"Pipeline '{pipeline_id}' sensor '{sensor_id}' requires path for file_sensor"
                )
            expanded_path = _expand_string(str(path_value), env_values)
            ssh_host = None if raw_sensor.get("host") is None else _expand_string(str(raw_sensor.get("host")), env_values)
            ssh_user = None if raw_sensor.get("user") is None else _expand_string(str(raw_sensor.get("user")), env_values)
            ssh_port = int(raw_sensor.get("port", 22))
            base_kwargs = {
                "sensor_id": sensor_id,
                "sensor_type": "file_sensor",
                "title": title,
                "enabled": enabled,
                "path_source": expanded_path,
                "pattern": _expand_string(str(raw_sensor.get("pattern") or "*"), env_values),
                "recursive": bool(raw_sensor.get("recursive", False)),
                "ignore_existing": ignore_existing,
                "task_id": None if task_id is None else str(task_id),
                "ssh_binary": _expand_string(str(raw_sensor.get("ssh_binary") or "ssh"), env_values),
                "connect_timeout": int(raw_sensor.get("connect_timeout", 8)),
                "ssh_host": ssh_host,
                "ssh_user": ssh_user,
                "ssh_port": ssh_port,
                "ssh_key_file": _resolve_path(
                    None if raw_sensor.get("key_file") is None else str(raw_sensor.get("key_file")),
                    workspace,
                    env_values,
                ),
            }
            if _is_sftp_path(expanded_path):
                sftp_location = _parse_sftp_location(expanded_path)
                if ssh_host is None:
                    base_kwargs["ssh_host"] = sftp_location["ssh_host"]
                if ssh_user is None:
                    base_kwargs["ssh_user"] = sftp_location["ssh_user"]
                if raw_sensor.get("port") is None:
                    base_kwargs["ssh_port"] = sftp_location["ssh_port"]
                sensors[sensor_id] = SensorDefinition(**base_kwargs, remote_path=sftp_location["remote_path"])
            else:
                sensors[sensor_id] = SensorDefinition(
                    **base_kwargs,
                    path=_resolve_path(expanded_path, workspace, env_values),
                )
            continue

        connection_value = raw_sensor.get("connection") or raw_sensor.get("database_url") or raw_sensor.get("dsn")
        connection_env = raw_sensor.get("connection_env")
        if connection_value is None and connection_env is not None:
            connection_value = env_values.get(str(connection_env))
        database_value = raw_sensor.get("database") or raw_sensor.get("path")
        table = raw_sensor.get("table")
        if (not connection_value and not database_value) or not table:
            raise ConfigError(
                f"Pipeline '{pipeline_id}' sensor '{sensor_id}' requires connection or database and table for sql_sensor"
            )
        database_path = None
        if database_value is not None:
            database_path = _resolve_path(str(database_value), workspace, env_values)
        sensors[sensor_id] = SensorDefinition(
            sensor_id=sensor_id,
            sensor_type="sql_sensor",
            title=title,
            enabled=enabled,
            database=database_path,
            connection=None if connection_value is None else _expand_string(str(connection_value), env_values),
            table=_expand_string(str(table), env_values),
            cursor_column=_expand_string(str(raw_sensor.get("cursor_column") or "rowid"), env_values),
            where=None if raw_sensor.get("where") is None else _expand_string(str(raw_sensor.get("where")), env_values),
            ignore_existing=ignore_existing,
            task_id=None if task_id is None else str(task_id),
        )

    return sensors


def _validate_task_graph(pipeline_id: str, tasks: dict[str, TaskDefinition]) -> None:
    """Validate dependency references and detect task cycles."""
    for task in tasks.values():
        for dependency in task.depends_on:
            if dependency not in tasks:
                raise ConfigError(
                    f"Pipeline '{pipeline_id}' task '{task.task_id}' depends on unknown task '{dependency}'"
                )

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(task_id: str) -> None:
        if task_id in visited:
            return
        if task_id in visiting:
            raise ConfigError(f"Pipeline '{pipeline_id}' contains a cycle at task '{task_id}'")
        visiting.add(task_id)
        for dependency in tasks[task_id].depends_on:
            visit(dependency)
        visiting.remove(task_id)
        visited.add(task_id)

    for task_id in tasks:
        visit(task_id)


def _detect_parallelism(tasks: dict[str, TaskDefinition]) -> bool:
    """Return whether the DAG can execute more than one task at the same time."""
    remaining_dependencies = {
        task_id: set(task.depends_on) for task_id, task in tasks.items()
    }
    ready = sorted(task_id for task_id, dependencies in remaining_dependencies.items() if not dependencies)
    processed: set[str] = set()

    while ready:
        if len(ready) > 1:
            return True
        current_batch = list(ready)
        ready = []
        for task_id in current_batch:
            processed.add(task_id)
            for candidate_id, dependencies in remaining_dependencies.items():
                if task_id in dependencies:
                    dependencies.remove(task_id)
                    if not dependencies and candidate_id not in processed and candidate_id not in ready:
                        ready.append(candidate_id)
        ready.sort()
    return False


def _validate_pipeline_trigger_graph(pipelines: dict[str, PipelineDefinition]) -> None:
    """Validate downstream trigger references and prevent trigger loops."""
    for pipeline in pipelines.values():
        for target in pipeline.triggers_on_success:
            if target not in pipelines:
                raise ConfigError(
                    f"Pipeline '{pipeline.pipeline_id}' triggers unknown pipeline '{target}'"
                )

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(pipeline_id: str) -> None:
        if pipeline_id in visited:
            return
        if pipeline_id in visiting:
            raise ConfigError(f"Pipeline trigger cycle detected at '{pipeline_id}'")
        visiting.add(pipeline_id)
        for target in pipelines[pipeline_id].triggers_on_success:
            visit(target)
        visiting.remove(pipeline_id)
        visited.add(pipeline_id)

    for pipeline_id in pipelines:
        visit(pipeline_id)


def _build_single_task_pipeline(raw_pipeline: dict[str, Any]) -> dict[str, Any]:
    """Build a backwards-compatible single-task pipeline definition."""
    if "entrypoint" in raw_pipeline or "script" in raw_pipeline:
        entrypoint = _ensure_mapping(raw_pipeline.get("entrypoint"), "entrypoint")
        return {
            "main": {
                "type": "python",
                "path": raw_pipeline.get("script") or entrypoint.get("path"),
                "python": entrypoint.get("python"),
                "args": entrypoint.get("args", raw_pipeline.get("args", [])),
                "cwd": entrypoint.get("cwd") or raw_pipeline.get("working_dir"),
                "env": entrypoint.get("env", {}),
            }
        }
    raise ConfigError("Pipeline must define either 'tasks' or an entrypoint/script")


def _parse_task(
    pipeline_id: str,
    task_id: str,
    raw_task: dict[str, Any],
    *,
    workspace: Path,
    default_python: str,
    inherited_env: dict[str, str],
    env_values: dict[str, str],
) -> TaskDefinition:
    """Parse one task block into a runtime definition."""
    if not TASK_ID_PATTERN.match(task_id):
        raise ConfigError(
            f"Pipeline '{pipeline_id}' contains invalid task id '{task_id}'. Use letters, numbers, _ or -."
        )

    raw_task_type = str(raw_task.get("type") or "python").lower()
    task_type = "python" if raw_task_type == "python_call" else raw_task_type
    if task_type not in {"python", "cli", "api", "ssh", "email", "webhook"}:
        raise ConfigError(
            f"Pipeline '{pipeline_id}' task '{task_id}' uses unsupported type '{raw_task_type}'"
        )

    title = _expand_string(
        str(raw_task.get("title") or raw_task.get("name") or task_id.replace("_", " ").title()),
        env_values,
    )
    description = _expand_string(str(raw_task.get("description") or ""), env_values)
    depends_on = _parse_depends_on(
        raw_task.get("depends_on"),
        f"Pipeline '{pipeline_id}' task '{task_id}' depends_on",
    )
    enabled = bool(raw_task.get("enabled", True))

    task_env = dict(inherited_env)
    task_env.update(
        {
            str(key): _expand_string(str(value), env_values)
            for key, value in _ensure_mapping(
                raw_task.get("env"),
                f"Pipeline '{pipeline_id}' task '{task_id}' env",
            ).items()
        }
    )

    if task_type == "python":
        function_name = raw_task.get("function") or raw_task.get("method")
        callable_ref = raw_task.get("call") or raw_task.get("callable")
        if function_name or callable_ref:
            if callable_ref is None and function_name is not None:
                if raw_task.get("module") is not None:
                    callable_ref = (
                        f"{_expand_string(str(raw_task['module']), env_values)}:"
                        f"{_expand_string(str(function_name), env_values)}"
                    )
                elif raw_task.get("path") is not None or raw_task.get("script") is not None:
                    callable_path = _resolve_path(
                        str(raw_task.get("path") or raw_task.get("script")),
                        workspace,
                        env_values,
                    )
                    if callable_path is not None:
                        callable_ref = f"{callable_path}::{_expand_string(str(function_name), env_values)}"
            if callable_ref is None:
                raise ConfigError(
                    f"Pipeline '{pipeline_id}' task '{task_id}' requires call/module+function/path+function for python callable tasks"
                )
            callable_text = _expand_string(str(callable_ref), env_values)
            if "::" in callable_text:
                raw_path, callable_name = callable_text.split("::", 1)
                resolved_callable_path = _resolve_path(raw_path, workspace, env_values)
                if resolved_callable_path is None:
                    raise ConfigError(
                        f"Pipeline '{pipeline_id}' task '{task_id}' callable path could not be resolved"
                    )
                callable_text = f"{resolved_callable_path}::{callable_name}"
            raw_args = _expand_value(raw_task.get("args", []), env_values)
            raw_kwargs = _expand_value(
                _ensure_mapping(
                    raw_task.get("kwargs"),
                    f"Pipeline '{pipeline_id}' task '{task_id}' kwargs",
                ),
                env_values,
            )
            if not isinstance(raw_args, list):
                raise ConfigError(
                    f"Pipeline '{pipeline_id}' task '{task_id}' args must be a list"
                )
            return TaskDefinition(
                task_id=task_id,
                title=title,
                task_type="python",
                description=description,
                depends_on=depends_on,
                enabled=enabled,
                call=callable_text,
                args=tuple(raw_args),
                kwargs={str(key): value for key, value in raw_kwargs.items()},
                cwd=_resolve_path(raw_task.get("cwd"), workspace, env_values) or workspace,
                env=task_env,
            )
        path_value = raw_task.get("path") or raw_task.get("script")
        if not path_value:
            raise ConfigError(
                f"Pipeline '{pipeline_id}' task '{task_id}' requires path/script or a callable target for python tasks"
            )
        path = _resolve_path(str(path_value), workspace, env_values)
        if path is None or not path.exists():
            raise ConfigError(
                f"Pipeline '{pipeline_id}' task '{task_id}' points to a missing script: {path_value}"
            )
        raw_args = _expand_value(raw_task.get("args", []), env_values)
        if not isinstance(raw_args, list):
            raise ConfigError(
                f"Pipeline '{pipeline_id}' task '{task_id}' args must be a list"
            )
        return TaskDefinition(
            task_id=task_id,
            title=title,
            task_type="python",
            description=description,
            depends_on=depends_on,
            enabled=enabled,
            path=path,
            python=_expand_string(str(raw_task.get("python") or default_python), env_values),
            args=tuple(raw_args),
            cwd=_resolve_path(raw_task.get("cwd"), workspace, env_values),
            env=task_env,
        )

    if task_type == "cli":
        command = raw_task.get("command")
        path_value = raw_task.get("path") or raw_task.get("script")
        raw_args = _expand_value(raw_task.get("args", []), env_values)
        if not isinstance(raw_args, list):
            raise ConfigError(
                f"Pipeline '{pipeline_id}' task '{task_id}' args must be a list"
            )
        if not command and not path_value:
            raise ConfigError(
                f"Pipeline '{pipeline_id}' task '{task_id}' requires command or path for cli tasks"
            )
        resolved_path = _resolve_path(str(path_value), workspace, env_values) if path_value else None
        if path_value and (resolved_path is None or not resolved_path.exists()):
            raise ConfigError(
                f"Pipeline '{pipeline_id}' task '{task_id}' points to a missing cli path: {path_value}"
            )
        return TaskDefinition(
            task_id=task_id,
            title=title,
            task_type="cli",
            description=description,
            depends_on=depends_on,
            enabled=enabled,
            path=resolved_path,
            command=None if command is None else _expand_string(str(command), env_values),
            args=tuple(raw_args),
            cwd=_resolve_path(raw_task.get("cwd"), workspace, env_values),
            env=task_env,
        )

    if task_type == "api" or task_type == "webhook":
        url = raw_task.get("url")
        if not url:
            raise ConfigError(
                f"Pipeline '{pipeline_id}' task '{task_id}' requires url for {task_type} tasks"
            )
        headers = {
            str(key): _expand_string(str(value), env_values)
            for key, value in _ensure_mapping(
                raw_task.get("headers"),
                f"Pipeline '{pipeline_id}' task '{task_id}' headers",
            ).items()
        }
        token = raw_task.get("token")

        method = str(raw_task.get("method", "POST" if task_type == "webhook" else "GET")).upper()

        return TaskDefinition(
            task_id=task_id,
            title=title,
            task_type=task_type,
            description=description,
            depends_on=depends_on,
            enabled=enabled,
            url=_expand_string(str(url), env_values),
            method=method,
            headers=headers,
            body=None if raw_task.get("body") is None else _expand_string(str(raw_task.get("body")), env_values),
            token=_expand_string(str(token), env_values) if token is not None else None,
            expected_status=_normalize_expected_status(
                raw_task.get("expected_status"),
                f"Pipeline '{pipeline_id}' task '{task_id}' expected_status",
            ),
            env=task_env,
        )

    if task_type == "email":
        return TaskDefinition(
            task_id=task_id,
            title=title,
            task_type="email",
            description=description,
            depends_on=depends_on,
            enabled=enabled,
            smtp_host=_expand_string(str(raw_task.get("smtp_host") or "localhost"), env_values),
            smtp_port=int(raw_task.get("smtp_port") or 587),
            smtp_user=_expand_string(str(raw_task.get("smtp_user", "")), env_values) or None,
            smtp_password=_expand_string(str(raw_task.get("smtp_password", "")), env_values) or None,
            email_to=tuple(str(item) for item in _expand_value(list(raw_task.get("to") or []), env_values)),
            email_subject=_expand_string(str(raw_task.get("subject") or "Piply Notification"), env_values),
            email_body=_expand_string(str(raw_task.get("body") or ""), env_values),
            env=task_env,
        )

    host = raw_task.get("host")
    if not host:
        raise ConfigError(f"Pipeline '{pipeline_id}' task '{task_id}' requires host for ssh tasks")

    return TaskDefinition(
        task_id=task_id,
        title=title,
        task_type="ssh",
        description=description,
        depends_on=depends_on,
        enabled=enabled,
        host=_expand_string(str(host), env_values),
        user=None if raw_task.get("user") is None else _expand_string(str(raw_task.get("user")), env_values),
        port=int(raw_task.get("port", 22)),
        key_file=_resolve_path(raw_task.get("key_file"), workspace, env_values),
        command=None if raw_task.get("command") is None else _expand_string(str(raw_task.get("command")), env_values),
        ssh_binary=_expand_string(str(raw_task.get("ssh_binary") or "ssh"), env_values),
        connect_timeout=int(raw_task.get("connect_timeout", 8)),
        env=task_env,
    )


def load_project(
    config_path: str | Path | None = None,
    *,
    default_max_parallel_tasks: int | None = None,
) -> ProjectDefinition:
    """Load and validate a Piply project definition."""
    path = Path(config_path).resolve() if config_path else discover_config()
    if not path.exists():
        raise ConfigError(f"Config file '{path}' does not exist")

    try:
        raw_data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Could not parse '{path.name}': {exc}") from exc

    if not isinstance(raw_data, dict):
        raise ConfigError("The root of the config file must be a mapping")

    settings = load_settings(path)
    env_values = settings.env_values
    effective_default_max_parallel_tasks = (
        default_max_parallel_tasks or settings.default_max_parallel_tasks
    )

    defaults = _ensure_mapping(raw_data.get("defaults"), "defaults")
    timezone_name = _expand_string(str(raw_data.get("timezone") or defaults.get("timezone") or "UTC"), env_values)
    workspace = _resolve_path(str(raw_data.get("workspace", ".")), path.parent, env_values) or path.parent
    if not workspace.exists():
        raise ConfigError(f"Configured workspace does not exist: {workspace}")

    default_python = _expand_string(str(defaults.get("python") or sys.executable), env_values)
    default_env = {
        str(key): _expand_string(str(value), env_values)
        for key, value in _ensure_mapping(defaults.get("env"), "defaults.env").items()
    }

    raw_pipelines = raw_data["pipelines"] if "pipelines" in raw_data else raw_data.get("jobs")
    if raw_pipelines is None:
        raise ConfigError("Config must define a 'pipelines' mapping")
    if not isinstance(raw_pipelines, dict):
        raise ConfigError("Config 'pipelines' must be a mapping")

    pipelines: dict[str, PipelineDefinition] = {}
    for pipeline_id, raw_pipeline in raw_pipelines.items():
        if not isinstance(raw_pipeline, dict):
            raise ConfigError(f"Pipeline '{pipeline_id}' must be a mapping")

        schedule_timezone = _expand_string(str(raw_pipeline.get("timezone") or timezone_name), env_values)
        try:
            schedule = _parse_schedule(raw_pipeline.get("schedule"), schedule_timezone)
        except ScheduleError as exc:
            raise ConfigError(
                f"Pipeline '{pipeline_id}' has an invalid schedule: {exc}"
            ) from exc

        max_parallel_tasks = _parse_execution(
            raw_pipeline.get("execution"),
            pipeline_id,
            default_max_parallel_tasks=effective_default_max_parallel_tasks,
            explicit_max_parallel_tasks=raw_pipeline.get("max_parallel_tasks"),
        )
        retry_policy = _parse_retry_policy(raw_pipeline.get("retry"), pipeline_id)

        title = _expand_string(str(raw_pipeline.get("title") or raw_pipeline.get("name") or pipeline_id), env_values)
        description = _expand_string(str(raw_pipeline.get("description") or ""), env_values)
        tags = tuple(
            _expand_string(str(tag), env_values)
            for tag in _ensure_list(raw_pipeline.get("tags"), f"Pipeline '{pipeline_id}' tags")
        )
        enabled = bool(raw_pipeline.get("enabled", True))
        max_concurrent_runs = int(raw_pipeline.get("max_concurrent_runs", 1))
        if max_concurrent_runs < 1:
            raise ConfigError(
                f"Pipeline '{pipeline_id}' must have max_concurrent_runs greater than zero"
            )

        pipeline_env = dict(default_env)
        pipeline_env.update(
            {
                str(key): _expand_string(str(value), env_values)
                for key, value in _ensure_mapping(
                    raw_pipeline.get("env"),
                    f"Pipeline '{pipeline_id}' env",
                ).items()
            }
        )

        raw_tasks = raw_pipeline.get("tasks")
        if raw_tasks is None:
            raw_tasks = _build_single_task_pipeline(raw_pipeline)
        if not isinstance(raw_tasks, dict) or not raw_tasks:
            raise ConfigError(f"Pipeline '{pipeline_id}' must define a non-empty tasks mapping")

        tasks: dict[str, TaskDefinition] = {}
        for task_id, raw_task in raw_tasks.items():
            if not isinstance(raw_task, dict):
                raise ConfigError(f"Pipeline '{pipeline_id}' task '{task_id}' must be a mapping")
            tasks[task_id] = _parse_task(
                pipeline_id,
                task_id,
                raw_task,
                workspace=workspace,
                default_python=default_python,
                inherited_env=pipeline_env,
                env_values=env_values,
            )

        _validate_task_graph(pipeline_id, tasks)
        parallelizable = _detect_parallelism(tasks)
        sensors = _parse_sensors(
            raw_pipeline.get("sensors"),
            pipeline_id,
            workspace=workspace,
            task_ids=set(tasks),
            env_values=env_values,
        )

        triggers_on_success = tuple(
            _expand_string(str(item), env_values)
            for item in _ensure_list(
                raw_pipeline.get("triggers_on_success"),
                f"Pipeline '{pipeline_id}' triggers_on_success",
            )
        )
        if pipeline_id in triggers_on_success:
            raise ConfigError(f"Pipeline '{pipeline_id}' cannot trigger itself on success")

        pipelines[pipeline_id] = PipelineDefinition(
            pipeline_id=pipeline_id,
            title=title,
            description=description,
            tasks=tasks,
            tags=tags,
            schedule=schedule,
            enabled=enabled,
            max_concurrent_runs=max_concurrent_runs,
            parallelizable=parallelizable,
            max_parallel_tasks=max_parallel_tasks,
            triggers_on_success=triggers_on_success,
            retry_policy=retry_policy,
            sensors=sensors,
        )

    _validate_pipeline_trigger_graph(pipelines)

    return ProjectDefinition(
        version=str(raw_data.get("version", "1")),
        title=str(raw_data.get("title") or path.parent.name),
        config_path=path,
        workspace=workspace,
        default_python=default_python,
        timezone_name=timezone_name,
        pipelines=pipelines,
    )
