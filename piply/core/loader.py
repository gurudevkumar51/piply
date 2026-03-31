"""Project loader for Piply YAML configuration files."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any

import yaml

from piply.settings import load_settings

from .models import PipelineDefinition, ProjectDefinition, TaskDefinition
from .scheduling import CronSchedule, IntervalSchedule, ScheduleError, parse_interval


class ConfigError(ValueError):
    """Raised when the Piply YAML configuration is invalid."""


TASK_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


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


def _expand_string(value: str) -> str:
    """Expand environment variables and user-home markers in strings."""
    return os.path.expandvars(os.path.expanduser(value))


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


def _resolve_path(value: str | None, base_dir: Path) -> Path | None:
    """Resolve a possibly relative path against the project workspace."""
    if value is None:
        return None
    expanded = Path(_expand_string(value))
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


def _parse_depends_on(raw_value: Any, label: str) -> tuple[str, ...]:
    """Parse and validate task dependency ids."""
    depends_on = tuple(str(item) for item in _ensure_list(raw_value, label))
    for dependency in depends_on:
        if not TASK_ID_PATTERN.match(dependency):
            raise ConfigError(f"{label} contains an invalid task id '{dependency}'")
    return depends_on


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
) -> TaskDefinition:
    """Parse one task block into a runtime definition."""
    if not TASK_ID_PATTERN.match(task_id):
        raise ConfigError(
            f"Pipeline '{pipeline_id}' contains invalid task id '{task_id}'. Use letters, numbers, _ or -."
        )

    task_type = str(raw_task.get("type") or "python").lower()
    if task_type not in {"python", "python_call", "cli", "api", "ssh", "email", "webhook"}:
        raise ConfigError(
            f"Pipeline '{pipeline_id}' task '{task_id}' uses unsupported type '{task_type}'"
        )

    title = str(raw_task.get("title") or raw_task.get("name") or task_id.replace("_", " ").title())
    description = str(raw_task.get("description") or "")
    depends_on = _parse_depends_on(
        raw_task.get("depends_on"),
        f"Pipeline '{pipeline_id}' task '{task_id}' depends_on",
    )
    enabled = bool(raw_task.get("enabled", True))

    task_env = dict(inherited_env)
    task_env.update(
        {
            str(key): _expand_string(str(value))
            for key, value in _ensure_mapping(
                raw_task.get("env"),
                f"Pipeline '{pipeline_id}' task '{task_id}' env",
            ).items()
        }
    )

    if task_type == "python" or task_type == "python_call":
        function_name = raw_task.get("function") or raw_task.get("method")
        callable_ref = raw_task.get("call") or raw_task.get("callable")
        
        if function_name or callable_ref:
            if callable_ref is None and function_name is not None:
                if raw_task.get("module") is not None:
                    callable_ref = f"{raw_task['module']}:{function_name}"
                elif raw_task.get("path") is not None or raw_task.get("script") is not None:
                    callable_path = _resolve_path(str(raw_task.get("path") or raw_task.get("script")), workspace)
                    if callable_path is not None:
                        callable_ref = f"{callable_path}::{function_name}"
            if callable_ref is None:
                raise ConfigError(
                    f"Pipeline '{pipeline_id}' task '{task_id}' requires call/module+function/path+function for python callable tasks"
                )
            callable_text = _expand_string(str(callable_ref))
            if "::" in callable_text:
                raw_path, callable_name = callable_text.split("::", 1)
                resolved_callable_path = _resolve_path(raw_path, workspace)
                if resolved_callable_path is None:
                    raise ConfigError(
                        f"Pipeline '{pipeline_id}' task '{task_id}' callable path could not be resolved"
                    )
                callable_text = f"{resolved_callable_path}::{callable_name}"
            raw_args = raw_task.get("args", [])
            raw_kwargs = _ensure_mapping(
                raw_task.get("kwargs"),
                f"Pipeline '{pipeline_id}' task '{task_id}' kwargs",
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
                cwd=_resolve_path(raw_task.get("cwd"), workspace) or workspace,
                env=task_env,
            )
        else:
            path_value = raw_task.get("path") or raw_task.get("script")
            if not path_value:
                raise ConfigError(
                    f"Pipeline '{pipeline_id}' task '{task_id}' requires path/script for python tasks"
                )
            path = _resolve_path(str(path_value), workspace)
            if path is None or not path.exists():
                raise ConfigError(
                    f"Pipeline '{pipeline_id}' task '{task_id}' points to a missing script: {path_value}"
                )
            raw_args = raw_task.get("args", [])
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
                python=str(raw_task.get("python") or default_python),
                args=tuple(str(item) for item in raw_args),
                cwd=_resolve_path(raw_task.get("cwd"), workspace),
                env=task_env,
            )

    if task_type == "cli":
        command = raw_task.get("command")
        if not command:
            raise ConfigError(
                f"Pipeline '{pipeline_id}' task '{task_id}' requires command for cli tasks"
            )
        return TaskDefinition(
            task_id=task_id,
            title=title,
            task_type="cli",
            description=description,
            depends_on=depends_on,
            enabled=enabled,
            command=str(command),
            cwd=_resolve_path(raw_task.get("cwd"), workspace),
            env=task_env,
        )

    if task_type == "api" or task_type == "webhook":
        url = raw_task.get("url")
        if not url:
            raise ConfigError(
                f"Pipeline '{pipeline_id}' task '{task_id}' requires url for {task_type} tasks"
            )
        headers = {
            str(key): _expand_string(str(value))
            for key, value in _ensure_mapping(
                raw_task.get("headers"),
                f"Pipeline '{pipeline_id}' task '{task_id}' headers",
            ).items()
        }
        token = raw_task.get("token")
        
        # Determine default method
        method = str(raw_task.get("method", "POST" if task_type == "webhook" else "GET")).upper()
        
        return TaskDefinition(
            task_id=task_id,
            title=title,
            task_type=task_type,
            description=description,
            depends_on=depends_on,
            enabled=enabled,
            url=_expand_string(str(url)),
            method=method,
            headers=headers,
            body=None if raw_task.get("body") is None else str(raw_task.get("body")),
            token=_expand_string(str(token)) if token is not None else None,
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
            smtp_host=str(raw_task.get("smtp_host") or "localhost"),
            smtp_port=int(raw_task.get("smtp_port") or 587),
            smtp_user=_expand_string(str(raw_task.get("smtp_user", ""))) or None,
            smtp_password=_expand_string(str(raw_task.get("smtp_password", ""))) or None,
            email_to=tuple(str(item) for item in list(raw_task.get("to") or [])),
            email_subject=str(raw_task.get("subject") or "Piply Notification"),
            email_body=str(raw_task.get("body") or ""),
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
        host=str(host),
        user=None if raw_task.get("user") is None else str(raw_task.get("user")),
        port=int(raw_task.get("port", 22)),
        key_file=_resolve_path(raw_task.get("key_file"), workspace),
        command=None if raw_task.get("command") is None else str(raw_task.get("command")),
        ssh_binary=str(raw_task.get("ssh_binary") or "ssh"),
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
    effective_default_max_parallel_tasks = (
        default_max_parallel_tasks or settings.default_max_parallel_tasks
    )

    defaults = _ensure_mapping(raw_data.get("defaults"), "defaults")
    timezone_name = str(raw_data.get("timezone") or defaults.get("timezone") or "UTC")
    workspace = _resolve_path(str(raw_data.get("workspace", ".")), path.parent) or path.parent
    if not workspace.exists():
        raise ConfigError(f"Configured workspace does not exist: {workspace}")

    default_python = str(defaults.get("python") or sys.executable)
    default_env = {
        str(key): _expand_string(str(value))
        for key, value in _ensure_mapping(defaults.get("env"), "defaults.env").items()
    }

    raw_pipelines = raw_data.get("pipelines") or raw_data.get("jobs")
    if not isinstance(raw_pipelines, dict) or not raw_pipelines:
        raise ConfigError("Config must define a non-empty 'pipelines' mapping")

    pipelines: dict[str, PipelineDefinition] = {}
    for pipeline_id, raw_pipeline in raw_pipelines.items():
        if not isinstance(raw_pipeline, dict):
            raise ConfigError(f"Pipeline '{pipeline_id}' must be a mapping")

        schedule_timezone = str(raw_pipeline.get("timezone") or timezone_name)
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

        title = str(raw_pipeline.get("title") or raw_pipeline.get("name") or pipeline_id)
        description = str(raw_pipeline.get("description") or "")
        tags = tuple(
            str(tag)
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
                str(key): _expand_string(str(value))
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
            )

        _validate_task_graph(pipeline_id, tasks)
        parallelizable = _detect_parallelism(tasks)

        triggers_on_success = tuple(
            str(item)
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
