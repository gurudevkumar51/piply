"""Project loader for Piply YAML configuration files."""

from __future__ import annotations

import copy
import os
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

from piply.pipeline.expander import (
    EntityMap,
    ExpansionError,
    expand_task_templates,
    merge_entity_maps,
    parse_entity_map,
)
from piply.settings import load_settings

from .conditions import ConditionError, evaluate_value
from .models import (
    PipelineDefinition,
    ProjectDefinition,
    RetryPolicy,
    SensorDefinition,
    TaskDefinition,
    UpstreamFailureBehavior,
)
from .notifications import (
    NotificationError,
    parse_notifications,
    parse_pipeline_notifications,
)
from .scheduling import CronSchedule, IntervalSchedule, ScheduleError, parse_interval
from .secrets import load_secret_values, secret_env_aliases


class ConfigError(ValueError):
    """Raised when the Piply YAML configuration is invalid."""


TASK_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
ENV_TOKEN_PATTERN = re.compile(r"\$(\w+)|\$\{([^}]+)\}|%([^%]+)%")
BRACE_TOKEN_PATTERN = re.compile(r"(?<!\{)\{([A-Za-z_][A-Za-z0-9_]*)\}(?!\})")


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


def _read_yaml_document(path: Path) -> dict[str, Any]:
    """Read one YAML config file, naming the file in any error."""
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Could not parse '{path.name}': {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"The root of '{path.name}' must be a mapping")
    return data


#: Blocks whose *members* may be spread across files. Splitting the config is
#: only useful if `pipelines:` can appear in several files, but merging two
#: definitions of the same pipeline is never what anyone means — so recursion
#: stops one level inside each of these, and a repeated member is an error.
_MERGEABLE_PATHS = frozenset(
    {
        "",
        "pipelines",
        "jobs",
        "pipeline_templates",
        "pipeline_deployments",
        "notifications",
        "notifications.teams",
        "notifications.groups",
        "connections",
        "entities",
        "variables",
        "defaults",
        "secrets",
    }
)


#: Patterns where `*` matches exactly one path segment. Splitting a pipeline's
#: *blocks* across files is the point of `piply_sensor.yaml` — tasks in one file,
#: sensors in another. Splitting a single block is not: two files both defining
#: `tasks` for one pipeline stays an error, because merging them has no obvious
#: reading.
_MERGEABLE_PATTERNS = ("pipelines.*", "jobs.*")


def _is_mergeable(dotted: str) -> bool:
    """Whether members below this path may come from different files."""
    if dotted in _MERGEABLE_PATHS:
        return True
    parts = dotted.split(".")
    for pattern in _MERGEABLE_PATTERNS:
        expected = pattern.split(".")
        if len(expected) == len(parts) and all(
            want == "*" or want == have for want, have in zip(expected, parts, strict=True)
        ):
            return True
    return False


def _record_origins(value: Any, source: Path, origins: dict[str, Path], dotted: str) -> None:
    """Remember which file supplied a key, and its members."""
    origins[dotted] = source
    if _is_mergeable(dotted) and isinstance(value, dict):
        for key, child in value.items():
            _record_origins(child, source, origins, f"{dotted}.{key}")


def _merge_included_document(
    base: dict[str, Any],
    incoming: dict[str, Any],
    source: Path,
    origins: dict[str, Path],
    root: Path,
    prefix: str = "",
) -> None:
    """Merge one included file into the combined config.

    A key defined in two files is an **error**, never last-wins: silently
    preferring one file would mean editing a pipeline and watching nothing
    change, which is exactly the confusion splitting the config is meant to
    remove. The error names both files and points at the pipeline, not at some
    leaf field deep inside it.
    """
    for key, value in incoming.items():
        dotted = f"{prefix}{key}"
        if key not in base:
            base[key] = value
            # Record the members too, not just the container, or a later
            # duplicate would be blamed on the root file instead of this one.
            _record_origins(value, source, origins, dotted)
            continue
        existing = base[key]
        if _is_mergeable(dotted) and isinstance(existing, dict) and isinstance(value, dict):
            _merge_included_document(existing, value, source, origins, root, prefix=f"{dotted}.")
            continue
        # Absent from `origins` means it came from the root file.
        first = origins.get(dotted, root)
        raise ConfigError(f"'{dotted}' is defined in more than one config file: '{first.name}' and '{source.name}'")


def load_raw_config(path: Path) -> tuple[dict[str, Any], list[Path]]:
    """Read the root config plus anything it includes.

    A config with no `include:` behaves exactly as it always has — the feature
    is purely additive. Returns the merged mapping and every file that fed it,
    so the caller can watch them all for changes.
    """
    raw_data = _read_yaml_document(path)
    patterns = raw_data.pop("include", None)
    if patterns in (None, "", False):
        return raw_data, [path]

    if isinstance(patterns, str):
        patterns = [patterns]
    if not isinstance(patterns, list):
        raise ConfigError("'include' must be a list of file paths or glob patterns")

    sources = [path]
    origins: dict[str, Path] = {}
    for pattern in patterns:
        text = str(pattern).strip()
        if not text:
            continue
        # Sorted so the same set of files always merges in the same order, which
        # keeps duplicate errors reproducible rather than filesystem-dependent.
        matches = sorted(path.parent.glob(text))
        if not matches:
            raise ConfigError(f"include pattern '{text}' matched no files next to '{path.name}'")
        for match in matches:
            resolved = match.resolve()
            if resolved == path.resolve() or resolved in sources:
                continue
            document = _read_yaml_document(resolved)
            if "include" in document:
                raise ConfigError(f"'{resolved.name}' uses 'include', which only the root config file may do.")
            _merge_included_document(raw_data, document, resolved, origins, path)
            sources.append(resolved)

    return raw_data, sources


def _expand_string(value: str, env_values: dict[str, str] | None = None) -> str:
    """Expand environment variables, config variables, and user-home markers."""
    merged_env = dict(os.environ)
    merged_env.update(env_values or {})

    def replace_env(match: re.Match[str]) -> str:
        name = match.group(1) or match.group(2) or match.group(3) or ""
        return merged_env.get(name, match.group(0))

    def replace_brace(match: re.Match[str]) -> str:
        name = match.group(1)
        return merged_env.get(name, match.group(0))

    expanded = ENV_TOKEN_PATTERN.sub(replace_env, os.path.expanduser(value))
    return BRACE_TOKEN_PATTERN.sub(replace_brace, expanded)


def _expand_env_only(value: str, env_values: dict[str, str] | None = None) -> str:
    """Expand environment tokens but leave ``{name}`` placeholders in place.

    ``run_if`` expressions are resolved at execution time so that a value can be
    substituted as a quoted literal. Expanding the braces here would turn
    ``"{report} == 'payment'"`` into ``payment == 'payment'``, where ``payment``
    parses as a bare name instead of a string.
    """
    merged_env = dict(os.environ)
    merged_env.update(env_values or {})

    def replace_env(match: re.Match[str]) -> str:
        name = match.group(1) or match.group(2) or match.group(3) or ""
        return merged_env.get(name, match.group(0))

    return ENV_TOKEN_PATTERN.sub(replace_env, value)


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


def _deep_merge_mapping(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge deployment overrides into a pipeline template."""
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if key in {"template", "pipeline_template"}:
            continue
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_mapping(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _normalize_pipeline_definitions(raw_data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return runnable pipeline definitions from simple and advanced YAML shapes."""
    raw_simple_pipelines = raw_data["pipelines"] if "pipelines" in raw_data else raw_data.get("jobs")
    pipeline_definitions: dict[str, dict[str, Any]] = {}

    if raw_simple_pipelines is not None:
        simple_pipelines = _ensure_mapping(raw_simple_pipelines, "pipelines")
        for pipeline_id, raw_pipeline in simple_pipelines.items():
            if not isinstance(raw_pipeline, dict):
                raise ConfigError(f"Pipeline '{pipeline_id}' must be a mapping")
            pipeline_definitions[str(pipeline_id)] = copy.deepcopy(raw_pipeline)

    raw_templates = _ensure_mapping(raw_data.get("pipeline_templates"), "pipeline_templates")
    deployments = _ensure_mapping(raw_data.get("pipeline_deployments"), "pipeline_deployments")
    templates: dict[str, dict[str, Any]] = {}
    for template_id, raw_template in raw_templates.items():
        if not isinstance(raw_template, dict):
            raise ConfigError(f"Pipeline template '{template_id}' must be a mapping")
        templates[str(template_id)] = copy.deepcopy(raw_template)

    for deployment_id, raw_deployment in deployments.items():
        deployment_key = str(deployment_id)
        if not isinstance(raw_deployment, dict):
            raise ConfigError(f"Pipeline deployment '{deployment_key}' must be a mapping")
        template_id = raw_deployment.get("template") or raw_deployment.get("pipeline_template")
        if not template_id:
            raise ConfigError(f"Pipeline deployment '{deployment_key}' must define template")
        if str(template_id) not in templates:
            raise ConfigError(f"Pipeline deployment '{deployment_key}' references unknown template '{template_id}'")
        if deployment_key in pipeline_definitions:
            raise ConfigError(f"Pipeline deployment '{deployment_key}' conflicts with an existing pipeline id")

        expanded = _deep_merge_mapping(templates[str(template_id)], raw_deployment)
        expanded["_template_id"] = str(template_id)
        expanded["_deployment_id"] = deployment_key
        expanded.setdefault("title", deployment_key.replace("_", " ").replace("-", " ").title())

        deployment_variables = _ensure_mapping(
            expanded.get("variables"), f"Pipeline deployment '{deployment_key}' variables"
        )
        deployment_variables = dict(deployment_variables)
        if raw_deployment.get("tenant") is not None:
            deployment_variables.setdefault("tenant", str(raw_deployment["tenant"]))
            deployment_variables.setdefault("tenant_id", str(raw_deployment["tenant"]))
        if raw_deployment.get("tenant_id") is not None:
            deployment_variables.setdefault("tenant", str(raw_deployment["tenant_id"]))
            deployment_variables.setdefault("tenant_id", str(raw_deployment["tenant_id"]))
        if raw_deployment.get("environment") is not None:
            deployment_variables.setdefault("environment", str(raw_deployment["environment"]))
        expanded["variables"] = deployment_variables

        pipeline_definitions[deployment_key] = expanded

    if not pipeline_definitions:
        raise ConfigError("Config must define a 'pipelines' mapping or 'pipeline_deployments' mapping")
    return pipeline_definitions


def _render_variable_value(value: Any) -> str:
    """Render a resolved variable as the string the rest of the loader expects."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)


def _parse_variables(raw_value: Any, label: str, env_values: dict[str, str]) -> dict[str, str]:
    """Parse reusable config variables and allow earlier variables in later values.

    A value may be conditional, either inline (``true if env == "dev" else false``)
    or as an explicit ``{if:, then:, else:}`` mapping. Conditions see the
    environment plus every variable defined before them, so ordering matters the
    same way it already does for plain interpolation.
    """
    raw_variables = _ensure_mapping(raw_value, label)
    variables: dict[str, str] = {}
    scoped_values = dict(env_values)
    for raw_key, raw_variable in raw_variables.items():
        key = str(raw_key)
        context = scoped_values | variables
        try:
            resolved = evaluate_value(raw_variable, context, f"{label}.{key}")
        except ConditionError as exc:
            raise ConfigError(str(exc)) from exc
        variables[key] = _expand_string(_render_variable_value(resolved), context)
    return variables


def _unused_global_entity_warnings(
    pipeline_id: str,
    *,
    raw_tasks: Any,
    root_entities: EntityMap,
    own_entities: EntityMap,
) -> list[str]:
    """Warn when a project-level entity expands a pipeline that never uses it.

    A top-level `entities:` block applies to *every* pipeline. That is fine for
    the pipeline it was written for and silently wrong for the rest: a nightly
    cleanup job beside one entity-driven pipeline quietly runs three times, and
    a summary email is sent three times, because the generated tasks are
    identical apart from their ids. Nothing fails, so nobody notices.
    """
    inherited = [name for name in root_entities if name not in own_entities]
    if not inherited or not isinstance(raw_tasks, dict) or not raw_tasks:
        return []

    # A task opting out entirely, or selecting dimensions by name, is a
    # deliberate choice and never needs warning about.
    referenced = repr(raw_tasks)
    warnings: list[str] = []
    for name in inherited:
        if f"{{{name}}}" in referenced or f"{{{name}_value}}" in referenced:
            continue
        if any(isinstance(task, dict) and isinstance(task.get("entities"), list | bool) for task in raw_tasks.values()):
            continue
        count = len(root_entities[name])
        warnings.append(
            f"Pipeline '{pipeline_id}' is expanded {count}x by the project-level entity "
            f"'{name}', but no task uses {{{name}}}. That runs identical tasks {count} times. "
            f"Move the entity onto the pipelines that need it, or set 'entities: false' here."
        )
    return warnings


def _parse_entities(raw_value: Any, label: str, env_values: dict[str, str]):
    """Parse entities after normal Piply string interpolation."""
    try:
        return parse_entity_map(_expand_value(raw_value, env_values), label)
    except ExpansionError as exc:
        raise ConfigError(str(exc)) from exc


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
            raw_value.get(
                "max_parallel_tasks",
                raw_value.get("workers", default_max_parallel_tasks),
            )
        )
    else:
        raise ConfigError(f"Pipeline '{pipeline_id}' execution must be an int, string, or mapping")

    if mode not in {None, "sequential", "parallel"}:
        raise ConfigError(f"Pipeline '{pipeline_id}' execution mode must be 'sequential' or 'parallel'")
    if max_parallel_tasks < 1:
        raise ConfigError(f"Pipeline '{pipeline_id}' max_parallel_tasks must be greater than zero")
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


def _parse_duration_seconds(raw_value: Any, label: str) -> int | None:
    """Parse a timeout value from seconds or a compact duration string."""
    if raw_value in (None, "", False):
        return None
    if isinstance(raw_value, bool):
        raise ConfigError(f"{label} must be a positive duration, not a boolean")
    if isinstance(raw_value, int | float):
        seconds = int(raw_value)
    else:
        text = str(raw_value).strip().lower()
        match = re.fullmatch(r"(\d+)(ms|s|m|h)?", text)
        if not match:
            raise ConfigError(f"{label} must be a duration such as 30, 30s, 5m, or 1h")
        value = int(match.group(1))
        unit = match.group(2) or "s"
        if unit == "ms":
            seconds = max(1, round(value / 1000))
        elif unit == "m":
            seconds = value * 60
        elif unit == "h":
            seconds = value * 3600
        else:
            seconds = value
    if seconds <= 0:
        raise ConfigError(f"{label} must be greater than zero")
    return seconds


PRIORITY_ALIASES = {
    "lowest": -2,
    "low": -1,
    "normal": 0,
    "default": 0,
    "medium": 0,
    "high": 1,
    "higher": 2,
    "highest": 3,
    "critical": 5,
}


def _parse_priority(raw_value: Any, label: str) -> int:
    """Parse an explicit numeric priority, a named level, or star shorthand."""
    if raw_value in (None, "", False):
        return 0
    if isinstance(raw_value, bool):
        raise ConfigError(f"{label} priority must be a number, a named level, or star shorthand")
    if isinstance(raw_value, int | float):
        return int(raw_value)
    text = str(raw_value).strip()
    if text and set(text) == {"*"}:
        return len(text)
    if text.lower() in PRIORITY_ALIASES:
        return PRIORITY_ALIASES[text.lower()]
    try:
        return int(text)
    except ValueError as exc:
        raise ConfigError(
            f"{label} priority must be a number, a named level "
            f"({', '.join(sorted(PRIORITY_ALIASES))}), or star shorthand like ***"
        ) from exc


def _normalize_task_priority_suffixes(raw_tasks: dict[str, Any], pipeline_id: str) -> dict[str, Any]:
    """Strip ``*`` suffixes from task ids and fold them into an explicit priority.

    ``extract***`` is shorthand for a task named ``extract`` with priority 3. The
    id is normalized here, before dependency validation and entity expansion, so
    the rest of the loader only ever sees the clean id. An explicit ``priority``
    key on the task always wins over the suffix.
    """
    normalized: dict[str, Any] = {}
    for raw_task_id, raw_task in raw_tasks.items():
        task_id = str(raw_task_id)
        stripped = task_id.rstrip("*")
        star_count = len(task_id) - len(stripped)
        if star_count == 0:
            normalized[task_id] = raw_task
            continue
        if not stripped:
            raise ConfigError(f"Pipeline '{pipeline_id}' has a task id made only of '*' characters")
        if stripped in normalized:
            raise ConfigError(
                f"Pipeline '{pipeline_id}' task '{stripped}' is declared twice once the '*' priority suffix is removed"
            )
        if isinstance(raw_task, dict):
            raw_task = copy.deepcopy(raw_task)
            raw_task.setdefault("priority", star_count)
        normalized[stripped] = raw_task
    return normalized


def _parse_notify(raw_value: Any, pipeline_id: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Parse a pipeline ``notify`` block into failure and success recipients.

    A bare list is the common case and means "on failure", because that is what
    people actually want to be told about.
    """
    if raw_value in (None, "", False):
        return (), ()

    def _emails(value: Any, label: str) -> tuple[str, ...]:
        if value in (None, "", False):
            return ()
        items = value if isinstance(value, list) else [value]
        addresses: list[str] = []
        for item in items:
            address = str(item).strip()
            if not address:
                continue
            if "@" not in address:
                raise ConfigError(f"{label} contains an invalid email address '{address}'")
            addresses.append(address)
        return tuple(addresses)

    label = f"Pipeline '{pipeline_id}' notify"
    if isinstance(raw_value, list | str):
        return _emails(raw_value, label), ()
    if not isinstance(raw_value, dict):
        raise ConfigError(f"{label} must be a list of addresses or a mapping")
    return (
        _emails(raw_value.get("on_failure"), f"{label}.on_failure"),
        _emails(raw_value.get("on_success"), f"{label}.on_success"),
    )


def _parse_depends_on(raw_value: Any, label: str) -> tuple[str, ...]:
    """Parse and validate task dependency ids."""
    depends_on = tuple(str(item) for item in _ensure_list(raw_value, label))
    for dependency in depends_on:
        if not TASK_ID_PATTERN.match(dependency):
            raise ConfigError(f"{label} contains an invalid task id '{dependency}'")
    return depends_on


def _parse_upstream_failure_behavior(raw_task: dict[str, Any], label: str) -> UpstreamFailureBehavior:
    """Parse task-level behavior when one or more upstream dependencies fail."""
    fail_flag = bool(raw_task.get("fail_if_upstream_failed", False))
    continue_flag = bool(raw_task.get("continue_if_upstream_failed", False))
    if fail_flag and continue_flag:
        raise ConfigError(f"{label} cannot set both fail_if_upstream_failed and continue_if_upstream_failed")
    if fail_flag:
        return "fail"
    if continue_flag:
        return "continue"

    raw_value = raw_task.get("on_upstream_failure")
    if raw_value in (None, "", False):
        return "skip"
    behavior = str(raw_value).strip().lower()
    if behavior not in {"skip", "fail", "continue"}:
        raise ConfigError(f"{label} on_upstream_failure must be one of: skip, fail, continue")
    return behavior  # type: ignore[return-value]


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


def _resolve_connection_value(
    raw_value: Any,
    *,
    env_values: dict[str, str],
    connections: dict[str, str],
) -> str | None:
    """Resolve a SQL connection value from env, secrets, or global connections."""
    if raw_value in (None, "", False):
        return None
    if isinstance(raw_value, dict):
        for key in ("connection", "url", "database_url", "dsn"):
            if raw_value.get(key) is not None:
                return _expand_string(str(raw_value[key]), env_values)
        if raw_value.get("env") is not None:
            return env_values.get(str(raw_value["env"]))
        if raw_value.get("secret") is not None:
            secret_key = str(raw_value["secret"])
            return env_values.get(f"secret:{secret_key}") or env_values.get(f"secret.{secret_key}")
        if raw_value.get("ref") is not None:
            return connections.get(str(raw_value["ref"]))
        return None

    value = _expand_string(str(raw_value), env_values)
    if value.startswith("@"):
        return connections.get(value[1:])
    return value


def _parse_connections(raw_value: Any, env_values: dict[str, str]) -> dict[str, str]:
    """Parse root-level reusable connection strings."""
    if raw_value in (None, "", False):
        return {}
    if not isinstance(raw_value, dict):
        raise ConfigError("connections must be a mapping")

    connections: dict[str, str] = {}
    for connection_id, raw_connection in raw_value.items():
        connection_value = _resolve_connection_value(
            raw_connection,
            env_values=env_values,
            connections=connections,
        )
        if connection_value:
            connections[str(connection_id)] = connection_value
    return connections


def _normalize_sensor_connection(connection: str | None, workspace: Path) -> str | None:
    """Resolve relative sqlite connection strings against the project workspace."""
    if not connection:
        return connection
    parsed = urlparse(connection)
    if parsed.scheme.lower() not in {"sqlite", "sqlite3", "sqlite+pysqlite"}:
        return connection
    raw_path = parsed.path or parsed.netloc
    if not raw_path:
        return connection
    if raw_path in {":memory:", "/:memory:"}:
        return f"{parsed.scheme}:///:memory:"
    if raw_path.startswith("/") and not raw_path.startswith("//"):
        raw_path = raw_path[1:]
    if raw_path.startswith("//"):
        raw_path = raw_path[1:]
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = (workspace / candidate).resolve()
    return f"{parsed.scheme}:///{candidate.as_posix()}"


def _parse_sensors(
    raw_value: Any,
    pipeline_id: str,
    *,
    workspace: Path,
    task_ids: set[str],
    env_values: dict[str, str],
    connections: dict[str, str],
) -> dict[str, SensorDefinition]:
    """Parse configured sensor definitions for one pipeline."""
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
        if sensor_type not in {"file_sensor", "sql_sensor", "api_sensor"}:
            raise ConfigError(f"Pipeline '{pipeline_id}' sensor '{sensor_id}' uses unsupported type '{sensor_type}'")

        task_id = raw_sensor.get("task_id")
        if task_id is not None and str(task_id) not in task_ids:
            raise ConfigError(f"Pipeline '{pipeline_id}' sensor '{sensor_id}' points to unknown task '{task_id}'")

        title = str(raw_sensor.get("title") or raw_sensor.get("name") or sensor_id.replace("_", " ").title())
        enabled = bool(raw_sensor.get("enabled", True))
        ignore_existing = bool(raw_sensor.get("ignore_existing", True))

        if sensor_type == "file_sensor":
            path_value = raw_sensor.get("path")
            if not path_value:
                raise ConfigError(f"Pipeline '{pipeline_id}' sensor '{sensor_id}' requires path for file_sensor")
            expanded_path = _expand_string(str(path_value), env_values)
            ssh_host = (
                None if raw_sensor.get("host") is None else _expand_string(str(raw_sensor.get("host")), env_values)
            )
            ssh_user = (
                None if raw_sensor.get("user") is None else _expand_string(str(raw_sensor.get("user")), env_values)
            )
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

        if sensor_type == "api_sensor":
            url = raw_sensor.get("url") or raw_sensor.get("endpoint")
            if not url:
                raise ConfigError(f"Pipeline '{pipeline_id}' sensor '{sensor_id}' requires url for api_sensor")
            raw_headers = _ensure_mapping(
                raw_sensor.get("headers"),
                f"Pipeline '{pipeline_id}' sensor '{sensor_id}' headers",
            )
            raw_expected = raw_sensor.get("expected_status", [200, 201, 202, 204])
            expected_status = tuple(int(item) for item in _ensure_list(raw_expected, "expected_status"))
            sensors[sensor_id] = SensorDefinition(
                sensor_id=sensor_id,
                sensor_type="api_sensor",
                title=title,
                enabled=enabled,
                url=_expand_string(str(url), env_values),
                method=_expand_string(str(raw_sensor.get("method") or "GET"), env_values).upper(),
                headers={str(key): _expand_string(str(value), env_values) for key, value in raw_headers.items()},
                body=None if raw_sensor.get("body") is None else _expand_string(str(raw_sensor["body"]), env_values),
                token=None if raw_sensor.get("token") is None else _expand_string(str(raw_sensor["token"]), env_values),
                expected_status=expected_status,
                cursor_path=None
                if raw_sensor.get("cursor_path") is None
                else _expand_string(str(raw_sensor["cursor_path"]), env_values),
                ignore_existing=ignore_existing,
                task_id=None if task_id is None else str(task_id),
            )
            continue

        connection_value = raw_sensor.get("connection") or raw_sensor.get("database_url") or raw_sensor.get("dsn")
        connection_ref = (
            raw_sensor.get("connection_ref") or raw_sensor.get("connection_name") or raw_sensor.get("connection_id")
        )
        if connection_value is not None and str(connection_value).startswith("@"):
            connection_ref = str(connection_value)[1:]
            connection_value = None
        if connection_value is None and connection_ref is not None:
            connection_value = connections.get(str(connection_ref))
            if connection_value is None:
                raise ConfigError(
                    f"Pipeline '{pipeline_id}' sensor '{sensor_id}' references unknown connection '{connection_ref}'"
                )
        connection_env = raw_sensor.get("connection_env")
        if connection_value is None and connection_env is not None:
            connection_value = env_values.get(str(connection_env))
        connection_value = _normalize_sensor_connection(
            None if connection_value is None else _expand_string(str(connection_value), env_values),
            workspace,
        )
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
            connection=connection_value,
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
    remaining_dependencies = {task_id: set(task.depends_on) for task_id, task in tasks.items()}
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
                raise ConfigError(f"Pipeline '{pipeline.pipeline_id}' triggers unknown pipeline '{target}'")

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
        raise ConfigError(f"Pipeline '{pipeline_id}' task '{task_id}' uses unsupported type '{raw_task_type}'")

    title = _expand_string(
        str(raw_task.get("title") or raw_task.get("name") or task_id.replace("_", " ").title()),
        env_values,
    )
    description = _expand_string(str(raw_task.get("description") or ""), env_values)
    depends_on = _parse_depends_on(
        raw_task.get("depends_on"),
        f"Pipeline '{pipeline_id}' task '{task_id}' depends_on",
    )
    on_upstream_failure = _parse_upstream_failure_behavior(
        raw_task,
        f"Pipeline '{pipeline_id}' task '{task_id}'",
    )
    enabled = bool(raw_task.get("enabled", True))
    priority = _parse_priority(raw_task.get("priority"), f"Pipeline '{pipeline_id}' task '{task_id}'")
    timeout_seconds = _parse_duration_seconds(
        raw_task.get("timeout_seconds", raw_task.get("timeout")),
        f"Pipeline '{pipeline_id}' task '{task_id}' timeout",
    )
    kill_grace_period_seconds = (
        _parse_duration_seconds(
            raw_task.get("kill_grace_period_seconds", raw_task.get("kill_grace_period")),
            f"Pipeline '{pipeline_id}' task '{task_id}' kill_grace_period",
        )
        or 5
    )
    run_if = raw_task.get("run_if")
    # Keep the pre-interpolation form of every field that can contain a
    # {variable}. A downstream or replayed run re-renders these against the
    # inherited variables, which is what lets a downstream pipeline be retried
    # without re-running the upstream pipeline that supplied them.
    variable_templates = {
        key: copy.deepcopy(raw_task[key])
        for key in ("command", "args", "kwargs", "url", "body", "headers", "subject", "to", "host", "user")
        if key in raw_task and raw_task[key] is not None
    }
    if "subject" in variable_templates:
        variable_templates["email_subject"] = variable_templates.pop("subject")
    if "body" in variable_templates and task_type == "email":
        variable_templates["email_body"] = variable_templates.pop("body")
    if "to" in variable_templates:
        variable_templates["email_to"] = variable_templates.pop("to")
    artifact_paths = tuple(
        _expand_string(str(item), env_values)
        for item in _ensure_list(
            raw_task.get("artifacts", raw_task.get("artifact_paths")),
            f"Pipeline '{pipeline_id}' task '{task_id}' artifacts",
        )
    )

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
                    raise ConfigError(f"Pipeline '{pipeline_id}' task '{task_id}' callable path could not be resolved")
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
                raise ConfigError(f"Pipeline '{pipeline_id}' task '{task_id}' args must be a list")
            return TaskDefinition(
                task_id=task_id,
                title=title,
                task_type="python",
                description=description,
                depends_on=depends_on,
                on_upstream_failure=on_upstream_failure,
                enabled=enabled,
                priority=priority,
                timeout_seconds=timeout_seconds,
                kill_grace_period_seconds=kill_grace_period_seconds,
                run_if=None if run_if is None else _expand_env_only(str(run_if), env_values),
                artifact_paths=artifact_paths,
                call=callable_text,
                args=tuple(raw_args),
                kwargs={str(key): value for key, value in raw_kwargs.items()},
                cwd=_resolve_path(raw_task.get("cwd"), workspace, env_values) or workspace,
                env=task_env,
                variable_templates=variable_templates,
            )
        path_value = raw_task.get("path") or raw_task.get("script")
        if not path_value:
            raise ConfigError(
                f"Pipeline '{pipeline_id}' task '{task_id}' requires path/script or a callable target for python tasks"
            )
        path = _resolve_path(str(path_value), workspace, env_values)
        if path is None or not path.exists():
            raise ConfigError(f"Pipeline '{pipeline_id}' task '{task_id}' points to a missing script: {path_value}")
        raw_args = _expand_value(raw_task.get("args", []), env_values)
        if not isinstance(raw_args, list):
            raise ConfigError(f"Pipeline '{pipeline_id}' task '{task_id}' args must be a list")
        return TaskDefinition(
            task_id=task_id,
            title=title,
            task_type="python",
            description=description,
            depends_on=depends_on,
            on_upstream_failure=on_upstream_failure,
            enabled=enabled,
            priority=priority,
            timeout_seconds=timeout_seconds,
            kill_grace_period_seconds=kill_grace_period_seconds,
            run_if=None if run_if is None else _expand_env_only(str(run_if), env_values),
            artifact_paths=artifact_paths,
            path=path,
            python=_expand_string(str(raw_task.get("python") or default_python), env_values),
            args=tuple(raw_args),
            cwd=_resolve_path(raw_task.get("cwd"), workspace, env_values),
            env=task_env,
            variable_templates=variable_templates,
        )

    if task_type == "cli":
        command = raw_task.get("command")
        path_value = raw_task.get("path") or raw_task.get("script")
        raw_args = _expand_value(raw_task.get("args", []), env_values)
        raw_shell = raw_task.get("shell")
        shell_name = None if raw_shell in (None, "", False, True) else _expand_string(str(raw_shell), env_values)
        if not isinstance(raw_args, list):
            raise ConfigError(f"Pipeline '{pipeline_id}' task '{task_id}' args must be a list")
        if not command and not path_value:
            raise ConfigError(f"Pipeline '{pipeline_id}' task '{task_id}' requires command or path for cli tasks")
        resolved_path = _resolve_path(str(path_value), workspace, env_values) if path_value else None
        if path_value and (resolved_path is None or not resolved_path.exists()):
            raise ConfigError(f"Pipeline '{pipeline_id}' task '{task_id}' points to a missing cli path: {path_value}")
        return TaskDefinition(
            task_id=task_id,
            title=title,
            task_type="cli",
            description=description,
            depends_on=depends_on,
            on_upstream_failure=on_upstream_failure,
            enabled=enabled,
            priority=priority,
            timeout_seconds=timeout_seconds,
            kill_grace_period_seconds=kill_grace_period_seconds,
            run_if=None if run_if is None else _expand_env_only(str(run_if), env_values),
            artifact_paths=artifact_paths,
            path=resolved_path,
            command=None if command is None else _expand_string(str(command), env_values),
            shell=shell_name,
            args=tuple(raw_args),
            cwd=_resolve_path(raw_task.get("cwd"), workspace, env_values),
            env=task_env,
            variable_templates=variable_templates,
        )

    if task_type == "api" or task_type == "webhook":
        url = raw_task.get("url")
        if not url:
            raise ConfigError(f"Pipeline '{pipeline_id}' task '{task_id}' requires url for {task_type} tasks")
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
            on_upstream_failure=on_upstream_failure,
            enabled=enabled,
            priority=priority,
            timeout_seconds=timeout_seconds,
            kill_grace_period_seconds=kill_grace_period_seconds,
            run_if=None if run_if is None else _expand_env_only(str(run_if), env_values),
            artifact_paths=artifact_paths,
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
            variable_templates=variable_templates,
        )

    if task_type == "email":
        return TaskDefinition(
            task_id=task_id,
            title=title,
            task_type="email",
            description=description,
            depends_on=depends_on,
            on_upstream_failure=on_upstream_failure,
            enabled=enabled,
            priority=priority,
            timeout_seconds=timeout_seconds,
            kill_grace_period_seconds=kill_grace_period_seconds,
            run_if=None if run_if is None else _expand_env_only(str(run_if), env_values),
            artifact_paths=artifact_paths,
            # Left unset when absent, so the task inherits the central SMTP
            # settings. Defaulting to localhost here would silently override
            # them on every task.
            smtp_host=(
                None if raw_task.get("smtp_host") is None else _expand_string(str(raw_task["smtp_host"]), env_values)
            ),
            smtp_port=int(raw_task.get("smtp_port") or 587),
            smtp_user=_expand_string(str(raw_task.get("smtp_user", "")), env_values) or None,
            smtp_password=_expand_string(str(raw_task.get("smtp_password", "")), env_values) or None,
            email_to=tuple(str(item) for item in _expand_value(list(raw_task.get("to") or []), env_values)),
            email_subject=_expand_string(str(raw_task.get("subject") or "Piply Notification"), env_values),
            email_body=_expand_string(str(raw_task.get("body") or ""), env_values),
            env=task_env,
            variable_templates=variable_templates,
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
        on_upstream_failure=on_upstream_failure,
        enabled=enabled,
        priority=priority,
        timeout_seconds=timeout_seconds,
        kill_grace_period_seconds=kill_grace_period_seconds,
        run_if=None if run_if is None else _expand_env_only(str(run_if), env_values),
        artifact_paths=artifact_paths,
        host=_expand_string(str(host), env_values),
        user=None if raw_task.get("user") is None else _expand_string(str(raw_task.get("user")), env_values),
        port=int(raw_task.get("port", 22)),
        key_file=_resolve_path(raw_task.get("key_file"), workspace, env_values),
        command=None if raw_task.get("command") is None else _expand_string(str(raw_task.get("command")), env_values),
        ssh_binary=_expand_string(str(raw_task.get("ssh_binary") or "ssh"), env_values),
        connect_timeout=int(raw_task.get("connect_timeout", 8)),
        env=task_env,
        variable_templates=variable_templates,
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

    raw_data, config_sources = load_raw_config(path)

    settings = load_settings(path)
    env_values = settings.env_values
    effective_default_max_parallel_tasks = default_max_parallel_tasks or settings.default_max_parallel_tasks

    defaults = _ensure_mapping(raw_data.get("defaults"), "defaults")
    timezone_name = _expand_string(str(raw_data.get("timezone") or defaults.get("timezone") or "UTC"), env_values)
    workspace = _resolve_path(str(raw_data.get("workspace", ".")), path.parent, env_values) or path.parent
    if not workspace.exists():
        raise ConfigError(f"Configured workspace does not exist: {workspace}")

    secret_values = load_secret_values(raw_data.get("secrets"), workspace=workspace, env_values=env_values)
    if secret_values:
        env_values = dict(env_values)
        env_values.update(secret_env_aliases(secret_values))

    root_variables = _parse_variables(defaults.get("variables"), "defaults.variables", env_values)
    root_variables.update(_parse_variables(raw_data.get("variables"), "variables", env_values | root_variables))
    root_values = env_values | root_variables

    connections = _parse_connections(raw_data.get("connections"), root_values)
    try:
        # Expanded first so `webhook: ${TEAMS_PROD_WEBHOOK}` resolves from the
        # environment and secrets files, never from a literal in the YAML.
        notifications, notification_warnings = parse_notifications(
            _expand_value(raw_data.get("notifications"), root_values), root_values
        )
    except NotificationError as exc:
        raise ConfigError(str(exc)) from exc
    root_entities = _parse_entities(raw_data.get("entities"), "entities", root_values)

    default_python = _expand_string(str(defaults.get("python") or sys.executable), root_values)
    # Kept unexpanded so each pipeline can resolve it against its own variables.
    # A shared default such as DBT_CLIENT: "{tenant}" is only useful if every
    # deployment renders it with that deployment's tenant.
    raw_default_env = {
        str(key): str(value) for key, value in _ensure_mapping(defaults.get("env"), "defaults.env").items()
    }

    raw_pipelines = _normalize_pipeline_definitions(raw_data)

    pipelines: dict[str, PipelineDefinition] = {}
    project_warnings: list[str] = list(notification_warnings)
    for pipeline_id, raw_pipeline in raw_pipelines.items():
        pipeline_variables = dict(root_variables)
        pipeline_variables.update(
            _parse_variables(
                raw_pipeline.get("variables"),
                f"Pipeline '{pipeline_id}' variables",
                root_values | pipeline_variables,
            )
        )
        pipeline_values = root_values | pipeline_variables
        own_entities = _parse_entities(
            raw_pipeline.get("entities"), f"Pipeline '{pipeline_id}' entities", pipeline_values
        )
        pipeline_entities = merge_entity_maps(root_entities, own_entities)
        project_warnings.extend(
            _unused_global_entity_warnings(
                pipeline_id,
                raw_tasks=raw_pipeline.get("tasks"),
                root_entities=root_entities,
                own_entities=own_entities,
            )
        )

        schedule_timezone = _expand_string(str(raw_pipeline.get("timezone") or timezone_name), pipeline_values)
        try:
            schedule = _parse_schedule(raw_pipeline.get("schedule"), schedule_timezone)
        except ScheduleError as exc:
            raise ConfigError(f"Pipeline '{pipeline_id}' has an invalid schedule: {exc}") from exc

        max_parallel_tasks = _parse_execution(
            raw_pipeline.get("execution"),
            pipeline_id,
            default_max_parallel_tasks=effective_default_max_parallel_tasks,
            explicit_max_parallel_tasks=raw_pipeline.get("max_parallel_tasks"),
        )
        pipeline_timeout_seconds = _parse_duration_seconds(
            raw_pipeline.get("timeout_seconds", raw_pipeline.get("timeout")),
            f"Pipeline '{pipeline_id}' timeout",
        )
        retry_policy = _parse_retry_policy(raw_pipeline.get("retry"), pipeline_id)

        title = _expand_string(
            str(raw_pipeline.get("title") or raw_pipeline.get("name") or pipeline_id),
            pipeline_values,
        )
        description = _expand_string(str(raw_pipeline.get("description") or ""), pipeline_values)
        tags = tuple(
            _expand_string(str(tag), pipeline_values)
            for tag in _ensure_list(raw_pipeline.get("tags"), f"Pipeline '{pipeline_id}' tags")
        )
        enabled = bool(raw_pipeline.get("enabled", True))
        max_concurrent_runs = int(raw_pipeline.get("max_concurrent_runs", 1))
        if max_concurrent_runs < 1:
            raise ConfigError(f"Pipeline '{pipeline_id}' must have max_concurrent_runs greater than zero")

        pipeline_env = {key: _expand_string(value, pipeline_values) for key, value in raw_default_env.items()}
        pipeline_env.update(
            {
                str(key): _expand_string(str(value), pipeline_values)
                for key, value in _ensure_mapping(
                    raw_pipeline.get("env"),
                    f"Pipeline '{pipeline_id}' env",
                ).items()
            }
        )

        env_file_paths = _ensure_list(raw_pipeline.get("env_files"), f"Pipeline '{pipeline_id}' env_files")
        if raw_pipeline.get("env_file"):
            env_file_paths.append(str(raw_pipeline["env_file"]))
        for env_file_path in env_file_paths:
            expanded_env_file = _expand_string(str(env_file_path), pipeline_values)
            # A missing env file loads nothing rather than raising, because it is
            # legitimately absent in some environments. Record it so `validate`
            # and `plan` can say so: the silent version of this presents much
            # later as "my credentials aren't set", which is hard to trace back.
            resolved_env_file = Path(expanded_env_file)
            if not resolved_env_file.is_absolute():
                resolved_env_file = workspace / resolved_env_file
            if not resolved_env_file.exists():
                project_warnings.append(
                    f"Pipeline '{pipeline_id}' env_file '{env_file_path}' was not found at "
                    f"{resolved_env_file}. Paths resolve against workspace ('{workspace}'), "
                    "not the config file. No variables were loaded from it."
                )
            pipeline_env.update(
                load_secret_values(
                    {"backend": "file", "path": expanded_env_file},
                    workspace=workspace,
                    env_values=pipeline_values,
                )
            )

        raw_tasks = raw_pipeline.get("tasks")
        if raw_tasks is None:
            raw_tasks = _build_single_task_pipeline(raw_pipeline)
        if not isinstance(raw_tasks, dict) or not raw_tasks:
            raise ConfigError(f"Pipeline '{pipeline_id}' must define a non-empty tasks mapping")
        raw_tasks = _normalize_task_priority_suffixes(raw_tasks, pipeline_id)

        task_entity_maps = {
            str(task_id): _parse_entities(
                raw_task.get("entities"),
                f"Pipeline '{pipeline_id}' task '{task_id}' entities",
                pipeline_values,
            )
            for task_id, raw_task in raw_tasks.items()
            if isinstance(raw_task, dict)
            and "entities" in raw_task
            and raw_task.get("entities") not in (None, "", False)
            # A list selects existing dimensions; only a mapping declares values.
            and not isinstance(raw_task.get("entities"), list)
        }
        try:
            runtime_task_specs = expand_task_templates(
                raw_tasks,
                pipeline_entities=pipeline_entities,
                task_entities=task_entity_maps,
            )
        except ExpansionError as exc:
            raise ConfigError(f"Pipeline '{pipeline_id}' has invalid entity expansion: {exc}") from exc

        tasks: dict[str, TaskDefinition] = {}
        template_task_ids = {str(task_id) for task_id in raw_tasks}
        for task_spec in runtime_task_specs:
            task_id = task_spec.runtime_id
            raw_task = task_spec.raw_task
            if not isinstance(raw_task, dict):
                raise ConfigError(f"Pipeline '{pipeline_id}' task '{task_id}' must be a mapping")
            task = _parse_task(
                pipeline_id,
                task_id,
                raw_task,
                workspace=workspace,
                default_python=default_python,
                inherited_env=pipeline_env,
                env_values=pipeline_values | task_spec.variables,
            )
            task.template_id = task_spec.template_id if task_spec.template_id != task_spec.runtime_id else None
            task.entity_key = task_spec.entity_key
            task.entity_values = dict(task_spec.entity_values)
            # An entity's '*' suffix raises the priority of the tasks generated
            # for it, on top of whatever the task template already declared.
            task.priority += task_spec.priority
            tasks[task_id] = task

        _validate_task_graph(pipeline_id, tasks)
        parallelizable = _detect_parallelism(tasks)
        sensors = _parse_sensors(
            raw_pipeline.get("sensors"),
            pipeline_id,
            workspace=workspace,
            task_ids=set(tasks) | template_task_ids,
            env_values=pipeline_values,
            connections=connections,
        )

        triggers_on_success = tuple(
            _expand_string(str(item), pipeline_values)
            for item in _ensure_list(
                raw_pipeline.get("triggers_on_success"),
                f"Pipeline '{pipeline_id}' triggers_on_success",
            )
        )
        if pipeline_id in triggers_on_success:
            raise ConfigError(f"Pipeline '{pipeline_id}' cannot trigger itself on success")

        notify_on_failure, notify_on_success = _parse_notify(raw_pipeline.get("notify"), pipeline_id)
        try:
            alert_on_failure, alert_on_success = parse_pipeline_notifications(
                raw_pipeline.get("notifications"), pipeline_id
            )
        except NotificationError as exc:
            raise ConfigError(str(exc)) from exc

        pipelines[pipeline_id] = PipelineDefinition(
            pipeline_id=pipeline_id,
            title=title,
            description=description,
            tasks=tasks,
            variables=pipeline_variables,
            template_id=None if raw_pipeline.get("_template_id") is None else str(raw_pipeline.get("_template_id")),
            deployment_id=None
            if raw_pipeline.get("_deployment_id") is None
            else str(raw_pipeline.get("_deployment_id")),
            tags=tags,
            schedule=schedule,
            enabled=enabled,
            max_concurrent_runs=max_concurrent_runs,
            parallelizable=parallelizable,
            max_parallel_tasks=max_parallel_tasks,
            timeout_seconds=pipeline_timeout_seconds,
            triggers_on_success=triggers_on_success,
            notify_on_failure=notify_on_failure,
            notify_on_success=notify_on_success,
            alert_on_failure=alert_on_failure,
            alert_on_success=alert_on_success,
            retry_policy=retry_policy,
            sensors=sensors,
        )

    _validate_pipeline_trigger_graph(pipelines)

    return ProjectDefinition(
        version=str(raw_data.get("version", "1")),
        title=_expand_string(str(raw_data.get("title") or path.parent.name), root_values),
        config_path=path,
        workspace=workspace,
        default_python=default_python,
        timezone_name=timezone_name,
        pipelines=pipelines,
        config_sources=tuple(config_sources),
        notifications=notifications,
        warnings=tuple(project_warnings),
    )
