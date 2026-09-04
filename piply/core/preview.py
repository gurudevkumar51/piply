"""Execution preview used by ``piply plan`` and the dry-run UI.

The preview answers the question "what would happen if I ran this right now?"
without touching the store or starting a process: it resolves variables, shows
the entity expansion, orders tasks the way the scheduler would, and renders the
interpolated command for every task.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .graph import topological_order
from .models import PipelineDefinition, TaskDefinition

_PLACEHOLDER_PATTERN = re.compile(r"(?<!\{)\{([A-Za-z_][A-Za-z0-9_]*)\}(?!\})")

#: Task fields that can carry a `{name}` placeholder and are worth prompting for.
#:
#: `run_if` is excluded on purpose: its placeholders are resolved at execution
#: time against entity values, and are quoted as literals when they are, so
#: asking a user to fill them in would be wrong.
_PLACEHOLDER_FIELDS = (
    "command",
    "path",
    "cwd",
    "url",
    "body",
    "token",
    "host",
    "user",
    "key_file",
    "email_subject",
    "email_body",
    "smtp_host",
    "smtp_user",
    "call",
    "python",
    "shell",
)

#: Fields holding a collection whose members can carry placeholders.
_PLACEHOLDER_COLLECTION_FIELDS = ("args", "artifact_paths", "email_to")
_PLACEHOLDER_MAPPING_FIELDS = ("env", "kwargs", "headers")


def _placeholders_in(value: object) -> set[str]:
    """Return the `{name}` placeholders inside one value, however nested."""
    if value is None or isinstance(value, bool | int | float):
        return set()
    if isinstance(value, str):
        return set(_PLACEHOLDER_PATTERN.findall(value))
    if isinstance(value, dict):
        found: set[str] = set()
        for key, item in value.items():
            found |= _placeholders_in(key) | _placeholders_in(item)
        return found
    if isinstance(value, list | tuple | set):
        found = set()
        for item in value:
            found |= _placeholders_in(item)
        return found
    # Path and anything else stringifiable.
    return set(_PLACEHOLDER_PATTERN.findall(str(value)))


def unresolved_placeholders(pipeline: PipelineDefinition) -> dict[str, tuple[str, ...]]:
    """Return each unresolved placeholder and the tasks that still reference it.

    A placeholder that a variable already satisfied has been substituted by the
    time this runs, so anything still spelled `{name}` genuinely has no value.
    Entity values are per-task, so they are checked per task rather than
    globally.

    This is what lets a manual run of a normally-downstream pipeline ask for the
    values its upstream would have supplied, instead of executing a command with
    a literal `{practice}` in it.
    """
    found: dict[str, list[str]] = {}
    for task_id, task in pipeline.tasks.items():
        names: set[str] = set()
        for field_name in _PLACEHOLDER_FIELDS:
            names |= _placeholders_in(getattr(task, field_name, None))
        for field_name in _PLACEHOLDER_COLLECTION_FIELDS:
            names |= _placeholders_in(getattr(task, field_name, ()))
        for field_name in _PLACEHOLDER_MAPPING_FIELDS:
            names |= _placeholders_in(getattr(task, field_name, {}))

        # An entity placeholder is filled in when the task runs, so it is not
        # something to ask a person for.
        names -= set(task.entity_values)
        for name in names:
            found.setdefault(name, []).append(task_id)

    return {name: tuple(sorted(task_ids)) for name, task_ids in sorted(found.items())}


@dataclass(slots=True)
class PreviewTask:
    """One task as it would be executed, with its resolved command."""

    task_id: str
    title: str
    task_type: str
    depends_on: tuple[str, ...]
    command: str
    enabled: bool
    priority: int
    timeout_seconds: int | None
    run_if: str | None
    run_if_result: bool | None
    on_upstream_failure: str
    template_id: str | None
    entity_key: str | None
    entity_values: dict[str, str]
    artifact_paths: tuple[str, ...]
    env: dict[str, str]
    stage: int
    will_run: bool
    skip_reason: str | None = None


@dataclass(slots=True)
class PipelinePreview:
    """The full dry-run result for one pipeline."""

    pipeline_id: str
    title: str
    description: str
    template_id: str | None
    deployment_id: str | None
    execution_mode: str
    max_parallel_tasks: int
    timeout_seconds: int | None
    schedule_text: str
    next_run_at: datetime | None
    retry_summary: str
    variables: dict[str, str]
    entities: dict[str, list[str]]
    triggers_on_success: tuple[str, ...]
    tasks: list[PreviewTask]
    stages: list[list[str]]
    execution_order: list[str]
    warnings: list[str] = field(default_factory=list)

    @property
    def task_count(self) -> int:
        """Return how many tasks the preview covers."""
        return len(self.tasks)

    @property
    def runnable_task_count(self) -> int:
        """Return how many tasks would actually execute."""
        return sum(1 for task in self.tasks if task.will_run)

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-friendly payload for the API and UI."""
        return {
            "pipeline_id": self.pipeline_id,
            "title": self.title,
            "description": self.description,
            "template_id": self.template_id,
            "deployment_id": self.deployment_id,
            "execution_mode": self.execution_mode,
            "max_parallel_tasks": self.max_parallel_tasks,
            "timeout_seconds": self.timeout_seconds,
            "schedule_text": self.schedule_text,
            "next_run_at": None if self.next_run_at is None else self.next_run_at.isoformat(),
            "retry_summary": self.retry_summary,
            "variables": self.variables,
            "entities": self.entities,
            "triggers_on_success": list(self.triggers_on_success),
            "task_count": self.task_count,
            "runnable_task_count": self.runnable_task_count,
            "stages": self.stages,
            "execution_order": self.execution_order,
            "warnings": self.warnings,
            "tasks": [
                {
                    "task_id": task.task_id,
                    "title": task.title,
                    "task_type": task.task_type,
                    "depends_on": list(task.depends_on),
                    "command": task.command,
                    "enabled": task.enabled,
                    "priority": task.priority,
                    "timeout_seconds": task.timeout_seconds,
                    "run_if": task.run_if,
                    "run_if_result": task.run_if_result,
                    "on_upstream_failure": task.on_upstream_failure,
                    "template_id": task.template_id,
                    "entity_key": task.entity_key,
                    "entity_values": task.entity_values,
                    "artifact_paths": list(task.artifact_paths),
                    "env": task.env,
                    "stage": task.stage,
                    "will_run": task.will_run,
                    "skip_reason": task.skip_reason,
                }
                for task in self.tasks
            ],
        }


def _stage_map(pipeline: PipelineDefinition) -> dict[str, int]:
    """Return the dependency depth for every task."""
    depths: dict[str, int] = {}
    for task in topological_order(pipeline):
        if not task.depends_on:
            depths[task.task_id] = 0
            continue
        depths[task.task_id] = max(depths.get(item, 0) for item in task.depends_on) + 1
    return depths


def _scheduler_order(pipeline: PipelineDefinition, stages: dict[str, int]) -> list[str]:
    """Order tasks the way the engine would: dependency first, then priority."""
    declared_index = {task.task_id: index for index, task in enumerate(topological_order(pipeline))}
    return [
        task_id
        for task_id, _ in sorted(
            stages.items(),
            key=lambda item: (item[1], -pipeline.tasks[item[0]].priority, declared_index[item[0]], item[0]),
        )
    ]


def _evaluate_condition(task: TaskDefinition, context: dict[str, object]) -> tuple[bool | None, str | None]:
    """Evaluate ``run_if`` against the preview context when possible."""
    if not task.run_if:
        return None, None
    from piply.engine.local_engine import safe_condition_eval

    merged = dict(context)
    merged.update(task.entity_values)
    try:
        result = safe_condition_eval(task.run_if, merged)
    except ValueError as exc:
        return None, str(exc)
    return result, None if result else f"run_if evaluated false: {task.run_if}"


def build_pipeline_preview(
    pipeline: PipelineDefinition,
    *,
    context: dict[str, object] | None = None,
    now: datetime | None = None,
) -> PipelinePreview:
    """Build the dry-run preview for one already-resolved pipeline definition."""
    current = now or datetime.now(timezone.utc)
    preview_context: dict[str, object] = dict(pipeline.variables)
    preview_context.update(context or {})

    stages = _stage_map(pipeline)
    order = _scheduler_order(pipeline, stages)
    warnings: list[str] = []

    entities: dict[str, list[str]] = {}
    for task in pipeline.tasks.values():
        for key, value in task.entity_values.items():
            values = entities.setdefault(key, [])
            if value not in values:
                values.append(value)

    tasks: list[PreviewTask] = []
    for task_id in order:
        task = pipeline.tasks[task_id]
        run_if_result, condition_note = _evaluate_condition(task, preview_context)
        skip_reason: str | None = None
        will_run = True
        if not task.enabled:
            will_run = False
            skip_reason = "disabled in config"
        elif run_if_result is False:
            will_run = False
            skip_reason = condition_note
        elif run_if_result is None and condition_note is not None:
            warnings.append(f"Task '{task_id}': {condition_note}")

        # Only flag real `{name}` placeholders. A command that legitimately
        # contains braces, such as an inline dict or JSON body, is not a warning.
        unresolved = sorted(set(_PLACEHOLDER_PATTERN.findall(task.command_preview)))
        if unresolved:
            warnings.append(
                f"Task '{task_id}' still contains unresolved placeholder(s): "
                + ", ".join(f"{{{name}}}" for name in unresolved)
            )

        tasks.append(
            PreviewTask(
                task_id=task.task_id,
                title=task.title,
                task_type=task.task_type,
                depends_on=task.depends_on,
                command=task.command_preview,
                enabled=task.enabled,
                priority=task.priority,
                timeout_seconds=task.timeout_seconds,
                run_if=task.run_if,
                run_if_result=run_if_result,
                on_upstream_failure=task.on_upstream_failure,
                template_id=task.template_id,
                entity_key=task.entity_key,
                entity_values=dict(task.entity_values),
                artifact_paths=task.artifact_paths,
                env=dict(task.env),
                stage=stages[task_id],
                will_run=will_run,
                skip_reason=skip_reason,
            )
        )

    stage_count = (max(stages.values()) + 1) if stages else 0
    grouped_stages = [[task_id for task_id in order if stages[task_id] == depth] for depth in range(stage_count)]

    return PipelinePreview(
        pipeline_id=pipeline.pipeline_id,
        title=pipeline.title,
        description=pipeline.description,
        template_id=pipeline.template_id,
        deployment_id=pipeline.deployment_id,
        execution_mode=pipeline.execution_mode,
        max_parallel_tasks=pipeline.max_parallel_tasks,
        timeout_seconds=pipeline.timeout_seconds,
        schedule_text=pipeline.schedule.describe() if pipeline.schedule else "Manual only",
        next_run_at=pipeline.schedule.next_after(current) if pipeline.schedule else None,
        retry_summary=pipeline.retry_policy.summary,
        variables=dict(pipeline.variables),
        entities=entities,
        triggers_on_success=pipeline.triggers_on_success,
        tasks=tasks,
        stages=grouped_stages,
        execution_order=order,
        warnings=warnings,
    )
