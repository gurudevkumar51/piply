"""Shared DAG helpers for execution planning and retry selection."""

from __future__ import annotations

from piply.core.models import PipelineDefinition, TaskDefinition


def topological_order(pipeline: PipelineDefinition) -> list[TaskDefinition]:
    """Return tasks in dependency-safe order."""
    ordered: list[TaskDefinition] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(task_id: str) -> None:
        if task_id in visited:
            return
        if task_id in visiting:
            raise RuntimeError(f"Task cycle detected at {task_id}")
        visiting.add(task_id)
        task = pipeline.tasks[task_id]
        for dependency in task.depends_on:
            visit(dependency)
        visiting.remove(task_id)
        visited.add(task_id)
        ordered.append(task)

    for task_id in pipeline.tasks:
        visit(task_id)
    return ordered


def dependents_map(pipeline: PipelineDefinition) -> dict[str, set[str]]:
    """Return a mapping of task ids to their direct downstream dependents."""
    mapping = {task_id: set() for task_id in pipeline.tasks}
    for task in pipeline.tasks.values():
        for dependency in task.depends_on:
            mapping.setdefault(dependency, set()).add(task.task_id)
    return mapping


def downstream_closure(pipeline: PipelineDefinition, seed_task_ids: set[str]) -> set[str]:
    """Return every task reachable from the supplied seed tasks."""
    dependents = dependents_map(pipeline)
    closure = set(seed_task_ids)
    stack = list(seed_task_ids)

    while stack:
        task_id = stack.pop()
        for dependent in dependents.get(task_id, set()):
            if dependent not in closure:
                closure.add(dependent)
                stack.append(dependent)

    return closure


def upstream_closure(pipeline: PipelineDefinition, seed_task_ids: set[str]) -> set[str]:
    """Return every dependency required to execute the supplied task ids."""
    closure = set(seed_task_ids)
    stack = list(seed_task_ids)

    while stack:
        task_id = stack.pop()
        for dependency in pipeline.tasks[task_id].depends_on:
            if dependency not in closure:
                closure.add(dependency)
                stack.append(dependency)

    return closure
