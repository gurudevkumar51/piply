"""Retry planning keeps startover and resume behavior deterministic."""

from __future__ import annotations

from dataclasses import dataclass

from piply.core.models import PipelineDefinition, RetryMode, TaskRunRecord

from .graph import downstream_closure


@dataclass(slots=True)
class RetryPlan:
    """RetryPlan describes which tasks should rerun versus be reused."""

    mode: RetryMode
    selected_task_id: str | None
    rerun_task_ids: tuple[str, ...]
    reuse_task_ids: tuple[str, ...]


def build_retry_plan(
    pipeline: PipelineDefinition,
    previous_task_runs: list[TaskRunRecord],
    *,
    mode: RetryMode,
    task_id: str | None = None,
) -> RetryPlan:
    """Build a deterministic retry plan for one completed pipeline run."""
    previous_by_id = {task.task_id: task for task in previous_task_runs}

    if task_id is not None:
        previous_task = previous_by_id.get(task_id)
        if previous_task is None:
            raise ValueError(f"Unknown task '{task_id}' for retry selection.")
        if previous_task.status == "success":
            raise ValueError("Select a failed or skipped task to retry from the UI.")

    if mode == "startover":
        return RetryPlan(
            mode=mode,
            selected_task_id=task_id,
            rerun_task_ids=tuple(pipeline.tasks.keys()),
            reuse_task_ids=(),
        )

    unresolved = {
        task.task_id
        for task in previous_task_runs
        if task.status in {"failed", "skipped", "queued", "running"}
    }
    if not unresolved:
        raise ValueError("This run does not have failed or skipped tasks to resume.")

    rerun = downstream_closure(pipeline, unresolved)
    reuse = tuple(
        current_task_id
        for current_task_id in pipeline.tasks
        if current_task_id not in rerun
        and previous_by_id.get(current_task_id) is not None
        and previous_by_id[current_task_id].status == "success"
    )

    return RetryPlan(
        mode=mode,
        selected_task_id=task_id,
        rerun_task_ids=tuple(task_id for task_id in pipeline.tasks if task_id in rerun),
        reuse_task_ids=reuse,
    )
