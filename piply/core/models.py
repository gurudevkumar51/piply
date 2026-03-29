"""Core runtime models shared by the loader, engine, store, and API."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Protocol


RunStatus = Literal["queued", "running", "success", "failed"]
TaskStatus = Literal["queued", "running", "success", "failed", "skipped"]
TriggerType = Literal["manual", "schedule", "api", "pipeline"]
TaskType = Literal["python", "cli", "api", "ssh"]
ExecutionMode = Literal["sequential", "parallel"]
RetryMode = Literal["startover", "resume"]


class SchedulePlan(Protocol):
    """A schedulable plan exposes current and next execution slots."""

    timezone_name: str

    def describe(self) -> str:
        ...

    def current_slot(self, now_utc: datetime) -> datetime | None:
        ...

    def next_after(self, now_utc: datetime) -> datetime | None:
        ...


@dataclass(slots=True)
class TaskDefinition:
    """A task is a single executable unit inside a pipeline."""

    task_id: str
    title: str
    task_type: TaskType
    description: str = ""
    depends_on: tuple[str, ...] = ()
    enabled: bool = True
    path: Path | None = None
    python: str | None = None
    args: tuple[str, ...] = ()
    command: str | None = None
    cwd: Path | None = None
    env: dict[str, str] = field(default_factory=dict)
    url: str | None = None
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    body: str | None = None
    token: str | None = None
    expected_status: tuple[int, ...] = (200, 201, 202, 204)
    host: str | None = None
    user: str | None = None
    port: int = 22
    key_file: Path | None = None
    ssh_binary: str = "ssh"
    connect_timeout: int = 8

    @property
    def operator_label(self) -> str:
        """Return a short operator name for UI and log labels."""
        return self.task_type.upper()

    @property
    def working_directory(self) -> Path | None:
        """Resolve the working directory used by subprocess-style tasks."""
        if self.cwd is not None:
            return self.cwd
        if self.path is not None:
            return self.path.parent
        return None

    @property
    def command_preview(self) -> str:
        """Return the human-readable command or target for this task."""
        if self.task_type == "python":
            python_executable = self.python or "python"
            path_text = str(self.path) if self.path else "<missing script>"
            parts = [python_executable, path_text, *self.args]
            return " ".join(parts)
        if self.task_type == "cli":
            return self.command or "<missing command>"
        if self.task_type == "api":
            return f"{self.method.upper()} {self.url or '<missing url>'}"
        if self.task_type == "ssh":
            target = "@".join(part for part in [self.user, self.host] if part)
            remote = self.command or "echo piply-ssh-ok"
            return f"ssh {target} {remote}".strip()
        return self.task_id


@dataclass(slots=True)
class PipelineDefinition:
    """A pipeline groups tasks, scheduling rules, and downstream triggers."""

    pipeline_id: str
    title: str
    description: str
    tasks: dict[str, TaskDefinition]
    tags: tuple[str, ...] = ()
    schedule: SchedulePlan | None = None
    enabled: bool = True
    max_concurrent_runs: int = 1
    execution_mode: ExecutionMode = "sequential"
    max_parallel_tasks: int = 4
    triggers_on_success: tuple[str, ...] = ()

    @property
    def task_count(self) -> int:
        """Return the number of configured tasks."""
        return len(self.tasks)

    @property
    def first_task(self) -> TaskDefinition | None:
        """Return the first declared task for summary views."""
        return next(iter(self.tasks.values()), None)

    @property
    def command_preview(self) -> str:
        """Return a compact execution preview for cards and tables."""
        if self.task_count == 0:
            return "No tasks configured"
        if self.task_count == 1 and self.first_task is not None:
            return self.first_task.command_preview
        ordered_ids = list(self.tasks.keys())
        preview = " -> ".join(ordered_ids[:4])
        if len(ordered_ids) > 4:
            preview += " -> ..."
        return f"{self.task_count} tasks | {preview}"

    @property
    def primary_entry(self) -> str:
        """Return the main script or command shown in summary cards."""
        first_task = self.first_task
        if first_task is None:
            return "No tasks configured"
        if first_task.path is not None:
            return str(first_task.path)
        return first_task.command_preview

    @property
    def execution_summary(self) -> str:
        """Return a short human-readable execution strategy label."""
        if self.execution_mode == "parallel":
            return f"Parallel up to {self.max_parallel_tasks} tasks"
        return "Sequential execution"

    def is_schedulable(self) -> bool:
        """Return whether the pipeline can be launched by the scheduler."""
        return self.enabled and self.schedule is not None


@dataclass(slots=True)
class ProjectDefinition:
    """A project is a loaded Piply workspace rooted at one config file."""

    version: str
    title: str
    config_path: Path
    workspace: Path
    default_python: str
    timezone_name: str
    pipelines: dict[str, PipelineDefinition]

    @property
    def pipeline_count(self) -> int:
        """Return the number of pipelines in the loaded project."""
        return len(self.pipelines)


@dataclass(slots=True)
class RunRecord:
    """A pipeline run stores lifecycle state, counts, and retry lineage."""

    run_id: str
    pipeline_id: str
    pipeline_title: str
    status: RunStatus
    trigger: TriggerType
    command: str
    primary_entry: str
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    scheduled_for: datetime | None = None
    exit_code: int | None = None
    error: str | None = None
    log_count: int = 0
    task_count: int = 0
    successful_tasks: int = 0
    failed_tasks: int = 0
    skipped_tasks: int = 0
    retry_of: str | None = None
    retry_mode: RetryMode | None = None
    retry_task_id: str | None = None

    @property
    def duration_seconds(self) -> float | None:
        """Return the elapsed runtime in seconds when available."""
        if self.started_at is None:
            return None
        end_time = self.finished_at or datetime.now(timezone.utc)
        return max(0.0, (end_time - self.started_at).total_seconds())


@dataclass(slots=True)
class TaskRunRecord:
    """A task run stores per-task state inside one pipeline run."""

    run_id: str
    task_id: str
    title: str
    task_type: str
    status: TaskStatus
    position: int
    command_preview: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    exit_code: int | None = None
    error: str | None = None
    depends_on: tuple[str, ...] = ()
    log_count: int = 0

    @property
    def duration_seconds(self) -> float | None:
        """Return the elapsed task runtime in seconds when available."""
        if self.started_at is None:
            return None
        end_time = self.finished_at or datetime.now(timezone.utc)
        return max(0.0, (end_time - self.started_at).total_seconds())


@dataclass(slots=True)
class LogRecord:
    """A log entry belongs to one run and may also belong to one task."""

    run_id: str
    created_at: datetime
    stream: str
    message: str
    task_id: str | None = None


@dataclass(slots=True)
class DashboardStats:
    """DashboardStats aggregates the most important runtime counters."""

    total_pipelines: int
    scheduled_pipelines: int
    total_runs: int
    running_runs: int
    successful_runs: int
    failed_runs: int

    @property
    def success_rate(self) -> float:
        """Return the percentage of completed runs that succeeded."""
        completed = self.successful_runs + self.failed_runs
        if completed == 0:
            return 0.0
        return round((self.successful_runs / completed) * 100, 1)


@dataclass(slots=True)
class PipelineSummary:
    """A pipeline summary is the UI-friendly view of one pipeline."""

    pipeline_id: str
    title: str
    description: str
    enabled: bool
    paused: bool
    schedule_text: str
    next_run_at: datetime | None
    next_run_label: str
    tags: tuple[str, ...]
    primary_entry: str
    command_preview: str
    max_concurrent_runs: int
    execution_mode: ExecutionMode
    max_parallel_tasks: int
    task_count: int
    trigger_targets: tuple[str, ...]
    latest_task_states: dict[str, TaskStatus] = field(default_factory=dict)
    last_run: RunRecord | None = None
    active_runs: int = 0

    @property
    def execution_summary(self) -> str:
        """Return a compact execution label for cards and headers."""
        if self.execution_mode == "parallel":
            return f"Parallel up to {self.max_parallel_tasks} tasks"
        return "Sequential execution"


def utc_now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)
