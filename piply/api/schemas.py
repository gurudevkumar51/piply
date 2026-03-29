"""API response and request models for the FastAPI surface."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from piply.core.models import (
    DashboardStats,
    LogRecord,
    PipelineDefinition,
    PipelineSummary,
    RunRecord,
    TaskDefinition,
    TaskRunRecord,
)


def _format_time(value: datetime) -> str:
    """Render a log timestamp in the short UI-friendly format."""
    return value.astimezone().strftime("%H:%M:%S.%f")[:-3]


class RetryRequest(BaseModel):
    """RetryRequest selects retry mode and the optionally clicked task."""

    mode: str
    task_id: str | None = None


class RunResponse(BaseModel):
    """RunResponse exposes the run-level state returned by the API."""

    id: str
    pipeline_id: str
    pipeline_title: str
    status: str
    trigger: str
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
    duration_seconds: float | None = None
    retry_of: str | None = None
    retry_mode: str | None = None
    retry_task_id: str | None = None

    @classmethod
    def from_record(cls, record: RunRecord) -> "RunResponse":
        """Convert one RunRecord to an API response."""
        return cls(
            id=record.run_id,
            pipeline_id=record.pipeline_id,
            pipeline_title=record.pipeline_title,
            status=record.status,
            trigger=record.trigger,
            command=record.command,
            primary_entry=record.primary_entry,
            created_at=record.created_at,
            started_at=record.started_at,
            finished_at=record.finished_at,
            scheduled_for=record.scheduled_for,
            exit_code=record.exit_code,
            error=record.error,
            log_count=record.log_count,
            task_count=record.task_count,
            successful_tasks=record.successful_tasks,
            failed_tasks=record.failed_tasks,
            skipped_tasks=record.skipped_tasks,
            duration_seconds=record.duration_seconds,
            retry_of=record.retry_of,
            retry_mode=record.retry_mode,
            retry_task_id=record.retry_task_id,
        )


class TaskResponse(BaseModel):
    """TaskResponse exposes one pipeline task definition."""

    task_id: str
    title: str
    task_type: str
    description: str
    depends_on: list[str]
    enabled: bool
    command_preview: str

    @classmethod
    def from_definition(cls, definition: TaskDefinition) -> "TaskResponse":
        """Convert a TaskDefinition to an API response."""
        return cls(
            task_id=definition.task_id,
            title=definition.title,
            task_type=definition.task_type,
            description=definition.description,
            depends_on=list(definition.depends_on),
            enabled=definition.enabled,
            command_preview=definition.command_preview,
        )


class TaskRunResponse(BaseModel):
    """TaskRunResponse exposes one task execution inside a run."""

    run_id: str
    task_id: str
    title: str
    task_type: str
    status: str
    position: int
    command_preview: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    exit_code: int | None = None
    error: str | None = None
    depends_on: list[str]
    log_count: int = 0
    duration_seconds: float | None = None

    @classmethod
    def from_record(cls, record: TaskRunRecord) -> "TaskRunResponse":
        """Convert one TaskRunRecord to an API response."""
        return cls(
            run_id=record.run_id,
            task_id=record.task_id,
            title=record.title,
            task_type=record.task_type,
            status=record.status,
            position=record.position,
            command_preview=record.command_preview,
            started_at=record.started_at,
            finished_at=record.finished_at,
            exit_code=record.exit_code,
            error=record.error,
            depends_on=list(record.depends_on),
            log_count=record.log_count,
            duration_seconds=record.duration_seconds,
        )


class LogResponse(BaseModel):
    """LogResponse exposes one raw log line."""

    run_id: str
    task_id: str | None = None
    created_at: datetime
    time_label: str
    stream: str
    message: str

    @classmethod
    def from_record(cls, record: LogRecord) -> "LogResponse":
        """Convert one LogRecord to an API response."""
        return cls(
            run_id=record.run_id,
            task_id=record.task_id,
            created_at=record.created_at,
            time_label=_format_time(record.created_at),
            stream=record.stream,
            message=record.message,
        )


class PipelineResponse(BaseModel):
    """PipelineResponse exposes one pipeline summary."""

    pipeline_id: str
    title: str
    description: str
    enabled: bool
    paused: bool
    schedule_text: str
    next_run_at: datetime | None = None
    next_run_label: str
    tags: list[str]
    primary_entry: str
    command_preview: str
    max_concurrent_runs: int
    execution_mode: str
    max_parallel_tasks: int
    execution_summary: str
    task_count: int
    trigger_targets: list[str]
    active_runs: int
    latest_task_states: dict[str, str]
    last_run: RunResponse | None = None

    @classmethod
    def from_summary(cls, summary: PipelineSummary) -> "PipelineResponse":
        """Convert one PipelineSummary to an API response."""
        return cls(
            pipeline_id=summary.pipeline_id,
            title=summary.title,
            description=summary.description,
            enabled=summary.enabled,
            paused=summary.paused,
            schedule_text=summary.schedule_text,
            next_run_at=summary.next_run_at,
            next_run_label=summary.next_run_label,
            tags=list(summary.tags),
            primary_entry=summary.primary_entry,
            command_preview=summary.command_preview,
            max_concurrent_runs=summary.max_concurrent_runs,
            execution_mode=summary.execution_mode,
            max_parallel_tasks=summary.max_parallel_tasks,
            execution_summary=summary.execution_summary,
            task_count=summary.task_count,
            trigger_targets=list(summary.trigger_targets),
            active_runs=summary.active_runs,
            latest_task_states=dict(summary.latest_task_states),
            last_run=RunResponse.from_record(summary.last_run) if summary.last_run else None,
        )


class PipelineDetailResponse(BaseModel):
    """PipelineDetailResponse adds tasks and recent runs to one pipeline summary."""

    pipeline: PipelineResponse
    tasks: list[TaskResponse]
    latest_task_runs: list[TaskRunResponse]
    recent_runs: list[RunResponse]

    @classmethod
    def from_payload(cls, payload: dict[str, object]) -> "PipelineDetailResponse":
        """Convert the service-layer pipeline detail payload to an API response."""
        pipeline = payload["pipeline"]
        summary = payload["summary"]
        latest_task_runs = payload["latest_task_runs"]
        recent_runs = payload["recent_runs"]
        assert isinstance(pipeline, PipelineDefinition)
        assert isinstance(summary, PipelineSummary)
        return cls(
            pipeline=PipelineResponse.from_summary(summary),
            tasks=[TaskResponse.from_definition(task) for task in pipeline.tasks.values()],
            latest_task_runs=[TaskRunResponse.from_record(item) for item in latest_task_runs],
            recent_runs=[RunResponse.from_record(item) for item in recent_runs],
        )


class DashboardStatsResponse(BaseModel):
    """DashboardStatsResponse exposes aggregate counts for the dashboard."""

    total_pipelines: int
    scheduled_pipelines: int
    total_runs: int
    running_runs: int
    successful_runs: int
    failed_runs: int
    success_rate: float

    @classmethod
    def from_stats(cls, stats: DashboardStats) -> "DashboardStatsResponse":
        """Convert DashboardStats to an API response."""
        return cls(
            total_pipelines=stats.total_pipelines,
            scheduled_pipelines=stats.scheduled_pipelines,
            total_runs=stats.total_runs,
            running_runs=stats.running_runs,
            successful_runs=stats.successful_runs,
            failed_runs=stats.failed_runs,
            success_rate=stats.success_rate,
        )


class SchedulerResponse(BaseModel):
    """SchedulerResponse exposes scheduler metadata for the dashboard."""

    running: bool
    heartbeat: str | None = None
    config_path: str
    database_path: str


class DashboardResponse(BaseModel):
    """DashboardResponse is the full payload returned by /api/dashboard."""

    project: dict[str, str]
    stats: DashboardStatsResponse
    pipelines: list[PipelineResponse]
    recent_runs: list[RunResponse]
    scheduler: SchedulerResponse


class RunDetailResponse(BaseModel):
    """RunDetailResponse adds task runs and logs to one run summary."""

    run: RunResponse
    task_runs: list[TaskRunResponse]
    logs: list[LogResponse]
