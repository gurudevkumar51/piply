"""API response and request models for the FastAPI surface."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from piply.core.models import (
    DashboardStats,
    LogRecord,
    PipelineDefinition,
    PipelineSummary,
    RunRecord,
    TaskDefinition,
    TaskOutputRecord,
    TaskRunRecord,
)


def _format_time(value: datetime) -> str:
    """Render a log timestamp in the short UI-friendly format."""
    return value.astimezone().strftime("%H:%M:%S.%f")[:-3]


class RetryRequest(BaseModel):
    """RetryRequest selects retry mode and the optionally clicked task."""

    mode: str
    task_id: str | None = None


class TriggerRunRequest(BaseModel):
    """TriggerRunRequest carries optional UI-provided CLI command overrides."""

    command_overrides: dict[str, str] = Field(default_factory=dict)
    params: dict[str, object] = Field(default_factory=dict)
    tenant_id: str | None = None
    #: Values for `{placeholder}` variables the config does not supply, used
    #: when running a normally-downstream pipeline by hand. They are applied the
    #: same way an upstream pipeline's variables are, so nothing new has to
    #: understand them.
    variables: dict[str, str] = Field(default_factory=dict)


class PreviewRequest(BaseModel):
    """PreviewRequest carries the values a dry run should resolve against."""

    params: dict[str, object] = Field(default_factory=dict)
    command_overrides: dict[str, str] = Field(default_factory=dict)
    tenant_id: str | None = None
    source_run_id: str | None = None


class BackfillScheduleRequest(BaseModel):
    """BackfillScheduleRequest selects the historic window to materialize."""

    start: datetime
    end: datetime
    limit: int = Field(default=200, ge=1, le=1000)


class PruneRequest(BaseModel):
    """PruneRequest optionally overrides the configured retention window."""

    run_retention_days: int | None = Field(default=None, ge=0)
    log_retention_days: int | None = Field(default=None, ge=0)
    max_runs_per_pipeline: int | None = Field(default=None, ge=0)
    dry_run: bool = False
    vacuum: bool = True


class SmtpSettingsRequest(BaseModel):
    """Central SMTP configuration submitted by an administrator."""

    host: str | None = None
    port: int | None = Field(default=None, ge=1, le=65535)
    username: str | None = None
    #: Omit to keep the stored password; it is never returned by the API.
    password: str | None = None
    from_address: str | None = None
    use_tls: bool | None = None
    use_ssl: bool | None = None
    timeout_seconds: int | None = Field(default=None, ge=1, le=300)


class SmtpTestRequest(BaseModel):
    """Where to send a test message."""

    recipient: str


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
    parent_run_id: str | None = None
    parent_pipeline_id: str | None = None
    tenant_id: str | None = None

    @classmethod
    def from_record(cls, record: RunRecord) -> RunResponse:
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
            parent_run_id=record.parent_run_id,
            parent_pipeline_id=record.parent_pipeline_id,
            tenant_id=record.tenant_id,
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
    on_upstream_failure: str
    priority: int = 0
    timeout_seconds: int | None = None
    kill_grace_period_seconds: int = 5
    run_if: str | None = None
    artifact_paths: list[str] = Field(default_factory=list)
    shell: str | None = None
    template_id: str | None = None
    entity_key: str | None = None
    entity_values: dict[str, str] = Field(default_factory=dict)

    @classmethod
    def from_definition(cls, definition: TaskDefinition) -> TaskResponse:
        """Convert a TaskDefinition to an API response."""
        return cls(
            task_id=definition.task_id,
            title=definition.title,
            task_type=definition.task_type,
            description=definition.description,
            depends_on=list(definition.depends_on),
            enabled=definition.enabled,
            command_preview=definition.command_preview,
            on_upstream_failure=definition.on_upstream_failure,
            priority=definition.priority,
            timeout_seconds=definition.timeout_seconds,
            kill_grace_period_seconds=definition.kill_grace_period_seconds,
            run_if=definition.run_if,
            artifact_paths=list(definition.artifact_paths),
            shell=definition.shell,
            template_id=definition.template_id,
            entity_key=definition.entity_key,
            entity_values=definition.entity_values,
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
    priority: int = 0
    timeout_seconds: int | None = None
    run_if: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    exit_code: int | None = None
    error: str | None = None
    depends_on: list[str]
    log_count: int = 0
    duration_seconds: float | None = None
    output_type: str | None = None
    output_preview: str | None = None
    output_is_json: bool = False

    @classmethod
    def from_record(cls, record: TaskRunRecord) -> TaskRunResponse:
        """Convert one TaskRunRecord to an API response."""
        return cls(
            run_id=record.run_id,
            task_id=record.task_id,
            title=record.title,
            task_type=record.task_type,
            status=record.status,
            position=record.position,
            command_preview=record.command_preview,
            priority=record.priority,
            timeout_seconds=record.timeout_seconds,
            run_if=record.run_if,
            started_at=record.started_at,
            finished_at=record.finished_at,
            exit_code=record.exit_code,
            error=record.error,
            depends_on=list(record.depends_on),
            log_count=record.log_count,
            duration_seconds=record.duration_seconds,
            output_type=record.output_type,
            output_preview=record.output_preview,
            output_is_json=record.output_is_json,
        )


class TaskOutputResponse(BaseModel):
    """TaskOutputResponse exposes persisted output metadata and optional JSON."""

    run_id: str
    task_id: str
    output_type: str
    preview: str
    is_json: bool
    json_value: object | None = None
    metadata: dict[str, object]
    size_bytes: int
    created_at: datetime | None = None

    @classmethod
    def from_record(cls, record: TaskOutputRecord) -> TaskOutputResponse:
        """Convert one TaskOutputRecord to an API response."""
        import json

        decoded = None
        if record.is_json and record.json_value is not None:
            decoded = json.loads(record.json_value)
        return cls(
            run_id=record.run_id,
            task_id=record.task_id,
            output_type=record.output_type,
            preview=record.preview,
            is_json=record.is_json,
            json_value=decoded,
            metadata=record.metadata,
            size_bytes=record.size_bytes,
            created_at=record.created_at,
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
    def from_record(cls, record: LogRecord) -> LogResponse:
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
    template_id: str | None = None
    deployment_id: str | None = None
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
    retry_summary: str
    last_run: RunResponse | None = None
    #: Newest first. Drives the run-history dots on the pipeline listing.
    recent_runs: list[RunResponse] = Field(default_factory=list)

    @classmethod
    def from_summary(cls, summary: PipelineSummary) -> PipelineResponse:
        """Convert one PipelineSummary to an API response."""
        return cls(
            pipeline_id=summary.pipeline_id,
            title=summary.title,
            description=summary.description,
            template_id=summary.template_id,
            deployment_id=summary.deployment_id,
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
            retry_summary=summary.retry_summary,
            last_run=RunResponse.from_record(summary.last_run) if summary.last_run else None,
            recent_runs=[RunResponse.from_record(item) for item in summary.recent_runs],
        )


class PipelineDetailResponse(BaseModel):
    """PipelineDetailResponse adds tasks and recent runs to one pipeline summary."""

    pipeline: PipelineResponse
    tasks: list[TaskResponse]
    latest_task_runs: list[TaskRunResponse]
    recent_runs: list[RunResponse]

    @classmethod
    def from_payload(cls, payload: dict[str, object]) -> PipelineDetailResponse:
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
    def from_stats(cls, stats: DashboardStats) -> DashboardStatsResponse:
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
    state: str | None = None
    label: str | None = None
    heartbeat: str | None = None
    heartbeat_age_seconds: float | None = None
    owner_pid: int | None = None
    owner_alive: bool | None = None
    started_at: str | None = None
    last_error: str | None = None
    config_path: str
    database_path: str
    queue_depth: int | None = None
    sensor_count: int | None = None
    accepting_work: bool | None = None
    queue_metrics: dict[str, object] = Field(default_factory=dict)
    worker_metrics: dict[str, object] = Field(default_factory=dict)


class UpcomingRunResponse(BaseModel):
    """UpcomingRunResponse previews one future scheduled slot."""

    scheduled_for: datetime
    label: str


class DashboardResponse(BaseModel):
    """DashboardResponse is the full payload returned by /api/dashboard."""

    project: dict[str, str]
    stats: DashboardStatsResponse
    pipelines: list[PipelineResponse]
    recent_runs: list[RunResponse]
    recent_failures: list[RunResponse] = Field(default_factory=list)
    active_pipelines: list[PipelineResponse] = Field(default_factory=list)
    runtime_trend: list[dict[str, object]] = Field(default_factory=list)
    runtime_metrics: dict[str, object] = Field(default_factory=dict)
    scheduler: SchedulerResponse


class RunDetailResponse(BaseModel):
    """RunDetailResponse adds task runs, logs, and pipeline lineage to one run."""

    run: RunResponse
    task_runs: list[TaskRunResponse]
    logs: list[LogResponse]
    upcoming_runs: list[UpcomingRunResponse] = Field(default_factory=list)
    downstream: list[dict[str, object]] = Field(default_factory=list)
    upstream: dict[str, object] | None = None
    artifacts: list[dict[str, object]] = Field(default_factory=list)
    has_run_config: bool = False


class TaskDetailResponse(BaseModel):
    """TaskDetailResponse exposes one task run, logs, and captured output."""

    run: RunResponse
    task_run: TaskRunResponse
    logs: list[LogResponse]
    output: TaskOutputResponse | None = None


class MatrixCellResponse(BaseModel):
    """One task/run status cell for the execution matrix."""

    run_id: str
    task_id: str
    status: str
    duration_seconds: float | None = None
    log_count: int = 0
    error: str | None = None
    output_preview: str | None = None


class MatrixRowResponse(BaseModel):
    """One task row in the execution matrix."""

    task: TaskResponse
    cells: list[MatrixCellResponse]


class ExecutionMatrixResponse(BaseModel):
    """Execution matrix payload for the grid view."""

    pipelines: list[PipelineResponse]
    selected_pipeline_id: str | None
    runs: list[RunResponse]
    rows: list[MatrixRowResponse]
    trend: list[dict[str, object]]
    filters: dict[str, object] = Field(default_factory=dict)
