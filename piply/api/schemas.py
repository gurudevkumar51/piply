"""
Pydantic schemas for API requests/responses.
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from piply.core.models import PipelineConfig


# Pipeline schemas
class PipelineCreate(BaseModel):
    """Create a new pipeline configuration."""
    name: str
    steps: List[Dict[str, Any]]
    tenants: Optional[List[str]] = None
    schedule: Optional[str] = None
    retries: int = 0
    timeout: Optional[int] = None
    variables: Dict[str, Any] = Field(default_factory=dict)


class PipelineResponse(BaseModel):
    """Pipeline metadata response."""
    id: int
    name: str
    steps_count: int
    tenants: Optional[List[str]]
    schedule: Optional[str]
    created_at: datetime
    run_count: int = 0
    last_run: Optional[datetime] = None

    class Config:
        from_attributes = True


class StepInfo(BaseModel):
    """Step information with dependencies."""
    name: str
    type: Optional[str]
    depends_on: List[str] = []


class PipelineDetail(BaseModel):
    """Detailed pipeline information."""
    name: str
    config: Dict[str, Any]
    steps: List[StepInfo]
    tenants: List[str]
    recent_runs: List["RunResponse"]


# Run schemas
class RunCreate(BaseModel):
    """Request to create a new pipeline run."""
    pipeline_name: str
    tenant: Optional[str] = None
    trigger_type: str = "manual"
    triggered_by: Optional[str] = None
    retry_of: Optional[int] = None
    config_overrides: Optional[Dict[str, Any]] = None


class RunResponse(BaseModel):
    """Pipeline run response."""
    id: int
    pipeline_name: str
    tenant: Optional[str]
    status: str
    started_at: datetime
    completed_at: Optional[datetime]
    trigger_type: str
    task_count: int
    tasks_completed: int

    class Config:
        from_attributes = True


class RunRetryRequest(BaseModel):
    """Request to retry a pipeline run."""
    run_id: int
    mode: str = Field(..., description="'resume' or 'startover'")
    tenant: Optional[str] = None


# Task schemas
class TaskResponse(BaseModel):
    """Task/step response."""
    id: int
    task_name: str
    status: str
    started_at: datetime
    completed_at: Optional[datetime]
    attempt: int
    error_message: Optional[str]

    class Config:
        from_attributes = True


class TaskLogResponse(BaseModel):
    """Task log response."""
    logs: str
    task_name: str
    run_id: int


# Tenant schemas
class TenantResponse(BaseModel):
    """Tenant information."""
    name: str
    run_count: int
    last_run: Optional[datetime]

    class Config:
        from_attributes = True


# Dashboard schemas
class DashboardStats(BaseModel):
    """Overall dashboard statistics."""
    total_pipelines: int
    total_runs: int
    running: int
    success: int
    failed: int
    recent_runs: List[Dict[str, Any]]
    tenant_summary: List[Dict[str, Any]]


PipelineDetail.model_rebuild()
