"""Execution matrix and cross-run log APIs."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Query, Request

from piply.api.schemas import (
    ExecutionMatrixResponse,
    LogResponse,
    MatrixCellResponse,
    MatrixRowResponse,
    PipelineResponse,
    RunResponse,
    TaskResponse,
)

router = APIRouter(tags=["execution"])


def _get_service(request: Request):
    """Resolve the shared PipelineService from the app state."""
    return request.app.state.service


@router.get("/api/execution-matrix", response_model=ExecutionMatrixResponse)
def get_execution_matrix(
    request: Request,
    pipeline_id: str | None = None,
    tenant: str | None = None,
    status: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    limit: int = Query(default=24, ge=1, le=100),
) -> ExecutionMatrixResponse:
    """Return task-by-run matrix data for the grid UI."""
    payload = _get_service(request).execution_matrix(
        pipeline_id=pipeline_id,
        tenant_id=tenant,
        status=status,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
    )
    return ExecutionMatrixResponse(
        pipelines=[PipelineResponse.from_summary(item) for item in payload["pipelines"]],
        selected_pipeline_id=payload["selected_pipeline_id"],
        runs=[RunResponse.from_record(item) for item in payload["runs"]],
        rows=[
            MatrixRowResponse(
                task=TaskResponse.from_definition(row["task"]),
                cells=[MatrixCellResponse(**cell) for cell in row["cells"]],
            )
            for row in payload["rows"]
        ],
        trend=payload["trend"],
        filters=payload.get("filters", {}),
    )


@router.get("/api/logs", response_model=list[LogResponse])
def search_logs(
    request: Request,
    q: str | None = None,
    pipeline_id: str | None = None,
    task_id: str | None = None,
    limit: int = Query(default=300, ge=1, le=1000),
) -> list[LogResponse]:
    """Search recent logs across runs."""
    logs = _get_service(request).search_logs(
        query=q,
        pipeline_id=pipeline_id,
        task_id=task_id,
        limit=limit,
    )
    return [LogResponse.from_record(item) for item in logs]
