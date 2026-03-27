"""
Pipeline-related API routes.
"""
from typing import List
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..schemas import PipelineResponse, PipelineDetail
from ..services.pipeline_service import PipelineService

router = APIRouter(prefix="/api/pipelines", tags=["pipelines"])


@router.get("", response_model=List[PipelineResponse])
def list_pipelines(db: Session = Depends(get_db)):
    """
    List all pipelines.
    Returns both pipelines that have been run (from database) and
    those discovered from YAML files.
    """
    service = PipelineService(db)
    try:
        return service.list_pipelines()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{pipeline_name}", response_model=PipelineDetail)
def get_pipeline(pipeline_name: str, db: Session = Depends(get_db)):
    """
    Get detailed information about a specific pipeline.
    Includes configuration, steps, tenants, and recent runs.
    """
    service = PipelineService(db)
    try:
        return service.get_pipeline(pipeline_name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
