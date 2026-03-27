"""
Pipeline service layer.
"""
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from datetime import datetime

from ..database import PipelineRun, TaskRun, RunStatus
from ..schemas import PipelineResponse, PipelineDetail, StepInfo
from ..utils.pipeline_finder import PipelineDiscovery


class PipelineService:
    """Business logic for pipeline management."""

    def __init__(self, db: Session, pipeline_discovery: PipelineDiscovery = None):
        self.db = db
        self.discovery = pipeline_discovery or PipelineDiscovery()

    def list_pipelines(self) -> List[PipelineResponse]:
        """
        List all pipelines, combining database history and filesystem discovery.
        Returns pipelines with their metadata and statistics.
        """
        result = []

        # Get pipelines from database (that have been run)
        db_pipelines = self.db.query(
            PipelineRun.pipeline_name,
            PipelineRun.tenant,
            PipelineRun.status,
            PipelineRun.started_at,
            PipelineRun.completed_at
        ).group_by(PipelineRun.pipeline_name).all()

        # Build a map of pipeline stats from database
        pipeline_stats = {}
        for p in db_pipelines:
            if p.pipeline_name not in pipeline_stats:
                pipeline_stats[p.pipeline_name] = {
                    "run_count": 0,
                    "last_run": None,
                    "tenants": set()
                }
            pipeline_stats[p.pipeline_name]["run_count"] += 1
            if p.last_run is None or (pipeline_stats[p.pipeline_name]["last_run"] and p.started_at > pipeline_stats[p.pipeline_name]["last_run"]):
                pipeline_stats[p.pipeline_name]["last_run"] = p.started_at
            if p.tenant:
                pipeline_stats[p.pipeline_name]["tenants"].add(p.tenant)

        # Add database pipelines to result
        for name, stats in pipeline_stats.items():
            result.append(PipelineResponse(
                id=hash(name) % (10**9),
                name=name,
                steps_count=0,  # Will be populated from YAML
                tenants=list(stats["tenants"]),
                schedule=None,
                run_count=stats["run_count"],
                last_run=stats["last_run"],
                created_at=stats["last_run"] or datetime.utcnow()
            ))

        # Discover additional pipelines from YAML files
        discovered = self.discovery.find_all_pipelines()
        for p in discovered:
            # Skip if already in result
            if any(r.name == p["name"] for r in result):
                continue

            result.append(PipelineResponse(
                id=hash(p["name"]) % (10**9),
                name=p["name"],
                steps_count=p["steps_count"],
                tenants=p["tenants"],
                schedule=p["schedule"],
                run_count=0,
                last_run=None,
                created_at=datetime.utcnow()
            ))

        return result

    def get_pipeline(self, pipeline_name: str) -> PipelineDetail:
        """
        Get detailed information about a specific pipeline.
        Loads pipeline definition from YAML and includes recent runs.
        """
        # Find pipeline in YAML files
        pipeline_data = self.discovery.find_pipeline_by_name(pipeline_name)
        if not pipeline_data:
            raise ValueError(f"Pipeline '{pipeline_name}' not found")

        # Get recent runs from database
        recent_runs = self.db.query(PipelineRun).filter(
            PipelineRun.pipeline_name == pipeline_name
        ).order_by(PipelineRun.started_at.desc()).limit(10).all()

        # Convert steps to StepInfo objects
        steps = [StepInfo(**s) for s in pipeline_data["steps"]]

        return PipelineDetail(
            name=pipeline_name,
            config=pipeline_data["config"],
            steps=steps,
            tenants=pipeline_data["tenants"],
            recent_runs=recent_runs
        )
