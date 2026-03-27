"""
Pipeline discovery utilities.
"""
import os
from typing import List, Optional
from piply.parser.yaml_parser import load_pipeline


class PipelineDiscovery:
    """Service for discovering pipeline definitions in the filesystem."""

    def __init__(self, search_paths: List[str] = None):
        self.search_paths = search_paths or [".", "examples"]

    def find_all_pipelines(self) -> List[dict]:
        """
        Scan search paths for YAML pipeline files and return basic info.
        Returns list of pipeline dicts with name, filepath, steps, tenants, schedule.
        """
        pipelines = []

        for search_dir in self.search_paths:
            if not os.path.exists(search_dir):
                continue

            for root, dirs, files in os.walk(search_dir):
                for filename in files:
                    if filename.endswith(('.yaml', '.yml')):
                        filepath = os.path.join(root, filename)
                        try:
                            pipeline_obj = load_pipeline(filepath)
                            pipeline_name = pipeline_obj.config.get(
                                "name", "unknown")
                            pipelines.append({
                                "name": pipeline_name,
                                "filepath": filepath,
                                "steps_count": len(pipeline_obj.steps),
                                "tenants": pipeline_obj.tenants,
                                "schedule": pipeline_obj.config.get("schedule")
                            })
                        except Exception:
                            # Skip files that can't be parsed
                            continue

        return pipelines

    def find_pipeline_by_name(self, pipeline_name: str) -> Optional[dict]:
        """
        Search for a specific pipeline by name.
        Returns pipeline dict with config, steps, tenants, filepath if found.
        """
        for search_dir in self.search_paths:
            if not os.path.exists(search_dir):
                continue

            for root, dirs, files in os.walk(search_dir):
                for filename in files:
                    if filename.endswith(('.yaml', '.yml')):
                        filepath = os.path.join(root, filename)
                        try:
                            pipeline_obj = load_pipeline(filepath)
                            obj_name = pipeline_obj.config.get("name")
                            if obj_name == pipeline_name:
                                return {
                                    "name": pipeline_name,
                                    "filepath": filepath,
                                    "config": pipeline_obj.config,
                                    "steps": [
                                        {
                                            "name": s.name,
                                            "type": s.config.get("type"),
                                            "depends_on": s.config.get("depends_on") or []
                                        }
                                        for s in pipeline_obj.steps
                                    ],
                                    "tenants": pipeline_obj.tenants
                                }
                        except Exception:
                            continue

        return None
