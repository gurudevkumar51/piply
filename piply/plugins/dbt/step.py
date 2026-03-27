"""DBT step implementation for Piply."""
import subprocess
import logging
from typing import Any, Dict, Optional
from piply.core.step import Step
from piply.core.context import PipelineContext

logger = logging.getLogger(__name__)


class DbtStep(Step):
    """
    A step that executes dbt commands.
    
    Configuration:
        command: dbt command to run (run, test, seed, etc.)
        models: list of models to run
        select: dbt selection criteria
        vars: variables to pass to dbt
        profiles_dir: path to profiles directory
        project_dir: path to dbt project directory
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.command = config.get("command", "run")
        self.models = config.get("models", [])
        self.select = config.get("select")
        self.vars = config.get("vars", {})
        self.profiles_dir = config.get("profiles_dir")
        self.project_dir = config.get("project_dir", ".")
    
    def _execute(self, context: PipelineContext) -> Dict[str, Any]:
        """Execute the dbt command."""
        cmd = ["dbt", self.command]
        
        # Add project directory
        if self.project_dir:
            cmd.extend(["--project-dir", self.project_dir])
        
        # Add profiles directory
        if self.profiles_dir:
            cmd.extend(["--profiles-dir", self.profiles_dir])
        
        # Add models
        if self.models:
            for model in self.models:
                cmd.extend(["--models", model])
        
        # Add select
        if self.select:
            cmd.extend(["--select", self.select])
        
        # Add vars
        if self.vars:
            import json
            cmd.extend(["--vars", json.dumps(self.vars)])
        
        logger.info(f"Executing dbt command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            logger.info(f"dbt output: {result.stdout}")
            if result.stderr:
                logger.warning(f"dbt stderr: {result.stderr}")
            
            return {
                "status": "success",
                "stdout": result.stdout,
                "stderr": result.stderr
            }
        except subprocess.CalledProcessError as e:
            logger.error(f"dbt command failed: {e.stderr}")
            raise RuntimeError(f"dbt command failed: {e.stderr}") from e
