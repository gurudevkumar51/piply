import os
import sys
from typing import Dict, List
from piply.core.context import PipelineContext
from piply.utils.logging import logger

# Configure Prefect to run in local mode BEFORE any Prefect imports
# These environment variables must be set before Prefect is imported anywhere
os.environ["PREFECT_TEST_MODE"] = "1"
os.environ["PREFECT_PROFILE"] = "test"
os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"
os.environ["PREFECT_API_KEY"] = ""
# Disable SSL verification for local mode
os.environ["PREFECT_SSL_VERIFY"] = "0"
# Use ephemeral server (no persistence needed)
os.environ["PREFECT_SERVER_ALLOW_EPHEMERAL_MODE"] = "1"


class PrefectEngine:
    """Prefect execution engine (runs in local/test mode)."""

    def run(self, pipeline, retry_failed: bool = False):
        """
        Execute the pipeline as a Prefect flow with DAG support.

        Args:
            pipeline: The pipeline to execute
            retry_failed: If True, only run steps that previously failed
        """
        logger.info(
            f"Initializing Prefect engine for pipeline: {pipeline.name}")

        # Import prefect here after environment is configured
        try:
            from prefect import flow, task
        except ImportError as e:
            logger.error(f"Prefect is not installed: {e}")
            raise ImportError("Please install prefect: pip install prefect")

        # Build step name to step mapping
        step_map = {step.name: step for step in pipeline.steps}

        # Validate dependencies exist
        for step in pipeline.steps:
            if step.depends_on:
                for dep in step.depends_on:
                    if dep not in step_map:
                        raise ValueError(
                            f"Step '{step.name}' depends on '{dep}' which does not exist")

        # Create Prefect tasks for each step
        tasks = {}
        for step in pipeline.steps:
            # Wrap step.run as a Prefect task with proper context handling
            @task(name=step.name, retries=step.retries, retry_delay_seconds=step.retry_delay)
            def create_task(step_obj=step, ctx: PipelineContext = pipeline.context):
                return step_obj.run(ctx)

            tasks[step.name] = create_task

        # Build the flow with dependencies
        @flow(name=pipeline.name, retries=pipeline.config.get("retries", 0))
        def pipeline_flow():
            # Track which tasks have been submitted
            submitted = set()
            results = {}

            # Simple topological execution (respects dependencies)
            def execute_step(step_name: str):
                if step_name in submitted:
                    return results[step_name]

                step = step_map[step_name]

                # Check if we should skip (for retry_failed mode)
                if retry_failed:
                    prev_output = pipeline.context.get_output(step_name)
                    if prev_output and prev_output.success:
                        logger.info(
                            f"Skipping '{step_name}' (already succeeded)")
                        submitted.add(step_name)
                        results[step_name] = prev_output
                        return prev_output

                # Execute dependencies first
                if step.depends_on:
                    logger.info(
                        f"Step '{step_name}' depends on: {step.depends_on}")
                    for dep in step.depends_on:
                        if dep not in submitted:
                            execute_step(dep)

                # Now execute this step
                logger.info(f"Executing step: {step_name}")
                task_fn = tasks[step_name]
                result = task_fn()
                results[step_name] = result
                submitted.add(step_name)
                return result

            # Execute all steps (in dependency order)
            for step in pipeline.steps:
                if step.name not in submitted:
                    execute_step(step.name)

        # Run the flow
        logger.info("Starting Prefect flow execution")
        pipeline_flow()
        logger.info("Prefect flow execution completed")
