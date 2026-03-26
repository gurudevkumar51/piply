from piply.engine.base import Engine
from typing import Optional


class LocalEngine(Engine):
    """
    Simple local sequential execution engine.
    Useful for testing or when Prefect is not available.
    """

    def run(self, pipeline, retry_failed: bool = False):
        """
        Execute pipeline steps sequentially (respects dependencies).

        Args:
            pipeline: The pipeline to execute
            retry_failed: If True, only run steps that previously failed
        """
        print(f"[LocalEngine] Running pipeline: {pipeline.name}")

        step_map = {step.name: step for step in pipeline.steps}

        # Validate dependencies
        for step in pipeline.steps:
            if step.depends_on:
                for dep in step.depends_on:
                    if dep not in step_map:
                        raise ValueError(
                            f"Step '{step.name}' depends on '{dep}' which does not exist")

        # Execute steps in order (respecting dependencies)
        submitted = set()

        def execute_step(step_name: str):
            if step_name in submitted:
                return

            step = step_map[step_name]

            # Check if we should skip (for retry_failed mode)
            if retry_failed:
                prev_output = pipeline.context.get_output(step_name)
                if prev_output and prev_output.success:
                    print(
                        f"[LocalEngine] Skipping '{step_name}' (already succeeded)")
                    submitted.add(step_name)
                    return

            # Execute dependencies first
            if step.depends_on:
                for dep in step.depends_on:
                    if dep not in submitted:
                        execute_step(dep)

            # Execute this step
            print(f"[LocalEngine] Executing step: {step_name}")
            result = step.run(pipeline.context)
            submitted.add(step_name)

            if not result.success:
                raise RuntimeError(
                    f"Step '{step_name}' failed: {result.error}")

        # Execute all steps
        for step in pipeline.steps:
            if step.name not in submitted:
                execute_step(step.name)

        print(f"[LocalEngine] Pipeline completed successfully")
