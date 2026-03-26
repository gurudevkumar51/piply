import time
from typing import Any, Optional
from piply.core.context import PipelineContext, StepOutput
from piply.utils.logging import logger


class Step:
    def __init__(self, name: str, config: dict):
        self.name = name
        self.config = config
        self.retries = config.get("retries", 0)
        self.retry_delay = config.get("retry_delay", 60)
        self.timeout = config.get("timeout")
        self.depends_on = config.get("depends_on", [])

    def run(self, context: PipelineContext) -> StepOutput:
        """
        Execute the step with retry logic.

        Args:
            context: The pipeline execution context

        Returns:
            StepOutput with execution results
        """
        attempt = 0
        last_error = None

        while attempt <= self.retries:
            try:
                logger.info(
                    f"Executing step '{self.name}' (attempt {attempt + 1})")
                result = self._execute(context)
                output = StepOutput(
                    step_name=self.name,
                    success=True,
                    result=result,
                    metadata={"attempts": attempt + 1}
                )
                context.set_output(self.name, output)
                logger.info(f"Step '{self.name}' completed successfully")
                return output

            except Exception as e:
                last_error = e
                attempt += 1
                if attempt <= self.retries:
                    logger.warning(
                        f"Step '{self.name}' failed: {e}. Retrying in {self.retry_delay}s...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(
                        f"Step '{self.name}' failed after {self.retries} retries: {e}")

        # All retries exhausted
        output = StepOutput(
            step_name=self.name,
            success=False,
            error=str(last_error),
            metadata={"attempts": attempt}
        )
        context.set_output(self.name, output)
        logger.error(f"Step '{self.name}' failed permanently")
        return output

    def _execute(self, context: PipelineContext) -> Any:
        """
        Actual step execution logic. Override in subclasses.

        Args:
            context: The pipeline execution context

        Returns:
            Step result (can be any type)
        """
        raise NotImplementedError(
            f"Step '{self.name}' must implement _execute method")
