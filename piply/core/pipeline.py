from typing import Dict, List, Optional
from piply.core.context import PipelineContext
from piply.utils.logging import logger


class Pipeline:
    def __init__(self, name: str, steps: List, tenants: Optional[List[str]] = None, config: Optional[Dict] = None):
        self.name = name
        self.steps = steps
        self.tenants = tenants or []
        self.config = config or {}
        self.context = PipelineContext(pipeline_name=name)

    def run(self, engine, tenant: Optional[str] = None, retry_failed: bool = False):
        """
        Run the pipeline with the given engine.

        Args:
            engine: The execution engine (Prefect, Local, etc.)
            tenant: Optional specific tenant to run for (overrides multi-tenant loop)
            retry_failed: If True, only retry failed steps
        """
        logger.info(f"Starting pipeline: {self.name}")

        if tenant:
            # Run for specific tenant only
            self.context.tenant = tenant
            logger.info(f"Running for tenant: {tenant}")
            engine.run(self, retry_failed=retry_failed)
        elif self.tenants:
            # Multi-tenant execution
            for tenant_name in self.tenants:
                logger.info(f"\n{'='*60}")
                logger.info(
                    f"Running pipeline '{self.name}' for tenant: {tenant_name}")
                logger.info(f"{'='*60}")
                self.context.tenant = tenant_name
                # Reset context for each tenant
                self.context.step_outputs.clear()
                engine.run(self, retry_failed=retry_failed)
        else:
            # Single execution (no tenants)
            engine.run(self, retry_failed=retry_failed)

        logger.info(f"Pipeline '{self.name}' completed")
