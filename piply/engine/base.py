from typing import Optional


class Engine:
    def run(self, pipeline, retry_failed: bool = False):
        """
        Execute a pipeline.

        Args:
            pipeline: The pipeline to execute
            retry_failed: If True, only retry previously failed steps
        """
        raise NotImplementedError
