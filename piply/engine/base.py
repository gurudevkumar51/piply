"""Execution engine interface shared by asynchronous and local backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable

from piply.core.models import PipelineDefinition, RunRecord
from piply.core.store import RunStore


LogCallback = Callable[[str], None]
CompletionCallback = Callable[[PipelineDefinition, RunRecord], None]


class BaseEngine(ABC):
    """Execution backends implement this interface."""

    @abstractmethod
    def dispatch(
        self,
        pipeline: PipelineDefinition,
        run: RunRecord,
        store: RunStore,
        *,
        wait: bool = False,
        on_log: LogCallback | None = None,
        on_success: CompletionCallback | None = None,
        on_failure: CompletionCallback | None = None,
        initial_task_statuses: dict[str, str] | None = None,
        retry_source_run_id: str | None = None,
    ) -> None:
        """Execute a pipeline run either asynchronously or in-process."""

    @abstractmethod
    def cancel(self, run_id: str) -> bool:
        """Request cancellation for one running pipeline."""
