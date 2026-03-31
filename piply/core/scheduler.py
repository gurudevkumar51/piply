"""Background scheduler loop for time-based pipeline launches."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from .service import PipelineService


class PipelineScheduler:
    """PipelineScheduler polls schedules and launches due pipeline runs."""

    def __init__(self, service: PipelineService, poll_interval: int = 10) -> None:
        self.service = service
        self.poll_interval = max(2, poll_interval)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the scheduler thread when it is not already running."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self.service.store.set_meta("scheduler_running", "true")
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="piply-scheduler",
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the scheduler thread and update the heartbeat flag."""
        self._stop_event.set()
        self.service.store.set_meta("scheduler_running", "false")
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def _run_loop(self) -> None:
        """Poll for due schedules until the scheduler is stopped."""
        while not self._stop_event.is_set():
            self.tick()
            self._stop_event.wait(self.poll_interval)

    def tick(self) -> None:
        """Evaluate due schedules and launch eligible pipeline runs."""
        now = datetime.now(timezone.utc)
        self.service.store.set_meta("scheduler_heartbeat", now.isoformat())
        self.service.reconcile_runtime_health()
        self.service.reload_project()

        for summary in self.service.list_pipelines():
            if not summary.enabled or summary.paused:
                continue
            pipeline = self.service.get_pipeline(summary.pipeline_id)
            if pipeline.schedule is None:
                continue
            if self.service.store.count_running_runs(pipeline.pipeline_id) >= pipeline.max_concurrent_runs:
                continue

            slot = pipeline.schedule.current_slot(now)
            if slot is None:
                continue
            if self.service.store.has_run_for_slot(pipeline.pipeline_id, slot):
                continue

            self.service.trigger_pipeline(
                pipeline.pipeline_id,
                trigger="schedule",
                scheduled_for=slot,
                wait=False,
            )
