"""Background scheduler loop for time-based pipeline launches."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from .service import PipelineService


class PipelineScheduler:
    """PipelineScheduler polls schedules and launches due pipeline runs."""

    def __init__(self, service: PipelineService, poll_interval: int | None = None) -> None:
        self.service = service
        resolved_poll_interval = poll_interval or service.settings.scheduler_poll_interval_seconds
        self.poll_interval = max(2, resolved_poll_interval)
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

    def tick(self, now: datetime | None = None) -> None:
        """Evaluate due schedules and launch eligible pipeline runs."""
        current = now or datetime.now(timezone.utc)
        self.service.store.set_meta("scheduler_heartbeat", current.isoformat())
        self.service.reconcile_runtime_health()
        self.service.reload_project()
        self.service.enqueue_due_schedules(now=current)
        self.service.poll_sensors(now=current)
        for _ in range(self.service.settings.queue_dispatch_batch_size):
            dispatched = self.service.drain_trigger_queue(
                now=current,
                limit=self.service.settings.queue_dispatch_batch_size,
            )
            if not dispatched:
                break
