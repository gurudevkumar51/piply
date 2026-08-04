"""Background scheduler loop for time-based pipeline launches."""

from __future__ import annotations

import os
import threading
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
        """Start the scheduler thread when it is not already running.

        Taking ownership also reconciles anything the previous scheduler left
        behind, so a restart after a crash never leaves orphaned RUNNING rows.
        """
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        recovered = self.service.recover_interrupted_executions()
        current = datetime.now(timezone.utc)
        self.service.store.set_meta_many(
            {
                "scheduler_last_error": "",
                "scheduler_owner_pid": str(os.getpid()),
                "scheduler_started_at": current.isoformat(),
                "scheduler_recovered_runs": str(len(recovered)),
                "scheduler_heartbeat": current.isoformat(),
                "scheduler_running": "true",
                "scheduler_state": "running",
            }
        )
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="piply-scheduler",
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the scheduler thread and update the heartbeat flag."""
        self._stop_event.set()
        current = datetime.now(timezone.utc)
        self.service.store.set_meta_many(
            {
                "scheduler_heartbeat": current.isoformat(),
                "scheduler_running": "false",
                "scheduler_state": "stopped",
            }
        )
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def _run_loop(self) -> None:
        """Poll for due schedules until the scheduler is stopped."""
        while not self._stop_event.is_set():
            try:
                self.tick()
            except Exception as exc:  # pragma: no cover - defensive thread crash handling
                # Written as one transaction so a reader never sees "crashed"
                # without the reason that caused it.
                self.service.store.set_meta_many(
                    {
                        "scheduler_last_error": str(exc) or exc.__class__.__name__,
                        "scheduler_heartbeat": datetime.now(timezone.utc).isoformat(),
                        "scheduler_running": "false",
                        "scheduler_state": "crashed",
                    }
                )
                break
            self._stop_event.wait(self.poll_interval)

    def tick(self, now: datetime | None = None) -> None:
        """Evaluate due schedules and launch eligible pipeline runs."""
        current = now or datetime.now(timezone.utc)
        self.service.store.set_meta("scheduler_state", "running")
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
