"""Background run heartbeat used to detect stale executions."""

from __future__ import annotations

import threading

from piply.core.store import RunStore


class RunHeartbeat:
    """Touch a run on a short interval while work is still executing."""

    def __init__(self, store: RunStore, run_id: str, interval_seconds: int) -> None:
        self.store = store
        self.run_id = run_id
        self.interval_seconds = max(2, interval_seconds)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the background heartbeat thread once."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name=f"piply-heartbeat-{self.run_id}",
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the heartbeat thread."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.interval_seconds)

    def _run_loop(self) -> None:
        """Touch the run until execution ends."""
        while not self._stop_event.is_set():
            self.store.touch_run(self.run_id)
            self._stop_event.wait(self.interval_seconds)
