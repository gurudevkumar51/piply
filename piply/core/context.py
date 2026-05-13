"""In-memory execution context for passing task outputs through a run."""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator, MutableMapping
from typing import Any


class RuntimeTaskContext(MutableMapping[str, Any]):
    """Thread-safe mapping exposed to Python call tasks as ``context``."""

    def __init__(self, initial: dict[str, Any] | None = None) -> None:
        self._values: dict[str, Any] = dict(initial or {})
        self._lock = threading.RLock()

    def __getitem__(self, key: str) -> Any:
        with self._lock:
            return self._values[key]

    def __setitem__(self, key: str, value: Any) -> None:
        with self._lock:
            self._values[key] = value

    def __delitem__(self, key: str) -> None:
        with self._lock:
            del self._values[key]

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            return iter(tuple(self._values))

    def __len__(self) -> int:
        with self._lock:
            return len(self._values)

    def snapshot(self) -> dict[str, Any]:
        """Return a shallow copy suitable for one task invocation."""
        with self._lock:
            return dict(self._values)

    def set_task_output(self, task_id: str, output: Any) -> None:
        """Store the output for a completed task."""
        self[task_id] = output

    def json_safe_snapshot(self) -> dict[str, Any]:
        """Return only values that can be safely represented as JSON."""
        with self._lock:
            items = list(self._values.items())

        safe: dict[str, Any] = {}
        for key, value in items:
            try:
                json.dumps(value)
            except (TypeError, ValueError):
                continue
            safe[key] = value
        return safe

    def to_env_json(self, *, max_chars: int = 60_000) -> str | None:
        """Render a bounded JSON context for subprocess tasks."""
        safe = self.json_safe_snapshot()
        if not safe:
            return None
        rendered = json.dumps(safe, ensure_ascii=False, sort_keys=True)
        if len(rendered) > max_chars:
            return None
        return rendered
