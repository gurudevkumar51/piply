"""A release gate for the documented run limit; uses disposable, held runs."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from piply.core.service import PipelineService
from piply.engine.base import BaseEngine


class HoldingEngine(BaseEngine):
    def dispatch(self, pipeline, run, store, **kwargs):
        pass

    def cancel(self, run_id):
        return False


def main() -> None:
    for key in list(os.environ):
        if key.startswith("PIPLY_"):
            del os.environ[key]
    results = {}
    with tempfile.TemporaryDirectory(prefix="piply-admission-") as folder:
        root = Path(folder)
        config = root / "piply.yaml"
        config.write_text(
            "version: '1'\nworkspace: .\npipelines:\n"
            "  one:\n    max_concurrent_runs: 1\n    tasks:\n"
            "      step: {type: cli, command: echo unused}\n"
            "  two:\n    max_concurrent_runs: 2\n    tasks:\n"
            "      step: {type: cli, command: echo unused}\n",
            encoding="utf-8",
        )
        for case in ("manual", "task", "retry", "queue"):
            service = PipelineService(config_path=config, database_path=root / f"{case}.db", engine=HoldingEngine())
            if case == "queue":
                for key in ("first", "second"):
                    service.enqueue_pipeline_trigger("two", trigger="schedule", dedupe_key=key)
                service.drain_trigger_queue()
                count = service.store.count_running_runs("two")
                results[case] = {"expected": 2, "active": count, "passed": count == 2}
                continue
            first = service.trigger_pipeline("one")
            if case == "retry":
                service.store.finish_run(first.run_id, status="failed")
                service.trigger_pipeline("one")
            try:
                if case == "manual":
                    service.trigger_pipeline("one")
                elif case == "task":
                    service.trigger_task("one", "step")
                else:
                    service.retry_run(first.run_id, mode="startover")
            except (ValueError, RuntimeError):
                pass  # Refusing an over-limit direct launch also satisfies admission.
            count = service.store.count_running_runs("one")
            results[case] = {"expected": 1, "active": count, "passed": count == 1}
    print(json.dumps({"checks": results, "passed": all(item["passed"] for item in results.values())}, indent=2))
    if not all(item["passed"] for item in results.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
