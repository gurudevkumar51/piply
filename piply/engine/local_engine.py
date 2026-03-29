"""Local execution engine with sequential and dependency-aware parallel modes."""

from __future__ import annotations

import threading
import traceback
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

from piply.core.graph import topological_order
from piply.core.models import PipelineDefinition, RunRecord, TaskDefinition
from piply.core.store import RunStore

from .base import BaseEngine, CompletionCallback, LogCallback
from .task_runner import TaskExecutionResult, TaskRunner


class LocalEngine(BaseEngine):
    """Run Piply task graphs with lightweight local workers."""

    def __init__(self) -> None:
        self._threads: dict[str, threading.Thread] = {}
        self._lock = threading.Lock()

    def dispatch(
        self,
        pipeline: PipelineDefinition,
        run: RunRecord,
        store: RunStore,
        *,
        wait: bool = False,
        on_log: LogCallback | None = None,
        on_success: CompletionCallback | None = None,
        initial_task_statuses: dict[str, str] | None = None,
        retry_source_run_id: str | None = None,
    ) -> None:
        """Execute a pipeline either inline or in a background thread."""
        if wait:
            self._execute(
                pipeline,
                run.run_id,
                store,
                on_log=on_log,
                on_success=on_success,
                initial_task_statuses=initial_task_statuses or {},
                retry_source_run_id=retry_source_run_id,
            )
            return

        thread = threading.Thread(
            target=self._execute,
            args=(pipeline, run.run_id, store),
            kwargs={
                "on_log": on_log,
                "on_success": on_success,
                "initial_task_statuses": initial_task_statuses or {},
                "retry_source_run_id": retry_source_run_id,
            },
            daemon=True,
            name=f"piply-run-{run.run_id}",
        )
        with self._lock:
            self._threads[run.run_id] = thread
        thread.start()

    def _execute(
        self,
        pipeline: PipelineDefinition,
        run_id: str,
        store: RunStore,
        *,
        on_log: LogCallback | None = None,
        on_success: CompletionCallback | None = None,
        initial_task_statuses: dict[str, str],
        retry_source_run_id: str | None,
    ) -> None:
        """Execute the full pipeline lifecycle for one run."""
        runner = TaskRunner(store=store, run_id=run_id, on_log=on_log)
        store.mark_running(run_id)
        runner.emit(f"Starting pipeline {pipeline.pipeline_id}")
        runner.emit(f"Flow: {pipeline.command_preview}")
        runner.emit(f"Execution mode: {pipeline.execution_summary}")
        if retry_source_run_id is not None:
            runner.emit(f"Retry source run: {retry_source_run_id}")

        task_statuses = dict(initial_task_statuses)
        for task_id in pipeline.tasks:
            if task_statuses.get(task_id) == "success":
                runner.emit(
                    "Skipping execution because this task succeeded in the selected retry source.",
                    task_id=task_id,
                )

        first_error: str | None = None

        try:
            if pipeline.execution_mode == "parallel" and pipeline.max_parallel_tasks > 1:
                first_error = self._execute_parallel(pipeline, run_id, store, runner, task_statuses)
            else:
                first_error = self._execute_sequential(pipeline, run_id, store, runner, task_statuses)

            failed_tasks = [
                task_id for task_id, status in task_statuses.items() if status == "failed"
            ]
            if failed_tasks:
                store.finish_run(run_id, status="failed", exit_code=1, error=first_error)
            else:
                store.finish_run(run_id, status="success", exit_code=0)
                runner.emit("Pipeline completed successfully.")
                final_run = store.get_run(run_id)
                if final_run is not None and on_success is not None:
                    on_success(pipeline, final_run)
        except Exception as exc:  # pragma: no cover - defensive path
            runner.emit(traceback.format_exc().rstrip())
            store.finish_run(run_id, status="failed", exit_code=1, error=str(exc))
        finally:
            with self._lock:
                self._threads.pop(run_id, None)

    def _execute_sequential(
        self,
        pipeline: PipelineDefinition,
        run_id: str,
        store: RunStore,
        runner: TaskRunner,
        task_statuses: dict[str, str],
    ) -> str | None:
        """Execute tasks one at a time in topological order."""
        first_error: str | None = None
        for task in topological_order(pipeline):
            if task.task_id in task_statuses:
                continue
            result = self._run_or_skip_task(task, run_id, store, runner, task_statuses)
            if result.status == "failed" and first_error is None:
                first_error = result.error or f"Task {task.task_id} failed"
        return first_error

    def _execute_parallel(
        self,
        pipeline: PipelineDefinition,
        run_id: str,
        store: RunStore,
        runner: TaskRunner,
        task_statuses: dict[str, str],
    ) -> str | None:
        """Execute ready tasks concurrently while respecting dependencies."""
        first_error: str | None = None
        pending = {task_id for task_id in pipeline.tasks if task_id not in task_statuses}
        in_flight: dict[Future[TaskExecutionResult], TaskDefinition] = {}

        with ThreadPoolExecutor(max_workers=pipeline.max_parallel_tasks) as executor:
            while pending or in_flight:
                scheduled_this_round = False

                for task_id in list(pending):
                    task = pipeline.tasks[task_id]
                    if any(dependency not in task_statuses for dependency in task.depends_on):
                        continue

                    pending.remove(task_id)
                    scheduled_this_round = True

                    dependency_statuses = [task_statuses[item] for item in task.depends_on]
                    if not task.enabled:
                        result = self._skip_task(
                            task,
                            run_id,
                            store,
                            runner,
                            "Task disabled in config.",
                        )
                        task_statuses[task.task_id] = result.status
                        continue

                    if any(status != "success" for status in dependency_statuses):
                        result = self._skip_task(
                            task,
                            run_id,
                            store,
                            runner,
                            "Skipped because one or more upstream tasks did not succeed.",
                        )
                        task_statuses[task.task_id] = result.status
                        continue

                    in_flight[executor.submit(self._execute_task, task, run_id, store, runner)] = task

                if not in_flight and not scheduled_this_round:
                    break

                if not in_flight:
                    continue

                done, _ = wait(list(in_flight.keys()), return_when=FIRST_COMPLETED)
                for future in done:
                    task = in_flight.pop(future)
                    result = future.result()
                    store.finish_task_run(
                        run_id,
                        task.task_id,
                        status=result.status,
                        exit_code=result.exit_code,
                        error=result.error,
                    )
                    task_statuses[task.task_id] = result.status
                    if result.status == "failed" and first_error is None:
                        first_error = result.error or f"Task {task.task_id} failed"

        return first_error

    def _run_or_skip_task(
        self,
        task: TaskDefinition,
        run_id: str,
        store: RunStore,
        runner: TaskRunner,
        task_statuses: dict[str, str],
    ) -> TaskExecutionResult:
        """Run one task in sequential mode or mark it skipped."""
        dependency_statuses = [task_statuses.get(item) for item in task.depends_on]
        if not task.enabled:
            result = self._skip_task(task, run_id, store, runner, "Task disabled in config.")
            task_statuses[task.task_id] = result.status
            return result

        if any(status != "success" for status in dependency_statuses):
            result = self._skip_task(
                task,
                run_id,
                store,
                runner,
                "Skipped because one or more upstream tasks did not succeed.",
            )
            task_statuses[task.task_id] = result.status
            return result

        result = self._execute_task(task, run_id, store, runner)
        store.finish_task_run(
            run_id,
            task.task_id,
            status=result.status,
            exit_code=result.exit_code,
            error=result.error,
        )
        task_statuses[task.task_id] = result.status
        return result

    def _skip_task(
        self,
        task: TaskDefinition,
        run_id: str,
        store: RunStore,
        runner: TaskRunner,
        reason: str,
    ) -> TaskExecutionResult:
        """Persist a skipped task and emit its reason once."""
        runner.emit(reason, task_id=task.task_id)
        store.finish_task_run(run_id, task.task_id, status="skipped", error=reason)
        return TaskExecutionResult(status="skipped", error=reason)

    def _execute_task(
        self,
        task: TaskDefinition,
        run_id: str,
        store: RunStore,
        runner: TaskRunner,
    ) -> TaskExecutionResult:
        """Execute one task through the shared TaskRunner."""
        store.mark_task_running(run_id, task.task_id)
        runner.emit(f"Running task {task.task_id} ({task.operator_label})", task_id=task.task_id)
        runner.emit(f"Plan: {task.command_preview}", task_id=task.task_id)
        return runner.run(task)
