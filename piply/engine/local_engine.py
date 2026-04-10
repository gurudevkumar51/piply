"""Local execution engine with sequential and dependency-aware parallel modes."""

from __future__ import annotations

import threading
import traceback
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

from piply.core.graph import topological_order
from piply.core.models import PipelineDefinition, RunRecord, TaskDefinition
from piply.core.store import RunStore

from .base import BaseEngine, CompletionCallback, LogCallback
from .heartbeat import RunHeartbeat
from .task_runner import TaskExecutionResult, TaskRunner


class LocalEngine(BaseEngine):
    """Run Piply task graphs with lightweight local workers."""

    def __init__(self, heartbeat_interval_seconds: int = 10) -> None:
        self._threads: dict[str, threading.Thread] = {}
        self._cancel_events: dict[str, threading.Event] = {}
        self._active_processes: dict[str, set[object]] = {}
        self._lock = threading.Lock()
        self.heartbeat_interval_seconds = max(2, heartbeat_interval_seconds)

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
        """Execute a pipeline either inline or in a background thread."""
        with self._lock:
            self._cancel_events[run.run_id] = threading.Event()
            self._active_processes[run.run_id] = set()
        if wait:
            self._execute(
                pipeline,
                run.run_id,
                store,
                on_log=on_log,
                on_success=on_success,
                on_failure=on_failure,
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
                "on_failure": on_failure,
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
        on_failure: CompletionCallback | None = None,
        initial_task_statuses: dict[str, str],
        retry_source_run_id: str | None,
    ) -> None:
        """Execute the full pipeline lifecycle for one run."""
        cancel_event = self._cancel_events.get(run_id) or threading.Event()
        runner = TaskRunner(
            store=store,
            run_id=run_id,
            on_log=on_log,
            is_cancelled=cancel_event.is_set,
            register_process=lambda process: self._register_process(run_id, process),
            unregister_process=lambda process: self._unregister_process(run_id, process),
        )
        heartbeat = RunHeartbeat(store, run_id, self.heartbeat_interval_seconds)
        store.mark_running(run_id)
        heartbeat.start()
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
                first_error = self._execute_parallel(
                    pipeline,
                    run_id,
                    store,
                    runner,
                    task_statuses,
                    cancel_event,
                )
            else:
                first_error = self._execute_sequential(
                    pipeline,
                    run_id,
                    store,
                    runner,
                    task_statuses,
                    cancel_event,
                )

            failed_tasks = [
                task_id for task_id, status in task_statuses.items() if status == "failed"
            ]
            if cancel_event.is_set():
                self._mark_pending_tasks_cancelled(pipeline, run_id, store, runner, task_statuses)
                store.finish_run(run_id, status="cancelled", exit_code=None, error="Run cancelled by user.")
            elif failed_tasks:
                store.finish_run(run_id, status="failed", exit_code=1, error=first_error)
                final_run = store.get_run(run_id)
                if final_run is not None and on_failure is not None:
                    on_failure(pipeline, final_run)
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
            heartbeat.stop()
            with self._lock:
                self._threads.pop(run_id, None)
                self._cancel_events.pop(run_id, None)
                self._active_processes.pop(run_id, None)

    def _execute_sequential(
        self,
        pipeline: PipelineDefinition,
        run_id: str,
        store: RunStore,
        runner: TaskRunner,
        task_statuses: dict[str, str],
        cancel_event: threading.Event,
    ) -> str | None:
        """Execute tasks one at a time in topological order."""
        first_error: str | None = None
        for task in topological_order(pipeline):
            if cancel_event.is_set():
                break
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
        cancel_event: threading.Event,
    ) -> str | None:
        """Execute ready tasks concurrently while respecting dependencies."""
        first_error: str | None = None
        pending = {task_id for task_id in pipeline.tasks if task_id not in task_statuses}
        in_flight: dict[Future[TaskExecutionResult], TaskDefinition] = {}

        with ThreadPoolExecutor(max_workers=pipeline.max_parallel_tasks) as executor:
            while pending or in_flight:
                if cancel_event.is_set():
                    for task_id in list(pending):
                        result = self._cancel_task(
                            pipeline.tasks[task_id],
                            run_id,
                            store,
                            runner,
                            "Task cancelled before execution because the pipeline was cancelled.",
                        )
                        task_statuses[task_id] = result.status
                        pending.remove(task_id)
                    if not in_flight:
                        break

                scheduled_this_round = False

                for task_id in list(pending):
                    if cancel_event.is_set():
                        break
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

    def _cancel_task(
        self,
        task: TaskDefinition,
        run_id: str,
        store: RunStore,
        runner: TaskRunner,
        reason: str,
    ) -> TaskExecutionResult:
        """Persist a cancelled task and emit its reason once."""
        runner.emit(reason, task_id=task.task_id)
        store.finish_task_run(run_id, task.task_id, status="cancelled", error=reason)
        return TaskExecutionResult(status="cancelled", error=reason)

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

    def _mark_pending_tasks_cancelled(
        self,
        pipeline: PipelineDefinition,
        run_id: str,
        store: RunStore,
        runner: TaskRunner,
        task_statuses: dict[str, str],
    ) -> None:
        """Mark any not-yet-finished tasks as cancelled after a run cancellation."""
        for task in pipeline.tasks.values():
            if task.task_id not in task_statuses:
                result = self._cancel_task(
                    task,
                    run_id,
                    store,
                    runner,
                    "Task cancelled because the pipeline was cancelled.",
                )
                task_statuses[task.task_id] = result.status

    def _register_process(self, run_id: str, process: object) -> None:
        """Track one active subprocess for later cancellation."""
        with self._lock:
            self._active_processes.setdefault(run_id, set()).add(process)

    def _unregister_process(self, run_id: str, process: object) -> None:
        """Stop tracking a subprocess after it exits."""
        with self._lock:
            self._active_processes.setdefault(run_id, set()).discard(process)

    def cancel(self, run_id: str) -> bool:
        """Request cancellation for one running pipeline."""
        with self._lock:
            cancel_event = self._cancel_events.get(run_id)
            processes = list(self._active_processes.get(run_id, set()))
        if cancel_event is None:
            return False

        cancel_event.set()
        for process in processes:
            try:
                if hasattr(process, "poll") and process.poll() is None:
                    process.terminate()
            except Exception:
                continue
        return True
