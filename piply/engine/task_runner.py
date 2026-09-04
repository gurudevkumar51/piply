"""TaskRunner executes one task at a time using lightweight operators."""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import json
import logging
import os
import queue
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from piply.core.context import RuntimeTaskContext
from piply.core.models import TaskDefinition
from piply.core.store import RunStore

#: Per-thread capture buffers, consulted by the proxy installed on `sys.stdout`
#: and `sys.stderr` while any Python task is running.
_capture_bindings = threading.local()

#: Guards installing and removing the proxy. `_capture_users` counts the threads
#: currently capturing, so the last one out restores the real streams.
_capture_lock = threading.Lock()
_capture_users = 0
_captured_streams: tuple[object, object] | None = None


class _ThreadRoutedStream:
    """Writes to the calling thread's capture buffer, or the real stream.

    `contextlib.redirect_stdout` swaps the process-global `sys.stdout`, which is
    wrong as soon as two Python tasks run at once: their enter/exit order
    interleaves, so one task's output is recorded against another task's log and
    the real stream is never put back. Routing per thread keeps each task's
    output its own, and leaves anything else printing in the process — uvicorn's
    logs, say — going where it always did.
    """

    def __init__(self, slot: str, real_stream) -> None:
        self._slot = slot
        self._real_stream = real_stream

    def _target(self):
        return getattr(_capture_bindings, self._slot, None) or self._real_stream

    def write(self, text: str) -> int:
        return self._target().write(text)

    def writelines(self, lines) -> None:
        self._target().writelines(lines)

    def flush(self) -> None:
        target = self._target()
        # Flushing a StringIO is a no-op; only the real stream needs it.
        if target is self._real_stream:
            self._real_stream.flush()

    def writable(self) -> bool:
        return True

    def __getattr__(self, name: str):
        # `encoding`, `fileno`, `buffer`, and friends belong to the real stream.
        return getattr(self._real_stream, name)


#: Stream handlers retargeted at the proxy, and the stream each one had before.
_retargeted_handlers: list[tuple[logging.StreamHandler, object]] = []


def _existing_stream_handlers():
    """Yield every `StreamHandler` currently attached anywhere."""
    loggers = [logging.getLogger()]
    loggers.extend(
        item
        for item in logging.Logger.manager.loggerDict.values()
        if isinstance(item, logging.Logger)
    )
    for logger in loggers:
        for handler in list(logger.handlers):
            if isinstance(handler, logging.StreamHandler):
                yield handler


def _retarget_log_handlers(replaced: tuple[object, object], proxies: tuple[object, object]) -> None:
    """Point existing log handlers at the proxy for the duration of a task.

    Redirecting `sys.stdout` alone is not enough for real code. A
    `StreamHandler` resolves `sys.stderr` when it is **constructed** and keeps a
    direct reference, so a module that calls `logging.basicConfig()` at import
    time — which is how most production code is written — writes straight past
    the proxy and its output never reaches the run log.

    Handlers created *after* this point need no help: they resolve `sys.stderr`
    while the proxy is installed. Nothing is added to the root logger, because a
    root handler would make a later `logging.basicConfig()` silently do nothing.
    """
    old_stdout, old_stderr = replaced
    new_stdout, new_stderr = proxies
    for handler in _existing_stream_handlers():
        stream = getattr(handler, "stream", None)
        if stream is old_stderr:
            _retargeted_handlers.append((handler, handler.setStream(new_stderr)))
        elif stream is old_stdout:
            _retargeted_handlers.append((handler, handler.setStream(new_stdout)))


def _restore_log_handlers() -> None:
    """Give every retargeted handler its original stream back."""
    while _retargeted_handlers:
        handler, original = _retargeted_handlers.pop()
        handler.setStream(original)


def _acquire_routing() -> None:
    """Install the routing proxy, if this is the first capture in flight."""
    global _capture_users, _captured_streams

    with _capture_lock:
        if _capture_users == 0:
            # Remember exactly what was replaced. Restoring `sys.__stdout__`
            # instead would fight pytest and anything else that legitimately
            # wraps the stream.
            _captured_streams = (sys.stdout, sys.stderr)
            proxies = (
                _ThreadRoutedStream("stdout", sys.stdout),
                _ThreadRoutedStream("stderr", sys.stderr),
            )
            _retarget_log_handlers(_captured_streams, proxies)
            sys.stdout, sys.stderr = proxies
        _capture_users += 1


def _release_routing() -> None:
    """Put the real streams back, once the last capture has finished."""
    global _capture_users, _captured_streams

    with _capture_lock:
        _capture_users -= 1
        if _capture_users == 0 and _captured_streams is not None:
            _restore_log_handlers()
            sys.stdout, sys.stderr = _captured_streams
            _captured_streams = None


class _StreamingLogSink:
    """Emits each complete line as the task prints it, rather than at the end.

    A long extraction that buffered its output until the task finished looked
    identical to one that had hung: nothing to watch, no way to tell progress
    from a stall. Subprocess tasks have always streamed line by line; this gives
    Python callables the same behaviour.
    """

    def __init__(self, on_line: Callable[[str], None]) -> None:
        self._on_line = on_line
        self._pending = ""
        self._lock = threading.Lock()
        self._closed = False

    def write(self, text: str) -> int:
        if not text:
            return 0
        with self._lock:
            if self._closed:
                return len(text)
            self._pending += text
            lines = self._pending.split("\n")
            self._pending = lines.pop()
        for line in lines:
            self._on_line(line.rstrip("\r"))
        return len(text)

    def writelines(self, lines) -> None:
        for line in lines:
            self.write(line)

    def flush(self) -> None:
        """No-op: lines are emitted as they complete, not on flush."""

    def writable(self) -> bool:
        return True

    def close(self) -> None:
        """Emit any trailing line that never ended in a newline, and stop.

        Closing matters for a timed-out task: the thread cannot be killed, so
        without this it would keep writing log lines against a task that has
        already been reported as finished.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            trailing, self._pending = self._pending, ""
        if trailing:
            self._on_line(trailing.rstrip("\r"))


class _OutputCapture:
    """One task's claim on `print` output, released exactly once.

    Used as a context manager on the thread running the task. A timed-out task
    is abandoned on a daemon thread that cannot be killed, so `release` may also
    be called from the waiting thread to stop that thread holding the process's
    streams hostage; whichever happens second does nothing.
    """

    def __init__(self, stdout_buffer, stderr_buffer) -> None:
        self._buffers = (stdout_buffer, stderr_buffer)
        self._lock = threading.Lock()
        self._holding = False

    def __enter__(self) -> _OutputCapture:
        _acquire_routing()
        with self._lock:
            self._holding = True
        _capture_bindings.stdout, _capture_bindings.stderr = self._buffers
        return self

    def __exit__(self, *_exc_info) -> None:
        # Only the running thread can clear its own bindings. An abandoned
        # thread keeps writing into a buffer nobody reads, which is what we want.
        _capture_bindings.stdout = None
        _capture_bindings.stderr = None
        self.release()

    def release(self) -> None:
        """Give up this task's claim. Safe to call twice, or from any thread."""
        with self._lock:
            if not self._holding:
                return
            self._holding = False
        _release_routing()


@dataclass(slots=True)
class TaskExecutionResult:
    """TaskExecutionResult captures the final status of one task invocation."""

    status: str
    exit_code: int | None = None
    error: str | None = None
    output: object | None = None


class TaskRunner:
    """TaskRunner owns operator execution and raw log emission for one run."""

    def __init__(
        self,
        *,
        store: RunStore,
        run_id: str,
        on_log: Callable[[str], None] | None = None,
        is_cancelled: Callable[[], bool] | None = None,
        register_process: Callable[[subprocess.Popen], None] | None = None,
        unregister_process: Callable[[subprocess.Popen], None] | None = None,
        context: RuntimeTaskContext | None = None,
    ) -> None:
        self.store = store
        self.run_id = run_id
        self.on_log = on_log
        self.is_cancelled = is_cancelled
        self.register_process = register_process
        self.unregister_process = unregister_process
        self.context = context if context is not None else RuntimeTaskContext()

    def run(self, task: TaskDefinition) -> TaskExecutionResult:
        """Dispatch one task to the correct lightweight operator."""
        if self.is_cancelled and self.is_cancelled():
            self.emit("Task cancelled before execution started.", task_id=task.task_id)
            return TaskExecutionResult(status="cancelled")

        if task.task_type == "python":
            if task.call:
                return self._run_python_call_task(task)

            command = [
                task.python or "python",
                str(task.path),
                *(str(item) for item in task.args),
            ]
            return self._run_subprocess(
                command=command,
                cwd=task.working_directory,
                env=task.env,
                task_id=task.task_id,
                timeout_seconds=task.timeout_seconds,
                kill_grace_period_seconds=task.kill_grace_period_seconds,
            )

        if task.task_type == "cli":
            if task.command is None and task.path is not None:
                return self._run_cli_path_task(task)
            command, use_shell = self._build_cli_command(task)
            return self._run_subprocess(
                command=command,
                cwd=task.working_directory,
                env=task.env,
                task_id=task.task_id,
                shell=use_shell,
                timeout_seconds=task.timeout_seconds,
                kill_grace_period_seconds=task.kill_grace_period_seconds,
            )

        if task.task_type == "api" or task.task_type == "webhook":
            return self._run_api_task(task)

        if task.task_type == "email":
            return self._run_email_task(task)

        if task.task_type == "ssh":
            return self._run_ssh_task(task)

        return TaskExecutionResult(status="failed", error=f"Unsupported task type {task.task_type}")

    def emit(self, message: str, *, task_id: str | None = None) -> None:
        """Append a raw log line and optionally echo it to the caller."""
        if not message:
            return
        self.store.append_log(self.run_id, message, task_id=task_id)
        if self.on_log:
            prefix = f"[{task_id}] " if task_id else ""
            self.on_log(f"{prefix}{message}")

    def for_context(self, context: RuntimeTaskContext) -> TaskRunner:
        """Return a runner sharing process/log wiring with a task-specific context."""
        return TaskRunner(
            store=self.store,
            run_id=self.run_id,
            on_log=self.on_log,
            is_cancelled=self.is_cancelled,
            register_process=self.register_process,
            unregister_process=self.unregister_process,
            context=context,
        )

    def _run_subprocess(
        self,
        *,
        command: list[str] | str,
        cwd,
        env: dict[str, str],
        task_id: str,
        shell: bool = False,
        timeout_seconds: int | None = None,
        kill_grace_period_seconds: int = 5,
    ) -> TaskExecutionResult:
        """Run a local process and stream its merged stdout/stderr."""
        environment = os.environ.copy()
        environment.update(env)
        environment.setdefault("PYTHONUNBUFFERED", "1")
        context_json = self.context.to_env_json()
        if context_json is not None:
            environment["PIPLY_CONTEXT_JSON"] = context_json
        environment["PIPLY_RUN_ID"] = self.run_id
        environment["PIPLY_TASK_ID"] = task_id

        try:
            output_lines: list[str] = []
            output_size = 0
            timed_out = False
            process = subprocess.Popen(
                command,
                cwd=None if cwd is None else str(cwd),
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                shell=shell,
            )
            if self.register_process is not None:
                self.register_process(process)
            assert process.stdout is not None

            line_queue: queue.Queue[str] = queue.Queue()
            reader_done = threading.Event()
            stdout_stream = process.stdout

            def _read_output() -> None:
                try:
                    for item in stdout_stream:
                        line_queue.put(item)
                except (ValueError, OSError):  # pragma: no cover - stream closed during kill
                    pass
                finally:
                    reader_done.set()

            reader = threading.Thread(target=_read_output, daemon=True)
            reader.start()
            deadline = None if timeout_seconds is None else time.monotonic() + timeout_seconds

            def _drain_queue() -> None:
                nonlocal output_size
                while True:
                    try:
                        line = line_queue.get_nowait()
                    except queue.Empty:
                        return
                    stripped_line = line.rstrip()
                    if output_size < 262_144:
                        output_lines.append(stripped_line)
                        output_size += len(stripped_line.encode("utf-8")) + 1
                    self.emit(stripped_line, task_id=task_id)

            while True:
                _drain_queue()

                if self.is_cancelled and self.is_cancelled() and process.poll() is None:
                    process.terminate()
                if deadline is not None and time.monotonic() >= deadline and process.poll() is None:
                    timed_out = True
                    self.emit(
                        f"Task timed out after {timeout_seconds} seconds; terminating process.",
                        task_id=task_id,
                    )
                    process.terminate()
                    try:
                        process.wait(timeout=kill_grace_period_seconds)
                    except subprocess.TimeoutExpired:
                        self.emit(
                            f"Task did not stop within the {kill_grace_period_seconds}s kill grace period; killing it.",
                            task_id=task_id,
                        )
                        process.kill()
                    break
                if process.poll() is not None and reader_done.is_set() and line_queue.empty():
                    break
                time.sleep(0.02)

            reader.join(timeout=1)
            _drain_queue()

            exit_code = process.wait()
            if timed_out:
                message = f"Task timed out after {timeout_seconds} seconds."
                return TaskExecutionResult(status="timed_out", exit_code=exit_code, error=message)
            if self.is_cancelled and self.is_cancelled():
                self.emit("Task cancelled.", task_id=task_id)
                return TaskExecutionResult(status="cancelled")
            if exit_code == 0:
                self.emit("Task completed successfully.", task_id=task_id)
                return TaskExecutionResult(
                    status="success",
                    exit_code=exit_code,
                    output="\n".join(output_lines),
                )

            message = f"Process exited with code {exit_code}."
            self.emit(message, task_id=task_id)
            return TaskExecutionResult(status="failed", exit_code=exit_code, error=message)
        except FileNotFoundError as exc:
            message = str(exc)
            self.emit(message, task_id=task_id)
            return TaskExecutionResult(status="failed", error=message)
        finally:
            if "process" in locals() and self.unregister_process is not None:
                self.unregister_process(process)

    def _build_cli_command(self, task: TaskDefinition) -> tuple[list[str] | str, bool]:
        """Build a CLI command, optionally using a named shell instead of the platform default."""
        command = task.command or ""
        if not task.shell:
            return command, True

        shell_name = task.shell.strip()
        shell_binary = Path(shell_name).name.lower()
        if shell_binary in {"bash", "bash.exe", "sh", "sh.exe", "zsh", "zsh.exe"}:
            return [shell_name, "-lc", command], False
        if shell_binary in {"powershell", "powershell.exe", "pwsh", "pwsh.exe"}:
            return [shell_name, "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command], False
        if shell_binary in {"cmd", "cmd.exe"}:
            return ["cmd.exe", "/d", "/s", "/c", command], False
        return [shell_name, "-lc", command], False

    def _run_cli_path_task(self, task: TaskDefinition) -> TaskExecutionResult:
        """Run a configured CLI path, including Windows batch files."""
        assert task.path is not None
        suffix = task.path.suffix.lower()
        if suffix in {".bat", ".cmd"}:
            command = [
                "cmd.exe",
                "/c",
                str(task.path),
                *(str(item) for item in task.args),
            ]
        elif suffix == ".ps1":
            command = [
                "powershell",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(task.path),
                *(str(item) for item in task.args),
            ]
        else:
            command = [str(task.path), *(str(item) for item in task.args)]
        return self._run_subprocess(
            command=command,
            cwd=task.working_directory,
            env=task.env,
            task_id=task.task_id,
            timeout_seconds=task.timeout_seconds,
            kill_grace_period_seconds=task.kill_grace_period_seconds,
        )

    def _load_callable(self, task: TaskDefinition):
        """Resolve a callable from a module reference or a file path reference."""
        if task.call is None:
            raise ValueError("Missing callable reference.")

        if "::" in task.call:
            raw_path, callable_name = task.call.split("::", 1)
            module_path = Path(raw_path)
            module_name = f"piply_call_{task.task_id}_{abs(hash(module_path))}"
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not import callable module from {module_path}")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        else:
            module_name, callable_name = task.call.rsplit(":", 1)
            search_path_added = False
            working_directory = task.working_directory
            if working_directory is not None and str(working_directory) not in sys.path:
                sys.path.insert(0, str(working_directory))
                search_path_added = True
            try:
                module = importlib.import_module(module_name)
            finally:
                if search_path_added and sys.path and sys.path[0] == str(working_directory):
                    sys.path.pop(0)

        callable_object = module
        for attribute_name in callable_name.split("."):
            callable_object = getattr(callable_object, attribute_name)
        return callable_object

    def _call_with_timeout(self, callable_object, task: TaskDefinition, stdout_buffer, stderr_buffer):
        """Invoke a callable, raising TimeoutError when the task timeout elapses.

        Python threads cannot be force-killed, so an over-running callable is
        abandoned on a daemon thread while the task itself is marked timed out.

        Output capture is bound here rather than by the caller because it is
        thread-scoped: with a timeout the callable runs on a worker thread, and a
        binding made on the calling thread would not reach it.
        """
        capture = _OutputCapture(stdout_buffer, stderr_buffer)
        if task.timeout_seconds is None:
            with capture:
                return self._invoke_callable(callable_object, task)

        box: dict[str, object] = {}

        def _invoke() -> None:
            try:
                with capture:
                    box["value"] = self._invoke_callable(callable_object, task)
            except BaseException as exc:  # noqa: BLE001 - re-raised on the calling thread
                box["error"] = exc

        worker = threading.Thread(target=_invoke, daemon=True, name=f"piply-call-{task.task_id}")
        worker.start()
        worker.join(timeout=task.timeout_seconds)
        if worker.is_alive():
            # The thread cannot be killed, so drop its claim on the process
            # streams rather than let a runaway task hold them indefinitely.
            capture.release()
            raise TimeoutError(f"Task timed out after {task.timeout_seconds} seconds.")
        error = box.get("error")
        if isinstance(error, BaseException):
            raise error
        return box.get("value")

    def _run_python_call_task(self, task: TaskDefinition) -> TaskExecutionResult:
        """Run one imported Python callable, streaming its printed output."""

        def _emit(line: str) -> None:
            self.emit(line, task_id=task.task_id)

        # Both streams emit through the same sink, so stdout and stderr appear
        # interleaved in the order the task actually produced them.
        stdout_sink = _StreamingLogSink(_emit)
        stderr_sink = _StreamingLogSink(_emit)

        try:
            callable_object = self._load_callable(task)
            result = self._call_with_timeout(callable_object, task, stdout_sink, stderr_sink)
        except TimeoutError as exc:
            stdout_sink.close()
            stderr_sink.close()
            message = str(exc)
            self.emit(message, task_id=task.task_id)
            return TaskExecutionResult(status="timed_out", exit_code=None, error=message)
        except Exception as exc:
            stdout_sink.close()
            stderr_sink.close()
            message = str(exc) or exc.__class__.__name__
            self.emit(message, task_id=task.task_id)
            return TaskExecutionResult(status="failed", error=message)

        stdout_sink.close()
        stderr_sink.close()

        if self.is_cancelled and self.is_cancelled():
            self.emit("Task cancelled.", task_id=task.task_id)
            return TaskExecutionResult(status="cancelled")

        if result is not None:
            # if isinstance(result, (dict, list, tuple, int, float, bool)):
            if isinstance(result, dict | list | tuple | int | float | bool):
                rendered = json.dumps(result, default=str)
            else:
                rendered = str(result)
            self.emit(f"Return value: {rendered}", task_id=task.task_id)

        self.emit("Task completed successfully.", task_id=task.task_id)
        return TaskExecutionResult(status="success", exit_code=0, output=result)

    def _invoke_callable(self, callable_object, task: TaskDefinition):
        """Invoke a Python callable, injecting context only when it explicitly asks."""
        kwargs = dict(task.kwargs)
        try:
            signature = inspect.signature(callable_object)
        except (TypeError, ValueError):
            return callable_object(*task.args, **kwargs)

        parameter = signature.parameters.get("context")
        if parameter is not None and "context" not in kwargs:
            positional_names = [
                name
                for name, candidate in signature.parameters.items()
                if candidate.kind
                in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                )
            ]
            if "context" not in positional_names[: len(task.args)]:
                kwargs["context"] = self.context.snapshot()
        return callable_object(*task.args, **kwargs)

    def _run_api_task(self, task: TaskDefinition) -> TaskExecutionResult:
        """Run one API task using urllib and optional bearer auth."""
        headers = dict(task.headers)
        if task.token and "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {task.token}"
        if task.body is not None and "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"

        body = task.body.encode("utf-8") if task.body is not None else None
        request = urllib.request.Request(
            url=task.url or "",
            data=body,
            headers=headers,
            method=task.method.upper(),
        )

        try:
            with urllib.request.urlopen(request, timeout=task.timeout_seconds or task.connect_timeout) as response:
                payload = response.read().decode("utf-8", errors="replace")
                status_code = response.getcode()
                preview = payload[:400] if payload else "<empty>"
                self.emit(f"Response {status_code}: {preview}", task_id=task.task_id)
                if self.is_cancelled and self.is_cancelled():
                    self.emit("Task cancelled.", task_id=task.task_id)
                    return TaskExecutionResult(status="cancelled")
                if status_code not in task.expected_status:
                    message = f"Unexpected status {status_code}. Expected one of {task.expected_status}."
                    return TaskExecutionResult(status="failed", error=message)
                return TaskExecutionResult(status="success", exit_code=status_code, output=payload)
        except urllib.error.HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="replace")
            message = f"HTTPError {exc.code}: {body_text[:400]}"
            self.emit(message, task_id=task.task_id)
            return TaskExecutionResult(status="failed", exit_code=exc.code, error=message)
        except urllib.error.URLError as exc:
            message = f"Request failed: {exc.reason}"
            self.emit(message, task_id=task.task_id)
            return TaskExecutionResult(status="failed", error=message)

    def _run_email_task(self, task: TaskDefinition) -> TaskExecutionResult:
        """Send one email, using central SMTP settings unless the task overrides them."""
        from piply.core.mailer import build_message, load_smtp_settings, resolve_for_task, send_message

        if not task.email_to:
            message = "No recipients specified for email task."
            self.emit(message, task_id=task.task_id)
            return TaskExecutionResult(status="failed", error=message)

        settings = resolve_for_task(load_smtp_settings(self.store), task)
        if not settings.configured:
            message = "No SMTP server is configured. Set one under Settings, or give this task its own smtp_host."
            self.emit(message, task_id=task.task_id)
            return TaskExecutionResult(status="failed", error=message)

        subject = task.email_subject or "Piply Notification"
        try:
            send_message(
                settings,
                build_message(settings, to=list(task.email_to), subject=subject, body=task.email_body or ""),
            )
            if self.is_cancelled and self.is_cancelled():
                self.emit("Task cancelled.", task_id=task.task_id)
                return TaskExecutionResult(status="cancelled")
            self.emit(f"Email sent to {', '.join(task.email_to)} via {settings.host}.", task_id=task.task_id)
            return TaskExecutionResult(
                status="success",
                exit_code=0,
                output={"to": list(task.email_to), "subject": subject, "smtp_host": settings.host},
            )
        except Exception as exc:
            message = f"Failed to send email: {exc}"
            self.emit(message, task_id=task.task_id)
            return TaskExecutionResult(status="failed", error=message)

    def _run_ssh_task(self, task: TaskDefinition) -> TaskExecutionResult:
        """Run one remote SSH command or connectivity probe."""
        target = f"{task.user}@{task.host}" if task.user else str(task.host)
        remote_command = task.command or "echo piply-ssh-ok"
        command = [
            task.ssh_binary,
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={task.connect_timeout}",
            "-p",
            str(task.port),
        ]
        if task.key_file is not None:
            command.extend(["-i", str(task.key_file)])
        command.extend([target, remote_command])

        return self._run_subprocess(
            command=command,
            cwd=task.working_directory,
            env=task.env,
            task_id=task.task_id,
            timeout_seconds=task.timeout_seconds,
            kill_grace_period_seconds=task.kill_grace_period_seconds,
        )
