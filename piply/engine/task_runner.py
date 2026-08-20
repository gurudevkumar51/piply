"""TaskRunner executes one task at a time using lightweight operators."""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import io
import json
import os
import queue
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from pathlib import Path

from piply.core.context import RuntimeTaskContext
from piply.core.models import TaskDefinition
from piply.core.store import RunStore


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

    def _call_with_timeout(self, callable_object, task: TaskDefinition):
        """Invoke a callable, raising TimeoutError when the task timeout elapses.

        Python threads cannot be force-killed, so an over-running callable is
        abandoned on a daemon thread while the task itself is marked timed out.
        """
        if task.timeout_seconds is None:
            return self._invoke_callable(callable_object, task)

        box: dict[str, object] = {}

        def _invoke() -> None:
            try:
                box["value"] = self._invoke_callable(callable_object, task)
            except BaseException as exc:  # noqa: BLE001 - re-raised on the calling thread
                box["error"] = exc

        worker = threading.Thread(target=_invoke, daemon=True, name=f"piply-call-{task.task_id}")
        worker.start()
        worker.join(timeout=task.timeout_seconds)
        if worker.is_alive():
            raise TimeoutError(f"Task timed out after {task.timeout_seconds} seconds.")
        error = box.get("error")
        if isinstance(error, BaseException):
            raise error
        return box.get("value")

    def _run_python_call_task(self, task: TaskDefinition) -> TaskExecutionResult:
        """Run one imported Python callable and capture its printed output."""
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()

        try:
            callable_object = self._load_callable(task)
            with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                result = self._call_with_timeout(callable_object, task)
        except TimeoutError as exc:
            for output in [stdout_buffer.getvalue().rstrip(), stderr_buffer.getvalue().rstrip()]:
                for line in output.splitlines():
                    self.emit(line, task_id=task.task_id)
            message = str(exc)
            self.emit(message, task_id=task.task_id)
            return TaskExecutionResult(status="timed_out", exit_code=None, error=message)
        except Exception as exc:
            stdio_output = [
                stdout_buffer.getvalue().rstrip(),
                stderr_buffer.getvalue().rstrip(),
            ]
            for output in stdio_output:
                if output:
                    for line in output.splitlines():
                        self.emit(line, task_id=task.task_id)
            message = str(exc) or exc.__class__.__name__
            self.emit(message, task_id=task.task_id)
            return TaskExecutionResult(status="failed", error=message)

        for output in [
            stdout_buffer.getvalue().rstrip(),
            stderr_buffer.getvalue().rstrip(),
        ]:
            if output:
                for line in output.splitlines():
                    self.emit(line, task_id=task.task_id)

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
