"""TaskRunner executes one task at a time using lightweight operators."""

from __future__ import annotations

import importlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from pathlib import Path

from piply.core.models import TaskDefinition
from piply.core.store import RunStore


@dataclass(slots=True)
class TaskExecutionResult:
    """TaskExecutionResult captures the final status of one task invocation."""

    status: str
    exit_code: int | None = None
    error: str | None = None


class TaskRunner:
    """TaskRunner owns operator execution and raw log emission for one run."""

    def __init__(
        self,
        *,
        store: RunStore,
        run_id: str,
        on_log: Callable[[str], None] | None = None,
    ) -> None:
        self.store = store
        self.run_id = run_id
        self.on_log = on_log

    def run(self, task: TaskDefinition) -> TaskExecutionResult:
        """Dispatch one task to the correct lightweight operator."""
        if task.task_type == "python" or task.task_type == "python_call":
            if task.call:
                return self._run_python_call_task(task)
            
            command = [task.python or "python", str(task.path), *(str(item) for item in task.args)]
            return self._run_subprocess(
                command=command,
                cwd=task.working_directory,
                env=task.env,
                task_id=task.task_id,
            )

        if task.task_type == "cli":
            return self._run_subprocess(
                command=task.command or "",
                cwd=task.working_directory,
                env=task.env,
                task_id=task.task_id,
                shell=True,
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

    def _run_subprocess(
        self,
        *,
        command: list[str] | str,
        cwd,
        env: dict[str, str],
        task_id: str,
        shell: bool = False,
    ) -> TaskExecutionResult:
        """Run a local process and stream its merged stdout/stderr."""
        environment = os.environ.copy()
        environment.update(env)

        try:
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
            assert process.stdout is not None
            for line in process.stdout:
                self.emit(line.rstrip(), task_id=task_id)

            exit_code = process.wait()
            if exit_code == 0:
                self.emit("Task completed successfully.", task_id=task_id)
                return TaskExecutionResult(status="success", exit_code=exit_code)

            message = f"Process exited with code {exit_code}."
            self.emit(message, task_id=task_id)
            return TaskExecutionResult(status="failed", exit_code=exit_code, error=message)
        except FileNotFoundError as exc:
            message = str(exc)
            self.emit(message, task_id=task_id)
            return TaskExecutionResult(status="failed", error=message)

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

    def _run_python_call_task(self, task: TaskDefinition) -> TaskExecutionResult:
        """Run one imported Python callable and capture its printed output."""
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()

        try:
            callable_object = self._load_callable(task)
            with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                result = callable_object(*task.args, **task.kwargs)
        except Exception as exc:
            stdio_output = [stdout_buffer.getvalue().rstrip(), stderr_buffer.getvalue().rstrip()]
            for output in stdio_output:
                if output:
                    for line in output.splitlines():
                        self.emit(line, task_id=task.task_id)
            message = str(exc) or exc.__class__.__name__
            self.emit(message, task_id=task.task_id)
            return TaskExecutionResult(status="failed", error=message)

        for output in [stdout_buffer.getvalue().rstrip(), stderr_buffer.getvalue().rstrip()]:
            if output:
                for line in output.splitlines():
                    self.emit(line, task_id=task.task_id)

        if result is not None:
            if isinstance(result, (dict, list, tuple, int, float, bool)):
                rendered = json.dumps(result, default=str)
            else:
                rendered = str(result)
            self.emit(f"Return value: {rendered}", task_id=task.task_id)

        self.emit("Task completed successfully.", task_id=task.task_id)
        return TaskExecutionResult(status="success", exit_code=0)

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
            with urllib.request.urlopen(request, timeout=task.connect_timeout) as response:
                payload = response.read().decode("utf-8", errors="replace")
                status_code = response.getcode()
                preview = payload[:400] if payload else "<empty>"
                self.emit(f"Response {status_code}: {preview}", task_id=task.task_id)
                if status_code not in task.expected_status:
                    message = f"Unexpected status {status_code}. Expected one of {task.expected_status}."
                    return TaskExecutionResult(status="failed", error=message)
                return TaskExecutionResult(status="success", exit_code=status_code)
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
        import smtplib
        from email.message import EmailMessage
        
        if not task.email_to:
            message = "No recipients specified for email task."
            self.emit(message, task_id=task.task_id)
            return TaskExecutionResult(status="failed", error=message)
            
        msg = EmailMessage()
        msg.set_content(task.email_body or "")
        msg["Subject"] = task.email_subject or "Piply Notification"
        msg["From"] = task.smtp_user or "piply@localhost"
        msg["To"] = ", ".join(task.email_to)
        
        try:
            with smtplib.SMTP(task.smtp_host or "localhost", task.smtp_port) as server:
                if task.smtp_user and task.smtp_password:
                    server.starttls()
                    server.login(task.smtp_user, task.smtp_password)
                server.send_message(msg)
            self.emit("Email sent successfully.", task_id=task.task_id)
            return TaskExecutionResult(status="success", exit_code=0)
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
        )
