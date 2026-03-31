"""CLI entry points for validating, running, and serving Piply projects."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import typer
import uvicorn

from piply.core.loader import ConfigError, discover_config, load_project
from piply.core.service import PipelineService
from piply.settings import load_settings

app = typer.Typer(help="Piply: lightweight orchestration for task-based Python workflows.")
tasks_app = typer.Typer(help="Inspect pipeline tasks.")
app.add_typer(tasks_app, name="tasks")


def _resolve_config(config: str | None) -> Path:
    if config:
        return Path(config).resolve()
    return discover_config()


def _server_command(host: str, port: int, reload: bool) -> list[str]:
    """Build the reusable uvicorn command for foreground and detached start."""
    command = [
        sys.executable,
        "-m",
        "uvicorn",
        "piply.api.app:create_app",
        "--factory",
        "--host",
        host,
        "--port",
        str(port),
    ]
    if reload:
        command.append("--reload")
    return command


@app.command()
def init(
    directory: str = typer.Argument(".", help="Directory to scaffold the project in."),
    force: bool = typer.Option(False, "--force", help="Overwrite existing files."),
) -> None:
    target_dir = Path(directory).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    config_path = target_dir / "piply.yaml"
    pipelines_dir = target_dir / "pipelines"
    extract_path = pipelines_dir / "extract.py"
    report_path = pipelines_dir / "report.py"

    if config_path.exists() and not force:
        raise typer.BadParameter(f"{config_path} already exists. Use --force to overwrite it.")

    pipelines_dir.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Piply Workspace",
                "workspace: .",
                "defaults:",
                "  python: python",
                "  env:",
                "    PIPLY_ENV: development",
                "pipelines:",
                "  extract_flow:",
                '    title: Extract Flow',
                '    description: Multi-task starter pipeline with a downstream trigger.',
                "    schedule:",
                "      every: 15m",
                "    max_parallel_tasks: 2",
                "    triggers_on_success:",
                "      - report_flow",
                "    tasks:",
                "      extract:",
                "        type: python",
                "        path: pipelines/extract.py",
                "        args: ['--records', '120']",
                "      validate:",
                "        type: cli",
                "        command: python -c \"print('Validating extracted payload...')\"",
                "        depends_on: [extract]",
                "      publish_manifest:",
                "        type: cli",
                "        command: python -c \"print('Publishing manifest for downstream flow...')\"",
                "        depends_on: [extract]",
                "  report_flow:",
                '    title: Report Flow',
                '    description: Triggered automatically after extract_flow succeeds.',
                "    tasks:",
                "      build_report:",
                "        type: python",
                "        path: pipelines/report.py",
                "        function: build_report",
                "        kwargs:",
                "          report_name: starter-report",
            ]
        ),
        encoding="utf-8",
    )

    extract_path.write_text(
        "\n".join(
            [
                "from __future__ import annotations",
                "",
                "import argparse",
                "import time",
                "",
                "",
                "def parse_args() -> argparse.Namespace:",
                "    parser = argparse.ArgumentParser()",
                "    parser.add_argument('--records', type=int, default=100)",
                "    return parser.parse_args()",
                "",
                "",
                "def main() -> None:",
                "    args = parse_args()",
                "    print(f'Extracting {args.records} records...')",
                "    for step in range(1, 4):",
                "        print(f'Chunk {step}/3 complete')",
                "        time.sleep(0.5)",
                "    print('Extract complete')",
                "",
                "",
                "if __name__ == '__main__':",
                "    main()",
            ]
        ),
        encoding="utf-8",
    )

    report_path.write_text(
        "\n".join(
            [
                "from __future__ import annotations",
                "",
                "def build_report(report_name: str = 'starter-report') -> str:",
                "    print(f'Generating downstream report: {report_name}')",
                "    print('Report complete.')",
                "    return report_name",
            ]
        ),
        encoding="utf-8",
    )

    typer.echo(f"Created {config_path}")
    typer.echo(f"Created {extract_path}")
    typer.echo(f"Created {report_path}")
    typer.echo("Run `piply validate` and `piply start` to launch the UI.")


@app.command()
def validate(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
) -> None:
    try:
        config_path = _resolve_config(config)
        project = load_project(config_path)
    except (ConfigError, FileNotFoundError) as exc:
        typer.echo(f"Validation failed: {exc}")
        raise typer.Exit(code=1) from exc

    typer.echo(f"Config: {config_path}")
    typer.echo(f"Project: {project.title}")
    typer.echo(f"Pipelines: {len(project.pipelines)}")
    for pipeline in project.pipelines.values():
        typer.echo(
            f"  - {pipeline.pipeline_id}: {pipeline.task_count} tasks | triggers {list(pipeline.triggers_on_success) or ['none']}"
        )


@app.command("list")
def list_pipelines(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
) -> None:
    service = PipelineService(config_path=_resolve_config(config))
    for summary in service.list_pipelines():
        status = "paused" if summary.paused else "enabled"
        typer.echo(f"{summary.pipeline_id} [{status}]")
        typer.echo(f"  {summary.schedule_text}")
        typer.echo(f"  {summary.task_count} tasks | {summary.execution_summary}")
        typer.echo(f"  {summary.command_preview}")


@tasks_app.command("list")
def list_tasks(
    pipeline_id: str = typer.Argument(..., help="Pipeline identifier."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
) -> None:
    service = PipelineService(config_path=_resolve_config(config))
    pipeline = service.get_pipeline(pipeline_id)
    for task in pipeline.tasks.values():
        deps = ", ".join(task.depends_on) if task.depends_on else "none"
        typer.echo(f"{task.task_id} [{task.task_type}]")
        typer.echo(f"  depends_on: {deps}")
        typer.echo(f"  command: {task.command_preview}")


@app.command()
def run(
    pipeline_id: str = typer.Argument(..., help="Pipeline identifier to run."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    wait: bool = typer.Option(
        True,
        "--wait/--detach",
        help="Wait and stream logs in the terminal.",
    ),
) -> None:
    service = PipelineService(config_path=_resolve_config(config))
    try:
        run_record = service.trigger_pipeline(
            pipeline_id,
            trigger="manual",
            wait=wait,
            on_log=typer.echo if wait else None,
        )
    except KeyError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc

    typer.echo(f"Run ID: {run_record.run_id}")
    if wait:
        run_record, _, _ = service.get_run(run_record.run_id)
        typer.echo(f"Finished with status: {run_record.status}")
        if run_record.error:
            typer.echo(run_record.error)
            raise typer.Exit(code=1)


@app.command()
def runs(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    limit: int = typer.Option(20, "--limit", help="Number of runs to show."),
) -> None:
    service = PipelineService(config_path=_resolve_config(config))
    for run_record in service.list_runs(limit=limit):
        typer.echo(
            f"{run_record.run_id}  {run_record.pipeline_id}  {run_record.status}  {run_record.successful_tasks}/{run_record.task_count} tasks"
        )


@app.command()
def logs(
    run_id: str = typer.Argument(..., help="Run ID to fetch logs for."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
) -> None:
    service = PipelineService(config_path=_resolve_config(config))
    try:
        _, _, raw_logs = service.get_run(run_id)
        # raw logs are returned newest first, we reverse them to print chronologically in terminal
        for log in reversed(raw_logs):
            typer.echo(f"[{log.created_at.strftime('%H:%M:%S.%f')[:-3]}] [{log.task_id or 'pipeline'}] {log.message}")
    except KeyError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1)


@tasks_app.command("run")
def run_task(
    pipeline_id: str = typer.Argument(..., help="Pipeline identifier."),
    task_id: str = typer.Argument(..., help="Task identifier."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    wait: bool = typer.Option(True, "--wait/--detach", help="Wait and stream logs in the terminal."),
) -> None:
    service = PipelineService(config_path=_resolve_config(config))
    try:
        run_record = service.trigger_pipeline(
            pipeline_id,
            trigger="manual",
            wait=wait,
            on_log=typer.echo if wait else None,
            retry_task_id=task_id, # Re-using retry mechanic for selective run
        )
    except KeyError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1)

    typer.echo(f"Run ID: {run_record.run_id}")
    if wait:
        run_record, _, _ = service.get_run(run_record.run_id)
        typer.echo(f"Finished with status: {run_record.status}")
        if run_record.error:
            typer.echo(run_record.error)
            raise typer.Exit(code=1)


@app.command()
def pause(
    pipeline_id: str = typer.Argument(..., help="Pipeline identifier to pause."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
) -> None:
    service = PipelineService(config_path=_resolve_config(config))
    try:
        service.set_pipeline_paused(pipeline_id, True)
        typer.echo(f"Pipeline '{pipeline_id}' scheduled runs paused.")
    except KeyError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1)


@app.command()
def resume(
    pipeline_id: str = typer.Argument(..., help="Pipeline identifier to resume."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
) -> None:
    service = PipelineService(config_path=_resolve_config(config))
    try:
        service.set_pipeline_paused(pipeline_id, False)
        typer.echo(f"Pipeline '{pipeline_id}' scheduled runs resumed.")
    except KeyError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1)


@app.command()
def stop(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
) -> None:
    service = PipelineService(config_path=_resolve_config(config))
    service.store.set_meta("shutdown_requested", "true")
    typer.echo("Shutdown requested. The background server will exit gracefully within a few seconds.")


@app.command()
def start(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    host: str = typer.Option("127.0.0.1", "--host", help="Bind address."),
    port: int = typer.Option(8000, "--port", help="Bind port."),
    reload: bool = typer.Option(False, "--reload", help="Enable auto reload."),
    detach: bool = typer.Option(False, "--detach", "-d", help="Run the web server in the background."),
) -> None:
    config_path = _resolve_config(config)
    settings = load_settings(config_path)
    environment = os.environ.copy()
    environment["PIPLY_CONFIG"] = str(config_path)
    if settings.database_path is not None:
        environment["PIPLY_DATABASE"] = str(settings.database_path)

    if detach:
        if reload:
            raise typer.BadParameter("--reload cannot be used with --detach.")
        log_dir = config_path.parent / ".piply"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "server.log"
        command = _server_command(host, port, reload=False)
        popen_kwargs: dict[str, object] = {
            "args": command,
            "env": environment,
            "stdout": log_path.open("a", encoding="utf-8"),
            "stderr": subprocess.STDOUT,
            "cwd": str(Path.cwd()),
        }
        if os.name == "nt":
            popen_kwargs["creationflags"] = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            popen_kwargs["start_new_session"] = True
        process = subprocess.Popen(**popen_kwargs)
        typer.echo(f"Piply started in background on http://{host}:{port}")
        typer.echo(f"PID: {process.pid}")
        typer.echo(f"Logs: {log_path}")
        return

    os.environ["PIPLY_CONFIG"] = str(config_path)
    if settings.database_path is not None:
        os.environ["PIPLY_DATABASE"] = str(settings.database_path)
    typer.echo(f"Using config: {config_path}")
    typer.echo(f"Starting Piply on http://{host}:{port}")
    uvicorn.run("piply.api.app:create_app", factory=True, host=host, port=port, reload=reload)


@app.command(hidden=True)
def ui(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    host: str = typer.Option("127.0.0.1", "--host", help="Bind address."),
    port: int = typer.Option(8000, "--port", help="Bind port."),
    reload: bool = typer.Option(False, "--reload", help="Enable auto reload."),
    detach: bool = typer.Option(False, "--detach", "-d", help="Run the web server in the background."),
) -> None:
    start(config=config, host=host, port=port, reload=reload, detach=detach)


if __name__ == "__main__":
    app()
