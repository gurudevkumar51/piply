"""CLI entry points for validating, running, and serving Piply projects."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path

import typer
import uvicorn

from piply.core.auth import AuthError, generate_password
from piply.core.loader import ConfigError, discover_config, load_project
from piply.core.service import PipelineService
from piply.settings import load_settings

app = typer.Typer(help="Piply: lightweight orchestration for task-based Python workflows.")
tasks_app = typer.Typer(help="Inspect pipeline tasks.")
app.add_typer(tasks_app, name="tasks")
users_app = typer.Typer(help="Manage accounts and pipeline permissions.")
app.add_typer(users_app, name="users")
RUN_PARAM_OPTION = typer.Option(
    None,
    "--param",
    help="Run parameter as KEY=VALUE. Repeat for multiple params; JSON values are accepted.",
)
TASK_PARAM_OPTION = typer.Option(
    None,
    "--param",
    help="Run parameter as KEY=VALUE. Repeat for multiple params; JSON values are accepted.",
)
PLAN_PARAM_OPTION = typer.Option(
    None,
    "--param",
    help="Preview parameter as KEY=VALUE. Repeat for multiple params; JSON values are accepted.",
)
BACKFILL_START_OPTION = typer.Option(None, "--from", help="Start of the schedule window to backfill.")
BACKFILL_END_OPTION = typer.Option(None, "--to", help="End of the schedule window to backfill.")
USER_GRANT_OPTION = typer.Option(
    None,
    "--grant",
    help="Grant as PIPELINE=actions, for example reports=view,run. Repeat for more pipelines.",
)


def _echo_permissions(user) -> None:
    """Print one account's pipeline grants."""
    if user.is_admin:
        typer.echo("  permissions: administrator (every pipeline, every action)")
        return
    if not user.permissions:
        typer.echo("  permissions: none yet")
        return
    for pipeline_id, actions in sorted(user.permissions.items()):
        label = "every pipeline" if pipeline_id == "*" else pipeline_id
        typer.echo(f"  {label}: {', '.join(sorted(actions))}")


def _describe_database(config_path: Path, settings) -> str:
    """Describe where runtime state will be written, and whether that is durable.

    Printing this at startup makes an ephemeral container path obvious on day
    one instead of after the first redeploy wipes the run history.
    """
    if settings.database_dsn is not None:
        from piply.core.sql_adapters import mask_connection_secret

        return f"{mask_connection_secret(settings.database_dsn)}  (PostgreSQL)"
    resolved = settings.database_path or (config_path.parent / ".piply" / "piply.db")
    if settings.database_path is None:
        return f"{resolved}  (default SQLite location; set PIPLY_DATABASE to move it)"
    return f"{resolved}  (SQLite)"


def _handle_interrupt(service: PipelineService) -> None:
    """Wind a foreground run down cleanly after Ctrl+C.

    Without this the run row would stay RUNNING until the heartbeat timeout,
    which is exactly the orphaned state the runtime is meant to avoid.
    """
    typer.echo("")
    typer.echo("Interrupted. Marking active executions as interrupted...")
    interrupted = service.shutdown_runtime("Run interrupted by Ctrl+C.")
    for run_id in interrupted:
        typer.echo(f"  {run_id} -> interrupted")
    if not interrupted:
        typer.echo("  no active runs needed recovery")


def _format_bytes(size: int) -> str:
    """Render a byte count using the largest sensible unit."""
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GB"


def _show_version(value: bool) -> None:
    """Print the installed Piply version for the top-level CLI option."""
    if not value:
        return
    try:
        current_version = package_version("mr-piply")
    except PackageNotFoundError:
        current_version = "0.1.6"
    typer.echo(current_version)
    raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        help="Show the Piply version and exit.",
        is_eager=True,
        callback=_show_version,
    ),
) -> None:
    """Piply command-line application."""


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


_ANSI = {
    "reset": "\033[0m",
    "dim": "\033[2m",
    "task": "\033[36m",
    "pipeline": "\033[35m",
    "error": "\033[31m",
}


def _dim(text: str, use_color: bool) -> str:
    """Render secondary CLI text."""
    return f"{_ANSI['dim']}{text}{_ANSI['reset']}" if use_color else text


def _format_log_line(line: dict[str, object], use_color: bool) -> str:
    """Render one streamed log line with its timestamp, pipeline, and task name.

    Every line carries the task label so interleaved output from parallel tasks
    stays readable.
    """
    created_at = str(line.get("created_at") or "")
    try:
        stamp = datetime.fromisoformat(created_at).astimezone().strftime("%H:%M:%S.%f")[:-3]
    except ValueError:
        stamp = created_at[:12]

    task_label = str(line.get("task_title") or line.get("task_id") or "pipeline")
    pipeline_label = str(line.get("pipeline_id") or "")
    message = str(line.get("message") or "")

    if not use_color:
        return f"[{stamp}] [{pipeline_label}] [{task_label}] {message}"

    color = _ANSI["error"] if line.get("stream") == "stderr" else _ANSI["task"]
    return (
        f"{_ANSI['dim']}[{stamp}]{_ANSI['reset']} "
        f"{_ANSI['pipeline']}[{pipeline_label}]{_ANSI['reset']} "
        f"{color}[{task_label}]{_ANSI['reset']} {message}"
    )


def _parse_params(param_items: list[str] | None) -> dict[str, object]:
    """Parse repeated KEY=VALUE CLI params, preserving JSON scalars when supplied."""
    parsed: dict[str, object] = {}
    for item in param_items or []:
        if "=" not in item:
            raise typer.BadParameter("--param must use KEY=VALUE.")
        key, raw_value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise typer.BadParameter("--param keys cannot be empty.")
        try:
            parsed[key] = json.loads(raw_value)
        except json.JSONDecodeError:
            parsed[key] = raw_value
    return parsed


@app.command()
def init(
    directory: str = typer.Argument(".", help="Directory to scaffold the project in."),
    force: bool = typer.Option(False, "--force", help="Overwrite existing files."),
) -> None:
    target_dir = Path(directory).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    config_path = target_dir / "piply.yaml"
    pipelines_dir = target_dir / "pipelines"
    sensor_inbox_dir = target_dir / "sensor_inbox"
    extract_path = pipelines_dir / "extract.py"
    report_path = pipelines_dir / "report.py"
    validate_path = pipelines_dir / "validate_cli.py"

    if config_path.exists() and not force:
        raise typer.BadParameter(f"{config_path} already exists. Use --force to overwrite it.")

    pipelines_dir.mkdir(parents=True, exist_ok=True)
    sensor_inbox_dir.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Piply Workspace",
                "workspace: .",
                "variables:",
                "  scripts_dir: pipelines",
                "  batch_id: demo-batch",
                "  conda_env: py312_extract",
                "defaults:",
                "  python: python",
                "  env:",
                "    PIPLY_ENV: development",
                "",
                "secrets:",
                "  backend: env",
                "  prefix: PIPLY_SECRET_",
                "",
                "connections:",
                "  local_sensor_db: sqlite:///sensor_demo.db",
                "  warehouse: ${secret:WAREHOUSE_DSN}",
                "",
                "pipelines:",
                "  extract_flow:",
                "    title: Extract Flow",
                "    description: Multi-task starter pipeline with context passing and a downstream trigger.",
                "    schedule:",
                "      every: 15m",
                "    retry:",
                "      attempts: 2",
                "      mode: resume",
                "      delay_seconds: 10",
                "    max_parallel_tasks: 2",
                "    timeout: 30m",
                "    triggers_on_success:",
                "      - report_flow",
                "    tasks:",
                "      # 'extract***' is shorthand for priority 3. 'priority: 3' works too.",
                "      extract***:",
                "        type: python",
                "        path: pipelines/extract.py",
                "        function: extract_data",
                "        timeout: 5m",
                "        kwargs:",
                "          records: 120",
                "",
                "      transform:",
                "        type: python",
                "        path: pipelines/extract.py",
                "        function: transform_data",
                "        depends_on: [extract]",
                "      validate:",
                "        type: cli",
                "        command: python {scripts_dir}/validate_cli.py {batch_id}",
                "        cwd: .",
                "        depends_on: [transform]",
                "      publish_manifest:",
                "        type: cli",
                "        priority: low",
                "        command: python -c \"print('Publishing manifest for downstream flow...')\"",
                "        depends_on: [transform]",
                "  report_flow:",
                "    title: Report Flow",
                "    description: Triggered automatically after extract_flow succeeds.",
                "    tasks:",
                "      build_report:",
                "        type: python",
                "        path: pipelines/report.py",
                "        function: build_report",
                "        kwargs:",
                "          report_name: starter-report",
                "        # Files matching these globs are recorded and become",
                "        # downloadable from the run page.",
                "        artifacts:",
                "          - 'reports/*.txt'",
                "",
                "  entity_mapping_examples:",
                "    title: Entity Mapping Examples",
                "    description: Disabled reference pipeline showing runtime task expansion with entities.",
                "    enabled: false",
                "    entities:",
                "      report:",
                "        - payment",
                "        - adjustment",
                "        - refund",
                "    max_parallel_tasks: 3",
                "    tasks:",
                "      extract_report:",
                "        type: python",
                "        path: pipelines/extract.py",
                "        function: extract_data",
                "        kwargs:",
                "          records: 25",
                '          report: "{report}"',
                "      validate_report:",
                "        type: cli",
                "        command: python {scripts_dir}/validate_cli.py {report}",
                "        cwd: .",
                "        depends_on: [extract_report]",
                "        # Lightweight conditional execution; a false result skips the task.",
                "        run_if: \"{report} != 'refund'\"",
                "      summarize_reports:",
                "        type: python",
                "        path: pipelines/extract.py",
                "        function: summarize_reports",
                "        entities: false",
                "        depends_on: [validate_report]",
                "",
                "  operator_examples:",
                "    title: Operator Examples",
                "    description: Disabled reference pipeline showing every built-in operator type.",
                "    enabled: false",
                "    tasks:",
                "      cli_example:",
                "        type: cli",
                "        command: python -c \"print('cli operator ok')\"",
                "      bash_env_example:",
                "        type: cli",
                "        shell: bash",
                "        command: set -a && source .env && set +a && conda run -n {conda_env} python {scripts_dir}/validate_cli.py ${APP_BATCH_ID}",
                "        cwd: .",
                "        depends_on: [cli_example]",
                "      api_example:",
                "        type: api",
                "        url: https://example.com/api/ping",
                "        method: GET",
                "        expected_status: [200]",
                "      webhook_example:",
                "        type: webhook",
                "        url: https://example.com/webhook",
                "        method: POST",
                '        body: \'{"event":"piply-demo"}\'',
                "        depends_on: [cli_example]",
                "        on_upstream_failure: continue",
                "      email_example:",
                "        type: email",
                "        smtp_host: ${SMTP_HOST}",
                "        smtp_user: ${SMTP_USER}",
                "        smtp_password: ${SMTP_PASSWORD}",
                "        to: [team@example.com]",
                "        subject: Piply starter notification",
                "        body: Starter notification from Piply.",
                "        depends_on: [cli_example]",
                "        on_upstream_failure: skip",
                "      ssh_example:",
                "        type: ssh",
                "        host: localhost",
                "        user: ${SSH_USER}",
                "        command: echo piply-ssh-ok",
                "        depends_on: [cli_example]",
                "        on_upstream_failure: fail",
                "",
                "  sensor_examples:",
                "    title: Sensor Examples",
                "    description: Disabled reference pipeline showing file, SQL, and API sensors.",
                "    enabled: false",
                "    sensors:",
                "      inbox_files:",
                "        type: file_sensor",
                "        path: sensor_inbox",
                "        pattern: '*.csv'",
                "        ignore_existing: true",
                "      inbound_rows:",
                "        type: sql_sensor",
                "        connection_ref: local_sensor_db",
                "        table: inbound_events",
                "        cursor_column: id",
                "        ignore_existing: true",
                "      external_api:",
                "        type: api_sensor",
                "        url: https://example.com/api/events",
                "        method: GET",
                "        cursor_path: version",
                "        expected_status: [200]",
                "        ignore_existing: true",
                "    tasks:",
                "      inspect_event:",
                "        type: cli",
                "        command: python -c \"print('sensor event received')\"",
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
                "def extract_data(records: int = 100, report: str = 'default') -> dict[str, object]:",
                "    print(f'Extracting {records} records for {report}...')",
                "    for step in range(1, 4):",
                "        print(f'Chunk {step}/3 complete')",
                "        time.sleep(0.3)",
                "    print('Extract complete')",
                "    return {'records': records, 'chunks': 3, 'report': report}",
                "",
                "",
                "def transform_data(context: dict[str, object]) -> dict[str, object]:",
                "    extracted = context.get('extract') or {}",
                "    if not isinstance(extracted, dict):",
                "        extracted = {}",
                "    records = int(extracted.get('records') or 0)",
                "    transformed = {'records': records + 1, 'source': 'extract_flow', 'report': extracted.get('report')}",
                '    print(f"Transformed payload: {transformed}")',
                "    return transformed",
                "",
                "",
                "def summarize_reports(context: dict[str, object]) -> dict[str, object]:",
                "    mapped = context.get('mapped') or {}",
                "    validate_outputs = mapped.get('validate_report') if isinstance(mapped, dict) else {}",
                "    if not isinstance(validate_outputs, dict):",
                "        validate_outputs = {}",
                "    print(f'Summarized mapped reports: {sorted(validate_outputs)}')",
                "    return {'reports': sorted(validate_outputs)}",
                "",
                "",
                "def main() -> None:",
                "    args = parse_args()",
                "    extract_data(records=args.records)",
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
                "from pathlib import Path",
                "",
                "",
                "def build_report(report_name: str = 'starter-report', context: dict[str, object] | None = None) -> str:",
                "    context = context or {}",
                "    upstream = context.get('transform') or context.get('upstream') or {}",
                "    if isinstance(upstream, dict) and 'transform' in upstream:",
                "        upstream = upstream['transform']",
                "    if isinstance(upstream, dict):",
                "        record_count = upstream.get('records')",
                "    else:",
                "        record_count = None",
                "    print(f'Generating downstream report: {report_name}')",
                "    if record_count is not None:",
                "        print(f'Upstream records: {record_count}')",
                "",
                "    # Written where the pipeline declares its artifacts glob.",
                "    reports_dir = Path('reports')",
                "    reports_dir.mkdir(exist_ok=True)",
                "    (reports_dir / f'{report_name}.txt').write_text(",
                "        f'report={report_name}\\nrecords={record_count}\\n', encoding='utf-8'",
                "    )",
                "    print('Report complete.')",
                "    return report_name",
            ]
        ),
        encoding="utf-8",
    )

    validate_path.write_text(
        "\n".join(
            [
                "from __future__ import annotations",
                "",
                "import sys",
                "",
                "",
                "def main() -> None:",
                "    batch_id = sys.argv[1] if len(sys.argv) > 1 else 'manual'",
                "    print(f'Validating batch {batch_id}')",
                "    print('Validation complete')",
                "",
                "",
                "if __name__ == '__main__':",
                "    main()",
            ]
        ),
        encoding="utf-8",
    )

    typer.echo(f"Created {config_path}")
    typer.echo(f"Created {extract_path}")
    typer.echo(f"Created {report_path}")
    typer.echo(f"Created {validate_path}")
    typer.echo(f"Created {sensor_inbox_dir}")
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
    tenant: str | None = typer.Option(None, "--tenant", help="Tenant id to attach to this run."),
    param: list[str] | None = RUN_PARAM_OPTION,
    wait: bool = typer.Option(
        True,
        "--wait/--detach",
        help="Wait and stream logs in the terminal.",
    ),
) -> None:
    service = PipelineService(config_path=_resolve_config(config))
    try:
        params = _parse_params(param)
        initial_context = {"params": params} if params else {}
        run_record = service.trigger_pipeline(
            pipeline_id,
            trigger="manual",
            wait=wait,
            on_log=typer.echo if wait else None,
            tenant_id=tenant,
            initial_context=initial_context,
        )
    except KeyError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    except KeyboardInterrupt:
        _handle_interrupt(service)
        raise typer.Exit(code=130) from None

    typer.echo(f"Run ID: {run_record.run_id}")
    if wait:
        run_record, _, _ = service.get_run(run_record.run_id)
        typer.echo(f"Finished with status: {run_record.status}")
        if run_record.error:
            typer.echo(run_record.error)
            raise typer.Exit(code=1)


@tasks_app.command("retry")
def retry_task(
    run_id: str = typer.Argument(..., help="Failed run identifier."),
    task_id: str = typer.Argument(..., help="Failed task identifier to retry from."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    mode: str = typer.Option("resume", "--mode", help="Retry mode: resume or startover."),
    wait: bool = typer.Option(True, "--wait/--detach", help="Wait and stream logs in the terminal."),
) -> None:
    service = PipelineService(config_path=_resolve_config(config))
    normalized_mode = mode.strip().lower()
    if normalized_mode not in {"resume", "startover"}:
        raise typer.BadParameter("--mode must be 'resume' or 'startover'.")
    try:
        run_record = service.retry_run(
            run_id,
            mode=normalized_mode,  # type: ignore[arg-type]
            task_id=task_id,
            wait=wait,
            on_log=typer.echo if wait else None,
        )
    except (KeyError, ValueError) as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    except KeyboardInterrupt:
        _handle_interrupt(service)
        raise typer.Exit(code=130) from None

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
    run_id: str | None = typer.Argument(None, help="Run ID to fetch logs for. Omit to follow across runs."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    follow: bool = typer.Option(False, "--follow", "-f", help="Stream new log lines as they are written."),
    pipeline_id: str | None = typer.Option(None, "--pipeline", help="Only show logs from this pipeline."),
    task_id: str | None = typer.Option(None, "--task", help="Only show logs from this task."),
    limit: int = typer.Option(200, "--limit", help="How many recent lines to print before following."),
    color: bool = typer.Option(True, "--color/--no-color", help="Colorize the task name and stream."),
    interval: float = typer.Option(1.0, "--interval", help="Seconds between polls while following."),
) -> None:
    """Print run logs, optionally following new lines as they arrive."""
    service = PipelineService(config_path=_resolve_config(config))
    if run_id is not None and service.store.get_run(run_id) is None:
        typer.echo(f"Unknown run '{run_id}'")
        raise typer.Exit(code=1)

    use_color = color and sys.stdout.isatty()
    # The tail is read newest-first in SQL and re-sorted, so a huge log history
    # never has to be pulled into memory just to show the last few lines.
    backlog = service.store.recent_logs(
        run_id=run_id,
        pipeline_id=pipeline_id,
        task_id=task_id,
        limit=max(1, limit),
    )
    for line in backlog:
        typer.echo(_format_log_line(line, use_color))

    if not follow:
        return

    cursor = int(backlog[-1]["id"]) if backlog else service.store.latest_log_id()

    typer.echo(_dim("-- following new log lines, press Ctrl+C to stop --", use_color))
    try:
        while True:
            batch = service.tail_logs(
                run_id=run_id,
                pipeline_id=pipeline_id,
                task_id=task_id,
                after_id=cursor,
                limit=500,
            )
            for line in batch:
                typer.echo(_format_log_line(line, use_color))
                cursor = int(line["id"])
            time.sleep(max(0.1, interval))
    except KeyboardInterrupt:
        typer.echo(_dim("-- stopped following --", use_color))


@tasks_app.command("run")
def run_task(
    pipeline_id: str = typer.Argument(..., help="Pipeline identifier."),
    task_id: str = typer.Argument(..., help="Task identifier."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    tenant: str | None = typer.Option(None, "--tenant", help="Tenant id to attach to this task-scoped run."),
    param: list[str] | None = TASK_PARAM_OPTION,
    wait: bool = typer.Option(True, "--wait/--detach", help="Wait and stream logs in the terminal."),
) -> None:
    service = PipelineService(config_path=_resolve_config(config))
    try:
        params = _parse_params(param)
        initial_context = {"params": params} if params else {}
        run_record = service.trigger_task(
            pipeline_id,
            task_id,
            trigger="task",
            wait=wait,
            on_log=typer.echo if wait else None,
            tenant_id=tenant,
            initial_context=initial_context,
        )
    except KeyError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    except KeyboardInterrupt:
        _handle_interrupt(service)
        raise typer.Exit(code=130) from None

    typer.echo(f"Run ID: {run_record.run_id}")
    if wait:
        run_record, _, _ = service.get_run(run_record.run_id)
        typer.echo(f"Finished with status: {run_record.status}")
        if run_record.error:
            typer.echo(run_record.error)
            raise typer.Exit(code=1)


@app.command()
def plan(
    pipeline_id: str | None = typer.Argument(None, help="Pipeline to preview. Omit to preview every pipeline."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    tenant: str | None = typer.Option(None, "--tenant", help="Tenant id to resolve variables against."),
    param: list[str] | None = PLAN_PARAM_OPTION,
    as_json: bool = typer.Option(False, "--json", help="Emit the preview as JSON."),
) -> None:
    """Show what a run would do without executing anything."""
    service = PipelineService(config_path=_resolve_config(config))
    params = _parse_params(param)
    try:
        previews = (
            [service.preview_pipeline(pipeline_id, params=params, tenant_id=tenant)]
            if pipeline_id
            else service.preview_project()
        )
    except KeyError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc

    if as_json:
        typer.echo(json.dumps([item.as_dict() for item in previews], indent=2, default=str))
        return

    for preview in previews:
        typer.echo("")
        typer.echo(f"{preview.title}  ({preview.pipeline_id})")
        if preview.deployment_id:
            typer.echo(f"  deployment : {preview.deployment_id} from template {preview.template_id}")
        typer.echo(f"  schedule   : {preview.schedule_text}")
        typer.echo(f"  execution  : {preview.execution_mode}, up to {preview.max_parallel_tasks} parallel tasks")
        typer.echo(f"  retry      : {preview.retry_summary}")
        if preview.timeout_seconds:
            typer.echo(f"  timeout    : {preview.timeout_seconds}s")
        if preview.triggers_on_success:
            typer.echo(f"  downstream : {', '.join(preview.triggers_on_success)}")

        if preview.variables:
            typer.echo("  resolved variables:")
            for key, value in sorted(preview.variables.items()):
                typer.echo(f"    {key} = {value}")

        if preview.entities:
            typer.echo("  expanded entities:")
            for key, values in sorted(preview.entities.items()):
                typer.echo(f"    {key}: {', '.join(values)}")

        typer.echo(f"  execution order ({preview.runnable_task_count}/{preview.task_count} will run):")
        for stage_index, stage in enumerate(preview.stages, start=1):
            typer.echo(f"    stage {stage_index}:")
            for task_id in stage:
                task = next(item for item in preview.tasks if item.task_id == task_id)
                marker = "run " if task.will_run else "skip"
                extras = []
                if task.priority:
                    extras.append(f"priority {task.priority}")
                if task.timeout_seconds:
                    extras.append(f"timeout {task.timeout_seconds}s")
                if task.depends_on:
                    extras.append(f"after {', '.join(task.depends_on)}")
                if task.skip_reason:
                    extras.append(task.skip_reason)
                detail = f"  ({'; '.join(extras)})" if extras else ""
                typer.echo(f"      [{marker}] {task.task_id} [{task.task_type}]{detail}")
                typer.echo(f"             {task.command}")
                if task.artifact_paths:
                    typer.echo(f"             artifacts: {', '.join(task.artifact_paths)}")

        for warning in preview.warnings:
            typer.echo(f"  warning: {warning}")


@app.command()
def prune(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    run_days: int | None = typer.Option(None, "--run-days", help="Delete finished runs older than this many days."),
    log_days: int | None = typer.Option(None, "--log-days", help="Delete log lines older than this many days."),
    max_runs: int | None = typer.Option(None, "--max-runs", help="Keep at most this many runs per pipeline."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Report what would be removed without deleting."),
    vacuum: bool = typer.Option(True, "--vacuum/--no-vacuum", help="Run SQLite VACUUM after pruning."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
) -> None:
    """Delete run history, logs, and artifact records beyond the retention window."""
    service = PipelineService(config_path=_resolve_config(config))
    overrides: dict[str, int] = {}
    if run_days is not None:
        overrides["run_retention_days"] = run_days
    if log_days is not None:
        overrides["log_retention_days"] = log_days
    if max_runs is not None:
        overrides["max_runs_per_pipeline"] = max_runs

    if not dry_run and not yes:
        planned = service.prune(dry_run=True, vacuum=False, **overrides)
        typer.echo(
            f"About to delete {planned['runs_deleted']} run(s) and at least "
            f"{planned['logs_deleted']} log line(s) from {service.database_location}."
        )
        if not typer.confirm("Continue?"):
            typer.echo("Aborted. Nothing was deleted.")
            raise typer.Exit(code=1)

    summary = service.prune(dry_run=dry_run, vacuum=vacuum, **overrides)
    prefix = "Would delete" if dry_run else "Deleted"
    typer.echo(f"{prefix} {summary['runs_deleted']} run(s), {summary['logs_deleted']} log line(s).")
    if not dry_run:
        typer.echo(f"Removed {summary['artifacts_deleted']} artifact record(s).")
        before = summary["database_bytes_before"]
        after = summary["database_bytes_after"]
        typer.echo(f"Database size: {_format_bytes(before)} -> {_format_bytes(after)}")


@app.command()
def backfill(
    target: str = typer.Argument(..., help="Run id to replay, or pipeline id when using --from/--to."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    start: datetime | None = BACKFILL_START_OPTION,
    end: datetime | None = BACKFILL_END_OPTION,
    limit: int = typer.Option(200, "--limit", help="Maximum number of slots to queue."),
    wait: bool = typer.Option(False, "--wait/--detach", help="Wait and stream logs when replaying one run."),
) -> None:
    """Replay a historic run, or queue every scheduled slot in a past window.

    Replaying a run reuses the exact configuration it captured, so a downstream
    pipeline can be re-run without re-running the upstream chain.
    """
    service = PipelineService(config_path=_resolve_config(config))

    if start is not None or end is not None:
        if start is None or end is None:
            raise typer.BadParameter("Both --from and --to are required to backfill a schedule window.")
        try:
            slots = service.backfill_schedule(
                target,
                start=start.astimezone(timezone.utc) if start.tzinfo else start.replace(tzinfo=timezone.utc),
                end=end.astimezone(timezone.utc) if end.tzinfo else end.replace(tzinfo=timezone.utc),
                limit=limit,
            )
        except (KeyError, ValueError) as exc:
            typer.echo(str(exc))
            raise typer.Exit(code=1) from exc
        typer.echo(f"Queued {len(slots)} scheduled slot(s) for '{target}'.")
        for slot in slots:
            typer.echo(f"  {slot.isoformat()}")
        return

    try:
        run_record = service.backfill_run(target, wait=wait, on_log=typer.echo if wait else None)
    except (KeyError, ValueError) as exc:
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
def artifacts(
    run_id: str = typer.Argument(..., help="Run id to list artifacts for."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    task_id: str | None = typer.Option(None, "--task", help="Only list artifacts from this task."),
) -> None:
    """List the files produced by one run."""
    service = PipelineService(config_path=_resolve_config(config))
    try:
        records = service.list_run_artifacts(run_id, task_id)
    except KeyError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc

    if not records:
        typer.echo("No artifacts were recorded for this run.")
        return
    for record in records:
        state = "" if record.get("exists") else "  (missing on disk)"
        typer.echo(f"{record['task_id']}  {_format_bytes(int(record['size_bytes']))}  {record['path']}{state}")


@app.command()
def backup(
    destination: str = typer.Argument(..., help="Target file, or a directory to write a timestamped file into."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
) -> None:
    """Snapshot the runtime database while Piply keeps running.

    Uses SQLite's online backup API, so this is safe to run against a live
    server and against a database with an active write-ahead log.
    """
    service = PipelineService(config_path=_resolve_config(config))
    try:
        written = service.store.backup_to(destination)
    except (OSError, RuntimeError) as exc:
        typer.echo(f"Backup failed: {exc}")
        raise typer.Exit(code=1) from exc

    typer.echo(f"Source: {service.database_location}")
    typer.echo(f"Backup: {written} ({_format_bytes(written.stat().st_size)})")


@app.command()
def restore(
    source: str = typer.Argument(..., help="Backup file to restore from."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
) -> None:
    """Replace the runtime database with a backup.

    Stop the server first: restoring under a running scheduler would leave it
    holding handles to the database it is about to lose.
    """
    source_path = Path(source).resolve()
    if not source_path.is_file():
        typer.echo(f"Backup file not found: {source_path}")
        raise typer.Exit(code=1)

    settings = load_settings(_resolve_config(config))
    config_path = _resolve_config(config)
    if settings.database_dsn is not None:
        typer.echo(
            "piply restore only writes to a SQLite store. This runtime uses PostgreSQL; "
            "restore it with your database's own tooling, for example 'pg_restore'."
        )
        raise typer.Exit(code=1)
    target = settings.database_path or (config_path.parent / ".piply" / "piply.db")

    if target.exists() and not yes:
        typer.echo(f"This will overwrite {target} ({_format_bytes(target.stat().st_size)}).")
        if not typer.confirm("Continue?"):
            typer.echo("Aborted. Nothing was changed.")
            raise typer.Exit(code=1)

    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        # Keep the displaced database next to the target rather than deleting it.
        rollback = target.with_suffix(target.suffix + ".replaced")
        shutil.copy2(target, rollback)
        typer.echo(f"Previous database kept at {rollback}")
    # Copy through SQLite so the restored file is checkpointed and WAL-free.
    connection = sqlite3.connect(source_path)
    try:
        for stale in (target, Path(f"{target}-wal"), Path(f"{target}-shm")):
            stale.unlink(missing_ok=True)
        with sqlite3.connect(target) as destination:
            connection.backup(destination)
    finally:
        connection.close()

    typer.echo(f"Restored {source_path} -> {target}")


@app.command()
def diagnostics(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    as_json: bool = typer.Option(False, "--json", help="Emit the diagnostics payload as JSON."),
) -> None:
    """Print scheduler, worker, sensor, and reconciliation health."""
    service = PipelineService(config_path=_resolve_config(config))
    payload = service.diagnostics()
    if as_json:
        typer.echo(json.dumps(payload, indent=2, default=str))
        return

    scheduler = payload["scheduler"]
    workers = payload["workers"]
    queue = payload["queue"]
    typer.echo(f"Scheduler   : {scheduler['label']} (state {scheduler['state']})")
    typer.echo(f"Heartbeat   : {scheduler['heartbeat'] or 'never'}")
    if scheduler.get("last_error"):
        typer.echo(f"Last error  : {scheduler['last_error']}")
    typer.echo(f"Workers     : {workers['running_runs']} run(s), {workers['running_tasks']} task(s) running")
    typer.echo(f"Queue       : {queue.get('queued', 0)} queued, {queue.get('due', 0)} due")

    running_tasks = payload["running_tasks"]
    typer.echo(f"Running now : {len(running_tasks)} task(s)")
    for task in running_tasks:
        elapsed = task.get("running_seconds") or 0
        typer.echo(f"  {task['pipeline_id']} / {task['task_id']}  ({elapsed:.0f}s, run {task['run_id']})")

    summary = payload["sensor_summary"]
    typer.echo(f"Sensors     : {summary['healthy']} healthy, {summary['failing']} failing, {summary['idle']} idle")
    for sensor in payload["sensors"]:
        if sensor["status"] == "failing":
            typer.echo(f"  FAILING {sensor['pipeline_id']}/{sensor['sensor_id']}: {sensor['last_error']}")

    reconciliation = payload["reconciliation"]
    typer.echo(f"Recovery    : last ran {reconciliation['last_recovery_at'] or 'never'}")
    typer.echo(f"              recovered {reconciliation['last_recovered_runs']} interrupted run(s) at startup")
    database = payload["database"]
    size = int(database.get("size_bytes") or 0)
    size_label = f" ({_format_bytes(size)})" if size else ""
    typer.echo(f"Database    : {database['path']} [{database.get('backend', 'sqlite')}]{size_label}")


@users_app.command("create")
def users_create(
    username: str = typer.Argument(..., help="Username to create."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    password: str | None = typer.Option(None, "--password", help="Password. Generated when omitted."),
    role: str = typer.Option("user", "--role", help="admin or user."),
    grant: list[str] | None = USER_GRANT_OPTION,
) -> None:
    """Create an account, optionally granting pipeline permissions.

    Creating the first account switches authentication on for the install.
    """
    service = PipelineService(config_path=_resolve_config(config))
    secret = password or generate_password()
    permissions: dict[str, object] = {}
    for item in grant or []:
        if "=" not in item:
            raise typer.BadParameter("--grant must use PIPELINE=actions, for example reports=view,run")
        pipeline_id, actions = item.split("=", 1)
        permissions[pipeline_id.strip()] = actions

    try:
        user = service.create_user(username, secret, role=role, permissions=permissions)
    except AuthError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc

    typer.echo(f"Created {user.role} '{user.username}'.")
    if password is None:
        typer.echo(f"Password: {secret}")
        typer.echo("Store it now. It is hashed and cannot be shown again.")
    _echo_permissions(user)


@users_app.command("list")
def users_list(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
) -> None:
    """List accounts and their pipeline permissions."""
    service = PipelineService(config_path=_resolve_config(config))
    users = service.list_users()
    if not users:
        typer.echo("No accounts exist. Authentication is off unless PIPLY_AUTH_ENABLED is set.")
        return
    for user in users:
        state = "active" if user.is_active else "disabled"
        typer.echo(f"{user.username}  [{user.role}, {state}]  last login: {user.last_login_at or 'never'}")
        _echo_permissions(user)


@users_app.command("passwd")
def users_passwd(
    username: str = typer.Argument(..., help="Account to update."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    password: str | None = typer.Option(None, "--password", help="New password. Generated when omitted."),
) -> None:
    """Set a new password for an account."""
    service = PipelineService(config_path=_resolve_config(config))
    secret = password or generate_password()
    try:
        service.update_user(username, password=secret)
    except AuthError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    typer.echo(f"Password updated for '{username}'.")
    if password is None:
        typer.echo(f"Password: {secret}")


@users_app.command("grant")
def users_grant(
    username: str = typer.Argument(..., help="Account to grant."),
    pipeline_id: str = typer.Argument(..., help="Pipeline id, or '*' for every pipeline."),
    actions: str = typer.Argument(..., help="Comma-separated: view, edit, run, or all."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
) -> None:
    """Grant pipeline permissions to an account."""
    service = PipelineService(config_path=_resolve_config(config))
    try:
        user = service.grant_permission(username, pipeline_id, actions)
    except AuthError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    typer.echo(f"Updated permissions for '{username}'.")
    _echo_permissions(user)


@users_app.command("revoke")
def users_revoke(
    username: str = typer.Argument(..., help="Account to change."),
    pipeline_id: str = typer.Argument(..., help="Pipeline id, or '*'."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
) -> None:
    """Remove every permission an account holds on one pipeline."""
    service = PipelineService(config_path=_resolve_config(config))
    try:
        user = service.revoke_permission(username, pipeline_id)
    except AuthError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    typer.echo(f"Revoked '{pipeline_id}' for '{username}'.")
    _echo_permissions(user)


@users_app.command("disable")
def users_disable(
    username: str = typer.Argument(..., help="Account to disable."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
) -> None:
    """Disable an account without deleting it."""
    service = PipelineService(config_path=_resolve_config(config))
    try:
        service.update_user(username, is_active=False)
    except AuthError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    typer.echo(f"Disabled '{username}'.")


@users_app.command("delete")
def users_delete(
    username: str = typer.Argument(..., help="Account to delete."),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to piply.yaml"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
) -> None:
    """Delete an account and every permission it held."""
    service = PipelineService(config_path=_resolve_config(config))
    if not yes and not typer.confirm(f"Delete account '{username}'?"):
        typer.echo("Aborted.")
        raise typer.Exit(code=1)
    try:
        service.delete_user(username)
    except AuthError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    typer.echo(f"Deleted '{username}'.")


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
        raise typer.Exit(code=1) from exc


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
        raise typer.Exit(code=1) from exc


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
    if settings.database_dsn is not None:
        environment["PIPLY_DATABASE"] = settings.database_dsn
    elif settings.database_path is not None:
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
    if settings.database_dsn is not None:
        os.environ["PIPLY_DATABASE"] = settings.database_dsn
    elif settings.database_path is not None:
        os.environ["PIPLY_DATABASE"] = str(settings.database_path)
    typer.echo(f"Using config: {config_path}")
    typer.echo(f"Runtime database: {_describe_database(config_path, settings)}")
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
