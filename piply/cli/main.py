import typer
import os
import json
from pathlib import Path
from typing import List, Optional
from piply.runner import run_pipeline
from piply.parser.yaml_parser import load_pipeline
from piply.engine.local_engine import LocalEngine
from piply.engine.prefect_engine import PrefectEngine
from piply.utils.logging import logger, setup_logger

app = typer.Typer()


@app.command()
def init(
    project_name: Optional[str] = typer.Option(
        None, "--name", "-n", help="Project name"),
    engine: str = typer.Option(
        "prefect", "--engine", "-e", help="Execution engine (prefect or local)")
):
    """Initialize a new piply project with a sample piply.yaml."""
    config_content = f"""
pipeline:
  name: {project_name or "my_pipeline"}
  schedule: "0 0 * * *"  # Daily at midnight (optional)
  retries: 1
  timeout: 3600  # seconds (optional)
  variables:
    environment: "dev"

  steps:
    - name: extract_data
      type: python
      function: "pipelines.extract.main"
      retries: 2
      retry_delay: 30

    - name: transform_data
      type: shell
      command: "echo 'Transforming data...'"
      depends_on:
        - extract_data

    - name: load_to_warehouse
      type: dbt
      command: "run"
      models:
        - "staging"
        - "marts"
      depends_on:
        - transform_data
"""
    config_path = Path("piply.yaml")
    if config_path.exists():
        overwrite = typer.confirm("piply.yaml already exists. Overwrite?")
        if not overwrite:
            typer.echo("Aborted.")
            raise typer.Abort()

    config_path.write_text(config_content.strip())
    typer.echo(f"✓ Created {config_path}")

    # Create example Python module
    example_dir = Path("pipelines")
    example_dir.mkdir(exist_ok=True)

    extract_file = example_dir / "extract.py"
    extract_file.write_text('''
def main(context):
    """Extract data from source."""
    print(f"Extracting data for tenant: {context.tenant}")
    # Your extraction logic here
    return {"status": "success", "rows": 1000}
''')
    typer.echo(f"✓ Created {extract_file}")

    typer.echo("\nNext steps:")
    typer.echo("  1. Edit piply.yaml to customize your pipeline")
    typer.echo("  2. Run: piply run")
    typer.echo("  3. Add more steps as needed")


@app.command()
def run(
    file: str = typer.Option("piply.yaml", "--file",
                             "-f", help="Path to pipeline YAML"),
    engine: str = typer.Option(
        "local", "--engine", "-e", help="Execution engine (prefect, local, auto; default: local)"),
    tenant: Optional[str] = typer.Option(
        None, "--tenant", "-t", help="Run for specific tenant only"),
    retry_failed: bool = typer.Option(
        False, "--retry-failed", help="Only retry failed steps")
):
    """Run a pipeline."""
    if not os.path.exists(file):
        typer.echo(f"Error: File '{file}' not found")
        raise typer.Exit(1)

    try:
        # Set log level
        setup_logger(level="INFO")

        typer.echo(f"Loading pipeline from {file}...")
        pipeline = load_pipeline(file)

        # Select engine
        engine_lower = engine.lower()
        if engine_lower == "auto":
            typer.echo("Auto-selecting engine...")
            try:
                eng = PrefectEngine()
                typer.echo("Using Prefect engine")
            except Exception as e:
                typer.echo(f"Prefect not available ({e}), using local engine")
                eng = LocalEngine()
        elif engine_lower == "prefect":
            try:
                eng = PrefectEngine()
            except Exception as e:
                typer.echo(f"Warning: Prefect initialization failed: {e}")
                typer.echo("Falling back to local engine...")
                eng = LocalEngine()
        elif engine_lower == "local":
            eng = LocalEngine()
        else:
            typer.echo(
                f"Error: Unknown engine '{engine}'. Use 'prefect', 'local', or 'auto'")
            raise typer.Exit(1)

        # Run pipeline
        try:
            pipeline.run(eng, tenant=tenant, retry_failed=retry_failed)
        except FileNotFoundError as e:
            # Prefect SSL/certificate errors
            typer.echo(
                f"\nError: Prefect encountered a configuration issue: {e}")
            typer.echo(
                "This usually means Prefect is trying to connect to a server.")
            typer.echo("Please use the local engine instead:")
            typer.echo("  piply run --engine local")
            raise typer.Exit(1)
        except Exception as e:
            typer.echo(f"Error: {e}")
            logger.exception("Pipeline execution failed")
            raise typer.Exit(1)

    except Exception as e:
        typer.echo(f"Error: {e}")
        logger.exception("Pipeline execution failed")
        raise typer.Exit(1)


@app.command()
def list_dags(
    directory: str = typer.Option(
        ".", "--dir", "-d", help="Directory to scan for pipeline files")
):
    """List all available pipelines (DAGs)."""
    typer.echo("\nAvailable Pipelines:")
    typer.echo("=" * 60)

    # Look for YAML files
    yaml_files = list(Path(directory).glob("*.yaml")) + \
        list(Path(directory).glob("*.yml"))

    if not yaml_files:
        typer.echo("No pipeline YAML files found.")
        return

    for yaml_file in yaml_files:
        try:
            pipeline = load_pipeline(str(yaml_file))
            status = "✓"
            typer.echo(f"{status} {pipeline.name:30} ({yaml_file.name})")
            typer.echo(
                f"   Steps: {len(pipeline.steps)}, Tenants: {len(pipeline.tenants)}")
        except Exception as e:
            typer.echo(f"✗ {yaml_file.name:30} (Error: {e})")


@app.command()
def test(
    pipeline_file: str = typer.Argument(..., help="Path to pipeline YAML"),
    tenant: Optional[str] = typer.Option(
        None, "--tenant", "-t", help="Test with specific tenant")
):
    """Test pipeline execution (dry run with local engine)."""
    typer.echo(f"Testing pipeline: {pipeline_file}")

    try:
        pipeline = load_pipeline(pipeline_file)
        typer.echo(f"  Name: {pipeline.name}")
        typer.echo(f"  Steps: {[s.name for s in pipeline.steps]}")
        typer.echo(f"  Tenants: {pipeline.tenants or ['none']}")

        # Run with local engine for testing
        engine = LocalEngine()
        pipeline.run(engine, tenant=tenant)

        typer.echo("\n✓ Test completed successfully")
    except Exception as e:
        typer.echo(f"\n✗ Test failed: {e}")
        raise typer.Exit(1)


@app.command()
def tasks_list(
    pipeline_file: str = typer.Argument(..., help="Path to pipeline YAML")
):
    """List tasks in a pipeline."""
    try:
        pipeline = load_pipeline(pipeline_file)
        typer.echo(f"\nTasks in pipeline '{pipeline.name}':")
        typer.echo("=" * 60)

        for i, step in enumerate(pipeline.steps, 1):
            deps = ", ".join(step.depends_on) if step.depends_on else "none"
            typer.echo(f"{i}. {step.name}")
            typer.echo(f"   Type: {step.config.get('type', 'unknown')}")
            typer.echo(f"   Depends on: {deps}")
            typer.echo(f"   Retries: {step.retries}")
            typer.echo()
    except Exception as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(1)


@app.command()
def tasks_run(
    pipeline_file: str = typer.Argument(..., help="Path to pipeline YAML"),
    task: Optional[str] = typer.Option(
        None, "--task", "-t", help="Run specific task only"),
    all_tasks: bool = typer.Option(False, "--all", "-a", help="Run all tasks"),
    engine: str = typer.Option(
        "local", "--engine", "-e", help="Execution engine")
):
    """Run specific tasks or all tasks."""
    if not task and not all_tasks:
        typer.echo("Error: Must specify either --task or --all")
        raise typer.Exit(1)

    try:
        pipeline = load_pipeline(pipeline_file)

        if task:
            # Run specific task
            step = next((s for s in pipeline.steps if s.name == task), None)
            if not step:
                typer.echo(f"Error: Task '{task}' not found in pipeline")
                raise typer.Exit(1)

            typer.echo(f"Running task: {step.name}")
            if engine.lower() == "prefect":
                eng = PrefectEngine()
            else:
                eng = LocalEngine()

            # Create a minimal pipeline with just this task
            from piply.core.pipeline import Pipeline as NewPipeline
            single_pipeline = NewPipeline(
                name=pipeline.name,
                steps=[step],
                tenants=pipeline.tenants
            )
            single_pipeline.run(eng)
        else:
            # Run all tasks
            typer.echo("Running all tasks...")
            if engine.lower() == "prefect":
                eng = PrefectEngine()
            else:
                eng = LocalEngine()
            pipeline.run(eng)

    except Exception as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(1)


@app.command()
def logs(
    pipeline_file: str = typer.Argument(..., help="Path to pipeline YAML"),
    task: Optional[str] = typer.Option(
        None, "--task", "-t", help="Show logs for specific task"),
    follow: bool = typer.Option(
        False, "--follow", "-f", help="Follow log output")
):
    """View pipeline logs."""
    typer.echo("Logs command not yet implemented.")
    typer.echo("Currently, logs are printed to stdout during execution.")
    if task:
        typer.echo(f"Would show logs for task: {task}")
    else:
        typer.echo("Would show pipeline logs")


@app.command()
def ui(
    host: str = typer.Option("0.0.0.0", "--host", "-h",
                             help="Host to bind to"),
    port: int = typer.Option(8000, "--port", "-p", help="Port to bind to"),
    reload: bool = typer.Option(
        False, "--reload", "-r", help="Enable auto-reload for development")
):
    """Start the Piply web UI server."""
    import uvicorn
    from piply.api.app import app as fastapi_app

    typer.echo(f"Starting Piply UI server on http://{host}:{port}")
    typer.echo("Press Ctrl+C to stop")

    uvicorn.run(
        fastapi_app,
        host=host,
        port=port,
        reload=reload
    )


if __name__ == "__main__":
    app()
