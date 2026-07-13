# Piply

Piply is a lightweight Python pipeline framework for teams that want YAML-defined workflows, schedules, retries, logs, sensors, and an operations UI without running a heavy orchestration stack.

It stays small on purpose:

- local dependency-aware DAG execution
- SQLite for runs, logs, task outputs, queue state, sensors, and pause overrides
- FastAPI plus server-rendered UI
- no Redis, Celery, Airflow, Prefect, or external queue required

## Features

- Multi-task pipelines with `depends_on`
- Python script, Python callable, CLI, API, webhook, email, and SSH tasks
- Reusable YAML `variables` with `{name}` interpolation
- `.env`, environment variables, explicit secrets, and reusable SQL connections
- Task output passing through `context["task_id"]`
- Pipeline-to-pipeline output and tenant context passing
- Metadata-driven `entities` expansion for reusable task templates
- Optional `pipeline_templates` and `pipeline_deployments` for advanced reuse
- Per-task upstream failure behavior: `skip`, `fail`, or `continue`
- Heartbeat-backed runtime recovery for interrupted runs and stale scheduler state
- Schedules, sensors, retries, cancellation, reruns, and searchable logs
- Dashboard, Pipelines, Execution Matrix, Logs, Settings, and run detail pages

## Quick Start

```bash
pip install -e .
copy .env.example .env
piply validate --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml
```

Open `http://127.0.0.1:8000`.

Run on a different port when `8000` is already in use:

```bash
piply start --config piply-demo/piply.yaml --port 8080
piply start --config piply-demo/piply.yaml --host 0.0.0.0 --port 8080
```

Create a starter workspace:

```bash
piply init my-piply-project
piply run extract_flow --config my-piply-project/piply.yaml --wait
```

## Minimal YAML

```yaml
version: "1"
title: Piply Workspace
workspace: .

variables:
  scripts_dir: pipelines
  batch_id: demo-batch

connections:
  app_db: sqlite:///sensor_demo.db

pipelines:
  extract_flow:
    schedule:
      every: 15m
    retry:
      attempts: 2
      mode: resume
      delay_seconds: 10
    triggers_on_success:
      - report_flow
    tasks:
      extract:
        type: python
        path: "{scripts_dir}/extract.py"
        function: extract_data

      transform:
        type: python
        path: "{scripts_dir}/extract.py"
        function: transform_data
        depends_on: [extract]

      validate:
        type: cli
        command: python {scripts_dir}/validate_cli.py {batch_id}
        cwd: .
        depends_on: [transform]

  report_flow:
    tasks:
      build_report:
        type: python
        path: "{scripts_dir}/report.py"
        function: build_report
```

Python callable tasks can consume upstream outputs:

```python
def transform_data(context):
    extracted = context["extract"]
    return {"records": extracted["records"] + 1}
```

For Bash-specific CLI commands, set `shell: bash`:

```yaml
tasks:
  load_env_and_run:
    type: cli
    shell: bash
    command: set -a && source .env && set +a && conda run -n py312_extract python {scripts_dir}/job.py
    cwd: .
```

## Dynamic Entity Mapping

Use `entities` when one task template should run once per business value. Piply expands templates at runtime into normal DAG tasks, so existing retries, logs, outputs, and parallel execution continue to work.

```yaml
pipelines:
  extract_flow:
    entities:
      report:
        - payment
        - adjustment
        - refund
    max_parallel_tasks: 3
    tasks:
      extract:
        type: python
        path: pipelines/extract.py
        function: extract_data
        kwargs:
          report: "{report}"

      validate:
        type: cli
        command: python validate.py --report {report}
        depends_on: [extract]
```

Runtime tasks are generated as `payment.extract -> payment.validate`, `adjustment.extract -> adjustment.validate`, and `refund.extract -> refund.validate`.

## Advanced Deployments

Simple `pipelines:` YAML remains the default. For repeated tenant or environment rollouts, define a reusable template and deployment-specific schedules or variables:

```yaml
pipeline_templates:
  report_pipeline:
    tasks:
      extract:
        type: python
        path: pipelines/extract.py
        function: extract_data
        kwargs:
          tenant: "{tenant}"

pipeline_deployments:
  client_a_reporting:
    template: report_pipeline
    schedule:
      every: 15m
    variables:
      tenant: client_a

  client_b_reporting:
    template: report_pipeline
    schedule:
      cron: "0 * * * *"
    tenant: client_b
```

Each deployment becomes a normal runnable pipeline id, so the scheduler, UI, CLI, and API keep working without a second execution model.

## Common CLI

```bash
piply init my-piply-project
piply validate --config piply-demo/piply.yaml
piply list --config piply-demo/piply.yaml
piply run extract_flow --config piply-demo/piply.yaml --wait
piply run extract_flow --tenant acme --param batch=2026-05-26 --config piply-demo/piply.yaml
piply tasks list extract_flow --config piply-demo/piply.yaml
piply tasks run extract_flow validate --tenant acme --param region=west --config piply-demo/piply.yaml
piply tasks retry <run_id> <task_id> --mode resume --config piply-demo/piply.yaml
piply runs --config piply-demo/piply.yaml
piply logs <run_id> --config piply-demo/piply.yaml
piply pause extract_flow --config piply-demo/piply.yaml
piply resume extract_flow --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml --port 8080
piply stop --config piply-demo/piply.yaml
```

## Docs

- [Usage Guide](wiki/USAGE_GUIDE.md): detailed YAML examples, `.env`, multi-tenant runs, sensors, and every CLI command
- [Wiki Overview](wiki/README.md): architecture and feature summary
- [UI And API Guide](wiki/UI_API_GUIDE.md): screens, actions, and API examples
- [Implementation Summary](wiki/IMPLEMENTATION_SUMMARY.md): runtime modules and verification expectations
- [Technical Architecture](docs/architecture/technical_architecture.md): maintainer-focused deep dive into execution, scheduler, state, storage, UI, and extension points

## Roadmap

Planned features:

- `piply logs --follow`
- plugin hooks for custom operators and sensors
- managed external secret backends
- richer queue, worker, and artifact metrics
- UI-safe pipeline editing
- task groups, conditional branches, and richer mapped-run visualization
- Optional distributed runner while keeping local mode as the default
- # Task Priority Support

Introduce optional task priority support.

Goal:

When multiple runnable tasks are available for execution,
the scheduler should prefer higher-priority tasks.

User-friendly syntax:

tasks:

  extract***:
    type: python

  transform**:
    type: python

  validate*:
    type: python

Interpretation:

*** = priority 3
**  = priority 2
*   = priority 1

Internally normalize task IDs:

extract***
→ extract

transform**
→ transform

Store priority separately.

Equivalent explicit syntax:

tasks:

  extract:
    priority: 3

  transform:
    priority: 2

  validate:
    priority: 1

Both syntaxes should be supported.

Execution Rules:

- Higher priority tasks execute first.
- Only among currently runnable tasks.
- Dependencies still take precedence.
- Priority does not bypass dependencies.
- Equal-priority tasks may execute FIFO or randomly.

Requirements:

- Backward compatible.
- UI should display priority visually.
- DAG graph should indicate priority.
- Runtime metadata should persist priority.
- Dynamic task expansion should inherit priority.
- Scheduler should sort ready tasks using priority.

Recommended scheduling order:

1. Priority DESC
2. Ready Time ASC
3. Created Time ASC
4. Random tie-breaker
