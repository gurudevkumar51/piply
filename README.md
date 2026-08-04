# Piply

Piply is a lightweight Python pipeline framework for teams that want YAML-defined workflows, schedules, retries, logs, sensors, and an operations UI without running a heavy orchestration stack.

It stays small on purpose:

- local dependency-aware DAG execution
- SQLite by default for runs, logs, task outputs, queue state, and sensors,
  with optional PostgreSQL when you'd rather keep state in a managed database
- FastAPI plus server-rendered UI
- no Redis, Celery, Airflow, Prefect, or external queue required

## Features

**Authoring**

- Multi-task pipelines with `depends_on`
- Python script, Python callable, CLI, API, webhook, email, and SSH tasks
- Reusable YAML `variables` with `{name}` interpolation
- `.env`, environment variables, explicit secrets, and reusable SQL connections
- Metadata-driven `entities` expansion for reusable task templates
- Optional `pipeline_templates` and `pipeline_deployments` for tenant reuse
- Lightweight conditional execution: `run_if: "{report} == 'payment'"`
- Declared artifacts: `artifacts: ["out/*.csv"]`

**Execution**

- Task priority, via `priority: high`, `priority: "***"`, or an `extract***:` id
- Task and pipeline timeouts with a configurable kill grace period
- Per-task upstream failure behavior: `skip`, `fail`, or `continue`
- Task output passing through `context["task_id"]`
- Downstream pipelines inherit upstream variables, env, and outputs
- Every run stores its full runtime configuration, so a downstream run can be
  retried or replayed without re-running the upstream chain

**Operations**

- Schedules, sensors, retries, cancellation, reruns, and searchable logs
- Dry-run preview: `piply plan`, plus an in-UI execution preview
- Live log streaming: `piply logs --follow` with pipeline/run/task filters
- Retention and cleanup: `piply prune` with automatic SQLite `VACUUM`
- Backfill a single run or a whole schedule window
- Graceful shutdown and startup recovery — no orphaned RUNNING records
- Prometheus metrics at `GET /metrics` and a runtime Diagnostics page
- Airflow-style pipeline listing with template grouping, sorting, and filtering
- Optional PostgreSQL metadata store: `PIPLY_DATABASE=postgresql://...`

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

For plain commands, omit `shell` so Piply uses the platform default shell. Set
`shell: bash` only for Bash-specific syntax and only on machines where Bash is
installed and available on `PATH`:

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

### Deployment Variables In Downstream Pipelines

Variables from a deployment are automatically passed to a downstream pipeline started through `triggers_on_success`. This is useful when several deployments share one downstream workflow. Parent deployment variables take precedence for the triggered run; a manual run of the downstream pipeline uses the downstream pipeline's own variables or top-level defaults.

```yaml
pipelines:
  Bronze_to_Silver:
    tasks:
      dbt:
        type: cli
        command: DBT_CLIENT={practice} dbt run --selector appointment_silver

pipeline_templates:
  ECW_Extract_test:
    tasks:
      extract:
        type: cli
        command: echo extract

pipeline_deployments:
  BENNETT_ETL_Flow:
    template: ECW_Extract_test
    variables:
      practice: BENNETT
    triggers_on_success: [Bronze_to_Silver]
```

When `BENNETT_ETL_Flow` succeeds, Piply runs `Bronze_to_Silver` with `DBT_CLIENT=BENNETT`. A deployment for PALOS would use the same target and set `practice: PALOS`.

## Common CLI

```bash
# Project
piply --version
piply init my-piply-project
piply validate --config piply-demo/piply.yaml
piply list --config piply-demo/piply.yaml
piply plan extract_flow --config piply-demo/piply.yaml     # dry run, nothing executes

# Running
piply run extract_flow --config piply-demo/piply.yaml --wait
piply run extract_flow --tenant acme --param batch=2026-05-26
piply tasks list extract_flow
piply tasks run extract_flow validate --tenant acme --param region=west
piply tasks retry <run_id> <task_id> --mode resume

# Inspecting
piply runs --limit 20
piply logs <run_id>
piply logs --follow --pipeline extract_flow                # live, colored
piply artifacts <run_id>
piply diagnostics

# Maintaining
piply backfill <run_id>                                    # replay a run's config
piply backfill nightly --from 2026-07-01 --to 2026-07-08   # fill a schedule window
piply prune --dry-run
piply prune --run-days 14 --max-runs 100
piply backup /backups                                      # safe while running
piply restore /backups/piply-20260804T074211Z.db           # stop the server first

# Serving
piply pause extract_flow
piply resume extract_flow
piply start --config piply-demo/piply.yaml --port 8080
piply stop --config piply-demo/piply.yaml
```

## Docs

**Using Piply**

- [YAML Specification](docs/YAML_SPECIFICATION.md): every config key, with defaults
- [Execution Examples](docs/EXAMPLES.md): runnable patterns for each feature
- [UI Guide](docs/UI_GUIDE.md): every page and what it answers
- [Migration Guide](docs/MIGRATION.md): moving onto pipeline templates and deployments
- [Usage Guide](wiki/USAGE_GUIDE.md): longer-form walkthrough

**Understanding Piply**

- [Runtime Lifecycles](docs/LIFECYCLES.md): scheduler, pipeline, task, retry, recovery, retention
- [Technical Architecture](docs/architecture/technical_architecture.md): maintainer guide to the whole system
- [Future Features](docs/FUTURE_FEATURES.md): proposed ideas, ranked by value vs cost
- [Wiki Overview](wiki/README.md): architecture and feature summary
- [UI And API Guide](wiki/UI_API_GUIDE.md): screens, actions, and API examples

## Metadata Store

Piply keeps its own runtime state in SQLite by default, with nothing to
configure. Point it at PostgreSQL when you would rather that state lived in a
managed database, for example so a container redeploy cannot lose it:

```bash
pip install "mr-piply[postgres]"
export PIPLY_DATABASE="postgresql://piply:secret@db.internal:5432/piply"
piply start
```

The schema is created and migrated automatically, and no pipeline configuration
changes. `piply backup` / `piply restore` remain SQLite-only; use `pg_dump` and
`pg_restore` for PostgreSQL. Run one Piply instance per database either way.

Full details, including the Docker volume setup for the SQLite default, are in
[YAML Specification §11](docs/YAML_SPECIFICATION.md#11-runtime-storage-and-external-databases).

## Monitoring

```bash
curl -s http://127.0.0.1:8000/metrics
curl -s http://127.0.0.1:8000/api/diagnostics
```

`/metrics` exposes run and task counts by status, running tasks, queue depth and
age, scheduler health, run durations, and per-sensor health in the Prometheus
text format. It accepts the API bearer token, so a scraper does not need UI
credentials.

## Roadmap

Proposed features, ranked by value against cost, with the reasoning for
what is deliberately *not* planned: [Future Features](docs/FUTURE_FEATURES.md).
