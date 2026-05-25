# Piply

Piply is a lightweight Python pipeline framework for teams that want script orchestration, schedules, retries, logs, sensors, and a usable operations UI without adopting a heavy orchestration stack.

It is intentionally small:

- YAML-first pipeline definitions
- local execution with dependency-aware DAG scheduling
- SQLite for runs, logs, task outputs, queue state, sensors, and pause overrides
- FastAPI + server-rendered UI
- no Redis, Celery, Prefect, Airflow, or external queue required

## What You Get

- Multi-task pipelines with `depends_on`
- Sequential or parallel execution inferred from the DAG
- Python script and Python callable tasks
- CLI, API, webhook, email, and SSH task types
- Task output passing with `context["task_id"]`
- Pipeline-to-pipeline JSON output passing
- Per-task upstream failure behavior: `skip`, `fail`, or `continue`
- Schedule backfill through an internal SQLite queue
- File, SQL, and API sensors
- Reusable SQL connections and explicit secret references
- Retries, resume-from-failure, cancellation, and logs
- Queue and local worker metrics
- Dashboard, Pipelines, Execution Matrix, Logs, Settings, and run detail pages

## Quick Start

```bash
pip install -e .
copy .env.example .env
piply validate --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml
```

Open `http://127.0.0.1:8000`.

Create a starter project:

```bash
piply init my-piply-project
piply validate --config my-piply-project/piply.yaml
piply start --config my-piply-project/piply.yaml
```

The generated starter includes a runnable `extract_flow -> report_flow` chain plus disabled reference pipelines for optional operators and sensors.

## Example YAML

```yaml
version: "1"
title: Piply Workspace
workspace: .

secrets:
  backend: env
  prefix: PIPLY_SECRET_

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
    max_parallel_tasks: 2
    triggers_on_success:
      - report_flow
    sensors:
      inbound_rows:
        type: sql_sensor
        connection_ref: app_db
        table: inbound_events
        cursor_column: id
    tasks:
      extract:
        type: python
        path: pipelines/extract.py
        function: extract_data
        kwargs:
          records: 120

      transform:
        type: python
        path: pipelines/extract.py
        function: transform_data
        depends_on: [extract]

      validate:
        type: cli
        command: python -c "print('validated')"
        depends_on: [transform]

      publish_manifest:
        type: cli
        command: python -c "print('published')"
        depends_on: [transform]
        on_upstream_failure: skip

  report_flow:
    tasks:
      build_report:
        type: python
        path: pipelines/report.py
        function: build_report
```

Python callable tasks can consume upstream outputs:

```python
def transform_data(context):
    extracted = context["extract"]
    return {"records": extracted["records"] + 1}
```

When `extract_flow` triggers `report_flow`, JSON outputs are also passed into the downstream context.

## CLI

```bash
piply init
piply validate --config piply-demo/piply.yaml
piply list --config piply-demo/piply.yaml
piply run extract_flow --config piply-demo/piply.yaml --wait
piply tasks list extract_flow --config piply-demo/piply.yaml
piply tasks run extract_flow publish_manifest --config piply-demo/piply.yaml
piply tasks retry <run_id> <task_id> --mode resume --config piply-demo/piply.yaml
piply runs --config piply-demo/piply.yaml
piply logs <run_id> --config piply-demo/piply.yaml
piply pause extract_flow --config piply-demo/piply.yaml
piply resume extract_flow --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml
piply stop --config piply-demo/piply.yaml
```

## API Highlights

- `GET /api/dashboard`
- `GET /api/pipelines`
- `GET /api/pipelines/{pipeline_id}`
- `POST /api/pipelines/{pipeline_id}/run`
- `POST /api/pipelines/{pipeline_id}/tasks/{task_id}/run`
- `POST /api/pipelines/{pipeline_id}/chain/{target_pipeline_id}`
- `GET /api/runs`
- `GET /api/runs/{run_id}`
- `GET /api/runs/{run_id}/tasks/{task_id}`
- `GET /api/runs/{run_id}/tasks/{task_id}/output`
- `POST /api/runs/{run_id}/retry`
- `POST /api/runs/{run_id}/tasks/{task_id}/retry`
- `GET /api/metrics`
- `GET /api/execution-matrix`
- `GET /api/logs`

## Runtime Model

Piply runs locally and keeps state in SQLite. The scheduler materializes due schedules and sensor events into an internal queue, then dispatches work with per-pipeline backpressure. The engine executes tasks through lightweight local workers and records task runs, logs, output metadata, retries, lineage, and parent/child pipeline relationships. CLI `--wait` runs complete downstream pipeline triggers inline so chained smoke tests finish deterministically.

## Roadmap

Planned features:

- `piply logs --follow`
- reusable task templates / profiles
- plugin hooks for custom operators
- UI controls for editing pipeline definitions safely
- artifact retention policies for large outputs
- optional distributed runner while keeping local mode as the default

## More Docs

- `wiki/USAGE_GUIDE.md`
- `wiki/README.md`
- `wiki/UI_API_GUIDE.md`
- `wiki/IMPLEMENTATION_SUMMARY.md`
