# Piply

Piply is a lightweight workflow runner for teams that mainly want to run scripts, schedule them, see what happened, and keep growing into a fuller orchestration product over time.

In plain language:

- you describe work in one `piply.yaml`
- each pipeline contains one or more tasks
- tasks can run in sequence or in parallel based on dependencies
- pipelines can trigger other pipelines after success
- Piply gives you a built-in UI, API, CLI, logs, retries, sensors, and scheduling without forcing a heavy stack

The project is intentionally designed to stay small:

- standard-library-first runtime
- SQLite for state, queueing, logs, and sensor cursors
- FastAPI plus server-rendered pages for the web layer
- no Redis, no Celery, no Prefect dependency, no large orchestration engine underneath

## What Piply Is Good At

Piply is a good fit when you want to:

- run Python scripts or Python functions on a schedule
- run shell, batch, PowerShell, API, SSH, webhook, or email tasks in the same pipeline
- inspect task-by-task progress in a DAG-style UI
- retry from the failed task instead of re-running everything
- keep secrets in `.env` instead of hardcoding them in YAML
- start simple, but leave room for more operators, sensors, and enterprise features later

## How Piply Works

Think of Piply as three layers:

1. Configuration layer  
   You describe pipelines, tasks, retries, and sensors in `piply.yaml`.

2. Runtime layer  
   Piply reads the config, builds the DAG, runs tasks locally, and stores run state in SQLite.

3. Operator layer  
   The CLI, web UI, and API all sit on top of the same runtime service.

That means a manual run from the UI, a scheduled run, and an API-triggered run all follow the same execution path.

## Why Scheduled Runs Are More Robust

Piply does not rely on a single “scheduler tick fires the job right now” moment.

Instead, it uses a durable internal pull queue:

- due schedules are materialized into SQLite-backed queue rows
- sensor events also go into the same queue
- dispatch keeps FIFO order per pipeline
- active runs naturally rate-limit new work
- stale dispatch records are re-queued automatically
- missed schedule windows can be backfilled

This gives Piply BullMQ or SQS style behavior, but without introducing Redis or a cloud dependency.

## Quick Start

```bash
pip install -e .
copy .env.example .env
piply validate --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml
```

Open `http://127.0.0.1:8000`.

Detached background mode:

```bash
piply start --config piply-demo/piply.yaml -d
```

## First Mental Model

Here is the simplest way to think about Piply objects:

- project: one workspace and one `piply.yaml`
- pipeline: one DAG of related work
- task: one executable unit inside that DAG
- run: one execution of a pipeline
- task run: one execution record for a task inside a run
- sensor: a watcher that turns an external event into a pipeline trigger

## Example Config

```yaml
version: "1"
title: Demo Workspace
workspace: .
defaults:
  python: python
  env:
    APP_ENV: development

pipelines:
  extract_demo:
    title: Demo Customer Extract
    description: Extract, validate, and publish a batch.
    schedule:
      every: 10m
    retry:
      attempts: 2
      mode: resume
      delay_seconds: 10
    max_parallel_tasks: 2
    triggers_on_success:
      - report_demo
    tasks:
      extract:
        type: python
        path: pipelines/extract.py
        args: ["--records", "240"]

      validate_batch:
        type: cli
        command: python -c "print('Validating latest extract batch...')"
        depends_on: [extract]

      publish_manifest:
        type: cli
        command: python -c "print('Publishing manifest...')"
        depends_on: [extract]

  report_demo:
    title: Demo Report
    tasks:
      build_report:
        type: python
        path: pipelines/report.py
        function: build_report
        kwargs:
          report_name: nightly-demo
```

## DAG Execution Rules

You do not need to set `mode: parallel`.

Piply automatically inspects `depends_on` and decides whether tasks can run together. `max_parallel_tasks` only acts as the cap.

Example:

```yaml
tasks:
  extract:
    type: python
    path: pipelines/extract.py

  validate:
    type: cli
    command: python -c "print('validate')"
    depends_on: [extract]

  publish:
    type: cli
    command: python -c "print('publish')"
    depends_on: [extract]
```

`validate` and `publish` can run in parallel because they both depend on `extract` but not on each other.

## Retry Policy

Retries are configured per pipeline:

```yaml
pipelines:
  extract_demo:
    retry:
      attempts: 2
      mode: resume
      delay_seconds: 10
```

Meaning:

- `attempts`: automatic retries after the original run fails
- `mode: resume`: reuse successful upstream tasks and rerun failed or skipped work
- `mode: startover`: rerun the full pipeline
- `delay_seconds`: optional wait before Piply creates the retry run

You can also retry explicitly:

```bash
piply tasks retry <run_id> <task_id> --mode resume --config piply-demo/piply.yaml
```

## Supported Task Types

### `type: python`

Use the same `python` task type for both script execution and Python function calls.

Run a Python file:

```yaml
tasks:
  extract:
    type: python
    path: pipelines/extract.py
    args: ["--records", "100"]
```

Run a function from a file:

```yaml
tasks:
  build_report:
    type: python
    path: pipelines/report.py
    function: build_report
    kwargs:
      report_name: nightly
```

Run a function from a module:

```yaml
tasks:
  build_report:
    type: python
    module: reporting.jobs
    function: build_report
    kwargs:
      report_name: nightly
```

Supported callable forms:

- `path` + `function`
- `module` + `function`
- `call: package.module:function`
- `call: relative/or/absolute_file.py::function`

Older `python_call` configs are still accepted for backward compatibility, but new configs should just use `type: python`.

### `type: cli`

Run any shell command:

```yaml
tasks:
  validate:
    type: cli
    command: python -c "print('validated')"
```

Run a batch or shell file by path:

```yaml
tasks:
  windows_batch:
    type: cli
    path: scripts/check.cmd
    args: ["--quick"]

  nightly_batch:
    type: cli
    path: scripts/nightly.bat

  powershell_step:
    type: cli
    path: scripts/publish.ps1
    args: ["-Environment", "prod"]
```

Supported path execution includes:

- `.cmd`
- `.bat`
- `.ps1`
- direct executable files

The pipeline detail page also lets you override CLI commands from the UI for one manual run.

### `type: api`

```yaml
tasks:
  notify:
    type: api
    url: https://example.com/hooks/report
    method: POST
    token: ${PIPLY_API_TOKEN}
    headers:
      X-Source: piply
    body: '{"status": "done"}'
    expected_status: [200, 201, 202]
```

If `token` is supplied, Piply sends `Authorization: Bearer <token>`.

### `type: webhook`

```yaml
tasks:
  notify_slack:
    type: webhook
    url: https://hooks.slack.com/services/...
    body: '{"text": "Flow finished"}'
```

### `type: email`

```yaml
tasks:
  email_team:
    type: email
    smtp_host: smtp.internal.local
    smtp_user: ${SMTP_USER}
    smtp_password: ${SMTP_PASSWORD}
    to:
      - team@example.com
    subject: "Pipeline Success"
    body: "The nightly extract has finished."
```

### `type: ssh`

```yaml
tasks:
  remote_healthcheck:
    type: ssh
    host: demo-host
    user: deploy
    key_file: ${SSH_KEY_PATH}
    command: "echo remote-ok"
```

## Sensors

Sensors turn external change into a pipeline trigger.

### File Sensor

Watch a local path:

```yaml
pipelines:
  file_watch_demo:
    sensors:
      inbox_files:
        type: file_sensor
        path: sensor_inbox
        pattern: "*.csv"
        ignore_existing: true
    tasks:
      inspect_inbox:
        type: python
        path: pipelines/file_sensor_demo.py
        args: ["--path", "sensor_inbox"]
```

Watch a global absolute path:

```yaml
sensors:
  landing_files:
    type: file_sensor
    path: C:/data/incoming
    pattern: "*.json"
```

Watch an SFTP location:

```yaml
sensors:
  sftp_inbox:
    type: file_sensor
    path: ${SFTP_INBOX}
    pattern: "*.csv"
    key_file: ${SSH_KEY_PATH}
```

Example `.env`:

```env
SFTP_INBOX=sftp://demo@example.com:22/incoming
SSH_KEY_PATH=C:/keys/demo_id_rsa
```

Piply polls SFTP paths over SSH so you can keep the runtime light without adding a heavier file-watcher dependency.

### SQL Sensor

Use a local SQLite file path:

```yaml
sensors:
  inbound_rows:
    type: sql_sensor
    database: sensor_demo.db
    table: inbound_events
    cursor_column: id
    ignore_existing: true
```

Or use a connection string:

```yaml
sensors:
  inbound_rows:
    type: sql_sensor
    connection: ${APP_DB_URL}
    table: inbound_events
    cursor_column: id
    where: status = 'ready'
```

This makes the sensor independent from one local file and lets developers point the same pipeline at different databases through `.env` or deployment-specific environment variables.

Built-in support stays light:

- SQLite works out of the box
- PostgreSQL, MySQL, and some other backends can work when their DB driver is already installed in the project environment

## Secrets And `.env`

End users should keep sensitive values in `.env` or environment variables, not hardcode them inside `piply.yaml`.

Good examples of sensitive values:

- database passwords
- SMTP passwords
- SFTP or SSH key paths
- API tokens
- host-specific connection strings

Example:

```env
APP_DB_URL=postgresql://app_user:super-secret@db-host:5432/app
SMTP_USER=bot@example.com
SMTP_PASSWORD=change-me
SSH_KEY_PATH=C:/keys/demo_id_rsa
PIPLY_API_TOKEN=replace-with-long-token
```

Then reference them in YAML:

```yaml
tasks:
  email_team:
    type: email
    smtp_user: ${SMTP_USER}
    smtp_password: ${SMTP_PASSWORD}

sensors:
  inbound_rows:
    type: sql_sensor
    connection: ${APP_DB_URL}

tasks:
  remote_step:
    type: ssh
    key_file: ${SSH_KEY_PATH}
```

## Common Runtime Settings

If a value is not supplied, Piply uses a safe default.

```env
PIPLY_DEFAULT_MAX_PARALLEL_TASKS=4
PIPLY_STALE_RUN_TIMEOUT_SECONDS=3600
PIPLY_HEARTBEAT_INTERVAL_SECONDS=10
PIPLY_SCHEDULER_POLL_INTERVAL_SECONDS=10
PIPLY_QUEUE_DISPATCH_BATCH_SIZE=100
PIPLY_QUEUE_DISPATCH_STALE_SECONDS=300
PIPLY_UPCOMING_RUN_PREVIEW_COUNT=8
PIPLY_AUTH_ENABLED=true
PIPLY_AUTH_USERNAME=admin
PIPLY_AUTH_PASSWORD=change-me
PIPLY_API_TOKEN=replace-with-long-token
```

What they mean:

- `PIPLY_DEFAULT_MAX_PARALLEL_TASKS`: global fallback task concurrency
- `PIPLY_STALE_RUN_TIMEOUT_SECONDS`: when a run is considered stale and reconciled
- `PIPLY_HEARTBEAT_INTERVAL_SECONDS`: how often active runs update their heartbeat
- `PIPLY_SCHEDULER_POLL_INTERVAL_SECONDS`: scheduler tick frequency
- `PIPLY_QUEUE_DISPATCH_BATCH_SIZE`: queue drain size per scheduler pass
- `PIPLY_QUEUE_DISPATCH_STALE_SECONDS`: when abandoned queue dispatch records are returned to queued
- `PIPLY_UPCOMING_RUN_PREVIEW_COUNT`: how many future runs the UI and API preview
- `PIPLY_AUTH_*`: packaged Basic auth for the UI and API
- `PIPLY_API_TOKEN`: Bearer token auth for API usage

## UI Highlights

The built-in UI is intentionally light-themed and operator-focused.

Current pages include:

- dashboard
- pipeline list
- pipeline detail
- run list
- run detail

Current DAG features include:

- zoom
- pan
- flow, stage, and focus views
- edge labels
- live node colors
- live duration on task nodes
- task selection side panel
- task-level actions from the selected node

Pipeline detail page:

- run the full pipeline
- pause or resume scheduling
- delete a pipeline
- override CLI commands for one manual run
- select a DAG node to run just that task plus its upstream dependencies

Run detail page:

- live run duration
- cancel active runs
- delete finished runs
- filter logs by selected task
- retry from the failed task or start over
- preview 5 to 10 upcoming runs depending on config

## CLI Commands

### Working Commands

```bash
piply init
piply validate --config piply-demo/piply.yaml
piply list --config piply-demo/piply.yaml
piply tasks list extract_demo --config piply-demo/piply.yaml
piply tasks run extract_demo publish_manifest --config piply-demo/piply.yaml
piply tasks retry <run_id> <task_id> --mode resume --config piply-demo/piply.yaml
piply run extract_demo --config piply-demo/piply.yaml --wait
piply runs --config piply-demo/piply.yaml
piply logs <run_id> --config piply-demo/piply.yaml
piply pause extract_demo --config piply-demo/piply.yaml
piply resume extract_demo --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml -d
piply stop --config piply-demo/piply.yaml
piply ui --config piply-demo/piply.yaml
```

### Upcoming Commands And Ideas

- `piply logs --follow`
- richer sensor adapters for more database vendors
- webhook or API polling sensors
- task templates or reusable shared operator profiles
- secret backends beyond `.env`
- queue and worker metrics pages

## API Highlights

Important endpoints:

- `GET /api/dashboard`
- `GET /api/pipelines`
- `GET /api/pipelines/{pipeline_id}`
- `POST /api/pipelines/{pipeline_id}/run`
- `POST /api/pipelines/{pipeline_id}/tasks/{task_id}/run`
- `POST /api/pipelines/{pipeline_id}/pause`
- `POST /api/pipelines/{pipeline_id}/resume`
- `DELETE /api/pipelines/{pipeline_id}`
- `GET /api/runs`
- `GET /api/runs/{run_id}`
- `GET /api/runs/{run_id}/logs`
- `POST /api/runs/{run_id}/retry`
- `POST /api/runs/{run_id}/cancel`
- `DELETE /api/runs/{run_id}`

## Demo Workspace

The bundled `piply-demo` project is meant for development and testing.

It includes:

- `extract_demo` for scheduled multi-task execution
- `report_demo` for downstream pipeline triggering
- `file_watch_demo` for local file sensor testing
- `sql_watch_demo` for local SQL sensor testing
- helper scripts under `piply-demo/pipelines`

Useful demo commands:

```bash
piply validate --config piply-demo/piply.yaml
piply run extract_demo --config piply-demo/piply.yaml --wait
python piply-demo/pipelines/seed_sql_sensor.py --database piply-demo/sensor_demo.db --payload demo-row
python piply-demo/pipelines/file_sensor_demo.py --path piply-demo/sensor_inbox
python piply-demo/pipelines/sql_sensor_demo.py --database piply-demo/sensor_demo.db
```

## Current Product Direction

Piply is not trying to compete by being the heaviest orchestrator.

The direction is:

- keep the runtime understandable
- keep the dependencies light
- keep the architecture modular
- keep the UI professional enough for real operations work
- make it easy to add more operators, sensors, and deployment models later

## More Docs

- `wiki/README.md`
- `wiki/UI_API_GUIDE.md`
- `wiki/IMPLEMENTATION_SUMMARY.md`
