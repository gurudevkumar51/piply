# Piply Usage Guide

This guide shows the main Piply workflows in one place: YAML structure, task types, sensors, output passing, schedules, retries, UI/API usage, and every CLI command.

## 1. Install And Start

```bash
pip install -e .
piply validate --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml
```

Open `http://127.0.0.1:8000`.

Create a starter workspace:

```bash
piply init my-piply-project
piply validate --config my-piply-project/piply.yaml
piply run extract_flow --config my-piply-project/piply.yaml --wait
```

## 2. Project YAML Shape

```yaml
version: "1"
title: Piply Workspace
workspace: .
timezone: UTC

defaults:
  python: python
  env:
    APP_ENV: development

secrets:
  backend: env
  prefix: PIPLY_SECRET_

connections:
  local_events: sqlite:///sensor_demo.db
  warehouse: ${secret:WAREHOUSE_DSN}

pipelines:
  example_flow:
    title: Example Flow
    description: Human-readable operator description.
    schedule:
      every: 15m
    retry:
      attempts: 2
      mode: resume
      delay_seconds: 10
    max_parallel_tasks: 2
    tasks:
      extract:
        type: python
        path: pipelines/extract.py
```

Backward-compatible `jobs:` roots and older single-task style configs still load, but new projects should use `pipelines:`.

## 3. Python Tasks

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
  transform:
    type: python
    path: pipelines/extract.py
    function: transform_data
    kwargs:
      mode: clean
```

Run a function from a module:

```yaml
tasks:
  build_report:
    type: python
    module: reporting.jobs
    function: build_report
```

Use an explicit callable:

```yaml
tasks:
  build_report:
    type: python
    call: reporting.jobs:build_report
```

## 4. CLI And Shell Tasks

```yaml
tasks:
  validate:
    type: cli
    command: python -c "print('validated')"

  powershell_step:
    type: cli
    path: scripts/publish.ps1
    args: ["-Environment", "prod"]
```

Piply supports direct executable paths, `.cmd`, `.bat`, and `.ps1` path execution.

## 5. API, Webhook, Email, And SSH Tasks

```yaml
tasks:
  notify_api:
    type: api
    url: https://example.com/hooks/report
    method: POST
    token: ${secret:API_TOKEN}
    headers:
      X-Source: piply
    body: '{"status":"done"}'
    expected_status: [200, 201, 202]

  notify_webhook:
    type: webhook
    url: https://hooks.example.com/piply
    body: '{"text":"Pipeline finished"}'

  email_team:
    type: email
    smtp_host: smtp.example.com
    smtp_user: ${SMTP_USER}
    smtp_password: ${secret:SMTP_PASSWORD}
    to: [team@example.com]
    subject: Pipeline Complete
    body: The pipeline completed successfully.

  remote_check:
    type: ssh
    host: worker.example.com
    user: deploy
    key_file: ${SSH_KEY_PATH}
    command: echo remote-ok
```

## 6. Dependencies And Failure Behavior

```yaml
tasks:
  extract:
    type: python
    path: pipelines/extract.py

  transform:
    type: python
    path: pipelines/transform.py
    depends_on: [extract]

  notify:
    type: webhook
    url: https://hooks.example.com/piply
    depends_on: [transform]
    on_upstream_failure: continue
```

`on_upstream_failure` values:

- `skip`: default, keeps current backward-compatible behavior.
- `fail`: mark the downstream task failed if an upstream task failed or skipped.
- `continue`: run the downstream task even when an upstream task did not succeed.

Legacy booleans also work:

```yaml
fail_if_upstream_failed: true
continue_if_upstream_failed: true
```

## 7. Output Passing

Python callable return values are captured and stored as task output metadata. JSON-serializable outputs are passed downstream through `context`.

```python
def extract_data():
    return {"records": 120}


def transform_data(context):
    extracted = context["extract"]
    return {"records": extracted["records"] + 1}
```

```yaml
tasks:
  extract:
    type: python
    path: pipelines/ops.py
    function: extract_data

  transform:
    type: python
    path: pipelines/ops.py
    function: transform_data
    depends_on: [extract]
```

## 8. Pipeline Chaining

```yaml
pipelines:
  extract_flow:
    triggers_on_success:
      - report_flow
    tasks:
      extract:
        type: python
        path: pipelines/ops.py
        function: extract_data

  report_flow:
    tasks:
      build_report:
        type: python
        path: pipelines/report.py
        function: build_report
```

Downstream callable tasks can read upstream outputs with:

```python
def build_report(context):
    extract_output = context["upstream"]["extract"]
```

CLI `--wait` runs finish downstream pipeline triggers inline, which makes local chain smoke tests deterministic.

## 9. Schedules

Interval schedule:

```yaml
schedule:
  every: 15m
```

Cron schedule:

```yaml
schedule:
  cron: "0 2 * * *"
  timezone: Asia/Kolkata
```

Pause and resume schedule dispatch without editing YAML:

```bash
piply pause extract_flow --config piply-demo/piply.yaml
piply resume extract_flow --config piply-demo/piply.yaml
```

## 10. Retries

```yaml
retry:
  attempts: 2
  mode: resume
  delay_seconds: 10
```

Retry modes:

- `resume`: reuse successful upstream work and rerun failed/skipped work.
- `startover`: rerun the full pipeline.

Manual retry:

```bash
piply tasks retry <run_id> transform --mode resume --config piply-demo/piply.yaml
```

## 11. Sensors

Sensors poll external state and enqueue a pipeline or one task when something changes.

### File Sensor

```yaml
sensors:
  inbox_files:
    type: file_sensor
    path: sensor_inbox
    pattern: "*.csv"
    recursive: false
    ignore_existing: true
```

SFTP paths are polled through SSH:

```yaml
sensors:
  remote_inbox:
    type: file_sensor
    path: sftp://user@example.com:22/incoming
    pattern: "*.json"
    key_file: ${SSH_KEY_PATH}
```

### SQL Sensor

SQLite path:

```yaml
sensors:
  inbound_rows:
    type: sql_sensor
    database: sensor_demo.db
    table: inbound_events
    cursor_column: id
```

Connection string:

```yaml
sensors:
  inbound_rows:
    type: sql_sensor
    connection: postgresql://user:password@db.example.com:5432/app
    table: inbound_events
    cursor_column: id
```

Reusable connection:

```yaml
secrets:
  backend: env
  prefix: PIPLY_SECRET_

connections:
  warehouse: ${secret:WAREHOUSE_DSN}

pipelines:
  load_flow:
    sensors:
      inbound_rows:
        type: sql_sensor
        connection_ref: warehouse
        table: inbound_events
        cursor_column: id
```

Supported SQL schemes:

- `sqlite`, `sqlite3`, `sqlite+pysqlite`
- `postgres`, `postgresql`, `postgresql+psycopg`, `postgresql+psycopg2`
- `mysql`, `mysql+pymysql`, `mysql+mysqlconnector`
- `mariadb`, `mariadb+pymysql`
- `mssql`, `mssql+pyodbc`, `sqlserver`, `odbc`

Only SQLite is built in. Other schemes use optional drivers already installed in your environment.

### API Sensor

```yaml
sensors:
  remote_events:
    type: api_sensor
    url: https://example.com/events
    method: GET
    token: ${secret:API_TOKEN}
    cursor_path: version
    expected_status: [200]
    ignore_existing: true
```

If `cursor_path` is omitted, Piply hashes the response body and triggers when it changes.

## 12. Secrets And Connections

Environment-backed secrets:

```yaml
secrets:
  backend: env
  prefix: PIPLY_SECRET_
```

Direct values:

```yaml
secrets:
  values:
    API_TOKEN: ${PIPLY_API_TOKEN}
```

File-backed secrets:

```yaml
secrets:
  backend: file
  path: .piply-secrets.env
```

Supported secret file formats:

- `.env` style `KEY=value`
- `.json`
- `.yaml` / `.yml`

Use secrets explicitly:

```yaml
connections:
  app_db: ${secret:APP_DB_URL}

tasks:
  notify:
    type: api
    url: https://example.com/hook
    token: ${secret:API_TOKEN}
```

## 13. Runtime Metrics

Metrics are exposed in Dashboard, Settings, and the API:

```bash
curl http://127.0.0.1:8000/api/metrics
```

Current counters include:

- queue counts by status
- due queue items
- oldest due queue age
- running and queued runs
- running and queued tasks
- configured task capacity

## 14. CLI Reference

### `piply init`

Create a starter workspace.

```bash
piply init my-piply-project
piply init my-piply-project --force
```

### `piply validate`

Parse and validate YAML.

```bash
piply validate --config piply-demo/piply.yaml
```

### `piply list`

List configured pipelines with schedule and state summaries.

```bash
piply list --config piply-demo/piply.yaml
```

### `piply run`

Trigger one pipeline.

```bash
piply run extract_flow --config piply-demo/piply.yaml
piply run extract_flow --config piply-demo/piply.yaml --wait
```

### `piply tasks list`

List tasks inside a pipeline.

```bash
piply tasks list extract_flow --config piply-demo/piply.yaml
```

### `piply tasks run`

Run one selected task and its required upstream dependencies.

```bash
piply tasks run extract_flow publish_manifest --config piply-demo/piply.yaml
```

### `piply tasks retry`

Retry a failed run from a selected task.

```bash
piply tasks retry <run_id> publish_manifest --mode resume --config piply-demo/piply.yaml
piply tasks retry <run_id> publish_manifest --mode startover --config piply-demo/piply.yaml
```

### `piply runs`

Show recent runs.

```bash
piply runs --config piply-demo/piply.yaml
```

### `piply logs`

Show logs for one run.

```bash
piply logs <run_id> --config piply-demo/piply.yaml
```

### `piply pause` / `piply resume`

Pause or resume schedule dispatch.

```bash
piply pause extract_flow --config piply-demo/piply.yaml
piply resume extract_flow --config piply-demo/piply.yaml
```

### `piply start`

Start the UI, API, scheduler, and sensor polling loop.

```bash
piply start --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml --host 0.0.0.0 --port 8080
piply start --config piply-demo/piply.yaml -d
```

### `piply stop`

Ask a detached/local server to shut down.

```bash
piply stop --config piply-demo/piply.yaml
```

### `piply ui`

Compatibility alias for `piply start`.

```bash
piply ui --config piply-demo/piply.yaml
```

## 15. API Reference Highlights

```text
GET  /api/dashboard
GET  /api/metrics
GET  /api/pipelines
GET  /api/pipelines/{pipeline_id}
GET  /api/pipelines/{pipeline_id}/tasks/{task_id}
POST /api/pipelines/{pipeline_id}/run
POST /api/pipelines/{pipeline_id}/tasks/{task_id}/run
POST /api/pipelines/{pipeline_id}/chain/{target_pipeline_id}
POST /api/pipelines/{pipeline_id}/pause
POST /api/pipelines/{pipeline_id}/resume
GET  /api/runs
GET  /api/runs/{run_id}
GET  /api/runs/{run_id}/logs
GET  /api/runs/{run_id}/tasks/{task_id}
GET  /api/runs/{run_id}/tasks/{task_id}/output
POST /api/runs/{run_id}/retry
POST /api/runs/{run_id}/tasks/{task_id}/retry
POST /api/runs/{run_id}/cancel
GET  /api/execution-matrix
GET  /api/logs
```

## 16. UI Pages

- Dashboard: run summary, runtime trend, active pipelines, failures, queue/worker metrics.
- Pipelines: pipeline cards, trigger actions, schedule state.
- Pipeline detail: DAG, selected-node details, retry/run task actions.
- Execution Matrix: task rows by run columns.
- Logs: cross-run log search.
- Settings: schedules, runtime settings, queue metrics, worker metrics.
