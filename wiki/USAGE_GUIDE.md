# Piply Usage Guide

This guide shows the main Piply workflows in one place: YAML structure, task types, sensors, output passing, schedules, retries, UI/API usage, and every CLI command.

## 1. Install And Start

```bash
pip install -e .
piply validate --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml
```

Open `http://127.0.0.1:8000`.

If port `8000` is already busy, choose another port:

```bash
piply start --config piply-demo/piply.yaml --port 8080
piply start --config piply-demo/piply.yaml --host 0.0.0.0 --port 8080
```

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

variables:
  scripts_dir: pipelines
  batch_id: demo-batch
  conda_env: py312_extract

entities:
  report:
    - payment
    - adjustment

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

Backward-compatible `jobs:` roots and older single-task style configs still load, but new projects should use `pipelines:`. Advanced projects may also define `pipeline_templates:` plus `pipeline_deployments:`; each deployment becomes a normal runnable pipeline id.

### Reusable YAML Variables

Use `variables` when paths, environment names, tenant labels, or command fragments repeat. Piply expands `{name}` inside any string field after loading `.env`, environment values, and secrets.

```yaml
variables:
  scripts_dir: pipelines
  raw_dir: data/raw
  conda_env: py312_extract

pipelines:
  ingest_flow:
    variables:
      tenant_code: acme
    tasks:
      validate:
        type: cli
        command: conda run -n {conda_env} python {scripts_dir}/test_cli.py {tenant_code} {raw_dir}
        cwd: .
```

Pipeline-level `variables` override top-level variables only for that pipeline. If a YAML value begins with `{name}`, quote it, for example `path: "{scripts_dir}/extract.py"`.

### Entity-Mapped Task Templates

Use `entities` when one task template should run once per value. Entity values can be declared globally, per pipeline, or per task. When a pipeline has entities, Piply expands template tasks into runtime task ids before execution.

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

      transform:
        type: python
        path: pipelines/extract.py
        function: transform_data
        depends_on: [extract]

      validate:
        type: cli
        command: python validate.py --report {report}
        depends_on: [transform]
```

Runtime DAG:

```text
payment.extract -> payment.transform -> payment.validate
adjustment.extract -> adjustment.transform -> adjustment.validate
refund.extract -> refund.transform -> refund.validate
```

Inside each mapped Python task, Piply adds entity values to `context`:

```python
def transform_data(context):
    report = context["report"]
    extracted = context["extract"]
    return {"report": report, "records": extracted["records"]}
```

Reducer tasks can opt out of expansion and wait for all mapped dependencies:

```yaml
tasks:
  summarize:
    type: python
    path: pipelines/report.py
    function: summarize
    entities: false
    depends_on: [validate]
```

The reducer can read `context["mapped"]["validate"]`, keyed by runtime entity keys such as `payment`, `adjustment`, and `refund`.

Global entities apply to every pipeline unless a pipeline overrides the same entity name:

```yaml
entities:
  tenant:
    - acme
    - globex

pipelines:
  nightly:
    entities:
      report: [payment, refund]
```

This creates a small matrix such as `acme.payment.extract` and `globex.refund.extract`. A task can override or opt out:

```yaml
tasks:
  validate:
    type: cli
    command: python validate.py --tenant {tenant} --report {report}
    depends_on: [extract]

  notify_once:
    type: email
    entities: false
    depends_on: [validate]
```

Best practices:

- Keep template ids short and stable, for example `extract`, `transform`, `load`.
- Put business values in `entities`, not in copied task blocks.
- Use `max_parallel_tasks` to control mapped-task fan-out on a local machine.
- Use a reducer task with `entities: false` when you need one final summary after every mapped task finishes.
- Use the concrete runtime id, such as `payment.validate`, when retrying one mapped task; use the template id, such as `validate`, when running all mapped instances of that template.

### Pipeline Templates And Deployments

Use deployments when one workflow should run separately for different tenants, schedules, or environments. Simple `pipelines:` YAML remains the default; this section is optional.

```yaml
pipeline_templates:
  report_pipeline:
    retry:
      attempts: 1
      mode: resume
    tasks:
      extract:
        type: python
        path: pipelines/extract.py
        function: extract_data
        kwargs:
          tenant: "{tenant}"

      load:
        type: cli
        command: python load.py --tenant {tenant}
        depends_on: [extract]

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
    max_parallel_tasks: 2
```

Piply loads `client_a_reporting` and `client_b_reporting` as normal pipelines. The scheduler schedules the deployment ids, not the template id. Deployment-level values override template values, and shortcut fields such as `tenant:` are also exposed as `{tenant}` and `{tenant_id}` variables.

Deployments work with entity expansion:

```yaml
pipeline_templates:
  mapped_report:
    entities:
      report: [payment, refund]
    tasks:
      extract:
        type: cli
        command: python extract.py --tenant {tenant} --report {report}
```

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

For Bash-only syntax such as `set -a`, `source .env`, and `&&` chains that should run in Bash even on a machine whose default shell is not Bash, set `shell: bash`:

```yaml
tasks:
  run_conda_job:
    type: cli
    shell: bash
    command: set -a && source .env && set +a && conda run -n {conda_env} python {scripts_dir}/test_cli.py
    cwd: .
```

Supported explicit shells include `bash`, `sh`, `zsh`, `cmd`, `powershell`, and `pwsh`. If `shell` is omitted, Piply keeps the old behavior and uses the platform default shell for `command` tasks.

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

For entity-mapped tasks, downstream tasks in the same entity receive dependency aliases by template id. For example, `payment.transform` can read `context["extract"]` even though the stored runtime output id is `payment.extract`. All mapped outputs are also available under `context["mapped"][template_id][entity_key]`.

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

## 9. Multi-Tenant Runs And Params

The same pipeline can run for different tenants without duplicating YAML. Pass tenant and params from the CLI or API; Python callable tasks receive them in `context`.

```bash
piply run extract_flow --tenant acme --param batch=2026-05-26 --param region=west --config piply-demo/piply.yaml
piply tasks run extract_flow validate --tenant beta --param batch=nightly --config piply-demo/piply.yaml
```

```python
def extract_data(context):
    tenant_id = context.get("tenant_id")
    params = context.get("params", {})
    batch = params.get("batch")
    return {"tenant": tenant_id, "batch": batch, "records": 120}
```

When a pipeline triggers another pipeline, Piply preserves the `tenant_id` and passes JSON-safe upstream outputs in the downstream context.

## 10. Schedules

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

## 11. Retries

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

## 12. Sensors

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

## 13. Secrets And Connections

Piply automatically reads `.env` files from the current directory and from the directory that contains your `piply.yaml`. A `.env` file is just a text file with one `KEY=value` per line:

```env
APP_BATCH_ID=demo-batch
PIPLY_SECRET_API_TOKEN=replace-me
PIPLY_SECRET_WAREHOUSE_DSN=postgresql://user:password@db.example.com:5432/app
SMTP_PASSWORD=change-me
```

Use normal environment values directly:

```yaml
tasks:
  validate:
    type: cli
    command: python pipelines/validate.py ${APP_BATCH_ID}
```

Use secret values explicitly with `${secret:NAME}`. With `prefix: PIPLY_SECRET_`, `${secret:API_TOKEN}` reads `PIPLY_SECRET_API_TOKEN`.

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

## 14. Runtime Metrics

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

## 15. Runtime Recovery And State Lifecycle

Piply uses run heartbeats to keep runtime state consistent across shutdowns and restarts.

- Manual cancel keeps the run state as `cancelled`.
- Graceful service shutdown marks active runs as `interrupted`.
- Running tasks become `interrupted`.
- Queued tasks that never started become `cancelled`.
- On the next startup, stale `queued` or `running` runs are reconciled automatically if their heartbeat is older than `PIPLY_STALE_RUN_TIMEOUT_SECONDS`.

Typical run lifecycle:

```text
queued -> running -> success
queued -> running -> failed
queued -> running -> cancelled
queued -> running -> interrupted
```

Typical task lifecycle:

```text
queued -> running -> success
queued -> running -> failed
queued -> running -> interrupted
queued -> cancelled
queued -> skipped
```

The scheduler badge in the UI is heartbeat-aware:

- `scheduler live`: scheduler thread is running and heartbeats are fresh
- `scheduler offline`: scheduler is stopped or heartbeat went stale
- `scheduler crashed`: scheduler thread raised an exception and exited

## 16. CLI Reference

### `piply init`

Create a starter workspace.

```bash
piply init my-piply-project
piply init my-piply-project --force
```

The starter YAML includes a runnable pipeline chain plus disabled reference pipelines for entity mapping, built-in operators, and sensors.

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
piply run extract_flow --tenant acme --param batch=2026-05-26 --param count=25 --config piply-demo/piply.yaml
```

`--tenant` is stored as `context["tenant_id"]`. Each `--param KEY=VALUE` is available under `context["params"][KEY]`; JSON values such as `--param count=25` become numbers.

### `piply tasks list`

List tasks inside a pipeline.

```bash
piply tasks list extract_flow --config piply-demo/piply.yaml
```

### `piply tasks run`

Run one selected task and its required upstream dependencies.

```bash
piply tasks run extract_flow publish_manifest --config piply-demo/piply.yaml
piply tasks run extract_flow publish_manifest --tenant acme --param batch=nightly --config piply-demo/piply.yaml
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

Python script and CLI subprocess stdout/stderr are written to the run log as task-scoped lines. Piply sets `PYTHONUNBUFFERED=1` for subprocess tasks so normal `print(...)` output appears without waiting for process exit. The run detail page loads the full run log; `/api/runs/{run_id}/logs` remains paginated for very large logs.

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
piply start --config piply-demo/piply.yaml --port 8080
piply start --config piply-demo/piply.yaml --host 0.0.0.0 --port 8080
piply start --config piply-demo/piply.yaml -d
```

If a previous server is still holding the socket, either run `piply stop --config piply-demo/piply.yaml` or start on another free port.

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

## 17. API Reference Highlights

```text
GET  /api/dashboard
GET  /api/dashboard/scheduler
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

## 18. UI Pages

- Dashboard: run summary, runtime trend, active pipelines, failures, queue/worker metrics.
- Pipelines: pipeline cards, trigger actions, schedule state.
- Pipeline detail: DAG, denser merged metadata, selected-node details, retry/run task actions.
- Execution Matrix: task rows by run columns.
- Run detail: collapsible task-focus panel, log filtering, Re-Run action, retry-from-task, and long output preview drawer.
- Logs: cross-run log search.
- Settings: schedules, runtime settings, queue metrics, worker metrics.
