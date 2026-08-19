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

Deployment variables also flow into a downstream pipeline when the deployment completes successfully. This lets one downstream definition serve many practices or tenants:

```yaml
pipelines:
  Bronze_to_Silver:
    tasks:
      dbt:
        type: cli
        command: DBT_CLIENT={practice} dbt run --selector appointment_silver

pipeline_deployments:
  BENNETT_ETL_Flow:
    template: extract_template
    variables:
      practice: BENNETT
    triggers_on_success: [Bronze_to_Silver]
```

The triggered command is `DBT_CLIENT=BENNETT ...`. If `practice` is also defined globally as `GLOBAL`, the direct/manual `Bronze_to_Silver` run uses `GLOBAL`, while the triggered downstream run uses the parent deployment value `BENNETT`. This inheritance is limited to `triggers_on_success`; manually running `Bronze_to_Silver` requires a local or top-level `practice` value.

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
For plain commands, omit `shell` so Piply uses the platform default shell.

For Bash-only syntax such as `set -a`, `source .env`, and `&&` chains that should run in Bash even on a machine whose default shell is not Bash, set `shell: bash`:

```yaml
tasks:
  run_conda_job:
    type: cli
    shell: bash
    command: set -a && source .env && set +a && conda run -n {conda_env} python {scripts_dir}/test_cli.py
    cwd: .
```

Supported explicit shells include `bash`, `sh`, `zsh`, `cmd`, `powershell`, and `pwsh`. If `shell` is omitted, Piply keeps the old behavior and uses the platform default shell for `command` tasks. On Windows, `shell: bash` requires a working Bash installation such as Git Bash or WSL; otherwise the task will fail before the command runs.

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

The same triggered run exposes inherited deployment variables in `context["variables"]` and by name, for example `context["practice"]`.

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

Sensors poll external state and enqueue a pipeline or one task when something
changes. Sensors only poll while the server is running (`piply start`); the
scheduler polls each one on every tick.

A complete, copy-pasteable config with all three sensor types side by side is in
[Execution Examples §10](../docs/EXAMPLES.md#10-sensors), including the log
output each one produces and what happens when a sensor fails.

```bash
piply start          # sensors begin polling
piply diagnostics    # per-sensor status, poll counts, last error
```

`ignore_existing: true` records the current state on the first poll without
firing, so starting the server does not immediately trigger a run for data that
was already there. Set it to `false` to treat everything present at startup as
new.

Each sensor also accepts `task_id: <task>` to run a single task instead of the
whole pipeline, and `enabled: false` to park it without deleting it.

### What a fired sensor looks like

```
Triggered by sensor 'inbox'.
Detected new files: /srv/app/inbox/orders.csv
```

```
Triggered by sensor 'new_rows'.
Detected new rows in inbound_events from cursor 1 to 2.
```

```
Triggered by sensor 'feed'.
Detected API sensor change at https://example.com/api/events from cursor 1 to 2.
```

### When a sensor fails

A failed poll is recorded, never raised. One unreachable source cannot stop the
other sensors or crash the scheduler:

```
Sensors     : 2 healthy, 1 failing, 0 idle
  FAILING inventory/warehouse: OperationalError: could not connect to host
```

The same data is on the Diagnostics page, at `GET /api/sensors`, and as the
`piply_sensor_health` metric (`0` when the last poll failed). Passwords in
connection strings and URLs are redacted everywhere a sensor is displayed.

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

If `cursor_path` is omitted, Piply looks for a `cursor`, `version`, `updated_at`,
`last_modified`, `id`, or `count` field, then falls back to hashing the response
body and triggering when the hash changes.

### Connecting a SQL sensor to an external database

`sql_sensor` is the one place Piply talks to your database directly. Declare the
connection once at the root and reference it by name:

```yaml
connections:
  warehouse: ${secret:WAREHOUSE_DSN}
  reporting: postgresql://piply:${PGPASSWORD}@db.internal:5432/reporting
  local: sqlite:///app.db

pipelines:
  on_new_claims:
    sensors:
      claims:
        type: sql_sensor
        connection_ref: warehouse
        table: claims
        cursor_column: claim_id
        where: "status = 'new'"
    tasks:
      process:
        type: cli
        command: python process_claims.py
```

| Scheme | Driver to install |
| --- | --- |
| `sqlite`, `sqlite3`, `sqlite+pysqlite` | built in |
| `postgres`, `postgresql`, `postgresql+psycopg` | `pip install psycopg` |
| `postgresql+psycopg2` | `pip install psycopg2` |
| `mysql`, `mysql+pymysql`, `mariadb`, `mariadb+pymysql` | `pip install pymysql` |
| `mysql+mysqlconnector` | `pip install mysql-connector-python` |
| `mssql`, `mssql+pyodbc`, `sqlserver`, `odbc` | `pip install pyodbc` |

Drivers are imported lazily and are **not** Piply dependencies, so the package
stays light. Install only what you use. A missing driver or an unsupported
scheme marks that sensor `failing` with the reason rather than crashing.

`table` and `cursor_column` must be plain identifiers and are validated before
being interpolated into SQL. `where` is passed through as written, so keep it
literal rather than building it from untrusted input.

> **Note:** this is about *your* database. Piply's own runtime state always
> lives in a local SQLite file — see *Runtime storage* at the end of section 13.

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

### Runtime storage: where Piply keeps its own state

Two different things get called "the database", and they are unrelated.

**Runs, task runs, logs, task outputs, artifact records, the trigger queue,
sensor cursors, and scheduler metadata** live in the metadata store. Two
backends are supported, selected by `PIPLY_DATABASE`:

```bash
# Default: SQLite, no configuration needed
#   -> <config-dir>/.piply/piply.db

PIPLY_DATABASE=/var/lib/piply/piply.db                        # SQLite, explicit path
PIPLY_DATABASE=postgresql://piply:secret@db:5432/piply        # PostgreSQL
```

PostgreSQL is opt-in and needs a driver:

```bash
pip install "mr-piply[postgres]"
```

The schema is created and migrated automatically either way, and nothing in a
`piply.yaml` changes. `piply start` prints which backend is in use:

```
Runtime database: postgresql://piply:***@db:5432/piply  (PostgreSQL)
```

Differences worth knowing: `piply backup`/`restore` are SQLite-only (use
`pg_dump`/`pg_restore` for PostgreSQL), `piply prune` skips `VACUUM` on
PostgreSQL because autovacuum handles it, and diagnostics report a database size
of 0 for a server store. Run **one** Piply instance per database in both cases.

In practice:

- back up the SQLite file (or the whole `.piply/` directory to include the WAL),
  or use `pg_dump` for PostgreSQL
- run **one** Piply instance per database; ownership is tracked per process and
  writes are serialised in-process
- use `piply prune` to bound growth

**In Docker the default location is ephemeral.** With no volume mounted,
`/app/.piply/piply.db` lives in the container's writable layer and is destroyed
each time the container is replaced, so a redeploy starts with empty history.
`piply start` prints the path it will use so this is visible immediately:

```
Runtime database: /app/.piply/piply.db  (default location; set PIPLY_DATABASE to move it)
```

Mount a volume and point `PIPLY_DATABASE` at it:

```yaml
services:
  piply:
    environment:
      PIPLY_DATABASE: /var/lib/piply/piply.db
      PIPLY_ARTIFACTS_DIR: /var/lib/piply-artifacts
    volumes:
      - piply-state:/var/lib/piply
      - piply-artifacts:/var/lib/piply-artifacts

volumes:
  piply-state:
  piply-artifacts:
```

If the image runs as a non-root user, `mkdir` and `chown` those paths in the
Dockerfile before `USER`, otherwise a fresh named volume is root-owned and the
app cannot write to it. Full details in
[YAML Specification §11](../docs/YAML_SPECIFICATION.md#11-runtime-storage-and-external-databases).

### Backup and restore

```bash
piply backup /backups                    # timestamped file in that directory
piply backup /backups/before-deploy.db   # explicit filename
piply restore /backups/before-deploy.db  # stop the server first
```

`backup` uses SQLite's online backup API, so it is safe to run against a live
server. `restore` keeps the file it displaces as `piply.db.replaced`.

**Your databases** are reached two ways: from a `sql_sensor` (see section 12),
or from inside a task, which is usually the right layer since your code already
has the client, pooling, and migrations:

```yaml
secrets:
  backend: env

pipelines:
  warehouse_load:
    env:
      DATABASE_URL: ${secret:WAREHOUSE_DSN}
    tasks:
      load:
        type: python
        path: jobs/load.py        # reads os.environ["DATABASE_URL"]
      dbt_run:
        type: cli
        command: dbt run --project-dir warehouse
        depends_on: [load]
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

### `piply plan`

Preview a run without executing anything: DAG stages, execution order, resolved
variables, expanded entities, and every interpolated command.

```bash
piply plan --config piply-demo/piply.yaml            # every pipeline
piply plan extract_flow --config piply-demo/piply.yaml
piply plan extract_flow --param batch=2026-07-01 --json
```

Tasks that would be skipped are shown with the reason, and any command still
holding an unresolved `{placeholder}` is reported as a warning.

### `piply logs --follow`

Stream new log lines as they are written. Each line is rendered as
`[time] [pipeline] [task] message`, with the task name coloured.

```bash
piply logs --follow
piply logs --follow --pipeline extract_flow
piply logs --follow --task validate
piply logs <run_id> --follow --no-color --limit 50 --interval 0.5
```

### `piply artifacts`

List the files a run declared and produced.

```bash
piply artifacts <run_id>
piply artifacts <run_id> --task build_report
```

### `piply backfill`

Replay one historic run with the exact configuration it captured, or queue every
scheduled slot in a past window.

```bash
piply backfill <run_id>                    # replay one run
piply backfill <run_id> --wait
piply backfill nightly_report --from 2026-07-01T00:00:00 --to 2026-07-08T00:00:00
```

Replaying is how a downstream run is repaired without re-running the upstream
pipeline that supplied its variables.

### `piply prune`

Delete history beyond the retention window and reclaim disk with `VACUUM`.

```bash
piply prune --dry-run
piply prune --run-days 14 --log-days 7 --max-runs 100
piply prune --yes --no-vacuum
```

Defaults come from `PIPLY_RETENTION_RUN_DAYS`, `PIPLY_RETENTION_LOG_DAYS`, and
`PIPLY_RETENTION_MAX_RUNS_PER_PIPELINE`. Active runs are never removed.

### `piply backup` / `piply restore`

Snapshot and restore the runtime database.

```bash
piply backup /backups                    # timestamped file in that directory
piply backup /backups/before-deploy.db   # explicit filename
piply restore /backups/before-deploy.db --yes
```

`backup` uses SQLite's online backup API, so it is safe against a running
server. Stop the server before `restore`. The displaced database is kept
alongside the new one as `piply.db.replaced`.

### `piply diagnostics`

Print scheduler health, running tasks, sensor health, recovery state, and
storage usage.

```bash
piply diagnostics
piply diagnostics --json
```

### `piply users`

Manage accounts and pipeline permissions. Creating the first account switches
authentication on for the install.

```bash
piply users create admin --role admin              # password is generated and printed
piply users create alice --grant nightly=view,run
piply users list
piply users grant alice reports all
piply users grant alice '*' view                   # every pipeline
piply users revoke alice reports
piply users passwd alice
piply users disable alice
piply users delete alice --yes
```

Permissions are `view`, `edit`, and `run`; `edit` and `run` both imply `view`.
Piply refuses to delete, demote, or disable the only active admin. Full details
in [Authentication](../docs/AUTHENTICATION.md).

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
- Pipelines: Airflow-style listing grouped by template, sortable by upcoming or last run, filterable by running/failed/scheduled/paused.
  Each row shows its last five runs as colour-coded dots; click a dot to open that run.
- Pipeline detail: DAG first, one merged metadata strip, selected-node details, retry/run task actions, and an execution preview drawer.
- Run detail: run DAG including downstream pipeline nodes, task focus panel, log filtering, artifact browser, Re-Run, retry-from-task, and Replay config.
- Execution Matrix: task rows by run columns.
- Runs: filter by pipeline/status/trigger/date, sort, and see the full trigger lineage of each run.
- Logs: cross-run log search.
- Diagnostics: scheduler health, running tasks, sensor health, reconciliation state, storage and retention.
- Settings: schedules, runtime settings, queue metrics, worker metrics, plus central SMTP and user administration for admins.

See [../docs/UI_GUIDE.md](../docs/UI_GUIDE.md) for the full walkthrough.

## 19. Execution Control Keys

Options that change *how* a task runs rather than *what* it runs. All are
optional and all default to the pre-existing behaviour.

```yaml
pipelines:
  nightly:
    enabled: true                # false hides the pipeline from the scheduler
    tags: [tier1, ingest]        # shown as chips, searchable on the listing page
    max_concurrent_runs: 1       # how many runs of this pipeline may overlap
    timeout: 30m                 # ceiling for the whole run
    env_file: .env.production    # one extra env file
    env_files:                   # or several
      - .env.shared
      - .env.local
    tasks:
      heavy_query:
        type: cli
        command: python query.py
        priority: high           # 5 | high | "***" | or an id suffix, see below
        timeout: 5m              # ceiling for this task
        kill_grace_period: 10    # seconds between terminate and kill
        enabled: true
```

### Priority

Reorders tasks that are *already runnable*. Dependencies always win.

```yaml
tasks:
  extract***:      # id stays "extract", priority 3
    type: cli
    command: python extract.py
  transform**:     # priority 2
    type: cli
    command: python transform.py
  cleanup:
    type: cli
    priority: low  # -1; also: lowest low normal high higher highest critical
    command: python cleanup.py
```

### Timeouts

`timeout` accepts `30`, `30s`, `5m`, `1h`, or `500ms`. On expiry Piply logs the
reason, terminates the process, waits `kill_grace_period` (default 5s), then
kills it. The task ends `timed_out` and so does the run.

For `api` and `webhook` tasks the value is used as the HTTP timeout. For python
*callable* tasks the call runs on a worker thread; Python cannot force a thread
to stop, so the task is marked `timed_out` and the thread is abandoned as a
daemon.

### Conditional execution

```yaml
tasks:
  payment_only:
    type: cli
    run_if: "{report} == 'payment'"
    command: python export.py --kind payment
```

Supported: literals, `{placeholders}`, `==`, `!=`, `in`, `not in`, `and`, `or`,
`not`, and list literals. Anything else raises and fails the task loudly rather
than skipping it silently. A false condition marks the task `skipped`; the run
can still succeed.

### Artifacts

```yaml
tasks:
  build:
    type: python
    path: jobs/build.py
    cwd: .
    artifacts:
      - "out/*.csv"
      - "out/manifest.json"
```

Globs resolve against the task working directory. Files are recorded, not
copied. Browse them on the run page or with `piply artifacts <run_id>`.
Downloads are restricted to paths the run recorded, inside the workspace, the
config directory, or `PIPLY_ARTIFACTS_DIR`.

### SSH and sensor extras

`ssh` tasks and remote `file_sensor`s accept `ssh_binary` when `ssh` is not on
`PATH`, and `connect_timeout` to bound the handshake. `BatchMode=yes` is always
passed, so a key needing an interactive passphrase fails instead of hanging.
`sql_sensor` supports `where` to narrow the cursor query and `task_id` to run a
single task instead of the whole pipeline.

## 20. Complete Reference

This guide is a walkthrough. For the exhaustive list of every key, every alias,
every legacy shape, and every environment variable, see the specification.

| Topic | Where |
| --- | --- |
| Every config key, with defaults | [YAML Specification](../docs/YAML_SPECIFICATION.md) |
| Aliases and legacy config shapes | [YAML Specification §11](../docs/YAML_SPECIFICATION.md#11-aliases-and-legacy-keys) |
| Runtime environment variables | [YAML Specification §10](../docs/YAML_SPECIFICATION.md#10-environment-variables) |
| Templates and deployments migration | [Migration Guide](../docs/MIGRATION.md) |
| Scheduler, task, retry, recovery lifecycles | [Runtime Lifecycles](../docs/LIFECYCLES.md) |
| Backfill and retention | [Runtime Lifecycles §7](../docs/LIFECYCLES.md#7-backfill) |
| Every UI page | [UI Guide](../docs/UI_GUIDE.md) |
| Runnable examples for each feature | [Execution Examples](../docs/EXAMPLES.md) |
| Internals, for maintainers | [Technical Architecture](../docs/architecture/technical_architecture.md) |
| Ideas not built yet | [Future Features](../docs/FUTURE_FEATURES.md) |
