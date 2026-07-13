# Piply Wiki

## Overview

Piply is a lightweight DAG runner for script-heavy teams. It keeps the runtime small, but still gives you:

- multiple pipelines in one workspace
- multiple tasks per pipeline
- dependency-aware execution
- metadata-driven entity expansion for reusable task templates
- optional pipeline templates and deployments for tenant/environment reuse
- automatic schedule backfill through a durable internal queue
- graceful shutdown and startup recovery for interrupted runs
- pipeline-to-pipeline triggers
- pipeline-to-pipeline context passing (JSON outputs)
- file, SQL, and API sensors
- reusable SQL connections and explicit secret references
- retries, logs, and run history
- queue and local worker metrics
- packaged UI, API, and CLI

## Architecture

```text
CLI / API / UI
       |
PipelineService
       |
Loader + DeploymentExpander + EntityExpander + Scheduler + RunStore
       |
Internal trigger queue + sensor cursor state
       |
LocalEngine + TaskRunner + Heartbeats
       |
python / cli / api / webhook / email / ssh
       |
SQLite state + local processes
```

## Runtime Recovery

Piply now treats shutdown recovery as a first-class runtime behavior:

- graceful service shutdown stops accepting new work before the scheduler exits
- queued or running runs are marked `interrupted` instead of being left `running`
- actively running tasks are marked `interrupted`
- queued tasks that never started are marked `cancelled`
- startup reconciliation converts stale heartbeat records into terminal states
- the scheduler chip uses heartbeat freshness, not just a boolean flag, to decide whether it is live

## Core Design Choices

- keep SQLite as the default state store
- keep the queue internal instead of requiring Redis
- keep Python callable execution under `type: python`
- keep reusable YAML values lightweight with `variables` and `{name}` interpolation
- keep mapped task expansion metadata-driven and optional
- keep deployment architecture optional so simple `pipelines:` YAML remains simple
- keep secrets out of YAML by expanding `.env` and environment variables
- keep concurrency inferred from `depends_on`
- keep modules small enough to evolve without a large rewrite

## Configuration Summary

Top-level fields:

- `version`
- `title`
- `workspace`
- `timezone`
- `variables`
- `entities`
- `defaults`
- `secrets`
- `connections`
- `pipelines`
- `pipeline_templates`
- `pipeline_deployments`

Each pipeline can define:

- `title`
- `description`
- `schedule`
- `retry`
- `max_parallel_tasks`
- `max_concurrent_runs`
- `triggers_on_success`
- `sensors`
- `tasks`
- `variables`
- `entities`

Each task can also define:

- `entities`
- `on_upstream_failure` (`skip`, `fail`, `continue`)
- `shell` for CLI command tasks that need `bash`, `cmd`, `powershell`, or `pwsh`

## Variables And Shell Commands

Top-level variables can be reused anywhere in task, sensor, connection, or pipeline strings:

```yaml
variables:
  scripts_dir: pipelines
  conda_env: py312_extract

tasks:
  validate:
    type: cli
    shell: bash
    command: set -a && source .env && set +a && conda run -n {conda_env} python {scripts_dir}/validate.py
    cwd: .
```

Pipeline-level variables override top-level values for one pipeline. If a scalar starts with `{name}`, quote it, for example `path: "{scripts_dir}/extract.py"`.

## Pipeline Templates And Deployments

Advanced YAML can define reusable templates and concrete deployments:

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
```

`client_a_reporting` is loaded as a normal pipeline id. The scheduler, UI, CLI, and API all operate on deployment ids, while the template stays a reusable definition. Simple `pipelines:` definitions continue to work unchanged.

## Entity Expansion

`entities` turns static task definitions into runtime task templates without changing the old YAML shape.

```yaml
pipelines:
  extract_flow:
    entities:
      report: [payment, adjustment, refund]
    tasks:
      extract:
        type: python
        path: pipelines/extract.py
        function: extract_data
        kwargs:
          report: "{report}"
      load:
        type: cli
        command: python load.py --report {report}
        depends_on: [extract]
```

Runtime ids:

- `payment.extract -> payment.load`
- `adjustment.extract -> adjustment.load`
- `refund.extract -> refund.load`

Architecture layers:

- Pipeline Definition: YAML loader validates templates, variables, entities, task types, sensors, and schedules.
- Runtime Expansion: `piply.pipeline.expander` builds concrete runtime task ids and rewrites dependencies.
- Execution Engine: `LocalEngine` executes the expanded DAG with retries, logs, context, and outputs.

Best-practice naming:

- Runtime ids use `{entity_key}.{template_task_id}` for one dimension.
- Multi-dimension ids join entity keys with dots before the template id.
- Template ids remain stable for command overrides and task-scoped runs.
- Reducer tasks can use `entities: false` and depend on a mapped template to wait for every mapped runtime task.

## Python Task Model

Piply now treats file execution and callable execution as one task type:

```yaml
tasks:
  extract:
    type: python
    path: pipelines/extract.py
```

```yaml
tasks:
  build_report:
    type: python
    path: pipelines/report.py
    function: build_report
```

Backward compatibility:

- older `python_call` configs still load
- new configs should use `type: python`

## Output Passing

Task outputs are captured and can be consumed by downstream tasks automatically:

- Python callable tasks can declare a `context` parameter and read upstream outputs as `context["task_id"]`.
- Captured outputs are stored as bounded metadata and JSON when the return value is JSON-serializable.

Example:

```yaml
tasks:
  extract_data:
    type: python
    path: pipelines/ops.py
    function: extract_data
  transform_data:
    type: python
    path: pipelines/ops.py
    function: transform_data
    depends_on: [extract_data]
```

```python
def transform_data(context):
    upstream = context["extract_data"]
    return {"records": upstream["records"] + 1}
```

Pipeline-to-pipeline passing:

- When `triggers_on_success` launches a downstream pipeline, JSON outputs from the upstream run are attached to the downstream run context.
- Downstream callable tasks can read them via `context["task_id"]` and `context["upstream"]["task_id"]`.
- Tenant context is preserved as `context["tenant_id"]`.
- CLI/API run params are available as `context["params"]`.
- CLI runs with `--wait` also finish downstream pipeline triggers inline, which keeps local smoke tests deterministic.
- Mapped tasks receive same-entity aliases by template id, so `payment.transform` can read `context["extract"]` from `payment.extract`.
- Reducers that use `entities: false` can read all mapped values through `context["mapped"][template_id][entity_key]`.

## Sensors

### File Sensor

Supports:

- in-project relative paths
- absolute local paths
- SFTP URIs such as `sftp://user@host:22/incoming`

Example:

```yaml
sensors:
  inbox_files:
    type: file_sensor
    path: ${SFTP_INBOX}
    pattern: "*.csv"
    key_file: ${SSH_KEY_PATH}
```

### SQL Sensor

Supports:

- local SQLite file paths through `database`
- connection-string driven behavior through `connection`
- reusable top-level connection strings through `connection_ref`
- SQLite, Postgres, MySQL/MariaDB, and MSSQL/ODBC schemes when the matching optional driver is installed

Example:

```yaml
connections:
  warehouse: ${secret:WAREHOUSE_DSN}

sensors:
  inbound_rows:
    type: sql_sensor
    connection_ref: warehouse
    table: inbound_events
    cursor_column: id
```

SQLite works without any extra dependency. Other database backends can work when the matching driver is already installed in the project environment.

### API Sensor

API sensors poll an HTTP endpoint and trigger when a cursor path or response digest changes.

```yaml
sensors:
  remote_events:
    type: api_sensor
    url: https://example.com/events
    method: GET
    cursor_path: version
    expected_status: [200]
```

## Secrets, Connections, And `.env`

Piply expands `.env` and process environment variables through the loader, so end users can keep values like these out of YAML:

- DB passwords
- SMTP passwords
- API tokens
- SSH key paths
- SFTP locations

Typical pattern:

```env
APP_DB_URL=postgresql://app_user:secret@db-host:5432/app
SMTP_PASSWORD=change-me
SSH_KEY_PATH=C:/keys/demo_id_rsa
SFTP_INBOX=sftp://demo@example.com/incoming
```

Then reference those values in YAML:

```yaml
variables:
  batch_id: ${APP_BATCH_ID}

tasks:
  notify:
    type: api
    url: https://example.com/hook
    token: ${secret:API_TOKEN}
```

Root-level secret backends keep references explicit:

```yaml
secrets:
  backend: env
  prefix: PIPLY_SECRET_

connections:
  app_db: ${secret:APP_DB_URL}
  local_sensor_db: sqlite:///sensor_demo.db
```

## Runtime Settings

Main settings:

- `PIPLY_DEFAULT_MAX_PARALLEL_TASKS`
- `PIPLY_STALE_RUN_TIMEOUT_SECONDS`
- `PIPLY_HEARTBEAT_INTERVAL_SECONDS`
- `PIPLY_SCHEDULER_POLL_INTERVAL_SECONDS`
- `PIPLY_QUEUE_DISPATCH_BATCH_SIZE`
- `PIPLY_QUEUE_DISPATCH_STALE_SECONDS`
- `PIPLY_UPCOMING_RUN_PREVIEW_COUNT`
- `PIPLY_AUTH_ENABLED`
- `PIPLY_AUTH_USERNAME`
- `PIPLY_AUTH_PASSWORD`
- `PIPLY_API_TOKEN`

Defaults are applied automatically when users leave them unset.

## Queue Behavior

Scheduler robustness comes from the internal queue:

- due schedules are enqueued first
- sensors enqueue their own events
- dispatch is FIFO per pipeline
- active runs act as backpressure
- stale dispatches are re-queued
- `/api/metrics`, Dashboard, and Settings expose queue depth, due items, dispatching items, failed queue items, running tasks, queued tasks, and configured task capacity

## UI Notes

Current DAG pages support:

- zoom
- pan
- flow, stage, and focus views
- live status coloring
- duration labels on task nodes
- task selection side panels
- task actions from the selected node
- log filtering by selected task on the run page
- collapsible task-focus panel on the run page
- re-run from completed run detail pages
- long task output previews in a side drawer so run pages stay compact

Additional operator pages:

- Execution Matrix (`/execution-matrix`): task rows x run columns grid with a run-duration trend header
- Logs (`/logs`): search across recent runs
- Settings (`/settings`): schedule pause/resume, queue metrics, worker metrics, and runtime config visibility

## Working CLI Commands

- `piply init`
- `piply validate`
- `piply list`
- `piply tasks list`
- `piply tasks run`
- `piply tasks retry`
- `piply run`
- `piply runs`
- `piply logs`
- `piply pause`
- `piply resume`
- `piply start`
- `piply start -d`
- `piply stop`
- `piply ui`

## Starter Project

`piply init` creates:

- `extract_flow`: runnable scheduled pipeline with Python callable output passing, CLI tasks, retry policy, and a downstream trigger
- `report_flow`: runnable downstream pipeline that can read upstream JSON outputs through `context`
- `entity_mapping_examples`: disabled reference pipeline for entity-mapped task templates and a reducer task
- `operator_examples`: disabled reference pipeline for `cli`, `api`, `webhook`, `email`, and `ssh`
- `sensor_examples`: disabled reference pipeline for `file_sensor`, `sql_sensor`, and `api_sensor`
- `pipelines/extract.py`, `pipelines/report.py`, and `sensor_inbox/`

## Roadmap Pointers

- `piply logs --follow`
- managed secret-manager plugins
- plugin hooks for custom operators
- task groups, conditional branches, and richer matrix controls
- optional distributed runner

See `USAGE_GUIDE.md` for full YAML examples and every CLI command.
See `../docs/architecture/technical_architecture.md` for the deep maintainer guide.
