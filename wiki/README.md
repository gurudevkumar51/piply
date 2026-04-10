# Piply Wiki

## Overview

Piply is a lightweight DAG runner for script-heavy teams. It keeps the runtime small, but still gives you:

- multiple pipelines in one workspace
- multiple tasks per pipeline
- dependency-aware execution
- automatic schedule backfill through a durable internal queue
- pipeline-to-pipeline triggers
- file and SQL sensors
- retries, logs, and run history
- packaged UI, API, and CLI

## Architecture

```text
CLI / API / UI
       |
PipelineService
       |
Loader + Scheduler + RunStore
       |
Internal trigger queue + sensor cursor state
       |
LocalEngine + TaskRunner + Heartbeats
       |
python / cli / api / webhook / email / ssh
       |
SQLite state + local processes
```

## Core Design Choices

- keep SQLite as the default state store
- keep the queue internal instead of requiring Redis
- keep Python callable execution under `type: python`
- keep secrets out of YAML by expanding `.env` and environment variables
- keep concurrency inferred from `depends_on`
- keep modules small enough to evolve without a large rewrite

## Configuration Summary

Top-level fields:

- `version`
- `title`
- `workspace`
- `timezone`
- `defaults`
- `pipelines`

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

Example:

```yaml
sensors:
  inbound_rows:
    type: sql_sensor
    connection: ${APP_DB_URL}
    table: inbound_events
    cursor_column: id
```

SQLite works without any extra dependency. Other database backends can work when the matching driver is already installed in the project environment.

## Secrets And `.env`

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

## Roadmap Pointers

- `piply logs --follow`
- more SQL adapters
- API or webhook polling sensors
- reusable task templates
- external secret backends
