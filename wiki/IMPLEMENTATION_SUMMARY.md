# Piply Implementation Summary

## Current Direction

Piply is now centered on a lightweight local orchestration runtime rather than an external orchestration dependency.

The active implementation is built around:

- YAML pipeline definitions
- multi-task DAG execution
- downstream pipeline triggers
- a durable SQLite-backed internal queue
- task-level run and log tracking
- server-rendered UI plus JSON API
- modular runtime pieces that can grow over time

## Implemented Runtime Features

- multiple tasks per pipeline
- dependency validation and cycle detection
- DAG-inferred sequential or parallel execution
- pipeline-level retry policy with `resume` or `startover`
- manual targeted retry through `piply tasks retry`
- pipeline-to-pipeline triggers on success
- pipeline-to-pipeline output/context passing for JSON outputs
- tenant and params context propagation for pipeline and task-scoped runs
- CLI wait-mode dispatches downstream pipeline triggers inline for deterministic chained runs
- queue-backed schedule backfill for missed slots
- run cancellation
- run and pipeline deletion
- stale run reconciliation with heartbeats
- queue dispatch requeue for abandoned dispatches
- queue and local worker metrics through `/api/metrics`, Dashboard, and Settings
- per-task upstream failure behavior (`skip`, `fail`, `continue`)
- persisted task output metadata (`task_outputs`)

## Implemented Operator Features

- `python` for script execution
- `python` for callable execution through `path/module/call + function`
- `cli` for shell commands
- explicit `shell` selection for CLI commands (`bash`, `sh`, `zsh`, `cmd`, `powershell`, `pwsh`)
- `cli` path execution for `.cmd`, `.bat`, `.ps1`, and direct executables
- `api` with bearer token support
- `webhook`
- `email`
- `ssh`

## Implemented Sensor Features

- `file_sensor` for local paths
- `file_sensor` for SFTP URIs polled over SSH
- `sql_sensor` for local SQLite paths
- `sql_sensor` for connection-string based polling
- `sql_sensor` connection refs through top-level `connections`
- optional Postgres, MySQL/MariaDB, and MSSQL/ODBC adapters when drivers are installed
- `api_sensor` for lightweight HTTP polling
- optional task targeting inside sensor-triggered pipelines
- sensor cursor persistence in SQLite

## Config And Secret Handling

- `.env` files are loaded without adding a third-party dependency
- reusable YAML `variables` expand with `{name}` inside config strings
- config strings can expand secrets and connection values from `.env`
- `secrets` supports explicit env and file-backed secret values with `${secret:NAME}` references
- `connections` supports reusable SQL connection strings for sensors
- runtime settings fall back to defaults when omitted
- common settings now include scheduler and queue tuning controls
- `piply init` now scaffolds a runnable context-passing pipeline chain plus disabled operator and sensor examples

## UI State

Current UI behavior includes:

- light theme
- dashboard status cards
- pipeline DAG with flow, stage, and focus modes
- task selection panel on pipeline detail
- task-run action panel on run detail
- rerun action on completed run detail pages
- side drawer for long task output previews
- live task duration labels on graph nodes
- log filtering by selected task
- upcoming runs preview
- task-scoped execution from the pipeline page
- execution matrix grid view (`/execution-matrix`)
- searchable logs page (`/logs`)
- settings page (`/settings`)
- runtime metrics API (`/api/metrics`)

## Active Runtime Modules

Core:

- `piply/settings.py`
- `piply/core/models.py`
- `piply/core/loader.py`
- `piply/core/service.py`
- `piply/core/store.py`
- `piply/core/scheduler.py`
- `piply/core/scheduling.py`
- `piply/core/retry.py`
- `piply/core/graph.py`
- `piply/core/sensors.py`
- `piply/core/secrets.py`
- `piply/core/sql_adapters.py`

Execution:

- `piply/engine/base.py`
- `piply/engine/heartbeat.py`
- `piply/engine/local_engine.py`
- `piply/engine/task_runner.py`

HTTP and UI:

- `piply/api/app.py`
- `piply/api/auth.py`
- `piply/api/routes/dashboard.py`
- `piply/api/routes/pipelines.py`
- `piply/api/routes/runs.py`
- `piply/api/routes/execution.py`
- `piply/api/routes/ui.py`
- `piply/api/schemas.py`
- `piply/ui/static/app.js`
- `piply/ui/static/dag.js`
- `piply/ui/static/styles.css`
- `piply/ui/templates/`

## Cleanup Notes

Recent cleanup focused on keeping the project lean:

- removed checked-in `dist/` package artifacts
- removed duplicate imports and stale examples
- normalized Python callable execution under `type: python`
- kept sensors and scheduling centered on SQLite-backed state instead of adding heavier queue infrastructure
- continued using small focused modules instead of pushing more logic into giant files

## Verification Status

Current verification expectations:

- automated tests pass
- package compilation passes
- CLI validation passes
- demo run flow works
- API task-run route works
- API task detail + output routes work
- execution matrix routes work
- runtime metrics route works
- queue-backed scheduler and sensors stay covered by tests

## Working Commands

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

## Upcoming Commands And Todos

- `piply logs --follow`
- reusable task templates
- managed secret-manager plugins
- plugin hooks for custom operators
- artifact retention policies for large outputs
- optional distributed runner
