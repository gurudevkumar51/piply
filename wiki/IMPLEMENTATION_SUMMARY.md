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
- queue-backed schedule backfill for missed slots
- run cancellation
- run and pipeline deletion
- stale run reconciliation with heartbeats
- queue dispatch requeue for abandoned dispatches

## Implemented Operator Features

- `python` for script execution
- `python` for callable execution through `path/module/call + function`
- `cli` for shell commands
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
- optional task targeting inside sensor-triggered pipelines
- sensor cursor persistence in SQLite

## Config And Secret Handling

- `.env` files are loaded without adding a third-party dependency
- config strings can expand secrets and connection values from `.env`
- runtime settings fall back to defaults when omitted
- common settings now include scheduler and queue tuning controls

## UI State

Current UI behavior includes:

- light theme
- dashboard status cards
- pipeline DAG with flow, stage, and focus modes
- task selection panel on pipeline detail
- task-run action panel on run detail
- live task duration labels on graph nodes
- log filtering by selected task
- upcoming runs preview
- task-scoped execution from the pipeline page

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
- `piply/api/routes/ui.py`
- `piply/api/schemas.py`
- `piply/ui/static/app.js`
- `piply/ui/static/dag.js`
- `piply/ui/static/styles.css`
- `piply/ui/templates/`

## Cleanup Notes

Recent cleanup focused on keeping the project lean:

- removed unused `profiles.py`
- removed duplicate imports
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
- richer queue metrics and worker introspection
- more SQL sensor adapters
- API polling and webhook-trigger sensors
- reusable task templates
- external secret backends
