# Piply Implementation Summary

## Current Direction

Piply now centers on a lightweight local runtime instead of a Prefect-heavy architecture.

The active implementation is built around:

- YAML pipeline definitions
- task DAGs inside each pipeline
- downstream pipeline triggers
- lightweight scheduling
- SQLite-backed run tracking
- operator-friendly web UI
- small modular support layers for settings, auth, heartbeats, and DAG rendering

## Implemented Features

### Runtime

- multi-task pipelines with dependency validation
- single-task backward compatibility for older `entrypoint` or `script` configs
- downstream pipeline triggers with `triggers_on_success`
- local execution engine with task-level tracking
- DAG-inferred concurrency with per-pipeline and global worker caps
- retry runs with `resume` and `startover`
- run history and raw log persistence
- scheduler loop with pause and resume support
- stale run reconciliation using run heartbeats

### Operators

- `python` operator for local scripts and module callables
- `cli` operator for local shell commands
- `api` operator with bearer token support
- `webhook` operator for HTTP POST requests
- `email` operator for SMTP notifications
- `ssh` operator for remote command execution or connectivity probes

### UI

- light visual theme
- dashboard with scheduler status and recent runs
- pipeline detail page with richer DAG controls
- pipeline detail page with relative same-day next-run labels
- visible command previews for each task
- run detail page with compact header and graph/log side-by-side layout
- run detail page live duration updates and task-focused retry controls
- timestamps formatted as `HH:MM:SS.SSS`

### CLI

Working commands today:

- `piply init`
- `piply validate`
- `piply list`
- `piply tasks list`
- `piply run`
- `piply runs`
- `piply start`
- `piply start -d`
- `piply stop`
- `piply ui`

### Server Auth

- Basic auth for UI and API
- optional Bearer auth for API routes
- `.env` support without adding a heavy dependency

## Starter Project Behavior

`piply init` now creates:

- `piply.yaml`
- `pipelines/extract.py`
- `pipelines/report.py`

The starter config includes:

- one pipeline with three tasks
- one downstream report pipeline
- one pipeline trigger chain from extract to report
- a `python_call` example in the downstream flow

## Active Modules

Core runtime:

- `piply/settings.py`
- `piply/core/loader.py`
- `piply/core/models.py`
- `piply/core/scheduling.py`
- `piply/core/scheduler.py`
- `piply/core/service.py`
- `piply/core/store.py`
- `piply/core/retry.py`
- `piply/core/graph.py`

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
- `piply/ui/templates/`

## Verification Status

The current verification target is:

- tests passing
- package compilation passing
- CLI validation and run flow working
- API/UI smoke routes working
- legacy database compatibility preserved for manual runs

## Upcoming Commands And Todos

- `piply logs`
- `piply tasks run`
- CLI pause and resume commands
