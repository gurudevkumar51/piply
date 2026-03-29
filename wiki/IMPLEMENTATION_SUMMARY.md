# Piply Implementation Summary

## Current Direction

Piply now centers on a lightweight local runtime instead of a Prefect-heavy architecture.

The active implementation is built around:

- YAML pipeline definitions
- task graphs inside each pipeline
- downstream pipeline triggers
- lightweight scheduling
- SQLite-backed run tracking
- operator-friendly web UI

## Implemented Features

### Runtime

- multi-task pipelines with dependency validation
- single-task backward compatibility for older `entrypoint` or `script` configs
- downstream pipeline triggers with `triggers_on_success`
- local execution engine with task-level tracking
- explicit `sequential` and `parallel` execution modes
- retry runs with `resume` and `startover`
- run history and raw log persistence
- scheduler loop with pause and resume support

### Operators

- `python` operator for local scripts
- `cli` operator for local shell commands
- `api` operator with bearer token support and expected status validation
- `ssh` operator for remote command execution or connectivity probes

### UI

- light visual theme
- dashboard with scheduler status and recent runs
- pipeline detail page with DAG view
- pipeline detail page with relative same-day next-run labels
- visible command previews for each task
- run detail page with newest-first raw logs
- run detail page DAG and task-focused retry controls
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
- `piply ui`

## Starter Project Behavior

`piply init` now creates:

- `piply.yaml`
- `pipelines/extract.py`
- `pipelines/report.py`

The starter config includes:

- one pipeline with three tasks
- one downstream report pipeline
- one pipeline trigger chain from extract to report

## Active Modules

Core runtime:

- `piply/core/loader.py`
- `piply/core/models.py`
- `piply/core/scheduling.py`
- `piply/core/scheduler.py`
- `piply/core/service.py`
- `piply/core/store.py`

Execution:

- `piply/engine/base.py`
- `piply/engine/local_engine.py`
- `piply/engine/task_runner.py`

HTTP and UI:

- `piply/api/app.py`
- `piply/api/routes/dashboard.py`
- `piply/api/routes/pipelines.py`
- `piply/api/routes/runs.py`
- `piply/api/routes/ui.py`
- `piply/api/schemas.py`
- `piply/ui/templates/`
- `piply/ui/static/`

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
- richer connection management for API and SSH tasks
- notification hooks
- auth for the Piply server
- stronger DAG interactions in the browser
