# Piply UI And API Guide

## Overview

Piply ships with a bundled web UI and JSON API on top of the same service layer the CLI uses.

Current stack:

- FastAPI for HTTP routing
- Jinja2 templates for server-rendered pages
- small vanilla JS modules for graph rendering and actions
- SQLite for runs, logs, queue state, and sensor cursors
- a background scheduler thread for schedules and sensors

## Start The Server

```bash
piply start --config piply-demo/piply.yaml
```

Detached mode:

```bash
piply start --config piply-demo/piply.yaml -d
```

Optional flags:

```bash
piply start --config piply-demo/piply.yaml --host 0.0.0.0 --port 8080 --reload
```

Compatibility alias:

```bash
piply ui --config piply-demo/piply.yaml
```

## Authentication

Authentication can be defined in environment variables or a `.env` file:

```env
PIPLY_AUTH_ENABLED=true
PIPLY_AUTH_USERNAME=admin
PIPLY_AUTH_PASSWORD=change-me
PIPLY_API_TOKEN=replace-with-long-token
```

Behavior:

- UI routes use HTTP Basic auth
- API routes accept HTTP Basic auth
- API routes also accept Bearer tokens

## UI Routes

- `GET /`
- `GET /pipelines`
- `GET /pipelines/{pipeline_id}`
- `GET /runs`
- `GET /runs/{run_id}`

## DAG UI Features

Pipeline and run pages currently support:

- zoom
- pan
- flow view
- stage view
- focus view
- edge labels
- live node colors
- live task duration labels
- selected-task action panel

Pipeline detail page actions:

- run full pipeline
- pause or resume schedules
- delete pipeline
- override CLI commands for one manual run
- run a selected task with its upstream dependencies

Run detail page actions:

- cancel active run
- delete finished run
- retry from selected failed or skipped task
- start over the full run
- filter logs by selected task

## API Routes

### Dashboard

- `GET /api/dashboard`

Returns:

- project metadata
- stats
- pipeline summaries
- recent runs
- scheduler snapshot

### Pipelines

- `GET /api/pipelines`
- `GET /api/pipelines/{pipeline_id}`
- `POST /api/pipelines/{pipeline_id}/run`
- `POST /api/pipelines/{pipeline_id}/tasks/{task_id}/run`
- `POST /api/pipelines/{pipeline_id}/pause`
- `POST /api/pipelines/{pipeline_id}/resume`
- `DELETE /api/pipelines/{pipeline_id}`

Trigger a pipeline:

```bash
curl -X POST http://127.0.0.1:8000/api/pipelines/extract_demo/run
```

Trigger a task scope:

```bash
curl -X POST http://127.0.0.1:8000/api/pipelines/extract_demo/tasks/publish_manifest/run
```

Trigger a pipeline with one-off CLI overrides:

```bash
curl -X POST http://127.0.0.1:8000/api/pipelines/extract_demo/run \
  -H "Content-Type: application/json" \
  -d '{"command_overrides": {"validate_batch": "python --version"}}'
```

### Runs

- `GET /api/runs`
- `GET /api/runs/{run_id}`
- `GET /api/runs/{run_id}/logs`
- `POST /api/runs/{run_id}/retry`
- `POST /api/runs/{run_id}/cancel`
- `DELETE /api/runs/{run_id}`

Retry a failed run:

```bash
curl -X POST http://127.0.0.1:8000/api/runs/<run_id>/retry \
  -H "Content-Type: application/json" \
  -d '{"mode": "resume", "task_id": "flaky_step"}'
```

Cancel a queued or running run:

```bash
curl -X POST http://127.0.0.1:8000/api/runs/<run_id>/cancel
```

Delete a finished run:

```bash
curl -X DELETE http://127.0.0.1:8000/api/runs/<run_id>
```

Paginate raw logs:

```bash
curl "http://127.0.0.1:8000/api/runs/<run_id>/logs?limit=200&offset=0"
```

## Log Behavior

- logs are returned newest first
- `time_label` uses `HH:MM:SS.SSS`
- task log filtering is done client-side on the run page for fast interaction

## Scheduler Snapshot Fields

The dashboard snapshot includes:

- scheduler running status
- last scheduler heartbeat
- config path
- database path
- queue depth
- sensor count

## Storage

Default runtime state location:

```text
.piply/piply.db
```

Important tables:

- `runs`
- `task_runs`
- `logs`
- `trigger_queue`
- `sensor_state`
- `pipeline_overrides`

## Current Gaps And Planned Additions

- `piply logs --follow`
- richer worker metrics
- more database adapters for SQL sensors
- more sensor types beyond file and SQL
