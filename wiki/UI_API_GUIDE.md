# Piply UI And API Guide

## Overview

Piply ships with a lightweight web UI and JSON API built on top of the same service layer the CLI uses.

Current stack:

- FastAPI for HTTP routing
- Jinja2 templates for server-rendered pages
- small vanilla JS modules for DAG and action handling
- SQLite for run metadata and raw logs
- a background scheduler thread for time-based triggers

The UI is intentionally light-themed and operator-focused.

## Start The Server

```bash
piply start --config piply-demo/piply.yaml
```

Optional flags:

```bash
piply start --config piply-demo/piply.yaml --host 0.0.0.0 --port 8080 --reload
piply start --config piply-demo/piply.yaml -d
```

Compatibility alias:

```bash
piply ui --config piply-demo/piply.yaml
```

Direct entry point:

```bash
python run_api.py
```

## Authentication

Enable packaged auth with env vars or a `.env` file:

```env
PIPLY_AUTH_ENABLED=true
PIPLY_AUTH_USERNAME=admin
PIPLY_AUTH_PASSWORD=change-me
PIPLY_API_TOKEN=replace-with-long-token
```

Behavior:

- UI pages use HTTP Basic auth
- API routes accept HTTP Basic auth and Bearer auth
- Bearer auth is API-only

Example:

```bash
curl http://127.0.0.1:8000/api/dashboard \
  -H "Authorization: Bearer replace-with-long-token"
```

## UI Routes

- `GET /` dashboard
- `GET /pipelines` pipeline list
- `GET /pipelines/{pipeline_id}` pipeline detail with live DAG view
- `GET /runs` run list
- `GET /runs/{run_id}` run detail with live duration, task states, and raw logs

## API Routes

### Dashboard

- `GET /api/dashboard`

Response includes:

- project metadata
- global stats
- pipeline summaries
- recent runs
- scheduler snapshot

### Pipelines

- `GET /api/pipelines`
- `GET /api/pipelines/{pipeline_id}`
- `POST /api/pipelines/{pipeline_id}/run`
- `POST /api/pipelines/{pipeline_id}/pause`
- `POST /api/pipelines/{pipeline_id}/resume`

Example:

```bash
curl http://127.0.0.1:8000/api/pipelines
```

Trigger a run:

```bash
curl -X POST http://127.0.0.1:8000/api/pipelines/extract_demo/run \
  -H "Content-Type: application/json" \
  -d "{}"
```

Pause a schedule:

```bash
curl -X POST http://127.0.0.1:8000/api/pipelines/extract_demo/pause \
  -H "Content-Type: application/json" \
  -d "{}"
```

### Runs

- `GET /api/runs`
- `GET /api/runs/{run_id}`
- `POST /api/runs/{run_id}/retry`

Filters for `GET /api/runs`:

- `pipeline_id`
- `status`
- `limit`

Example:

```bash
curl "http://127.0.0.1:8000/api/runs?pipeline_id=extract_demo&limit=20"
```

Run detail payload includes:

- pipeline-level run summary
- task-level run records
- raw logs

Retry a failed run:

```bash
curl -X POST http://127.0.0.1:8000/api/runs/<run_id>/retry \
  -H "Content-Type: application/json" \
  -d '{"mode": "resume", "task_id": "flaky_step"}'
```

## DAG UI Features

Pipeline and run pages now include:

- zoom controls
- pan by drag
- DAG and tree orientations
- dependency edge labels
- live status colors for task nodes
- clickable failed tasks for retry focus

## Log Ordering And Time Format

Raw logs are returned newest first in both the UI and API.

Each log entry includes:

- `created_at`
- `time_label`
- `task_id`
- `stream`
- `message`

`time_label` is formatted as:

```text
HH:MM:SS.SSS
```

## Pipeline Detail Page

The pipeline detail page is designed around two needs:

- show the flow shape clearly
- keep the exact command or script target visible

That page includes:

- a DAG-style graph with last-known task colors
- task cards
- task type badges
- recent run summaries
- trigger targets for downstream pipelines

When a pipeline still has an active run, the detail page polls for fresh task states.

## Run Detail Page

The run detail page includes:

- compact run header
- a DAG for the specific run
- live duration updates
- graph and logs side by side
- task-level progress cards
- exit code and duration
- pipeline command preview
- newest-first raw logs with improved wrapping
- retry controls for failed or skipped tasks

While a run is active, the page refreshes periodically.

## Storage

By default, Piply stores runtime state in:

```text
.piply/piply.db
```

The database path is resolved relative to the config file directory unless `PIPLY_DATABASE` is set.

Useful environment variables:

- `PIPLY_CONFIG`
- `PIPLY_DATABASE`
- `PIPLY_DEFAULT_MAX_PARALLEL_TASKS`
- `PIPLY_STALE_RUN_TIMEOUT_SECONDS`
- `PIPLY_HEARTBEAT_INTERVAL_SECONDS`

## Runtime Health

Piply uses run heartbeats to avoid false long-running states.

Behavior:

- active runs are touched while executing
- stale `queued` or `running` runs are reconciled automatically
- reconciled runs are marked failed and visible in the UI/API immediately

## Working API Features

- list pipelines and runs
- view pipeline detail and latest task states
- trigger manual runs
- retry failed runs with `resume` or `startover`
- pause and resume schedules
- inspect task-level run history
- inspect raw logs
- use Basic or Bearer auth when server auth is enabled

## Upcoming API And UI Work

- push-style streaming logs
- pagination for very large log sets
- connection profile pages
- richer notification management
