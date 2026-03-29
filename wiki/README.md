# Piply Wiki

## Overview

Piply is a lightweight workflow runner for Python-centric operations work. It favors a small runtime, readable configuration, and a polished built-in UI over large orchestration dependencies.

Core ideas:

- a project has one `piply.yaml`
- a project contains one or more pipelines
- each pipeline contains one or more tasks
- tasks form a DAG through `depends_on`
- pipelines can execute sequentially or in dependency-aware parallel mode
- a pipeline can trigger other pipelines after success

## Architecture

```text
CLI / API / UI
       |
PipelineService
       |
Loader + Scheduler + RunStore
       |
LocalEngine
       |
python / cli / api / ssh operators
       |
SQLite state + local process execution
```

## Configuration Model

Top-level fields:

- `version`
- `title`
- `workspace`
- `timezone`
- `defaults`
- `pipelines`

Example:

```yaml
version: "1"
title: My Piply Workspace
workspace: .
defaults:
  python: python
  env:
    APP_ENV: development

pipelines:
  extract_flow:
    title: Extract Flow
    description: Main extract and validation flow
    schedule:
      every: 15m
    execution:
      mode: parallel
      max_parallel_tasks: 2
    triggers_on_success:
      - report_flow
    tasks:
      extract:
        type: python
        path: pipelines/extract.py
      validate:
        type: cli
        command: python -c "print('validated')"
        depends_on: [extract]

  report_flow:
    tasks:
      build_report:
        type: python
        path: pipelines/report.py
```

## Scheduling

Supported schedule formats:

### Cron

```yaml
schedule:
  cron: "0 * * * *"
```

### Every

```yaml
schedule:
  every: 10m
```

### Interval Seconds

```yaml
schedule:
  interval_seconds: 300
```

Pipelines without a schedule are manual only.

## Execution Modes

Pipelines can run in one of two modes:

- `sequential` runs one ready task at a time
- `parallel` runs multiple ready tasks at once while still respecting dependencies

Example:

```yaml
execution:
  mode: parallel
  max_parallel_tasks: 3
```

## Task Operators

### Python Task

Runs a local Python file with optional args.

```yaml
tasks:
  main:
    type: python
    path: pipelines/job.py
    python: python
    args: ["--limit", "100"]
    cwd: .
```

### CLI Task

Runs a local command through the shell.

```yaml
tasks:
  validate:
    type: cli
    command: python -c "print('validated')"
```

### API Task

Makes an HTTP request using the standard library.

```yaml
tasks:
  webhook:
    type: api
    url: https://example.com/hook
    method: POST
    token: ${PIPLY_API_TOKEN}
    headers:
      X-Source: piply
    body: '{"hello": "world"}'
    expected_status: 201
```

### SSH Task

Runs a command on a remote host through SSH.

```yaml
tasks:
  remote_check:
    type: ssh
    host: localhost
    user: demo
    command: python /opt/jobs/check.py
```

If `command` is omitted, Piply uses a simple `echo piply-ssh-ok` probe.

## Dependency Rules

Task IDs:

- may contain letters, numbers, `_`, and `-`
- must be unique inside a pipeline

Validation rules:

- `depends_on` must point to existing task IDs
- task graphs cannot contain cycles
- `triggers_on_success` must point to existing pipelines
- pipeline trigger graphs cannot contain cycles

## Runtime Behavior

When a pipeline run starts:

1. a run record is created
2. task run records are created in queued state
3. tasks execute in dependency order using sequential or parallel scheduling
4. logs are written at both task and pipeline level
5. downstream pipelines are triggered after success when configured

## Retry Behavior

Failed runs can be retried in two modes:

- `resume` reuses successful upstream tasks and reruns failed or skipped work
- `startover` reruns the full pipeline

The run detail page lets operators click a failed task or DAG node and then choose the retry mode.

Run statuses:

- `queued`
- `running`
- `success`
- `failed`

Task statuses:

- `queued`
- `running`
- `success`
- `failed`
- `skipped`

## CLI Reference

### Initialize A Project

```bash
piply init
piply init my-project
piply init my-project --force
```

### Validate Config

```bash
piply validate --config piply-demo/piply.yaml
```

### List Pipelines

```bash
piply list --config piply-demo/piply.yaml
```

### List Tasks In A Pipeline

```bash
piply tasks list extract_demo --config piply-demo/piply.yaml
```

### Run A Pipeline

```bash
piply run extract_demo --config piply-demo/piply.yaml
piply run extract_demo --config piply-demo/piply.yaml --detach
```

### List Runs

```bash
piply runs --config piply-demo/piply.yaml --limit 20
```

### Start The Server

```bash
piply start --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml --reload
```

Compatibility alias:

```bash
piply ui --config piply-demo/piply.yaml
```

## API Reference

### Dashboard

- `GET /api/dashboard`

### Pipelines

- `GET /api/pipelines`
- `GET /api/pipelines/{pipeline_id}`
- `POST /api/pipelines/{pipeline_id}/run`
- `POST /api/pipelines/{pipeline_id}/pause`
- `POST /api/pipelines/{pipeline_id}/resume`

### Runs

- `GET /api/runs`
- `GET /api/runs/{run_id}`
- `POST /api/runs/{run_id}/retry`

### UI Pages

- `GET /`
- `GET /pipelines`
- `GET /pipelines/{pipeline_id}`
- `GET /runs`
- `GET /runs/{run_id}`

## UI Notes

The current UI emphasizes clarity for operators:

- light theme
- DAG-style pipeline view
- visible commands and script paths
- task run summaries
- newest-first raw log display
- timestamps shown as `HH:MM:SS.SSS`

## Storage

Default runtime state path:

```text
.piply/piply.db
```

Stored records include:

- pipeline runs
- task runs
- logs
- paused schedule overrides
- scheduler metadata

## Testing

Useful checks:

```bash
python -m pytest -q
python -m compileall piply
piply validate --config piply-demo/piply.yaml
piply run extract_demo --config piply-demo/piply.yaml --wait
```

Current automated tests cover:

- config loading for multi-task pipelines
- parallel execution config parsing
- downstream pipeline triggers
- run API log ordering and time labels
- API operator bearer token behavior
- SSH operator custom binary execution
- CLI starter project scaffolding
- resume retry behavior
- legacy database compatibility for manual runs

## Development Notes

The active code path is centered on:

- `piply/core/service.py`
- `piply/core/store.py`
- `piply/core/loader.py`
- `piply/core/scheduler.py`
- `piply/engine/local_engine.py`

The goal is to keep new features additive and modular without pulling in heavy orchestration frameworks.

## Upcoming Commands And Todos

- `piply logs`
- `piply tasks run`
- `piply pause`
- `piply resume`
- secrets helpers
- UI auth
- richer DAG controls
- notifications and webhooks
