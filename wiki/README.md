# Piply Wiki

## Overview

Piply is a lightweight workflow runner for Python-centric operations work. It favors a small runtime, readable configuration, and a polished built-in UI over large orchestration dependencies.

Core ideas:

- a project has one `piply.yaml`
- a project contains one or more pipelines
- each pipeline contains one or more tasks
- tasks form a DAG through `depends_on`
- parallelism is inferred from the DAG automatically
- a pipeline can trigger other pipelines after success

## Architecture

```text
CLI / API / UI
       |
PipelineService
       |
Loader + Scheduler + RunStore
       |
LocalEngine + RunHeartbeat
       |
python / python_call / cli / api / ssh operators
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
      publish:
        type: cli
        command: python -c "print('published')"
        depends_on: [extract]

  report_flow:
    tasks:
      build_report:
        type: python_call
        path: pipelines/report.py
        function: build_report
        kwargs:
          report_name: nightly
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

## DAG Concurrency

Piply no longer requires `mode: parallel` in the YAML.

Behavior:

- Piply inspects the task graph
- if only one task can be ready at a time, execution stays sequential
- if multiple tasks can become ready together, Piply runs them in parallel
- `max_parallel_tasks` caps that concurrency
- `PIPLY_DEFAULT_MAX_PARALLEL_TASKS` provides the global default when a pipeline does not override it

Legacy `execution.mode` is still accepted for backward compatibility, but new configs should prefer just `max_parallel_tasks`.

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
```

      build_report:
        type: python
        path: pipelines/report.py
        function: build_report
        kwargs:
          report_name: nightly
```

Supported inline callable forms:

- `path` + `function`
- `module` + `function`
- `call: package.module:function`
- `call: relative/or/absolute_file.py::function`

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

### Webhook Task

Makes a simple HTTP POST.

```yaml
tasks:
  notify_slack:
    type: webhook
    url: https://hooks.slack.com/services/...
    body: '{"text": "Flow finished"}'
```

### Email Task

Sends a notification via SMTP.

```yaml
tasks:
  notify_team:
    type: email
    smtp_host: smtp.internal.local
    email_to: ["team@example.com"]
    email_subject: "Pipeline Success"
    email_body: "The nightly extract has finished."
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

## Authentication And Runtime Settings

Piply supports configuration through environment variables and optional `.env` files.

Relevant settings:

- `PIPLY_CONFIG`
- `PIPLY_DATABASE`
- `PIPLY_DEFAULT_MAX_PARALLEL_TASKS`
- `PIPLY_STALE_RUN_TIMEOUT_SECONDS`
- `PIPLY_HEARTBEAT_INTERVAL_SECONDS`
- `PIPLY_AUTH_ENABLED`
- `PIPLY_AUTH_USERNAME`
- `PIPLY_AUTH_PASSWORD`
- `PIPLY_API_TOKEN`

Authentication behavior:

- UI routes use HTTP Basic auth when enabled
- API routes accept HTTP Basic auth and Bearer token auth
- static assets remain public so authenticated pages can load correctly

Runtime health behavior:

- active runs receive heartbeats while executing
- stale queued or running runs are reconciled automatically
- reconciled runs are marked failed and their remaining queued tasks are marked skipped

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
3. the engine executes tasks in dependency order
4. any parallel branches run up to the configured worker cap
5. logs are written at both task and pipeline level
6. downstream pipelines are triggered after success when configured

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
piply start --config piply-demo/piply.yaml -d
piply stop --config piply-demo/piply.yaml
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
- DAG controls for zoom, pan, tree view, and dependency labels
- visible commands and script paths
- live run duration updates
- run graph and logs laid out side by side
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
- run heartbeats

## Testing

Useful checks:

```bash
python -m pytest -q
python -m compileall piply tests
piply validate --config piply-demo/piply.yaml
piply run extract_demo --config piply-demo/piply.yaml --wait
```

Current automated tests cover:

- config loading for multi-task pipelines
- auto-detected DAG parallelism and worker limits
- downstream pipeline triggers
- run API log ordering and time labels
- API operator bearer token behavior
- SSH operator custom binary execution
- `python_call` operator execution
- CLI starter project scaffolding
- resume retry behavior
- stale run reconciliation
- auth middleware behavior
- legacy database compatibility for manual runs

## Development Notes

The active code path is centered on:

- `piply/settings.py`
- `piply/core/service.py`
- `piply/core/store.py`
- `piply/core/loader.py`
- `piply/core/scheduler.py`
- `piply/engine/local_engine.py`
- `piply/engine/task_runner.py`
- `piply/ui/static/dag.js`

The goal is to keep new features additive and modular without pulling in heavy orchestration frameworks.

## Upcoming Commands And Todos

- `piply logs`
- `piply tasks run`
- CLI pause and resume commands
