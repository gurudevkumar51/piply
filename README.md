# Piply

Piply is a lightweight orchestration layer for running and scheduling Python-first workflows with a professional built-in UI.

This version is intentionally small:

- YAML-defined pipelines and task graphs
- Local execution with Python's standard library
- SQLite-backed run history and raw logs
- Built-in scheduler for `cron`, `every`, and `interval_seconds`
- FastAPI + Jinja UI with a light operator-friendly theme
- Modular task operators for `python`, `cli`, `api`, and `ssh`

## What It Does

Piply can:

- run one pipeline with multiple tasks
- respect task dependencies inside a pipeline
- trigger one pipeline after another completes successfully
- execute tasks sequentially or in dependency-aware parallel mode
- schedule pipelines on a clock
- store task-level statuses and raw logs
- show a DAG-style flow view inspired by tools like Airflow while still keeping commands and scripts visible

## Quick Start

```bash
pip install -e .
piply validate --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml
```

Open `http://127.0.0.1:8000`.

## Example Config

```yaml
version: "1"
title: Piply Demo Workspace
workspace: .
defaults:
  python: python

pipelines:
  extract_demo:
    title: Demo Customer Extract
    description: Multi-task extract pipeline with a downstream trigger.
    schedule:
      every: 10m
    execution:
      mode: parallel
      max_parallel_tasks: 2
    triggers_on_success:
      - report_demo
    tasks:
      extract:
        type: python
        path: pipelines/extract.py
        args: ["--records", "240"]
      validate_batch:
        type: cli
        command: python -c "print('Validating latest extract batch...')"
        depends_on: [extract]
      publish_manifest:
        type: cli
        command: python -c "print('Publishing manifest for downstream reporting...')"
        depends_on: [extract]

  report_demo:
    title: Demo Report Build
    tasks:
      build_report:
        type: python
        path: pipelines/report.py
```

## Task Operators

### Python

```yaml
tasks:
  extract:
    type: python
    path: pipelines/extract.py
    args: ["--records", "100"]
```

### CLI

```yaml
tasks:
  validate:
    type: cli
    command: python -c "print('validated')"
    depends_on: [extract]
```

### API

```yaml
tasks:
  notify:
    type: api
    url: https://example.com/hooks/report
    method: POST
    token: ${PIPLY_API_TOKEN}
    headers:
      X-Source: piply
    body: '{"status": "done"}'
    expected_status: [200, 201, 202]
```

The `token` field becomes `Authorization: Bearer <token>`.

### SSH

```yaml
tasks:
  remote_healthcheck:
    type: ssh
    host: example.internal
    user: deploy
    command: python /opt/jobs/daily_check.py
```

If `command` is omitted, Piply runs a simple SSH probe using `echo piply-ssh-ok`.

## Working Commands

These commands work in the current codebase:

```bash
piply init [directory]
piply validate --config piply-demo/piply.yaml
piply list --config piply-demo/piply.yaml
piply tasks list extract_demo --config piply-demo/piply.yaml
piply run extract_demo --config piply-demo/piply.yaml
piply run extract_demo --config piply-demo/piply.yaml --detach
piply runs --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml --reload
piply ui --config piply-demo/piply.yaml
python run_api.py
```

Notes:

- `piply start` is the primary server command.
- `piply ui` is a compatibility alias for `piply start`.
- `piply init` creates two starter pipelines with three tasks in the main flow and a downstream report flow.

## UI Notes

The UI focuses on operator clarity:

- light color theme
- dashboard overview for pipelines and recent runs
- pipeline detail page with DAG view and visible command previews
- run detail page with task-level status cards and retry controls
- raw logs shown newest first
- timestamps formatted as `HH:MM:SS.SSS`

## Retry Modes

Failed runs can be retried from the UI or API in two ways:

- `resume` reuses successful upstream work and reruns failed or skipped tasks
- `startover` reruns the full pipeline from the beginning

## Project Layout

The active runtime lives in:

- `piply/core/loader.py`
- `piply/core/models.py`
- `piply/core/scheduling.py`
- `piply/core/scheduler.py`
- `piply/core/service.py`
- `piply/core/store.py`
- `piply/engine/local_engine.py`
- `piply/api/app.py`
- `piply/api/routes/`
- `piply/ui/templates/`
- `piply/ui/static/`

## Verification

Typical local checks:

```bash
python -m pytest -q
python -m compileall piply
piply validate --config piply-demo/piply.yaml
piply run extract_demo --config piply-demo/piply.yaml --wait
```

## Upcoming Commands And Todos

Planned next steps:

- `piply logs` for direct CLI log streaming without opening the UI
- `piply tasks run` for targeted task execution
- `piply pause` and `piply resume` CLI commands
- secrets and credential helpers
- richer DAG interactions such as zoom, pan, and edge labels
- optional notifications and webhooks for run outcomes
- packaged authentication for the web UI and API

## More Docs

- [wiki/README.md](wiki/README.md)
- [wiki/UI_API_GUIDE.md](wiki/UI_API_GUIDE.md)
- [wiki/IMPLEMENTATION_SUMMARY.md](wiki/IMPLEMENTATION_SUMMARY.md)
