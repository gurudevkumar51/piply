# Piply

Piply is a lightweight orchestration layer for running and scheduling Python-first workflows with a professional built-in UI.

This version stays intentionally small:

- YAML-defined pipelines and task DAGs
- local execution with Python's standard library plus a small FastAPI UI/API
- SQLite-backed run history, task history, and raw logs
- built-in scheduler for `cron`, `every`, and `interval_seconds`
- packaged auth through env vars or a `.env` file
- modular operators for `python`, `cli`, `api`, `webhook`, `email`, and `ssh`

## What It Does

Piply can:

- run one pipeline with multiple tasks
- infer sequential or parallel execution automatically from task dependencies
- cap maximum parallel execution globally or per pipeline
- trigger one pipeline after another completes successfully
- schedule pipelines on a clock
- store task-level statuses and raw logs
- retry failed runs with `resume` or `startover`
- detect stale "running" runs using heartbeats and mark them failed
- show an Airflow-style DAG view while still keeping commands and scripts visible

## Quick Start

```bash
pip install -e .
copy .env.example .env
piply validate --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml
```

Open `http://127.0.0.1:8000`.

Background mode:

```bash
piply start --config piply-demo/piply.yaml -d
```

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
        function: build_report
        kwargs:
          report_name: downstream-demo
```

`mode: parallel` is no longer required. Piply derives concurrency from the DAG and uses `max_parallel_tasks` only as the cap.

## Task Operators

### Python

```yaml
tasks:
  extract:
    type: python
    path: pipelines/extract.py
    args: ["--records", "100"]
```

```yaml
tasks:
  build_report:
    type: python
    path: pipelines/report.py
    function: build_report
    kwargs:
      report_name: nightly
```

Supported inline execution patterns:

- `path` + `function`
- `module` + `function`
- `call: package.module:function`
- `call: relative/or/absolute_file.py::function`

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

### Webhook & Email

```yaml
tasks:
  notify_slack:
    type: webhook
    url: https://hooks.slack.com/services/...
    body: '{"text": "Flow finished"}'

  notify_team:
    type: email
    smtp_host: smtp.internal.local
    email_to: ["team@example.com"]
    email_subject: "Pipeline Success"
    email_body: "The nightly extract has finished."
```

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

## Authentication And Runtime Settings

Piply reads settings from real environment variables and from an optional `.env` file in the current directory or next to `piply.yaml`.

Common settings:

```env
PIPLY_DEFAULT_MAX_PARALLEL_TASKS=4
PIPLY_STALE_RUN_TIMEOUT_SECONDS=3600
PIPLY_HEARTBEAT_INTERVAL_SECONDS=10
PIPLY_AUTH_ENABLED=true
PIPLY_AUTH_USERNAME=admin
PIPLY_AUTH_PASSWORD=change-me
PIPLY_API_TOKEN=replace-with-long-token
```

Behavior:

- UI pages use HTTP Basic auth
- API routes accept HTTP Basic auth and optional Bearer token auth
- stale active runs are auto-failed after the configured timeout

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
piply start --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml --reload
piply start --config piply-demo/piply.yaml -d
piply stop --config piply-demo/piply.yaml
piply ui --config piply-demo/piply.yaml
python run_api.py
```

Notes:

- `piply start` is the primary server command.
- `piply ui` is a compatibility alias for `piply start`.
- `piply init` creates two starter pipelines with three tasks in the main flow and a downstream `python_call` report flow.

## UI Notes

The UI focuses on operator clarity:

- light color theme
- richer DAG controls with zoom, pan, tree view, and edge labels
- live task status colors on running DAGs
- run detail page with live duration, retry controls, and graph/log split layout
- raw logs shown newest first
- timestamps formatted as `HH:MM:SS.SSS`
- small vanilla JS modules instead of a heavier frontend stack

## Verification

Typical local checks:

```bash
python -m pytest -q
python -m compileall piply tests
piply validate --config piply-demo/piply.yaml
piply run extract_demo --config piply-demo/piply.yaml --wait
```

## Upcoming Commands And Todos

Planned next steps:

- `piply logs` for direct CLI log streaming without opening the UI
- `piply tasks run` for targeted task execution
- in task box of graph view for every running, success or failed show the duration of task
- instead of cli command we should be able to run any command from the UI
- for cli operator we should give any batch file as well to execute


## More Docs

- [wiki/README.md](wiki/README.md)
- [wiki/UI_API_GUIDE.md](wiki/UI_API_GUIDE.md)
- [wiki/IMPLEMENTATION_SUMMARY.md](wiki/IMPLEMENTATION_SUMMARY.md)
