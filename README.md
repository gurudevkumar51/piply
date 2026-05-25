# Piply

Piply is a lightweight Python pipeline framework for teams that want YAML-defined workflows, schedules, retries, logs, sensors, and an operations UI without running a heavy orchestration stack.

It stays small on purpose:

- local dependency-aware DAG execution
- SQLite for runs, logs, task outputs, queue state, sensors, and pause overrides
- FastAPI plus server-rendered UI
- no Redis, Celery, Airflow, Prefect, or external queue required

## Features

- Multi-task pipelines with `depends_on`
- Python script, Python callable, CLI, API, webhook, email, and SSH tasks
- Reusable YAML `variables` with `{name}` interpolation
- `.env`, environment variables, explicit secrets, and reusable SQL connections
- Task output passing through `context["task_id"]`
- Pipeline-to-pipeline output and tenant context passing
- Per-task upstream failure behavior: `skip`, `fail`, or `continue`
- Schedules, sensors, retries, cancellation, reruns, and searchable logs
- Dashboard, Pipelines, Execution Matrix, Logs, Settings, and run detail pages

## Quick Start

```bash
pip install -e .
copy .env.example .env
piply validate --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml
```

Open `http://127.0.0.1:8000`.

Create a starter workspace:

```bash
piply init my-piply-project
piply run extract_flow --config my-piply-project/piply.yaml --wait
```

## Minimal YAML

```yaml
version: "1"
title: Piply Workspace
workspace: .

variables:
  scripts_dir: pipelines
  batch_id: demo-batch

connections:
  app_db: sqlite:///sensor_demo.db

pipelines:
  extract_flow:
    schedule:
      every: 15m
    retry:
      attempts: 2
      mode: resume
      delay_seconds: 10
    triggers_on_success:
      - report_flow
    tasks:
      extract:
        type: python
        path: "{scripts_dir}/extract.py"
        function: extract_data

      transform:
        type: python
        path: "{scripts_dir}/extract.py"
        function: transform_data
        depends_on: [extract]

      validate:
        type: cli
        command: python {scripts_dir}/validate_cli.py {batch_id}
        cwd: .
        depends_on: [transform]

  report_flow:
    tasks:
      build_report:
        type: python
        path: "{scripts_dir}/report.py"
        function: build_report
```

Python callable tasks can consume upstream outputs:

```python
def transform_data(context):
    extracted = context["extract"]
    return {"records": extracted["records"] + 1}
```

For Bash-specific CLI commands, set `shell: bash`:

```yaml
tasks:
  load_env_and_run:
    type: cli
    shell: bash
    command: set -a && source .env && set +a && conda run -n py312_extract python {scripts_dir}/job.py
    cwd: .
```

## Common CLI

```bash
piply init my-piply-project
piply validate --config piply-demo/piply.yaml
piply list --config piply-demo/piply.yaml
piply run extract_flow --config piply-demo/piply.yaml --wait
piply run extract_flow --tenant acme --param batch=2026-05-26 --config piply-demo/piply.yaml
piply tasks list extract_flow --config piply-demo/piply.yaml
piply tasks run extract_flow validate --tenant acme --param region=west --config piply-demo/piply.yaml
piply tasks retry <run_id> <task_id> --mode resume --config piply-demo/piply.yaml
piply runs --config piply-demo/piply.yaml
piply logs <run_id> --config piply-demo/piply.yaml
piply pause extract_flow --config piply-demo/piply.yaml
piply resume extract_flow --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml
piply stop --config piply-demo/piply.yaml
```

## Docs

- [Usage Guide](wiki/USAGE_GUIDE.md): detailed YAML examples, `.env`, multi-tenant runs, sensors, and every CLI command
- [Wiki Overview](wiki/README.md): architecture and feature summary
- [UI And API Guide](wiki/UI_API_GUIDE.md): screens, actions, and API examples
- [Implementation Summary](wiki/IMPLEMENTATION_SUMMARY.md): runtime modules and verification expectations

## Roadmap

Planned features:

- `piply logs --follow`
- plugin hooks for custom operators and sensors
- managed external secret backends
- richer queue, worker, and artifact metrics
- UI-safe pipeline editing
- reusable task templates / profiles
- optional distributed runner while keeping local mode as the default
- Design and implement a metadata-driven dynamic pipeline orchestration framework inspired by Airflow/Prefect concepts while keeping compatibility with the existing Piply YAML structure.

The framework must support 3 clear layers:

1. Pipeline Definition
2. Runtime Expansion
3. Execution Engine

Goal:
Allow reusable task templates to dynamically generate runtime tasks based on entity values.

Example Runtime Expansion:
Given:

entities:
  report:
    - payment
    - adjustment
    - refund

and tasks:

extract -> load

the engine should dynamically generate:

payment.extract -> payment.load
adjustment.extract -> adjustment.load
refund.extract -> refund.load

Requirements:

1. Maintain backward compatibility with current Piply YAML structure.

2. Introduce a new optional "entities" section at:
- global level
- pipeline level
- task level

3. Tasks should behave as reusable templates instead of static runtime tasks.

4. Runtime engine should:
- expand tasks dynamically
- resolve dependencies
- build DAG internally
- support parallel execution
- support retries/checkpoints
- support context propagation

5. Existing task types must continue working:
- python
- cli
- api
- webhook
- email
- ssh

6. Preserve existing dependency syntax:

depends_on: [extract]

but internally map runtime dependencies as:

payment.extract -> payment.load

7. Support variable interpolation:

command: python extract.py --report {report}

kwargs:
  report: "{report}"

8. Proposed enhanced YAML design:

pipelines:
  extract_flow:

    entities:
      report:
        - payment
        - adjustment
        - refund

    tasks:

      extract:
        type: python
        path: pipelines/extract.py
        function: extract_data
        kwargs:
          report: "{report}"

      transform:
        type: python
        path: pipelines/extract.py
        function: transform_data
        depends_on: [extract]

      validate:
        type: cli
        command: python validate.py --report {report}
        depends_on: [transform]

9. Runtime DAG generated internally:

payment.extract
payment.transform
payment.validate

adjustment.extract
adjustment.transform
adjustment.validate

refund.extract
refund.transform
refund.validate

10. Framework architecture should include:

- YAML parser
- entity expander
- DAG builder
- dependency resolver
- execution engine
- retry manager
- context manager
- logging/observability layer

11. Recommend best internal architecture using:
- Python
- Pydantic
- AsyncIO/Celery
- NetworkX
- plugin-based task executors

12. Design should be scalable for future support of:
- matrix expansion
- tenant-based execution
- dynamic branching
- conditional tasks
- task groups
- distributed execution
- Airflow/Prefect style dynamic task mapping

13. Suggest best practices for:
- runtime task naming
- execution tracking
- retry semantics
- state management
- lineage tracking
- observability
- DAG visualization

14. Recommend enterprise-grade folder structure and class design for implementation.

The solution should prioritize:
- scalability
- clean architecture
- metadata-driven execution
- extensibility
- maintainability
- backward compatibility
- minimal YAML complexity


 
