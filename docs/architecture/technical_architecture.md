# Piply Technical Architecture

## Purpose

This document is the maintainer-facing map of Piply. It explains how YAML becomes runnable state, how the scheduler and engine cooperate, where state lives, how recovery works, and where future contributors can safely extend the system.

## System Overview

Piply is a local-first orchestration stack with four major runtime layers:

1. Definition layer: parse YAML, variables, deployments, schedules, sensors, secrets, and task templates.
2. Coordination layer: `PipelineService` decides what to run, when to run it, and how to present state to the CLI, API, and UI.
3. Persistence layer: `RunStore` persists runs, tasks, logs, queue items, sensor cursors, and scheduler metadata in SQLite.
4. Execution layer: `LocalEngine` and `TaskRunner` execute the DAG and keep heartbeat/state updates flowing.

High-level request flow:

```text
CLI / API / UI action
  -> PipelineService
  -> RunStore creates run + queued task rows
  -> LocalEngine dispatches work
  -> TaskRunner executes operator
  -> RunStore persists task/run/log/output state
  -> UI/API reads the same store through PipelineService
```

## Core Concepts

### Pipeline

A pipeline is the configured workflow definition loaded from YAML. In code this is `PipelineDefinition`.

It owns:

- task definitions
- schedule definition
- retry policy
- trigger-on-success targets
- sensor definitions
- concurrency settings

### Pipeline Template And Deployment

Simple mode uses `pipelines:` directly and stays unchanged. Advanced mode adds:

- `pipeline_templates`: reusable workflow definitions
- `pipeline_deployments`: concrete runnable instances with schedule, variables, tenant, environment, and execution overrides

The loader expands deployments into ordinary `PipelineDefinition` records. After loading, the scheduler, API, CLI, UI, and engine all operate on deployment ids such as `client_a_reporting`.

```text
pipeline_templates.report_pipeline
  -> pipeline_deployments.client_a_reporting
  -> PipelineDefinition(client_a_reporting)
  -> Run
  -> TaskRun rows
```

### Run

A run is one execution instance of a pipeline. In code this is `RunRecord`.

It owns:

- lifecycle state
- trigger source
- timestamps
- retry lineage
- tenant metadata
- aggregate task counts

### Task

A task is one executable node inside a pipeline. In code this is `TaskDefinition`, and one persisted execution row is `TaskRunRecord`.

### Executor

`LocalEngine` is the built-in executor. It uses `TaskRunner` to execute:

- `python`
- `cli`
- `api`
- `webhook`
- `email`
- `ssh`

## Execution Architecture

Execution flow:

```text
PipelineDefinition
  -> RunStore.create_run()
  -> queued TaskRun rows
  -> LocalEngine.mark_running()
  -> task scheduling by dependency order
  -> TaskRunner executes operator
  -> RunStore.finish_task_run()
  -> RunStore.finish_run()
```

### Sequential vs Parallel

The execution mode is derived from the task graph and config:

- sequential: tasks run in topological order one at a time
- parallel: ready tasks are submitted to a `ThreadPoolExecutor`

Dependency gating still applies in both modes.

### Runtime Context

`RuntimeTaskContext` holds JSON-safe outputs and run context such as:

- upstream task outputs
- mapped entity aliases
- `tenant_id`
- `params`
- parent pipeline/run metadata

Downstream Python callable tasks consume it through an optional `context` parameter.

### Output Persistence

Successful task outputs are stored in two places:

- in-memory runtime context for downstream execution
- SQLite `task_outputs` table for UI/API inspection and resume-mode retries

## Scheduler Architecture

The scheduler is intentionally small:

- `PipelineScheduler` runs in one background thread
- every tick updates scheduler heartbeat metadata
- schedules are materialized into the durable `trigger_queue`
- sensors also enqueue trigger items
- queue items are drained into runs by `PipelineService.drain_trigger_queue()`

Important design choice:

- the scheduler schedules concrete deployment/pipeline ids, not reusable templates
- persistence happens before dispatch, which makes restart recovery simpler

Scheduler tick flow:

```text
tick()
  -> update scheduler heartbeat
  -> reconcile stale runs
  -> reload config if changed
  -> enqueue due schedules
  -> poll sensors
  -> drain trigger queue
```

### Crash Detection

Scheduler liveness is not just a boolean flag. The UI snapshot uses:

- `scheduler_state`
- `scheduler_heartbeat`
- heartbeat age

This allows the UI to distinguish:

- `running`
- `stopped`
- `stale`
- `crashed`

## Runtime State Management

### Run States

Piply currently uses:

- `queued`
- `running`
- `success`
- `failed`
- `cancelled`
- `interrupted`

### Task States

Piply currently uses:

- `queued`
- `running`
- `success`
- `failed`
- `skipped`
- `cancelled`
- `interrupted`

### State Transition Model

Common run transitions:

```text
queued -> running -> success
queued -> running -> failed
queued -> running -> cancelled
queued -> running -> interrupted
```

Common task transitions:

```text
queued -> running -> success
queued -> running -> failed
queued -> running -> interrupted
queued -> skipped
queued -> cancelled
```

### Recovery Model

Piply uses heartbeats in `runs.heartbeat_at` to detect orphaned executions.

There are two recovery paths:

1. Graceful shutdown
   - service stops accepting new work
   - scheduler is stopped
   - active runs are marked `interrupted`
   - running tasks are marked `interrupted`
   - queued tasks are marked `cancelled`

2. Startup reconciliation
   - stale `queued` and `running` runs older than `PIPLY_STALE_RUN_TIMEOUT_SECONDS` are repaired
   - stale scheduler heartbeats make the UI show the scheduler as offline even if an old flag says it was running

This prevents the UI from leaving runs or tasks permanently `running` after Ctrl+C or process loss.

## Retry Lifecycle

Retry planning lives in `piply/core/retry.py`.

Modes:

- `startover`: rerun everything
- `resume`: reuse successful task outputs and rerun unresolved tasks

Resume-mode unresolved work includes task states:

- `failed`
- `interrupted`
- `skipped`
- `queued`
- `running`

## Database Architecture

Primary tables:

- `runs`
- `task_runs`
- `logs`
- `task_outputs`
- `trigger_queue`
- `sensor_state`
- `pipeline_overrides`
- `meta`

Relationship summary:

```text
runs (1) -> (many) task_runs
runs (1) -> (many) logs
runs (1) -> (many) task_outputs
trigger_queue -> future run creation
sensor_state -> one cursor/snapshot per sensor key
meta -> scheduler/runtime process metadata
```

Important indexes:

- run lookup by pipeline and recency
- run lookup by status
- task lookup by run and position
- queue lookup by status and availability
- unique scheduled-slot materialization
- unique queue dedupe keys

## API Architecture

The API is thin by design.

Layers:

1. FastAPI route
2. `PipelineService` method
3. `RunStore` read/write
4. Pydantic response schema

Main route groups:

- `dashboard`
- `pipelines`
- `runs`
- `execution`
- `ui`

Notable API patterns:

- UI pages and JSON routes share the same service layer
- there is a dedicated scheduler snapshot route for live topbar updates
- task detail routes expose logs and captured outputs without duplicating execution logic

## Frontend Architecture

Frontend structure:

- server-rendered Jinja templates in `piply/ui/templates`
- shared actions in `piply/ui/static/app.js`
- DAG renderer in `piply/ui/static/dag.js`
- global styles in `piply/ui/static/styles.css`

Important pages:

- Dashboard
- Pipelines list
- Pipeline detail
- Runs list
- Run detail
- Execution Matrix
- Logs
- Settings

Run detail page behavior is intentionally client-heavy:

- log filtering is client-side
- the task-focus panel is collapsible and stored in local storage
- graph re-layout happens in-memory without refetching DAG data

## Dependency Graph Architecture

Graph responsibilities are split:

- loader validates dependencies
- `piply.core.graph` provides graph helpers such as topological ordering and closures
- `piply.pipeline.expander` rewrites mapped template dependencies into concrete runtime ids
- `dag.js` renders the graph in flow, stage, and focus layouts

The execution engine never re-parses YAML edges at runtime. It works on the already expanded graph.

## Project Structure

Repository guide:

- `piply/core/`: loader, store, service, scheduler, retry, graph, sensors, secrets, SQL adapters
- `piply/engine/`: execution backend, heartbeat, task runner
- `piply/pipeline/`: runtime entity expansion
- `piply/api/`: FastAPI app, schemas, auth, API and UI routes
- `piply/ui/`: templates and static assets
- `piply/cli/`: Typer commands
- `tests/`: runtime, scheduler, CLI, helpers, plugin, and API coverage
- `wiki/`: user-facing and implementation notes
- `docs/architecture/`: deep maintainer documentation

## YAML Definition Modes

Simple mode:

```yaml
pipelines:
  extract_flow:
    schedule:
      every: 15m
    tasks:
      extract:
        type: python
        path: pipelines/extract.py
```

Advanced mode:

```yaml
pipeline_templates:
  report_pipeline:
    tasks:
      extract:
        type: python
        path: pipelines/extract.py
        function: extract_data
        kwargs:
          tenant: "{tenant}"

pipeline_deployments:
  client_a_reporting:
    template: report_pipeline
    schedule:
      every: 15m
    variables:
      tenant: client_a
```

The loader deep-merges deployment overrides into the template, injects shortcut `tenant` and `tenant_id` variables when present, then runs the same validation/entity-expansion path used by simple pipelines.

## Extensibility Guide

### Add a New Task Type

Touch these layers:

- extend `TaskType` and task parsing in `piply.core.loader`
- add execution behavior in `TaskRunner.run()`
- update UI labels if a new operator badge is useful
- add tests for loader + runtime + API exposure

### Add a New Executor

Implement `BaseEngine`:

- `dispatch(...)`
- `cancel(run_id)`

Then inject it into `PipelineService`.

Important compatibility points:

- support `initial_task_statuses`
- preserve run/task/log persistence semantics
- preserve `initial_context` if possible

### Add a New Sensor

Touch:

- sensor parsing in `piply.core.loader`
- sensor model fields in `piply.core.models`
- polling logic in `piply.core.sensors`
- service polling branch in `PipelineService.poll_sensors()`

### Add a New Scheduler Capability

Prefer queue-first design:

- materialize new trigger events into `trigger_queue`
- keep dispatch logic centralized in `drain_trigger_queue()`
- avoid direct execution from the scheduler thread

### Add a New Runtime State

Before adding a state, update:

- `RunStatus` and/or `TaskStatus`
- store transition SQL
- retry planning rules
- scheduler/UI/API serialization
- CSS badge and DAG color mappings
- tests that assert lifecycle behavior

## Development Guide

Typical local workflow:

```bash
pip install -e .
pytest
piply validate --config piply-demo/piply.yaml
piply start --config piply-demo/piply.yaml
```

Useful debugging checkpoints:

- inspect `.piply/piply.db`
- inspect `meta` for scheduler state
- inspect `trigger_queue` for blocked or stale dispatches
- inspect `runs.heartbeat_at` for recovery issues
- use `/api/dashboard`, `/api/dashboard/scheduler`, and `/api/metrics`

## Deployment Notes

Piply is currently optimized for a local or single-host deployment model:

- one SQLite database
- one scheduler thread
- one in-process execution engine

This keeps the system simple, but it also means future distributed execution should preserve:

- queue durability
- idempotent run materialization
- recoverable heartbeat semantics
- explicit lifecycle transitions
