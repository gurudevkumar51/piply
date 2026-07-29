Here is a cleaned, prioritized feature backlog for **Piply**, grouped into **Must Have**, **Should Have**, **Nice to Have**, **Deprecation**, and **Performance Improvements**.

---

# Piply Feature Backlog

## Priority 1 (Must Have)

### 1. Task & Pipeline Timeout

Add configurable execution timeout support.

Example:

```yaml
tasks:
  extract:
    timeout: 300s
```

or

```yaml
timeout_seconds: 300
```

Requirements:

* Support task-level timeout.
* Future support for pipeline-level timeout.
* Automatically terminate hanging subprocesses.
* Mark task as `FAILED` or `TIMED_OUT`.
* Record timeout reason in logs.
* Support configurable kill grace period.
* Update UI with timeout information.

---

### 2. Task Priority Scheduling

Support execution priority for runnable tasks.

Example:

```yaml
tasks:

  extract***:
    ...

  transform**:
    ...

  validate*:
    ...
```

Equivalent explicit syntax:

```yaml
tasks:

  extract:
    priority: 3
```

Requirements:

* Support both shorthand (`***`) and explicit `priority`.
* Internally normalize task names.
* Scheduler should execute higher-priority runnable tasks first.
* Dependencies always take precedence.
* Display priority in UI and DAG.

---

### 3. Pipeline Log Streaming

Implement:

```bash
piply logs --follow
```

Requirements:

* Stream logs in real time.
* Filter by:

  * pipeline
  * run
  * task
* Optional color output.
* Similar UX to:

```
docker logs -f
kubectl logs -f
```

---

### 4. Retention / Cleanup

Prevent unlimited database growth.

Add:

```bash
piply prune
```

Requirements:

* Remove old:

  * runs
  * logs
  * task outputs
  * artifacts
* Configurable retention period.

Example:

```yaml
retention:
  runs: 30d
  logs: 90d
```

Support:

```
piply prune

piply prune --days 30
```

Automatically VACUUM SQLite.

---

### 5. Dry Run Mode

Support validation without execution.

Example:

```bash
piply run my_pipeline --dry-run
```

Display:

* DAG
* resolved variables
* entity expansion
* execution order
* interpolated commands

No tasks should execute.

---

# Priority 2 (Should Have)

### 6. Prometheus Metrics Endpoint

Expose:

```
GET /metrics
```

Requirements:

Export:

* total runs
* successful runs
* failed runs
* running tasks
* scheduler status
* queue length
* execution duration

Prometheus compatible.

---

### 7. Sensor Error Visibility

Current sensor failures are silently ignored.

Requirements:

* Log sensor failures.
* Show last polling error in UI.
* Maintain sensor health state.
* Surface scheduler polling failures.

---

### 8. Runtime Diagnostics Page

New UI page showing:

* Scheduler health
* Active workers
* Running tasks
* Queue size
* Last reconciliation
* Sensor health
* Last scheduler tick

---

# Priority 3 (Nice to Have)

### 9. Conditional Task Execution

Simple syntax only.

Example:

```yaml
run_if: "{report} == 'payment'"
```

Avoid introducing a full DSL.

---

### 10. Execution Preview UI

Allow previewing:

* expanded tasks
* execution graph
* resolved variables

before execution.

---

### 11. Artifact Browser

Allow viewing/downloading:

* outputs
* generated files
* manifests

from UI.

---

# Deprecation Candidates

## 1. Email Task

Deprecate:

```yaml
type: email
```

Recommend using:

```yaml
type: webhook
```

Benefits:

* Less maintenance
* Better security
* Compatible with:

  * Slack
  * Teams
  * SendGrid
  * Mailgun
  * Discord
  * Custom APIs

---

## 2. SSH Task

Deprecate:

```yaml
type: ssh
```

Recommend:

```yaml
type: cli
command: ssh user@host "..."
```

Simplifies maintenance.

---

## 3. Legacy YAML Formats

Remove legacy compatibility before 1.0.

Examples:

* old `jobs:` root
* single-task schema
* deprecated aliases

Move compatibility into a dedicated legacy adapter if required.

---

## 4. Duplicate Package Exports

Remove unnecessary re-export packages.

Consolidate imports into one canonical namespace.

---

## 5. SQL Driver Simplification

Officially support:

* SQLite
* PostgreSQL

Mark MySQL/MSSQL support as experimental or optional.

Reduce dependency complexity.

---

# Performance Improvements

## 1. Eliminate Duplicate Database Queries

Avoid repeated:

```
get_latest_run_for_pipeline()
```

Reuse existing results where possible.

---

## 2. Rate-Limit Runtime Reconciliation

Current issue:

Every dashboard/API request triggers stale-run reconciliation.

Requirements:

* Cache last reconciliation timestamp.
* Skip repeated scans within a configurable interval (e.g., 5–10 seconds).
* Continue immediate reconciliation during scheduler ticks.

Reduces unnecessary database scans and improves UI responsiveness.

---

# Future Features (Out of Scope for Now)

Avoid implementing these until Piply reaches a stable 1.0 release:

* Plugin system
* Distributed execution engine
* Remote workers
* Complex branching DSL
* Full workflow expression language
* Kubernetes-native orchestration

These can significantly increase maintenance complexity and move Piply away from its goal of being a lightweight, metadata-driven orchestration framework.

---

## Guiding Principle

Every new feature should align with Piply's core philosophy:

* **Simple by default** — common use cases require minimal YAML and configuration.
* **Powerful when needed** — advanced capabilities are optional and metadata-driven.
* **Lightweight** — avoid unnecessary infrastructure or heavy dependencies.
* **Backward compatible** — existing pipelines should continue to work without modification.
* **Observable and reliable** — provide clear logs, metrics, health status, and recovery mechanisms.


## UI Requirement
- In run page & graph view downstream pipelines are not visible, Ideally it should be shown as DAG view with running/failed/success/skipped/queued status, on click redirect to that pipeline run page
- 


## Important

- backfill: rememebr all config of any run for backfill in future.
- Pipeline page: view should be similar to Airflow Dag page
- in run page downstream pipeline is not visible. 