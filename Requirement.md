# Piply – Pending Requirement Summary
## 1. Pipeline Architecture
In Pipeline Templates & Deployments

Introduced an optional deployment architecture while maintaining full backward compatibility.

Requirements:
Maintain compatibility with dynamic task expansion.
variables, env & all other settings/policies, inherit from deployment
Provide migration strategy and updated documentation.

## 2. Runtime Reliability
Startup & Shutdown Recovery

Implement robust runtime recovery to prevent stale execution states.

Requirements
Graceful shutdown handling.
Mark running pipelines/tasks as Interrupted (or another terminal state).
Prevent orphaned RUNNING records.
Startup reconciliation for interrupted executions.
Accurate scheduler health detection.
Automated tests for:
Ctrl+C
scheduler restart
crash recovery
unexpected termination.
## 3. Scheduler Improvements
Task Priority

Support execution priority for runnable tasks.

Requirements:

Explicit priority configuration.
Optional shorthand notation.
Scheduler executes higher-priority runnable tasks first.
Dependency order must always take precedence.
Display priority in UI and DAG.
Task & Pipeline Timeout

Add configurable execution timeout.

Requirements:

Task-level timeout.
Future pipeline-level timeout.
Graceful process termination.
Timeout status tracking.
Timeout reason in logs.
Configurable kill grace period.
UI support.
## 4. Pipeline Operations
Dry Run

Implement execution preview without running tasks.

Display:

DAG
execution order
resolved variables
expanded entities
interpolated commands.
Log Streaming

Add real-time log streaming.

piply logs --follow

Support filtering by:

pipeline
run
task

Optional colored output.

Retention & Cleanup

Implement runtime cleanup.

piply prune

Requirements:

remove old runs
remove logs
remove artifacts
configurable retention
automatic SQLite VACUUM.
## 5. Monitoring & Observability
Prometheus Metrics

Expose runtime metrics through:

GET /metrics

Include:

run counts
success/failure counts
running tasks
queue size
scheduler health
execution duration.
Runtime Diagnostics

Add a diagnostics page displaying:

scheduler health
workers
running tasks
queue size
sensor health
reconciliation status
scheduler heartbeat.
Sensor Health

Improve visibility into sensor failures.

Requirements:

log polling failures
display latest error
maintain sensor health status
surface scheduler polling issues.
## 6. User Interface
Pipeline Page

Improve Pipeline listing page.

Requirements:

Airflow-like DAG listing experience.
Show last execution date/time.
Reduce card size.
Remove unnecessary summary/details from grid view.
Run Details

Improve run visualization.

Requirements:

Display downstream pipelines in DAG.
Show downstream execution status.
Allow navigation to downstream pipeline run.
Preserve graph visibility on smaller screens.
Pipeline Details

Improve information density.

Requirements:

Merge metadata sections.
Reduce vertical spacing.
Prioritize DAG visibility.
## 7. Execution Features
Conditional Task Execution

Support lightweight conditional execution.

Example:

run_if: "{report} == 'payment'"

Avoid introducing a complex expression language.

Execution Preview UI

Provide a visual preview before execution.

Display:

expanded tasks
DAG
resolved variables
execution order.
Artifact Browser

Allow browsing and downloading:

generated files
outputs
manifests
execution artifacts.
## 8. Backfill

Implement intelligent backfill support.

Requirements:

Preserve complete runtime configuration for every execution.
Allow future backfill using the original execution configuration.
Retain variables, schedules, selectors and runtime settings.
## 9. Documentation

Complete documentation updates covering:

YAML specification
runtime architecture
scheduler lifecycle
task lifecycle
pipeline lifecycle
recovery process
retry lifecycle
UI documentation
execution examples.
## 10. Technical Architecture Guide

Create a comprehensive maintainer guide covering:

system architecture
project structure
scheduler
execution engine
runtime state management
database architecture
API architecture
frontend architecture
DAG generation
extensibility
development workflow
testing
deployment
architecture diagrams.
## 11. Performance Improvements

Improve runtime efficiency by:

eliminating duplicate database queries
rate-limiting runtime reconciliation
reducing unnecessary database scans during dashboard/API requests.

## 12. print task name in all logs

## 13. In pipeline page give option to short by upcoming run, Filter by running

## 14. The biggest problem, if any task of downstream pipeline fails in between, I have to rerun all upstream pipline again to rerun that task. because downstream pipelines uses, upstream variables.

## 15. I experience one problem, Downstream pipeline doesn't carry upstream pipeline environment variables.

## 16. UI: In pipeline page, show the pipelines in group, like if a pipeline template deployed for multiple tenate then show those pipelines together. As of now by default it shorts by alphabetical order.

## 17. in run page graph view downstream pipeline is not visibles.

## 18. UI: As of now UI is more confusing
