# Piply development context

Last reviewed: 2026-10-06

This file is a quick handoff for future development work in this repository. It is meant to help a developer or coding agent regain context quickly without re-reviewing the full codebase from scratch.

## Current repository note

The reviewed candidate is committed on `codex/release-v0.3.4-validation`.
Inspect current status/diffs before editing; do not reset, restage wholesale,
or overwrite new user work. A clean review commit is not a publishing approval.

Release status: **HOLD v0.3.4**. See `docs/RELEASE_CHECK.md` for evidence and
`docs/BACKLOG.md` for acceptance criteria. P1.3 run admission is implemented and
its local gate passes. Native PostgreSQL and Windows Python 3.10-3.14 now pass;
Linux and the authorized review-branch CI matrix still need sign-off.
Candidate review commit: `0377680`. Push authentication currently blocks remote
CI; the owner has been asked to refresh saved Git access outside chat.

## Project identity

Piply is a local-first pipeline orchestrator. It provides:

- YAML-defined pipelines, tasks, templates, deployments, includes, variables, schedules, and sensors.
- CLI commands for validation, execution, planning, logs, users, setup, backfill, pruning, and operations.
- A FastAPI app with HTML UI, REST APIs, metrics, diagnostics, auth, notifications, and runtime operations.
- A local execution engine for Python, CLI, API, webhook, email, and SSH-style task operators.
- SQLite by default, with PostgreSQL support through optional dependencies.

Package metadata is in `pyproject.toml`.

- Distribution name: `mr-piply`
- Console command: `piply = "piply.cli.main:app"`
- Current reviewed version: `0.3.4`
- Python requirement: `>=3.10`

## Useful commands

Run these from the repository root.

```powershell
python -m pytest -q
python -m ruff check piply tests tools
python -m ruff format --check piply tests tools
python -m tools.check_run_admission
python -m piply.cli.main validate --config piply-demo/piply.yaml
python -m piply.cli.main plan --config piply-demo/piply.yaml
python -m piply.cli.main list --config piply-demo/piply.yaml
```

When tests create temp/cache files on Windows or in restricted sandboxes, prefer a repo-local base temp directory:

```powershell
python -m pytest -q --basetemp .pytest-tmp-codex -p no:cacheprovider
```

Do not use destructive git cleanup commands unless the user explicitly asks for them.

## High-level architecture

The codebase is layered like this:

```text
YAML config
  -> piply.core.loader / models / expander / conditions
  -> ProjectDefinition, PipelineDefinition, TaskDefinition

CLI / FastAPI routes / HTML UI
  -> piply.core.service.PipelineService

PipelineService
  -> RunStore for persistence
  -> LocalEngine for execution
  -> PipelineScheduler for schedules and sensors
  -> notification, artifact, auth, preview, diagnostics helpers

LocalEngine
  -> TaskRunner
  -> subprocess/function execution
  -> logs, outputs, artifacts, task state, downstream triggers
```

`PipelineService` is the coordination center. Most new runtime features should flow through it rather than bypassing it from routes or CLI code.

## Important modules

### App and API

- `piply/api/app.py`
  - FastAPI factory.
  - Creates `PipelineService`, `PipelineScheduler`, auth bootstrap, static files, and routers.
  - Registers observability and maintenance routes before wildcard pipeline/run routes to avoid route shadowing.
  - Respects `PIPLY_SCHEDULER_ENABLED=false`.

- `piply/api/routes/ui.py`
  - Main HTML UI pages.

- `piply/api/routes/pipelines.py`
  - Pipeline list/detail APIs and run endpoints.

- `piply/api/routes/runs.py`
  - Run detail and run-scoped APIs.

- `piply/api/routes/execution.py`
  - Runtime execution/cancel/retry-style endpoints.

- `piply/api/routes/maintenance.py`
  - Preview, artifacts, backfill, prune, log streaming, notification settings, SMTP settings, and database operations.

- `piply/api/routes/observability.py`
  - `/health`, `/metrics`, diagnostics, and sensor health endpoints.

- `piply/api/routes/accounts.py` and `piply/api/routes/setup.py`
  - User/session/setup routes.

- `piply/api/static.py` and `piply/api/routes/audit.py`
  - Platform-independent asset MIME types and admin-only audit listing.

### CLI

- `piply/cli/main.py`
  - Typer-based CLI entrypoint.
  - Covers init, validate, list, run, runs, logs, task runs, plan, prune, backfill, diagnostics, setup/admin/database/account operations, pause/resume, stop/start/ui.

When adding a user-facing capability, check whether it needs both an API/UI path and a CLI path.

### Definitions and config loading

- `piply/core/models.py`
  - Dataclasses for projects, pipelines, tasks, deployments, schedules, notifications, sensors, etc. API schemas are Pydantic models.

- `piply/core/loader.py`
  - YAML parser and include merger.
  - Handles environment tokens, brace variables, pipeline defaults, duplicate detection, included file defaults, deployments, templates, and entity expansion.
  - Preserves original task field templates in `TaskDefinition.variable_templates`; this is important for re-rendering downstream pipelines with inherited variables.

- `piply/pipeline/expander.py`
  - Expands template/entity style definitions into executable pipelines.

- `piply/core/conditions.py`
  - Safe condition/value evaluator used for `run_if` and conditional variable values.
  - Avoid replacing this with Python `eval`.

- `piply/core/preview.py`
  - Builds dry-run previews for CLI `plan` and UI/API preview.
  - Shows resolved variables, tasks, conditions, DAG stages, commands, and warnings.

### Runtime service

- `piply/core/service.py`
  - Central service layer.
  - Loads/reloads project config.
  - Starts runs, task-scoped runs, retries, backfills, downstream triggers, cancel/stop requests, preview, run summaries, logs, artifacts, settings, diagnostics, permissions, notifications, SMTP settings, database migration/copy, and pruning.
  - Stores run config snapshots so retries/backfills/downstream runs can replay the intended config instead of silently drifting with current globals.
  - Contains targeted and broad runtime recovery/reconciliation helpers.

### Persistence

- `piply/core/store.py`
  - `RunStore` persistence layer.
  - SQLite default and PostgreSQL optional support through dialect adapters.
  - Owns schema creation/migration, run/task records, logs, outputs, artifacts, sensors, notification deliveries, users, permissions, app metadata, run config snapshots, queue/status metrics, and pruning.

- `piply/core/sql_adapters.py` and `piply/core/dialects.py`
  - Database compatibility helpers.

### Execution engine

- `piply/engine/local_engine.py`
  - DAG execution engine.
  - Handles dependency scheduling, priority ordering, parallel/concurrency behavior, `run_if`, task and pipeline timeouts, artifact capture, cancellation, status transitions, and downstream behavior.

- `piply/engine/task_runner.py`
  - Executes individual task operators.
  - Handles subprocesses, Python callables, output streaming/capture, process tree termination, timeout handling, task environment, and operator-specific behavior.

- `piply/engine/heartbeat.py` and `piply/engine/base.py`
  - Execution heartbeat/base abstractions.

### Scheduling

- `piply/core/scheduler.py`
  - Background scheduler loop and ownership/heartbeat state.
  - Ticks schedules and sensors through `PipelineService`.

- `piply/core/scheduling.py`
  - Cron/schedule utility logic.

- `piply/core/sensors.py`
  - Sensor-related models/helpers.

### Operational helpers

- `piply/core/artifacts.py`
  - Declared artifact discovery and guarded downloads.

- `piply/core/notifications.py` and `piply/core/mailer.py`
  - Notification delivery, SMTP, Teams/webhook-style integration.

- `piply/core/auth.py`
  - Password hashing, session signatures, accounts, and permission helpers.
  - HTTP middleware and route guards are in `piply/api/auth.py`.

- `piply/core/audit.py`
  - Actor context, allowlisted event summaries, and audit filter parsing.

- `piply/core/processes.py`
  - Process inspection/termination helpers.

- `piply/core/secrets.py`
  - Secret/configuration helpers.

- `piply/core/outputs.py`, `context.py`, `retry.py`
  - Output/context/retry utilities.

## Runtime flows to keep in mind

### Config load and include flow

1. The loader reads `piply.yaml`.
2. Included YAML files are merged.
3. File-local `pipeline_defaults` are applied to pipelines/deployments from that file.
4. Project-level variables and deployment variables are normalized.
5. Templates/entities are expanded into concrete pipelines.
6. Loader checks validate the final dataclass project definition and its graphs.
7. The service fingerprints config sources so changes in included files can trigger reload.

Important: if a field may need to be resolved again later with inherited variables, preserve its original template form in `variable_templates`.

### Manual/API run flow

1. CLI/API/UI asks `PipelineService` to start a pipeline.
2. The service resolves deployment/template variables and runtime inputs.
3. A run config snapshot is built and stored.
4. `RunStore` creates the run and task rows.
5. `LocalEngine` starts execution.
6. `TaskRunner` executes each task and streams logs.
7. The store records task/run states, logs, outputs, artifacts, timings, and notifications.

### Task-scoped run flow

Task run APIs should execute only the selected task scope that the endpoint promises. Tests around this are asynchronous; they must wait for a terminal status before asserting task lists or outputs.

### Scheduler flow

1. `PipelineScheduler` runs in the app lifespan unless disabled.
2. Each tick checks due schedules and sensors.
3. It uses `PipelineService` rather than executing directly.
4. Scheduler ownership/heartbeat data is persisted so interrupted executions can be recovered.

### Downstream trigger flow

Downstream pipelines can be launched from upstream success/failure triggers. The intended behavior is:

- A downstream pipeline run directly should use its own/global variables.
- A downstream pipeline run triggered by an upstream run should inherit relevant upstream deployment/runtime variables unless explicitly overridden by the downstream run config.
- Inherited variables must re-render task fields that were originally templated.
- Downstream environment should include inherited context/output values where supported.
- Run config snapshots should preserve the effective variables used for the triggered run.

When changing this area, inspect:

- `PipelineService` downstream trigger methods.
- `PipelineService` run config snapshot/replay methods.
- `PipelineService` pipeline/environment cloning helpers.
- `TaskDefinition.variable_templates`.
- Tests covering downstream variable inheritance.

### Retry and backfill flow

Retries and backfills should use stored run config where available. This avoids accidentally re-running a historical deployment with changed global variables, changed deployment variables, or a modified included YAML file.

### Logs and artifacts

- Logs are streamed and persisted through `RunStore`.
- CLI/UI log follow should use existing log APIs/helpers rather than reading raw files.
- Artifacts are discovered from declared patterns and guarded by allowed roots before download.

### Shutdown and recovery

The app/service has logic to interrupt or reconcile active runs when the app exits or restarts. Tests around shutdown can be timing-sensitive on Windows because process termination and async status updates may lag slightly.

### Auth and permissions

Most operational routes must be permission guarded. Public routes should be intentionally public, such as health checks and setup bootstrap paths. New UI/API features should be checked against existing permission patterns before being exposed.

### Observability

Metrics are served in Prometheus text format when enabled. Diagnostics are admin-oriented and include database, scheduler, queue, and active run state.

## Documentation map

Primary docs live under `docs/`.

- `docs/README.md` - documentation index.
- `docs/YAML_SPECIFICATION.md` - most important config reference; keep it exhaustive.
- `docs/CLI.md` - CLI usage.
- `docs/API.md` - REST/API usage.
- `docs/UI_GUIDE.md` - UI usage.
- `docs/TEMPLATES_AND_DEPLOYMENTS.md` - templates/deployments.
- `docs/VARIABLES.md` - variables and interpolation.
- `docs/TASKS.md` - task types and task behavior.
- `docs/PIPELINES.md` - pipeline behavior.
- `docs/SENSORS.md` - sensors.
- `docs/NOTIFICATIONS.md` - notifications.
- `docs/AUTHENTICATION.md` - auth/users/permissions.
- `docs/DATABASE.md` - database setup/migration.
- `docs/ENVIRONMENT.md` - environment variables/settings.
- `docs/LIFECYCLES.md` - runtime lifecycle behavior.
- `docs/SECURITY.md` - security notes.
- `docs/AUDIT.md` - audit coverage and limits.
- `docs/RUNBOOK.md` - operating and recovery procedures.
- `docs/BACKLOG.md` - prioritized issues and acceptance criteria.
- `docs/RELEASE_CHECK.md` - candidate verification and outstanding publishing gates.
- `docs/FAQ.md` - common usage questions.
- `docs/EXAMPLES.md` - examples.
- `docs/MIGRATION.md` - migration guidance.
- `docs/ROADMAP.md` and `docs/FUTURE_FEATURES.md` - future planning.
- `docs/architecture/technical_architecture.md` - deeper maintainer architecture reference.

Whenever behavior changes, update at least the relevant user-facing doc and usually `docs/YAML_SPECIFICATION.md`.

## Tests map

Tests live under `tests/`.

Common areas:

- `tests/test_cli.py` - CLI command behavior.
- `tests/test_runtime.py` - API/runtime execution flows.
- `tests/test_scheduler.py` - scheduler behavior.
- `tests/test_features.py` - larger feature behavior.
- `tests/test_ui.py` - UI rendering/HTML behavior.
- `tests/test_auth.py` and `tests/test_security.py` - auth/security behavior.
- `tests/test_config_include.py` - include/default loading behavior.
- `tests/test_features.py` and `tests/test_config_include.py` - deployments and variables.
- `tests/test_run_controls.py` - pause gates, resume, and upcoming slots.
- `tests/test_audit.py` - attribution, permissions, filters, and retention.
- `tests/test_static_files.py` and `tests/test_release.py` - MIME handling, local graph assets, error escaping, packaging, and fresh-install regressions.
- `tests/test_postgres_store.py` - real PostgreSQL parity when `PIPLY_TEST_POSTGRES_DSN` is set; skipped otherwise. Explicit destructive-reset consent also requires `PIPLY_TEST_POSTGRES_RESET=1`.
- `tests/test_operations.py` - operational APIs/features.
- `tests/test_core.py`, `tests/test_helpers.py`, `tests/test_context_outputs.py`, `tests/test_plugins.py` - core helpers and plugin/context behavior.

For async runtime tests, avoid immediate assertions after a run starts. Poll until `success`, `failed`, `cancelled`, or another terminal status, with a small sleep between polls.

## Development guidelines

- Prefer changing `PipelineService` once over duplicating behavior in CLI, UI routes, and API routes.
- Keep route registration order in mind; wildcard pipeline/run routes can shadow specific operational endpoints.
- Preserve config snapshot semantics for retry, backfill, downstream, and task-scoped runs.
- Preserve original templated task fields when changes may affect variable inheritance.
- Avoid unsafe expression execution; extend the safe condition evaluator instead.
- When adding YAML fields, update:
  - definition dataclasses and API Pydantic models where applicable
  - loader behavior
  - preview output if user-visible
  - docs/YAML spec
  - examples/tests
  - UI/API schemas if surfaced there
- When adding a route, check auth/permission guards and whether HTML UI also needs it.
- When adding an operational feature, consider CLI, API, UI, docs, and tests together.
- When adding artifact/file download features, keep path traversal protections.
- For process execution changes, test on Windows and POSIX where possible.
- Keep subprocess command execution cross-platform; do not assume `/bin/bash` exists on Windows/WSL-style environments.

## Requirement files

The repository contains:

- `Requirement.md`
- `Requirment_advance.md`

The current codebase already appears to implement many items listed there, including priority, timeouts, dry-run preview, log streaming, pruning, metrics, diagnostics, sensor health, conditional execution, artifacts, backfill, downstream visibility/inheritance, grouping, and expanded documentation.

Before implementing more from those files, verify whether the feature is already present and whether the remaining work is a bug fix, UI polish, documentation update, or test coverage gap.

## Known watch-outs for future work

- Downstream variable inheritance is subtle. Direct downstream runs should use global/default variables, while upstream-triggered runs should inherit upstream deployment/runtime variables. Validate with both direct and triggered tests.
- API/runtime tests can race if they assert before background execution reaches a terminal state.
- Windows command execution should not assume a Unix shell. If a CLI task tries to execute through `/bin/bash`, inspect `TaskRunner` shell selection and the configured task command.
- The service layer is large. Small behavior changes may need focused regression tests around scheduler, task-scoped runs, retries, and downstream triggers.
- Docs are broad; stale docs are easy to create. Update examples whenever a YAML or CLI behavior changes.
- Fresh installs resolve newer dependencies than an existing laptop. Test the built wheel in an isolated environment with `tools/release_smoke.py`; source tests alone do not verify installed assets.
- Cron depends on IANA timezone data, including for UTC. Windows metadata now declares `tzdata`; Conda can hide this missing-dependency problem by supplying its own system database. Minimal POSIX images need OS data or an explicit tzdata install.
- Run creation reserves per-pipeline capacity atomically across stores/processes. Direct launches reject at capacity (API 409, CLI exit 1); automatic triggers remain queued. Keep the admission gate and real PostgreSQL race tests in release verification.
- Audit appends and operations are separate transactions. Do not describe the history as tamper-proof or claim rollback-on-audit-failure behavior.
