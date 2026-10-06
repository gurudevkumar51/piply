# Piply Backlog

The v0.3.4 [release assessment](RELEASE_CHECK.md) is currently HOLD. Prioritize
Linux and remote compatibility CI sign-off before publication. Native PostgreSQL
and Windows Python 3.10-3.14 are verified locally. P1.3 admission
is implemented and its local gate passes; other planned features are not all
prerequisites for this patch release. Existing verification sections below are
historical batch records, not substitutes for release sign-off.

This backlog consolidates the current open work after reviewing the codebase,
documentation, roadmap, future-features notes, and the requirement files.

Use it as the working queue. Older requirement notes contain several items that
are now shipped; those are not repeated here unless there is still a gap.

## Implementation Status

Updated 2026-10-06. This is a staged implementation queue; items remain pending
until their behavior and tests have been verified.

| Item | Status | Evidence / remaining work |
| --- | --- | --- |
| Fresh-install blank pipelines/graphs | Complete | Explicit JavaScript/CSS content types; incorrect/missing host mappings, cache/HEAD and security checks; live list/graph verification with incorrect MIME mappings. |
| P0.1 Run pause/resume | Complete | Persisted task-start gate; sequential/parallel execution; UI/API/CLI; cancellation, timeout, race and cross-process tests; live browser verification. |
| P0.2 Next 15 minutes on Runs page | Complete | Schedule window, held/disabled labels, permission and filter tests, periodic refresh; desktop/mobile browser checks. |
| P0.3 Run-page clarity | Complete | Attention summary and retry action above graph; live completion controls; refresh recovery; mobile Runs overflow fixed. |
| P1.1 Audit log | Complete | Persisted history; account/CLI/system actors; admin UI/API and local CLI; retention/migration; access, pagination and secret-handling regression tests. |
| P1.2 Session/token management | Partially implemented | Password-bound cookies added; named account tokens and individual session controls pending. |
| P1.3 Run concurrency enforcement | Implemented; CI sign-off pending | Atomic run/task reservation; shared manual/task/retry/queue capacity; 34 admission guards; four-path gate passes. Real PostgreSQL and full compatibility CI remain required. |
| P1.4 Audit transaction hardening | Pending | Changes and audit appends use separate transactions; a crash/storage failure can leave a gap. Documented in `docs/AUDIT.md`. |
| P2.1 Task retry | Pending | Automatic retries inside a task slot. Existing run resume bug fixed for cancelled/timed-out tasks. |
| P2.2 SLA warnings | Pending | Config, warnings, metrics, notifications. |
| P2.3 dbt artifacts | Pending | Artifact parsing and task result display. |
| P3.1 External log files | Pending | Durable files, live tail, prune/export compatibility. |
| P3.2 Resource pools | Pending | Shared limits across pipelines. |
| P3.3 Worker processes | Pending | Atomic claiming, ownership, SQLite/PostgreSQL verification. |
| P4.1 Typed run parameters | Pending | Declared contracts and forms. |
| P4.2 Full-text log search | Pending | Search indexing and compatibility. |
| P4.3 Run comparison | Pending | Config/output/status/duration comparison. |
| P4.4 Timeline | Pending | Task timing and critical path. |
| P4.5 Theme/navigation | Pending | Dark mode and keyboard controls. |
| P4.6 Plugin hooks | Pending | Inspect current plugin registry before extending public entry points. |

Additional confirmed fixes: run resume now includes `timed_out` and `cancelled`
tasks. Malformed non-ASCII session signatures are rejected cleanly. Existing Python-callable cancellation limits remain documented; running
callables safely in separate processes would require a separate compatibility
design. Downstream inheritance, Windows execution, async tests, and documentation
drift remain regression concerns to check with each relevant change.

### Release Review

The current [release assessment](RELEASE_CHECK.md) supersedes earlier environment
coverage notes. Release QA added local licensed graph libraries, safe error
banner escaping, explicit backup/restore handle closure, demo-package exclusion,
installed-wheel tooling, and a CLI callable-log capture fix found in fresh Conda.
The test-only pytest minimum now includes the upstream security fix. The
four-path admission reproducer now passes and remains a publishing gate. Real
PostgreSQL and compatibility CI sign-off are still required.

Before the admission follow-up, full suites on Conda Python 3.13.16 and Windows Python 3.14
each passed 341 tests with 20 PostgreSQL skips. Clean Windows QA identified the
undeclared timezone database; `tzdata` is now a Windows dependency. Python 3.14
QA also caught a parallel pause/resume race that could leave downstream tasks
queued in a successful run. The engine correction passes its deterministic
regression, 27 focused checks, both full suites, and installed-wheel parallel
browser verification. Upgrade, backup/restore, and rollback using the actual
published 0.3.3 package passed on disposable SQLite data. Release evidence
distinguishes these results from earlier developer-machine testing.

Follow-up security review should address CSRF/Origin enforcement for ambient
Basic credentials as well as P1.2 named account tokens. Current SameSite
cookies alone are not a complete cross-site request defense.

### Batch 1 Verification

- Final full regression suite: **294 passed, 19 skipped**. PostgreSQL parity
  checks are skipped without a configured test database. Ruff and diff
  whitespace checks passed.
- `tests/test_run_controls.py` covers active-task completion during pause,
  same-run resume, persisted gates, API permissions, CLI controls, cancellation,
  pause/resume races, wall-clock timeouts, schedule boundaries/filters and
  password-change session invalidation.
- Optional PostgreSQL parity test added; needs `PIPLY_TEST_POSTGRES_DSN` to run.
- Live browser: pause during extraction, extraction finishes while downstream
  tasks wait, then Resume completes all three tasks in the same run.
- Desktop 1280x900 and mobile 390x844: run actions, waiting/failure summary,
  upcoming schedule labels, responsive overflow, graph rendering, and clean
  browser console verified.

---

### Fresh-Install UI Fix Verification

- Reproduced the issue before the fix: all nine incorrect/missing MIME mapping
  cases failed, including JavaScript being served as `text/plain`.
- After the fix: **69 passed** across static-file, UI, security, and packaging
  tests, including 17 new static-file regression cases. Ruff and diff whitespace
  checks passed. Tests ran on Windows with Python 3.14; the installed Python 3.13
  environment lacks the runtime dependencies, so Conda/Python 3.13 was not tested
  directly.
- Live browser with deliberately incorrect `.js` and `.css` mappings: the
  pipeline list and three-node pipeline graph render, with no console errors.
- The runbook now covers updating the correct Conda environment, restarting,
  checking content types, and distinguishing local script failures from CDN
  or font connectivity issues.

---

### Audit Log Verification

- Final full regression suite: **331 passed, 20 skipped** on Windows with
  Python 3.14. PostgreSQL checks need `PIPLY_TEST_POSTGRES_DSN`; they were not
  exercised against a server. Ruff and diff whitespace checks passed.
- Twenty audit regression cases cover run/account/permission/settings actions,
  scheduler/sensor/retry attribution, concurrent request isolation, admin-only
  access, secret exclusion, query validation, stable pagination, deletion,
  restart, migration, and retention. Optional PostgreSQL parity coverage added.
- Live browser: filtering, older-page navigation, invalid-filter recovery,
  desktop rendering, and mobile 390x844 containment verified with a clean
  browser console.
- Added `docs/AUDIT.md` and updated API, CLI, database, security, UI, and runbook
  guidance. History begins after upgrading; older actions are not reconstructed.
- Crash-safe transactional recording remains follow-up P1.4. Normal interfaces
  cannot edit individual entries; retention and direct database access remain
  documented exceptions.

---

## Product Direction

Piply should stay:

- **lightweight**: no broker, no daemon stack, SQLite by default.
- **fast**: low overhead for local execution and quick troubleshooting.
- **secure**: clear permissions, masked secrets, auditable actions.
- **easy to use**: YAML remains the source of truth; the UI helps operators run
  and inspect pipelines without learning the internals.

New work should preserve backwards compatibility. Existing `piply.yaml` files
must continue to load without edits.

---

## Priority 0 - Operator Control And UI Clarity

These are the closest to the day-to-day pain described in `Requirement.md`.

### P0.1 Pause and resume a running pipeline between tasks

**Problem.** A user may need to inspect data or validate intermediate output
before allowing downstream tasks to continue. Resolved in batch 1 with separate
run-level controls; schedule pause remains a separate operation.

**Expected behavior.**

- Add a run-level pause/resume control.
- If a task is already running, let it finish.
- While the run is paused, do not start the next runnable task.
- Resume continues the same run from pending tasks.
- Do not kill the active task; this is not cancel.

**Likely touch points.**

- `piply/engine/local_engine.py`: DAG loop needs a pause gate before starting
  the next ready task.
- `piply/core/store.py`: persist run pause state so page refreshes and restarts
  do not lose it.
- `piply/core/service.py`: central pause/resume methods and permission checks.
- `piply/api/routes/runs.py`: run-level endpoints.
- `piply/ui/templates/run_detail.html`: controls and active paused state.
- `piply/cli/main.py`: optional `piply runs pause RUN_ID` and resume command.
- `tests/test_runtime.py` or `tests/test_operations.py`: pause while a task is
  active, verify next task waits, resume, then verify completion.

**Acceptance criteria.**

- Pausing a running run never terminates the current task.
- A paused run does not start additional tasks, even when dependencies are met.
- Resume starts the next eligible task without creating a new run.
- Cancel still cancels queued/running work.
- Diagnostics or run detail makes the paused state obvious.

### P0.2 Show pipelines scheduled in the next 15 minutes on the Runs page

**Problem.** The Runs page shows recent and active runs, but an operator also
needs to know what is about to start.

Resolved in batch 1. Next-slot previews are distinct from triggered run records.

**Expected behavior.**

- Add an "upcoming" section to `/runs`.
- Include pipelines whose next scheduled run is within the next 15 minutes.
- Keep active/running and completed run lists unchanged.
- Show pipeline id/title, next scheduled time, paused/disabled state, and a link
  to the pipeline detail page.

**Likely touch points.**

- `piply/core/service.py`: helper that lists upcoming scheduled pipelines within
  a time window.
- `piply/api/routes/ui.py`: include upcoming data for the Runs page.
- `piply/ui/templates/runs.html`: render a compact upcoming section.
- `piply/api/schemas.py`: add a response model only if this becomes API-visible.
- `tests/test_ui.py`: verify upcoming pipelines appear and inactive ones are
  labeled correctly.

**Acceptance criteria.**

- A pipeline scheduled in the next 15 minutes appears on `/runs`.
- Pipelines outside the window do not appear.
- Paused and disabled pipelines are not confused with active upcoming work.
- Existing run filtering and sorting behavior does not regress.

### P0.3 Reduce run-page confusion

**Problem.** Operators need a faster path from "something failed" to "what
should I do next?" The UI has many useful parts, but the Run page can still feel
dense.

Batch 1 adds attention summaries, live completion actions, refresh recovery,
and a mobile overflow correction. Further usability changes can be proposed
from specific operator feedback.

**Expected behavior.**

- Keep the run header compact and action-oriented.
- Make primary status, failed task, upstream run, downstream runs, and retry
  action visible without hunting.
- Keep the graph useful on smaller screens.
- Make upcoming scheduled runs visually distinct from actual triggered runs.

**Likely touch points.**

- `piply/ui/templates/run_detail.html`
- `piply/ui/static/styles.css`
- `tests/test_ui.py`

**Acceptance criteria.**

- Failed task and next action are visible in the first viewport.
- Downstream status is visible and clickable.
- No overlapping controls or clipped labels on mobile-width layouts.

---

## Priority 1 - Auditability And Access Control

### P1.1 Audit log

**Problem.** There is no durable record of who triggered, retried, cancelled,
deleted, paused, resumed, changed permissions, or updated settings.

Resolved with persisted action history, admin-only UI/API queries, and the local
`piply audit` command. See [Audit History](AUDIT.md) for coverage and limitations.

**Expected behavior.**

- Add an append-only audit table.
- Record actor, action, target, timestamp, and JSON detail.
- Include system actors such as scheduler and sensor.
- Surface as an admin-only UI page and CLI/API query.
- Retain or prune with existing retention policy.

**Likely touch points.**

- `piply/core/store.py`: schema and append/query methods.
- `piply/core/service.py`: write audit entries where operations are already
  centralized.
- `piply/api/routes/*`: admin query endpoint.
- `piply/ui/templates/settings.html` or a new audit page.
- `piply/cli/main.py`: `piply audit --since 7d`.
- `tests/test_auth.py`, `tests/test_operations.py`, `tests/test_security.py`.

**Acceptance criteria.**

- Manual run, retry, cancel, delete, pause/resume, permission grant, SMTP change,
  notification test, and database switch are audited.
- Non-admin users cannot read the audit log.
- Secrets are never stored in audit detail.

### P1.2 Session and token management

**Problem.** Password-bound cookies now invalidate sessions on password change.
Individual session controls remain missing, and the bearer token model is
global/admin-shaped.

**Expected behavior.**

- Invalidate existing sessions when a password changes.
- Add named API tokens per account.
- Allow admins to list and revoke sessions/tokens.
- Retain legacy `PIPLY_API_TOKEN` compatibility until a migration path is clear.

**Likely touch points.**

- `piply/core/auth.py`
- `piply/core/store.py`
- `piply/api/routes/accounts.py`
- `piply/ui/templates/settings.html`
- `docs/AUTHENTICATION.md`, `docs/API.md`, `docs/SECURITY.md`

**Acceptance criteria.**

- Changing a password invalidates old sessions.
- A user token only has the user's permissions.
- Admins can revoke individual tokens.
- Existing bearer-token installations keep working.

---

### P1.3 Run concurrency enforcement (implemented; CI sign-off pending)

Run creation now checks capacity and inserts run/task records in one transaction.
Queued and running records, including paused runs, reserve slots. Manual,
task-only, retry, chain, and single-run backfill requests reject excess work;
HTTP returns 409 and the CLI reports the limit with exit code 1. No rejected run
or successful-action audit is created. Automatic triggers retain their payload
and order in the queue, filling up to the configured limit on each drain.

SQLite uses `BEGIN IMMEDIATE` across instances/processes. PostgreSQL uses a
pipeline-row lock and explicit READ COMMITTED admission transactions. Existing
schedule-slot deduplication remains idempotent even at capacity. Failed inserts
roll back; failed engine dispatches release the slot as interrupted runs.

The four-path gate passes. Focused checks cover limits one/two, paused runs,
service/process/dispatcher races, task/tenant scope, API/CLI errors, and failure
recovery. The five admission cases now pass within the 25-case real PostgreSQL
16.15 parity run on Windows; service-container/Linux CI remains required.
Final full-suite and installed-wheel evidence belongs in the release assessment.

Final full suites on Conda Python 3.13.16 and standard Windows Python 3.14 each
passed 375 tests with 25 real-PostgreSQL skips. Corrected installed wheels pass
the smoke checks, including the unchanged four-path gate. Browser verification
confirmed a paused run still holds its slot and a rejected overlap creates no
extra run; same-run resume then completed all tasks. PostgreSQL/compatibility
CI must still pass before publication. The graph distributions hidden by the
old Git ignore rule now have narrow exceptions and must be committed with the
candidate. The user has now authorized a review branch for validation only;
staging/main merges and publication are not authorized.

### Compatibility Follow-Up

Native PostgreSQL 16.15 with psycopg 3.3.6 passed all 25 tests without skips.
Fresh Windows Python 3.10.21, 3.11.16 and 3.12.14 each passed 378 tests with 25
PostgreSQL skips in their default SQLite suites. The reset fixture now requires
`PIPLY_TEST_POSTGRES_RESET=1` as well as the disposable test DSN.

CI review-branch pushes validate without publishing. The PostgreSQL job now
uses a Node-enabled Ubuntu Actions image compatible with setup-python's build
manifest; a bare Node/Debian image was insufficient. Matrix jobs assert actual
OS/interpreter rather than trusting labels. Workflows are included in source
archive checks. Actual review CI results remain the release blocker.

---

### P1.4 Audit transaction hardening

Audit history currently appends after each accepted change in a separate
transaction. A crash or storage failure between the change and its event can
leave a history gap, or return an error after the change succeeded.

Make database mutations and their events atomic where they share a store. For
configuration-file changes, deliveries, and database switches, define a
recoverable intent/outcome protocol without claiming external side effects are
transactional. Test injected write failures and restart recovery on SQLite and
PostgreSQL. This remains application-managed history, not a tamper-proof ledger.

---

## Priority 2 - Reliability And Runtime Depth

### P2.1 Task-level retry

**Problem.** Pipeline-level retry is coarse. One flaky API or CLI task should
not force the whole run to retry.

**Expected behavior.**

```yaml
tasks:
  call_vendor_api:
    type: api
    retry:
      attempts: 3
      delay_seconds: 5
      backoff: exponential
      retry_on: [timeout, 5xx]
```

Retries happen inside the task slot; downstream tasks only see the final task
outcome.

**Likely touch points.**

- `piply/core/models.py`: task retry model.
- `piply/core/loader.py`: validation/defaults.
- `piply/engine/local_engine.py` and/or `piply/engine/task_runner.py`: retry
  execution loop and logging.
- `piply/core/retry.py`: reuse policy helpers where sensible.
- `docs/TASKS.md`, `docs/YAML_SPECIFICATION.md`, `docs/LIFECYCLES.md`.
- `tests/test_runtime.py`, `tests/test_features.py`.

**Acceptance criteria.**

- Successful retry records attempts in logs.
- Exhausted task retry fails according to normal failure rules.
- Pipeline-level retry still works and does not double-count task attempts.

### P2.2 SLA and expected-duration warnings

**Problem.** Slow pipelines are invisible until a hard timeout or failure.

**Expected behavior.**

```yaml
pipelines:
  nightly:
    sla:
      expected_duration: 15m
      must_finish_by: "06:00"
```

Show amber/warning state and optionally notify on SLA miss.

**Likely touch points.**

- `piply/core/models.py`, `piply/core/loader.py`
- `piply/core/service.py`, `piply/core/store.py`
- `piply/ui/templates/dashboard.html`, `piply/ui/templates/runs.html`
- `piply/core/notifications.py`
- `docs/PIPELINES.md`, `docs/YAML_SPECIFICATION.md`, `docs/NOTIFICATIONS.md`

**Acceptance criteria.**

- Running pipeline past expected duration is visible before timeout.
- Warning does not incorrectly mark the run failed.
- Metrics expose SLA breach counts.

### P2.3 dbt artifact awareness

**Problem.** dbt commonly appears as one opaque CLI task. Piply can parse dbt's
own artifacts after a task completes without adding a dbt dependency.

**Expected behavior.**

- Let a task declare a dbt artifact directory.
- Parse `run_results.json` after execution.
- Surface model status, timing, message, failures, rows affected, and relation.
- Optionally validate selector names from `manifest.json` in preview.

**Likely touch points.**

- New helper under `piply/core/` or `piply/engine/`.
- `piply/core/store.py`: task output or structured result storage.
- `piply/ui/templates/run_detail.html`
- `docs/TASKS.md`, `docs/EXAMPLES.md`, `docs/YAML_SPECIFICATION.md`

**Acceptance criteria.**

- No `dbt-core` dependency.
- Missing or malformed dbt artifacts never break an otherwise successful task.
- Parsed model failures are visible without reading the full log.

---

## Priority 3 - Scale And Storage

### P3.1 Log persistence outside SQLite

**Problem.** Logs dominate database growth for chatty jobs.

**Expected behavior.**

- Keep a recent tail in SQLite for live UI.
- Store complete logs in per-run files under a durable log directory.
- Serve older logs from files.
- Preserve current APIs as much as possible.

**Acceptance criteria.**

- Existing live tail behavior remains.
- Prune removes old log files.
- Backups and run exports are documented clearly.

### P3.2 Pipeline/resource concurrency pools

**Problem.** `max_parallel_tasks` is per pipeline; there is no cap for a shared
warehouse, API, browser, or external system across pipelines.

**Expected behavior.**

```yaml
pools:
  warehouse:
    max_concurrent: 3

tasks:
  load_bronze:
    pool: warehouse
```

**Acceptance criteria.**

- Pool limits apply across concurrently running pipelines.
- Queued work explains which pool is blocking it.
- Single-process default remains; no external queue required.

### P3.3 Multiple worker processes

**Problem.** One process owns all execution today. This is acceptable by design
until workload proves otherwise, but it limits scale.

**Expected behavior.**

- Optional worker mode.
- Atomic queue claiming.
- No broker required.
- Single-process mode remains the default.

**Acceptance criteria.**

- Two workers cannot run the same queued item.
- Crash recovery remains deterministic.
- SQLite and PostgreSQL behavior is explicitly tested.

---

## Priority 4 - Product Polish

### P4.1 Declared run-parameter forms

Manual runs already prompt for unresolved placeholders. The remaining feature is
a declared contract with types, defaults, enum values, and validation, rendered
as a UI form.

### P4.2 Full-text log search

SQLite FTS5 can improve log search without adding a dependency.

### P4.3 Run comparison

Compare two runs by duration, task status, outputs, and captured config.

### P4.4 Gantt/timeline view

Use task start/end times to show critical path and idle gaps.

### P4.5 Dark mode and keyboard navigation

Low-risk UI improvements if they stay consistent with the existing server-rendered
HTML and CSS variable approach.

### P4.6 Plugin hooks

Entry points for third-party operators and sensors. Useful once the internal
operator surface is stable enough to publish.

---

## Shipped Or Mostly Shipped From Older Requirement Notes

Verify before changing these areas; most of the requested behavior already
exists and should be treated as maintenance or polish, not greenfield work.

| Area | Current state |
| --- | --- |
| Pipeline templates and deployments | Shipped; see `docs/TEMPLATES_AND_DEPLOYMENTS.md`. |
| Include/split config | Shipped in 0.3.x; see `docs/PIPELINES.md` and `docs/FUTURE_FEATURES.md`. |
| Dynamic entity expansion | Shipped. |
| Priority | Shipped for tasks and entities. |
| Task and pipeline timeout | Shipped. |
| Dry-run preview | Shipped in CLI and UI. |
| Log streaming | Shipped in CLI, UI, and API. |
| Retention and prune | Shipped. |
| Metrics and diagnostics | Shipped. |
| Sensor health | Shipped. |
| Conditional execution | Shipped via `run_if`. |
| Artifact browser/download | Shipped. |
| Backfill and replay | Shipped using captured run config. |
| Downstream variable/env inheritance | Documented as shipped; regression-test carefully before changing. |
| Pipeline page grouping/sorting/filtering | Shipped; remaining work is polish only. |
| Run detail downstream graph | Documented as shipped; verify specific UI bugs before reworking. |

---

## Implementation Rules For Future Work

- Start at `PipelineService` when the feature crosses CLI, API, and UI.
- Add or update tests with the feature, especially for async runtime behavior.
- Preserve run configuration snapshots for retries, backfills, and downstream
  runs.
- Keep Windows command execution in mind; do not assume Bash.
- Add docs in the same change for any user-visible behavior.
- For YAML changes, update models, loader, preview, YAML spec, examples, and
  tests together.
- Do not introduce a runtime dependency unless it directly supports Piply's core
  promise and cannot reasonably be optional.
