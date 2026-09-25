# Piply Technical Architecture

The maintainer-facing map of Piply: how YAML becomes runnable state, how the
scheduler and engine cooperate, where state lives, how recovery works, and where
to extend the system safely.

For user-facing material see
[YAML_SPECIFICATION.md](../YAML_SPECIFICATION.md),
[LIFECYCLES.md](../LIFECYCLES.md),
[UI_GUIDE.md](../UI_GUIDE.md), and
[EXAMPLES.md](../EXAMPLES.md).

---

## 1. System Architecture

Piply is a local-first orchestrator with four layers.

| Layer | Responsibility | Key modules |
| --- | --- | --- |
| Definition | Parse YAML into typed runtime objects | `core/loader.py`, `pipeline/expander.py`, `core/scheduling.py`, `core/secrets.py` |
| Coordination | Decide what runs, when, and what the surfaces see | `core/service.py`, `core/scheduler.py` |
| Persistence | Durable runtime state | `core/store.py` |
| Execution | Run the DAG and stream results | `engine/local_engine.py`, `engine/task_runner.py` |

```text
   ┌──────────┐   ┌──────────┐   ┌─────────────────────┐
   │   CLI    │   │ HTTP API │   │ Server-rendered UI  │
   │ typer    │   │ FastAPI  │   │ Jinja + vanilla JS  │
   └────┬─────┘   └────┬─────┘   └──────────┬──────────┘
        └──────────────┼────────────────────┘
                       ▼
              ┌─────────────────┐
              │ PipelineService │  the only coordination entry point
              └────────┬────────┘
        ┌──────────────┼──────────────┬──────────────────┐
        ▼              ▼              ▼                  ▼
   ┌─────────┐   ┌──────────┐  ┌─────────────┐   ┌──────────────┐
   │ Loader  │   │ RunStore │  │  Scheduler  │   │ LocalEngine  │
   │  YAML   │   │  SQLite  │  │  poll loop  │   │ DAG executor │
   └─────────┘   └──────────┘  └─────────────┘   └──────┬───────┘
                                                        ▼
                                                 ┌────────────┐
                                                 │ TaskRunner │
                                                 └──────┬─────┘
                            python │ cli │ api │ webhook │ email │ ssh
```

Design constraints that shape everything else:

- **No external infrastructure.** SQLite plus threads. No Redis, Celery, or
  broker.
- **One coordination object.** CLI, API, and UI all go through
  `PipelineService`, which is why they never drift apart.
- **Persist before dispatch.** Triggers are written to the queue before they
  execute, so a crash loses nothing.
- **Ownership is explicit.** Every run records the pid that owns it, which makes
  recovery decidable rather than heuristic.

---

## 2. Project Structure

```text
piply/
├── api/
│   ├── app.py                  FastAPI factory, lifespan, router order
│   ├── auth.py                 Basic + Bearer middleware
│   ├── schemas.py              Pydantic request/response models
│   └── routes/
│       ├── dashboard.py        /api/dashboard
│       ├── execution.py        /api/execution-matrix, /api/logs, /api/metrics
│       ├── maintenance.py      preview, artifacts, backfill, prune, run config
│       ├── observability.py    /metrics, /api/diagnostics, /api/sensors
│       ├── pipelines.py        /api/pipelines/*
│       ├── runs.py             /api/runs/*
│       └── ui.py               server-rendered pages
├── cli/main.py                 typer commands
├── core/
│   ├── artifacts.py            artifact discovery + download path guard
│   ├── context.py              RuntimeTaskContext
│   ├── graph.py                topological order, upstream/downstream closure
│   ├── loader.py               YAML -> ProjectDefinition
│   ├── models.py               dataclasses shared by every layer
│   ├── outputs.py              task output serialisation
│   ├── preview.py              dry-run preview builder
│   ├── processes.py            cross-platform pid liveness
│   ├── retry.py                retry plan construction
│   ├── scheduler.py            PipelineScheduler
│   ├── scheduling.py           cron and interval schedules
│   ├── secrets.py              secret backends
│   ├── sensors.py              file / sql / api sensors
│   ├── service.py              PipelineService
│   ├── sql_adapters.py         SQL sensor connections
│   └── store.py                RunStore (SQLite)
├── engine/
│   ├── base.py                 BaseEngine interface
│   ├── heartbeat.py            RunHeartbeat
│   ├── local_engine.py         DAG execution, priority, timeouts, run_if
│   └── task_runner.py          operator implementations
├── pipeline/expander.py        entity expansion
├── settings.py                 environment-driven settings
└── ui/
    ├── static/{app.js,dag.js,styles.css}
    └── templates/*.html
```

Import direction is strictly one-way: `api` and `cli` depend on `core`; `core`
depends on `engine` only through the `BaseEngine` interface; `engine` depends on
`core.models` and `core.store`. Nothing in `core` imports `api`.

---

## 3. Definition Layer

`load_project()` turns one YAML file into a `ProjectDefinition`.

```text
piply.yaml
  ├─ load_settings()            .env + environment
  ├─ load_secret_values()       secret backend -> ${secret:NAME}
  ├─ _parse_variables()         ordered, later values may use earlier ones
  ├─ _normalize_pipeline_definitions()
  │     ├─ pipelines:            copied as-is
  │     └─ pipeline_deployments: deep-merged over pipeline_templates
  ├─ per pipeline:
  │     ├─ _parse_schedule()     CronSchedule | IntervalSchedule
  │     ├─ _parse_execution()    concurrency
  │     ├─ _parse_retry_policy()
  │     ├─ expand_task_templates()   entity expansion
  │     ├─ _parse_task()         one TaskDefinition per runtime task
  │     ├─ _validate_task_graph() unknown deps + cycles
  │     ├─ _detect_parallelism()  can this DAG actually branch?
  │     └─ _parse_sensors()
  └─ _validate_pipeline_trigger_graph()   downstream cycles
```

### Interpolation and `variable_templates`

`_expand_string` resolves `{name}`, `$NAME`, and `${secret:NAME}` at load time.
That is convenient but lossy: once `{tenant}` becomes `acme`, a downstream run
cannot re-render it for a different tenant.

So `_parse_task` also stores the **pre-interpolation** form of every field that
can contain a placeholder (`command`, `args`, `kwargs`, `url`, `body`,
`headers`, `subject`, `to`, `host`, `user`) in `TaskDefinition.variable_templates`.
`PipelineService._clone_pipeline_with_inherited_variables` re-renders those
templates against inherited variables. This is the mechanism that lets a
downstream pipeline receive the upstream deployment's values.

`run_if` is deliberately *not* brace-expanded at load time — see §6.

---

## 4. Coordination Layer

`PipelineService` owns config caching, dispatch, retries, backfill, previews,
diagnostics, and every read model the UI uses.

Notable behaviours:

- **Config reload.** `reload_project()` compares the config file mtime, so an
  edit is picked up on the next scheduler tick without a restart.
- **Engine compatibility.** `_detect_engine_initial_context_support()` inspects
  the engine's `dispatch` signature so a custom engine written before
  `initial_context` existed still works.
- **Shutdown gate.** `_ensure_accepting_new_work()` raises once shutdown has
  begun, so nothing new is created while the runtime is winding down.
- **Rate-limited reconciliation.** Full stale-run scans are throttled to one per
  `PIPLY_RECONCILE_INTERVAL_SECONDS`; `get_run()` reconciles only its own row.

### Run configuration snapshots

`_build_run_config()` captures, at creation time, everything needed to reproduce
an execution: variables, inherited variables and env, initial context, command
overrides, tenant, selectors (entity keys and task ids), and execution settings.
It is stored as JSON in `runs.run_config`.

`retry_run`, `trigger_task(source_run_id=...)`, `backfill_run`, and
`preview_pipeline(source_run_id=...)` all replay that snapshot instead of
re-deriving values from an upstream pipeline. This is the fix for "a downstream
task can only be retried by re-running the whole upstream chain".

---

## 5. Scheduler

One background thread, one tick function.

```text
tick()
  ├─ write heartbeat + state
  ├─ reconcile stale runs
  ├─ reload_project()               if the file changed
  ├─ enqueue_due_schedules()        materialise every missed slot
  ├─ poll_sensors()                 record health per sensor
  └─ drain_trigger_queue()          dispatch, one run per pipeline per pass
```

Design choices worth preserving:

- The scheduler schedules **concrete pipeline ids**, never templates.
- Schedule slots are **persisted before dispatch**, which is what makes
  restart-time backfill trivial.
- `start()` claims ownership by writing `scheduler_owner_pid`, and first calls
  `recover_interrupted_executions()` to adopt whatever the previous owner left.
- Crash metadata is written with `set_meta_many()` in a single transaction, so a
  reader never sees `state=crashed` without `last_error`.

Health is derived from three inputs — configured state, heartbeat age, and
whether the owning pid is alive — yielding `running`, `stale`, `crashed`, or
`stopped`.

---

## 6. Execution Engine

`LocalEngine.dispatch()` runs inline when `wait=True`, otherwise on a daemon
thread named `piply-run-<run_id>`.

### Scheduling within a run

Both the sequential and parallel paths use the same selection rule:

```python
def _task_sort_key(index_by_task_id, task):
    return (-task.priority, index_by_task_id[task.task_id], task.task_id)
```

applied to the set of tasks whose dependencies have all resolved. Dependency
readiness is the outer filter, so priority can reorder peers but never violate
the DAG. Ties fall back to declaration order, which keeps unprioritised
pipelines behaving exactly as they did before priority existed.

The parallel path also tracks `available_slots` so `max_parallel_tasks` is
respected across scheduling rounds rather than only within one.

### Timeouts

*Task level* — `TaskRunner._run_subprocess` reads the child's output on a reader
thread and polls a deadline in the main loop. On expiry it logs the reason,
terminates, waits `kill_grace_period_seconds`, then kills. The reader thread
exists because a blocking `for line in process.stdout` cannot be interrupted to
check a deadline.

For `python` callable tasks the call runs on a worker thread joined with a
timeout. Python cannot force a thread to stop, so the task is marked `timed_out`
and the abandoned daemon thread is left to finish on its own. This is documented
rather than hidden.

*Pipeline level* — `_start_pipeline_watchdog()` arms a `threading.Timer` that
sets the cancel event. In-flight processes are terminated by the runner's cancel
check, unfinished tasks are flipped to `timed_out` by
`store.mark_unfinished_tasks_timed_out()`, and the run ends `timed_out`.

### Conditions

`safe_condition_eval()` walks a parsed AST and supports only constants, names,
comparisons, membership tests, and boolean operators. Anything else raises.

`_resolve_condition_placeholders()` substitutes `{name}` as a **quoted literal**
before parsing. That is why the loader must not brace-expand `run_if`: expanding
`"{report} == 'payment'"` at load time would produce `payment == 'payment'`,
where `payment` parses as a bare name and evaluates to `None`.

There is no `eval()` anywhere in the path.

### Artifacts

After a successful task, declared globs are resolved relative to the task
working directory. Path, size, mtime, and content type are recorded; the files
themselves are never copied. Downloads are guarded twice: the path must be one
the run recorded, and it must resolve inside the workspace, the config
directory, or `PIPLY_ARTIFACTS_DIR`.

---

## 7. Runtime State Management

### Run and task statuses

`queued`, `running`, `success`, `failed`, `skipped`, `cancelled`, `interrupted`,
`timed_out`. Runs use the same set minus `skipped` in practice.

### Heartbeats

`RunHeartbeat` touches `runs.heartbeat_at` every
`PIPLY_HEARTBEAT_INTERVAL_SECONDS`. Task transitions and log writes touch it
too, so a busy run stays fresh even between heartbeat ticks.

### Recovery

Three complementary mechanisms, described in full in
[LIFECYCLES.md §8](../LIFECYCLES.md#8-recovery-process):

1. **Graceful shutdown** — stop accepting work, then interrupt what is active.
2. **Startup recovery** — interrupt runs whose `owner_pid` is no longer alive.
3. **Heartbeat reconciliation** — interrupt runs that went silent.

`core/processes.py::is_process_alive` is deliberately platform-specific. On
Windows `os.kill` terminates the target for any signal other than the console
control events, so it opens a `PROCESS_QUERY_LIMITED_INFORMATION` handle and
checks `GetExitCodeProcess` for `STILL_ACTIVE`. Checking only whether the handle
opens is not enough: a terminated process whose parent still holds a handle
remains openable, which would report a dead child as alive.

---

## 8. Database Architecture

SQLite in WAL mode. Schema is created idempotently and migrated forward with
`ALTER TABLE` guarded by `PRAGMA table_info`, so an old database upgrades in
place and a database from a newer version still opens.

| Table | Holds |
| --- | --- |
| `runs` | lifecycle, trigger, retry lineage, parent run, tenant, `owner_pid`, `run_config` |
| `task_runs` | per-task state, position, `priority`, `timeout_seconds`, `run_if` |
| `logs` | raw output, optionally attributed to a task |
| `task_outputs` | JSON output plus bounded preview metadata |
| `task_artifacts` | files a task declared and produced |
| `trigger_queue` | durable pending triggers with a dedupe key |
| `sensor_state` | per-sensor cursor or snapshot |
| `sensor_health` | poll counts, last success, last error |
| `pipeline_overrides` | pause state |
| `meta` | scheduler and runtime key/value state |

Indexes exist for the hot paths: runs by pipeline and by creation, task runs by
run and position, logs by run and by creation, and the queue by status and
availability. Two unique indexes carry real semantics:

- `idx_runs_unique_schedule_slot` — one run per `(pipeline_id, scheduled_for)`,
  which is what makes schedule backfill idempotent.
- `idx_trigger_queue_dedupe` — one queue item per `dedupe_key`.

### Concurrency

A single `threading.Lock` serialises writes; reads open their own short-lived
connection. Writes are small and batched into explicit transactions. This is
adequate for the single-node scope Piply targets and is the main thing to
revisit before supporting multiple worker processes.

### Query design

Read models are written to avoid N+1 queries. `list_pipelines()` uses three
aggregate queries — `latest_runs_by_pipeline()`, `active_run_counts_by_pipeline()`,
and `task_states_for_runs()` — rather than four queries per pipeline.

---

## 9. API Architecture

FastAPI, created by `create_app()`. The lifespan builds the service and
scheduler, starts the scheduler, watches for a `shutdown_requested` flag from
`piply stop`, and on exit calls `service.shutdown_runtime()` then
`scheduler.stop()`.

**Router order matters.** `maintenance.router` is registered before
`pipelines.router` and `runs.router` so its explicit sub-paths are not shadowed
by their `{pipeline_id}` / `{run_id}` wildcards.

| Group | Endpoints |
| --- | --- |
| Dashboard | `GET /api/dashboard`, `GET /api/dashboard/scheduler` |
| Pipelines | list, detail, run, task run, chain, pause, resume, delete |
| Runs | list, detail, logs, task detail, task output, retry, cancel, delete |
| Execution | `GET /api/execution-matrix`, `GET /api/logs`, `GET /api/metrics` |
| Operations | preview, artifacts, artifact download, backfill, prune, run config, `GET /api/logs/stream` |
| Observability | `GET /metrics`, `GET /api/diagnostics`, `GET /api/sensors` |

Auth is a middleware. UI paths require Basic; `/api/*` accepts Basic or Bearer;
`/metrics` accepts Bearer as well, so a scraper does not need UI credentials.

`/metrics` is rendered by hand into the Prometheus text exposition format —
adding `prometheus_client` would be the project's only runtime dependency purely
for formatting.

---

## 10. Frontend Architecture

Server-rendered Jinja templates plus vanilla JavaScript. No build step, no
framework, no bundler.

- `base.html` — shell, navigation, and the scheduler chip that polls
  `/api/dashboard/scheduler` every five seconds.
- `app.js` — `piplyRequest`, `escapeHtml`, `formatDurationSeconds`, and the
  shared trigger/retry/cancel/delete actions.
- `dag.js` — the graph renderer (see §11).
- `styles.css` — one stylesheet, CSS custom properties, no preprocessor.

Each page embeds its initial data as JSON via `|tojson` and then re-renders from
the API on a poll, so the first paint needs no round trip while live pages stay
current. Pages that show finished state do not poll at all.

All interpolated strings pass through `escapeHtml`.

---

## 11. DAG Generation

`ui/static/dag.js` is standalone: `renderInto(target, tasks, taskStateMap, options)`.

1. Build a `dagre` graph, nodes 230x104, edges from `depends_on`.
2. Lay out with `rankdir` from the current layout: Flow (LR), Stage (TB), or
   Focus (LR + lineage dimming).
3. Emit SVG — edges first, then node groups with status colour, a status dot,
   title, type, and a badge line carrying status, priority, timeout, and a
   `conditional` marker.
4. Apply pan/zoom through a group transform. Per-container state (`_dagState`)
   survives re-renders, so a live run does not reset the viewport every poll.

Two node kinds:

- **Task nodes** — solid border, click selects and updates the inspector.
- **Pipeline nodes** — dashed border, click navigates to the downstream run, or
  to its pipeline page when it has not been dispatched yet.

The run page builds downstream nodes from `RunDetailResponse.downstream`,
attaching them to the run's terminal tasks. `service.downstream_run_links()`
returns configured targets even when no child run exists yet, so the chain is
visible rather than silently absent.

---

## 12. Extensibility

### A new operator type

1. Add the literal to `TaskType` in `core/models.py`.
2. Parse its keys in `_parse_task` and add any placeholder-bearing fields to
   `variable_templates`.
3. Implement `_run_<type>_task` in `engine/task_runner.py` and dispatch to it
   from `TaskRunner.run`.
4. Handle it in `TaskDefinition.command_preview` so the UI and `piply plan`
   render it.
5. Add a test in `tests/test_plugins.py`.

### A new engine

Implement `BaseEngine.dispatch` and `cancel`, then pass an instance to
`PipelineService(engine=...)`. `_detect_engine_initial_context_support()` means
an engine that omits `initial_context` still works.

### A new sensor type

Add a `poll_<type>_sensor(sensor, state) -> (next_state, event | None)` in
`core/sensors.py`, parse it in `_parse_sensors`, and dispatch from
`PipelineService.poll_sensors`. Report failures by returning
`_failed_state(state, error)` rather than raising, so one broken sensor cannot
stop the others.

### A new page

Add a route in `api/routes/ui.py`, a template, and a nav link in `base.html`.
Reuse `pipeline-table`, `definition-list`, and `stat-grid` from `styles.css`.

---

## 13. Development Workflow

```bash
pip install -e ".[dev,test]"
pre-commit install

ruff check piply tests
ruff format piply tests
pytest -q

piply validate --config piply-demo/piply.yaml
piply plan     --config piply-demo/piply.yaml
piply start    --config piply-demo/piply.yaml
```

Conventions: line length 120, double quotes, a docstring on every public
function, and comments reserved for explaining *why* rather than *what*.

---

## 14. Testing

| File | Covers |
| --- | --- |
| `test_core.py` | loader, validation, variables, entity expansion |
| `test_helpers.py` | end-to-end runs, chaining, retries, cancellation |
| `test_runtime.py` | API surface, auth, shutdown, stale reconciliation |
| `test_scheduler.py` | schedule backfill, sensors, scheduler crash state |
| `test_context_outputs.py` | context passing, upstream failure behaviour |
| `test_plugins.py` | operator implementations |
| `test_cli.py` | CLI commands |
| `test_features.py` | priority, timeouts, `run_if`, artifacts, preview, backfill, prune, metrics |
| `test_recovery.py` | Ctrl+C, scheduler restart, crash, process kill |
| `test_ui.py` | page rendering, grouping, downstream links, deployments |

Tests use real subprocesses and a real SQLite file — no mocking of the runtime.
`test_recovery.py` launches and kills an actual child process to prove that
recovery works against genuine process death rather than a simulation.

Guidelines: one behaviour per test, assert on observable state (statuses, logs,
API payloads) rather than internals, and give each test its own `tmp_path`
database.

---

## 15. Deployment

### Single node

```bash
pip install mr-piply
piply start --config /srv/piply/piply.yaml --host 0.0.0.0 --port 8000
```

systemd:

```ini
[Unit]
Description=Piply
After=network.target

[Service]
Type=simple
User=piply
WorkingDirectory=/srv/piply
Environment=PIPLY_CONFIG=/srv/piply/piply.yaml
Environment=PIPLY_DATABASE=/var/lib/piply/piply.db
Environment=PIPLY_AUTH_USERNAME=ops
Environment=PIPLY_AUTH_PASSWORD=change-me
ExecStart=/usr/local/bin/piply start --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

`Restart=always` is safe: startup recovery interrupts whatever the killed
process owned, so a restart never leaves orphaned RUNNING rows.

### Operational notes

- **Run a single instance per database.** Ownership is per process and SQLite
  writes are serialised in-process; two servers on one file is out of scope.
- **Back up the database file**, or the whole `.piply` directory to include the
  WAL.
- **Schedule retention** — `piply prune` from cron, or call
  `POST /api/maintenance/prune`.
- **Monitor** `/metrics`; alert on `piply_scheduler_up == 0`,
  `piply_queue_oldest_age_seconds`, and `piply_sensor_health == 0`.
- **Behind a proxy**, forward `Authorization` so Bearer auth keeps working.

---

## 16. Diagrams

### Trigger to run

```text
schedule due ─┐
sensor fired ─┤
downstream   ─┼─► trigger_queue ──► drain_trigger_queue ──► trigger_pipeline
retry policy ─┘   (dedupe_key)      (one per pipeline)            │
                                                                  ▼
                                                    capture run_config snapshot
                                                                  │
                                                                  ▼
                                                    create run + queued tasks
                                                                  │
                                                                  ▼
                                                       LocalEngine.dispatch
```

### Task state machine

```text
                     ┌──────────► skipped   (disabled | run_if false | upstream)
                     │
  queued ──► running ┼──────────► success ──► capture output + artifacts
                     ├──────────► failed
                     ├──────────► timed_out (task or pipeline deadline)
                     ├──────────► cancelled (user or pipeline cancellation)
                     └──────────► interrupted (owning process stopped)
```

### Recovery decision

```text
                    active run found at startup
                                │
                    owner_pid still alive?
                     ┌──────────┴──────────┐
                    yes                    no
                     │                      │
            leave it alone         mark interrupted
        (another live process)     (crash / kill / power loss)
```

### Downstream inheritance

```text
upstream run (success)
  │  variables + shared env + JSON outputs + tenant_id
  ▼
trigger_queue item (payload)
  │
  ▼
downstream run
  ├─ re-renders variable_templates with the inherited values
  ├─ merges inherited env into every task
  └─ stores it all as run_config
        │
        └─► retry / replay uses this snapshot, not the upstream pipeline
```
