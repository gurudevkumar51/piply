# Piply Runtime Lifecycles

How work enters the system, how it executes, and what happens when something
stops unexpectedly.

---

## 1. Runtime Architecture

```text
        CLI                 HTTP API                Server-rendered UI
         |                      |                          |
         +----------+-----------+--------------+-----------+
                    |                          |
              PipelineService  <---------  Jinja templates
                    |
      +-------------+--------------+-----------------+
      |             |              |                 |
   Loader      RunStore       PipelineScheduler   LocalEngine
 (YAML ->      (SQLite)       (poll + dispatch)   (DAG execution)
 definitions)                                          |
                                                  TaskRunner
                                                       |
                              python | cli | api | webhook | email | ssh
```

`PipelineService` is the only component that the CLI, API, and UI talk to.
Everything else is reached through it, which is what keeps the three surfaces
consistent with each other.

---

## 2. Scheduler Lifecycle

```text
start()
  |
  +-- recover_interrupted_executions()   # adopt the previous owner's orphans
  +-- claim ownership (scheduler_owner_pid = current pid)
  +-- write heartbeat + state=running    # single transaction
  |
  v
tick()  every PIPLY_SCHEDULER_POLL_INTERVAL_SECONDS
  |
  +-- write heartbeat
  +-- reconcile stale runs
  +-- reload config if the file changed on disk
  +-- enqueue_due_schedules()   # backfills every missed slot
  +-- poll_sensors()            # records health per sensor
  +-- drain_trigger_queue()     # dispatch due triggers in order
  |
  v
stop()  -> state=stopped
crash   -> state=crashed, last_error recorded in the same transaction
```

### Health detection

`scheduler_snapshot()` reports one of four states:

| State | Condition |
| --- | --- |
| `running` | heartbeat is fresh and the owning process is alive |
| `stale` | heartbeat older than `3 x poll_interval`, owner still alive |
| `crashed` | owning process is gone, or the loop caught an exception |
| `stopped` | `stop()` was called |

Tracking the owner pid is what separates "busy tick" from "process was killed":
a stale heartbeat alone cannot tell those apart.

---

## 3. Trigger Queue

Every non-inline trigger goes through a durable SQLite queue rather than
executing directly. Sources: `schedule`, `sensor`, `pipeline`, `retry`.

```text
enqueue_trigger(dedupe_key)
  -> status=queued
  -> claim_queue_item()   -> status=dispatching
  -> trigger_pipeline()   -> status=dispatched (+ run id)
      on error            -> status=failed (+ error)
```

- `dedupe_key` is uniquely indexed, so a slot can never be materialised twice.
- Items abandoned in `dispatching` are requeued after
  `PIPLY_QUEUE_DISPATCH_STALE_SECONDS`.
- Only one run per pipeline is dispatched per drain pass, which preserves
  per-pipeline ordering.

---

## 4. Pipeline Run Lifecycle

```text
trigger_pipeline()
  |
  +-- resolve the definition
  |     +-- apply inherited variables and env (downstream runs)
  |     +-- apply manual command overrides
  |
  +-- capture the run configuration snapshot   # variables, env, context,
  |                                            # selectors, tenant, settings
  +-- create run (queued) + one queued task row per task
  |
  v
LocalEngine.dispatch()
  |
  +-- mark_running, start heartbeat, arm the pipeline timeout watchdog
  +-- execute sequentially or in parallel
  +-- capture outputs and artifacts per successful task
  |
  v
finish_run(status)
  |
  +-- success  -> enqueue downstream triggers (with variables, env, outputs)
  +-- failure  -> apply the retry policy
  +-- timeout  -> mark unfinished tasks timed_out
```

### Terminal statuses

| Outcome | Run status |
| --- | --- |
| all tasks acceptable | `success` |
| any task failed | `failed` |
| a task or the pipeline timed out | `timed_out` |
| a user cancelled it | `cancelled` |
| the owning process stopped | `interrupted` |

---

## 5. Task Lifecycle

```text
queued
  |
  +-- run_if false        -> skipped
  +-- enabled: false      -> skipped
  +-- dependency not ok   -> skip | fail | continue   (on_upstream_failure)
  |
  v
running   (mark_task_running, run heartbeat keeps ticking)
  |
  +-- exit 0              -> success  -> capture output + artifacts
  +-- exit != 0           -> failed
  +-- timeout elapsed     -> terminate, wait kill_grace_period, kill -> timed_out
  +-- cancelled           -> cancelled
  +-- process died        -> interrupted (by reconciliation)
```

### Selection order

Within a run the engine repeatedly picks from the tasks whose dependencies have
all resolved, ordered by:

1. dependency readiness (always first),
2. highest `priority`,
3. declaration order,
4. task id.

Dependency order can never be overridden by priority.

### Log attribution

Every line the runner emits carries the task id, and every rendered log line
shows the pipeline and task name — CLI, UI, and the log API alike. Only
genuinely pipeline-scoped messages (start, finish, downstream trigger) are
attributed to `pipeline`.

---

## 6. Concurrency and isolation

Two runs *can* overlap. Nothing serialises a pipeline against itself: trigger
`extract_flow` twice and you get two runs, both executing, both in the same
Piply process on different threads. The same is true of a manual run that starts
while a scheduled one is still going.

What that means for your code depends entirely on the task type.

| Task style | Isolation |
| --- | --- |
| `type: cli` | **Separate OS process.** Fully isolated. |
| `type: python` with `path:` only | **Separate OS process.** Fully isolated. |
| `type: python` with `function:` | **Same process, separate thread.** Partially isolated — see below. |

### What is isolated for a `function:` task

The task's own module is re-executed for every run, so its module-level state is
fresh each time. Two concurrent runs of the same task each get their own module
object; a global defined in that file is **not** shared.

### What is not

Anything the task **imports** is cached by Python in `sys.modules` and is shared
across every run in the process. A helper module holding a singleton is the
common case:

```python
# browser.py — SHARED between concurrent runs
_browser = None

def get_browser():
    global _browser
    if _browser is None:
        _browser = sync_playwright().start().chromium.launch()
    return _browser
```

Two overlapping runs will get the *same* browser here, and the second to start
will overwrite whatever the first stored. Verified behaviour: run A saw run B's
value by the time it finished.

### If you drive a browser, use a subprocess

Playwright's sync API is explicitly not thread-safe, and a browser session is
exactly the kind of resource that must not be shared. The reliable answer is to
give each run its own process:

```yaml
tasks:
  scrape:
    type: python
    path: scrape.py          # no `function:` — runs as a subprocess
```

or

```yaml
tasks:
  scrape:
    type: cli
    command: python scrape.py --tenant {tenant}
```

Both give full OS-level isolation: separate interpreter, separate module cache,
separate browser. Output still streams to the run log line by line, so nothing
is lost by choosing this.

### If you must use `function:`

Create the resource **inside** the function rather than in an imported module,
so its lifetime matches the run:

```python
def scrape():
    with sync_playwright() as playwright:     # per-run, not module-level
        browser = playwright.chromium.launch()
        ...
```

And keep per-run files apart — write to a directory named after the run rather
than a fixed path. `PIPLY_RUN_ID` and `PIPLY_TASK_ID` are set in the environment
for subprocess tasks.

### Preventing overlap entirely

There is no built-in "only one run at a time" setting yet. Until there is, the
options are to lengthen the schedule interval, or to take a lock in your own
code — a lock file, or an advisory lock in the database you are loading into.

---

## 7. Retry Lifecycle

Two modes, available automatically via the pipeline `retry` policy and manually
from the UI, API, and CLI.

```text
startover                       resume
---------                       ------
re-run every task               reuse successful tasks from the source run
                                re-run the failed task and everything after it
```

```text
run fails
  |
  +-- retry policy enabled and attempts remaining?
  |     no  -> log "retry policy exhausted", stop
  |     yes -> enqueue a retry trigger (after delay_seconds)
  |
  v
retry_run(mode, task_id)
  |
  +-- load the source run's configuration snapshot
  +-- re-apply its inherited variables, env, and context
  +-- build the retry plan (which tasks to reuse)
  +-- create a new run linked by retry_of / retry_mode / retry_task_id
```

Retry depth is computed by walking the `retry_of` chain, so the policy's
`attempts` limit holds across generations.

Because the retry replays the *stored* configuration rather than re-deriving it,
a downstream pipeline can be repaired on its own. Re-running the upstream
pipeline is no longer required just to recover its variables.

---

## 8. Backfill

Two distinct operations share the name.

**Replay one run** — re-execute a historic run with the exact configuration it
captured:

```bash
piply backfill <run-id>
```

```http
POST /api/runs/{run_id}/backfill
```

If the source run was task-scoped, the replay is task-scoped too.

**Fill a schedule window** — queue one run per scheduled slot in a past range:

```bash
piply backfill nightly_report --from 2026-07-01T00:00:00 --to 2026-07-08T00:00:00
```

```http
POST /api/pipelines/{pipeline_id}/backfill
{"start": "...", "end": "...", "limit": 200}
```

Slots are enqueued rather than executed inline, so the normal concurrency and
ordering rules still apply. The slot dedupe key prevents double materialisation
of a slot that already ran.

---

## 9. Recovery Process

Piply treats "no orphaned RUNNING rows" as an invariant. Three mechanisms
enforce it.

### Graceful shutdown

Triggered by the API lifespan shutdown, `piply stop`, or Ctrl+C during a
foreground CLI run.

```text
prepare_for_shutdown()          # stop accepting new work first
  -> runtime_accepting_work = false
  -> new trigger_* calls raise ValueError
shutdown_runtime()
  -> cancel each active run in the engine
  -> running tasks   -> interrupted
  -> queued tasks    -> cancelled
  -> the run itself  -> interrupted
```

### Startup recovery

Every `PipelineService` construction, and every `PipelineScheduler.start()`,
runs `recover_interrupted_executions()`:

```text
for each queued/running run:
    owner process still alive?
        yes -> leave it alone      # another live process owns it
        no  -> mark interrupted    # crash, kill, or power loss
```

Ownership is recorded as `runs.owner_pid` at creation time. Liveness is checked
per platform — on Windows through `OpenProcess` plus `GetExitCodeProcess`,
because `os.kill` there would terminate the target rather than probe it, and a
terminated-but-unreaped process stays openable.

This is what makes recovery safe when two services share one database inside a
single process, as the API and a CLI invocation can.

### Heartbeat reconciliation

A run's heartbeat is refreshed every `PIPLY_HEARTBEAT_INTERVAL_SECONDS` while it
executes. Anything silent for longer than `PIPLY_STALE_RUN_TIMEOUT_SECONDS` is
marked `interrupted`. Full scans are rate-limited to one per
`PIPLY_RECONCILE_INTERVAL_SECONDS`; reading a single run reconciles just that
row, which stays accurate without the table scan.

### Covered failure modes

| Failure | Handled by | Test |
| --- | --- | --- |
| Ctrl+C during a foreground run | graceful shutdown | `test_ctrl_c_marks_active_runs_interrupted` |
| new work during shutdown | accepting-work guard | `test_shutdown_rejects_new_work` |
| scheduler restart | startup recovery on `start()` | `test_scheduler_restart_reconciles_previous_owner` |
| scheduler thread crash | crash state + recorded error | `test_scheduler_marks_itself_crashed_when_tick_raises` |
| process killed outright | owner-pid recovery | `test_unexpected_termination_is_recovered_on_next_start` |
| stalled run, live process | heartbeat reconciliation | `test_service_reconciles_stale_running_runs` |
| concurrent service, same process | liveness check | `test_startup_leaves_runs_owned_by_a_live_process_alone` |

---

## 10. Retention

`piply prune` removes history beyond the configured window and reclaims disk.

```text
select expired runs
  |
  +-- finished runs older than run_retention_days
  +-- runs beyond max_runs_per_pipeline (newest kept)
  |
  v
delete logs, artifacts records, outputs, task rows, run rows
delete log lines older than log_retention_days
delete settled queue items
VACUUM
```

Active runs are never removed. `--dry-run` reports what would go without
deleting anything, and the Diagnostics page exposes the same preview.
