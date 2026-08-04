# Piply UI Guide

Seven pages, one job each. Start the server and open `http://127.0.0.1:8000`.

```bash
piply start --config piply.yaml
```

| Page | Path | Answers |
| --- | --- | --- |
| Dashboard | `/` | Is anything broken right now? |
| Pipelines | `/pipelines` | What exists, what runs next? |
| Pipeline detail | `/pipelines/{id}` | What does this pipeline do? |
| Runs | `/runs` | What ran recently? |
| Run detail | `/runs/{id}` | Why did this run behave that way? |
| Grid | `/execution-matrix` | Which task is flaky over time? |
| Logs | `/logs` | Where did that message come from? |
| Diagnostics | `/diagnostics` | Is the runtime itself healthy? |
| Settings | `/settings` | What configuration is active? |

---

## Pipelines

An Airflow-style listing rather than a wall of cards.

**Rows** show a status dot, the title and id, deployment/tag chips, upstream
(`←`) and downstream (`→`) pipelines, schedule, last run, next run, task count,
and state.

**Grouping** — deployments of the same template are grouped under a
`Template: <name>` heading, so ten tenants of one workflow read as one block
instead of ten alphabetically scattered rows. Toggle it off with *Group by
template* to get a flat list.

**Sort** — *Name*, *Upcoming run* (default; pipelines without a schedule sort
last), or *Last run*.

**Filter** — *All*, *Running*, *Failed*, *Scheduled*, *Paused*, plus a free-text
box that matches the id, title, description, template, deployment, and tags.

Sort and filter choices persist in local storage.

**Row actions** — *Run* triggers immediately, *Preview* opens the dry-run drawer
on the detail page, *Pause* / *Resume* toggles the schedule.

---

## Pipeline detail

Laid out so the DAG is the first substantial thing on screen.

1. **Header** — title, id, last-run status, and the actions: Run now, Preview,
   Pause/Resume, Delete.
2. **Metadata strip** — one merged row: next run, schedule, task count,
   execution mode, retry policy, timeout, concurrency, template/deployment,
   upstream and downstream pipelines, tags, primary entry, and the resolved
   variables. This replaces the former hero block plus five-card grid.
3. **Task graph** — the DAG, with the task inspector beside it.
4. **Manual command overrides** — edit a CLI command for one manual run.
5. **Tasks** and **Recent runs**.

### Execution preview

*Preview* opens a drawer showing exactly what a run would do, without running
anything:

- resolved variables,
- expanded entities,
- execution order grouped into stages,
- per task: run/skip decision, priority, timeout, `run_if`, artifact globs, and
  the fully interpolated command,
- warnings, including any command still holding an unresolved placeholder.

*Run it* launches the pipeline straight from the drawer. `piply plan` prints the
same information in a terminal.

---

## Run detail

**Header** — status, pipeline link, duration, task counts, exit code, retry
lineage, and the upstream run when this run was triggered by another pipeline.

Actions: *Cancel* while active; otherwise *Re-Run*, *Replay config*, *Delete*.
*Replay config* re-executes the run with the exact configuration it captured,
which is how a downstream run gets repaired without re-running its upstream.

**Run graph** — task nodes coloured by status, each showing status, priority,
timeout, and a `conditional` marker for `run_if` tasks.

Downstream pipelines appear as dashed nodes attached to the terminal tasks, with
their own execution status. Clicking one opens its run, or its pipeline page if
it has not been dispatched yet. *Downstream on/off* hides them.

Controls: zoom, center, and three layouts (Flow, Stage, Focus). Focus dims
everything outside the selected node's lineage. The graph keeps a usable height
on narrow screens instead of collapsing.

**Task focus** — the side panel for the selected task: type, status, duration,
log count, dependencies, command, output preview, and the actions *Filter logs*,
*Copy command*, *Resume from here*, and *Re-Run*.

**Downstream pipelines** — a table of triggered pipelines with status, start
time, and task counts.

**Artifacts** — files the run declared and produced, with size and a download
link. Downloads are restricted to paths the run actually recorded, inside the
workspace, the config directory, or `PIPLY_ARTIFACTS_DIR`.

**Raw logs** — newest first, filterable to one task. Every line shows its task.

---

## Diagnostics

Runtime health, refreshed every five seconds.

- **Counters** — running runs, running tasks, queued triggers, due triggers,
  failing sensors.
- **Scheduler** — state, heartbeat and its age, start time, owning pid and
  whether that process is alive, whether new work is being accepted, last error.
- **Reconciliation** — last startup recovery, how many runs it recovered, the
  stale-run timeout, the reconcile interval, and the last sensor error.
- **Running tasks** — every executing task with elapsed time, priority, timeout,
  and owning pid.
- **Sensors** — per-sensor status, last poll, poll and event counts, consecutive
  failures, and the latest error text.
- **Storage** — database path, size, retention window, and a *Preview prune*
  button that reports what `piply prune` would delete.

*Prometheus metrics* links to `/metrics`.

---

## Logs

Cross-run search filtered by text, pipeline, and task. Each line shows its
timestamp, run, task, and message.

For live output use the CLI, which colours the task name and follows new lines:

```bash
piply logs --follow
piply logs --follow --pipeline nightly_report
piply logs --follow --task extract
piply logs <run-id> --follow
```

The same data is available at `GET /api/logs/stream` with an `after` cursor.

---

## Grid (execution matrix)

Tasks as rows, recent runs as columns, one status cell per intersection. It is
the fastest way to spot a task that fails intermittently. Filter by pipeline,
tenant, status, and date range; click a cell for that task run's detail.

---

## Dashboard

Project header, run counters and success rate, active pipelines, recent runs,
recent failures, a run-duration trend, and the scheduler chip.

---

## Settings

Read-only view of the effective runtime configuration: config and database
paths, auth state, worker defaults, heartbeat and poll intervals, queue
settings, and retention.

---

## Status colours

| Colour | Statuses |
| --- | --- |
| green | `success` |
| blue, pulsing | `running` |
| red | `failed`, `timed_out` |
| amber | `queued`, `interrupted` |
| violet | `cancelled` |
| grey | `skipped`, `pending` |

The scheduler chip in the header polls every five seconds and shows *scheduler
live*, *not responding*, *crashed*, or *offline*.

---

## Authentication

Auth is off by default. Enable it with a username and password for the UI, or an
API token for programmatic access:

```bash
PIPLY_AUTH_USERNAME=ops
PIPLY_AUTH_PASSWORD=change-me
PIPLY_API_TOKEN=long-random-token
```

The UI uses HTTP Basic; `/api/*` accepts Basic or `Authorization: Bearer`.
`/metrics` accepts the bearer token too, so a Prometheus scraper does not need
UI credentials. `/logout` clears the browser's cached credentials.
