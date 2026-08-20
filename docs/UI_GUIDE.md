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
| Diagnostics | `/diagnostics` | Is the runtime itself healthy? **Admins only.** |
| Settings | `/settings` | What configuration is active? |

---

## Pipelines

An Airflow-style listing rather than a wall of cards.

**Rows** show a status dot, the title and id, deployment/tag chips, upstream
(`←`) and downstream (`→`) pipelines, the last five runs, schedule, last run,
next run, task count, and state.

**Run history dots** — the last five runs, oldest on the left, newest on the
right, coloured by status. Each dot is a link: click it to open that run. Hover
or focus for the status, how long ago it ran, its duration, and its run id. A
pipeline with fewer than five runs shows dashed placeholders so the dots stay
aligned down the page; one with none shows *no runs yet*.

The number of dots is configurable with `PIPLY_PIPELINE_RUN_HISTORY_COUNT`
(default 5, maximum 20).

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
2. **Metadata strip** — collapsible. Collapsed by default, leaving a one-line
   digest of next run, schedule, task count, and execution mode. Expanded it
   adds retry policy, timeout, concurrency, template/deployment, upstream and
   downstream pipelines, tags, primary entry, and the resolved variables.
3. **Task graph** — the DAG, with the task inspector beside it. *Hide task
   focus* collapses the inspector; the graph then takes the full width **and**
   grows to 560px tall, so collapsing both panels turns the page into a graph
   view. Both choices persist per browser.
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

## Runs

**Filters** across the top, all server-side so they work beyond the page limit
and are shareable as a URL: pipeline, status (including a *Failure-like* preset
covering failed, timed out, and interrupted), trigger type, and a from/to date
range. **Sort** by newest, oldest, longest, shortest, pipeline name, status, or
trigger. **Rows** picks 50 / 100 / 200 / 500.

Every control submits on change; *Reset* clears them.

**Trigger & lineage** is the column that answers "why did this run?". A coloured
chip names the trigger — `manual` blue, `schedule` teal, `pipeline` violet,
`sensor` green, `retry` amber — so the three kinds are distinguishable at a
glance.

For a pipeline-triggered run, the full chain is shown beneath the chip:

```
pipeline   ACME_ETL → silver → gold → semantic
```

Every step is a link to that run, coloured by its status, so you can walk back
up a multi-level chain without leaving the page. A step whose run has since been
pruned is shown dashed and greyed rather than being silently dropped.

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

**Administrators only.** The payload names filesystem paths, the config
location, the process id, and the metadata store, none of which a delegated
pipeline operator needs. Non-admins get a 403 rather than a filtered page.

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

## Sign in

With no accounts, there is no login page and everything is permitted. Once the
first account exists, `/login` appears and every page requires a session.

The header shows the signed-in username next to *Logout*.

**Settings → Users and permissions** (admins only) creates accounts, grants
pipeline actions, and deletes accounts. **Settings → Email (SMTP)** configures
the mail server once for the whole install, with a *Send test email* button.

A non-admin only sees the pipelines they were granted — on the Pipelines page,
the Runs page, and the API alike. Actions they lack permission for return 403
rather than being hidden and then failing.

See [AUTHENTICATION.md](AUTHENTICATION.md) for roles, grants, and the CLI.

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
