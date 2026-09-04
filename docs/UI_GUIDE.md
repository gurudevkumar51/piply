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

## First-run setup

A brand-new install opens `/setup` instead of the dashboard, asking where Piply
should keep its own data: a SQLite file or PostgreSQL. Piply connects before
saving, so a wrong host or an unreachable server is reported on the page rather
than at the next restart. The choice is written to `.env` and applied straight
away — no restart.

No pipelines run while this page is pending — the scheduler starts once you
have chosen, so nothing is written into a database you might replace.

You will not see this page if `PIPLY_DATABASE` is already set, or if the default
database already holds runs or accounts, so existing installs are unaffected.
Once configured, the page redirects away and cannot be used to repoint a running
system — changing the database later is an admin action under
[Settings](#database).

### Creating the first admin

Step 2 is optional: create the first administrator. Piply is open to anyone who
can reach it until an account exists, and creating one switches sign-in on for
everybody, so do this before putting Piply on a shared network. You are signed in
as the new account immediately — there is no separate sign-in step.

Skipping leaves authentication off, which is fine on a laptop. The step closes
permanently once any account exists.

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

### Collapsing template groups

With one template deployed per tenant, a single group can hold a row per tenant
and the page becomes a wall of near-identical entries. Click a group heading to
collapse it, or use *Expand all* / *Collapse all*.

A collapsed group keeps the information you scan for:

```
▸ Template: ECW_Extract   9        2 running   1 failed
▸ Template: Monthly_Flow  9                    1 paused
▾ Standalone pipelines    4
```

- The count pill and the running / failed / paused chips stay visible, so a
  group that needs attention is obvious while closed.
- Each group is remembered separately, and the state persists per browser.
- **Searching temporarily opens every group**, so a match can never hide behind
  a collapsed heading. Clearing the search restores what you had collapsed.
- The controls disappear when *Group by template* is off, since there are no
  groups to act on.
- Headings are real buttons, so they work by keyboard and with a screen reader.

### Missing runtime values

Some pipelines cannot resolve on their own. A downstream pipeline normally
receives `{practice}` from the deployment that triggers it, so starting it by
hand leaves that placeholder with no value.

Rather than running `dbt --target {practice}` literally, *Run* asks first:

```
Missing runtime values
Bronze_to_Silver

  These are normally supplied by BENNETT_ETL when it triggers this
  pipeline. A manual run has to provide them.

  practice      [____________________]  used by dbt
  batch_id      [____________________]  used by dbt
  report_date   [____________________]  used by report

                              [Cancel]  [Run Pipeline]
```

- Each field names the tasks that use it, so you can tell what a value affects.
- Every field is required. A blank one is highlighted and the dialog stays open.
- *Cancel*, `Esc`, or clicking outside all close it **without creating a run**.
- Pipelines that resolve on their own never show this — they run immediately.

The values are stored with the run, so a later **retry** or **backfill** reuses
them instead of asking again.

This appears wherever a run starts: the pipelines list, the dashboard, and the
pipeline detail page. Running a single task asks only for the values that task
actually uses.

**This is not an error dialog.** A genuine configuration problem — a missing
script, an unknown task type, a dependency cycle — is rejected when the config
loads, so those pipelines never reach the point of being runnable. Missing
runtime values are a normal, expected part of running a downstream pipeline by
hand.

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

### The task graph

The graph uses the **full width** of the page. Clicking any task node opens the
task panel beside it, showing the task id, type, status, duration, log count,
dependencies, and the resolved command, plus actions — run just that task, copy
the command, or filter the logs to it. Closing the panel returns the graph to
full width, and the choice is remembered per browser.

### Long task names

Entity expansion produces names like `payer_claim_status_dashboard / Load Bronze`,
which are wider than a node. Rather than letting them spill across the box, the
graph shortens them **from the middle** and keeps the full value on hover:

```
payer_claim_statu…ard / Load Bronze
```

The middle is dropped because both ends carry meaning — the entity distinguishes
`payer_` from `patient_`, and the suffix says which task it is. Nodes are sized
so the status line (`queued | priority 1 | timeout 300s`) always fits and most
names survive intact; only the longest are shortened.

The task-focus panel always shows the full, untruncated name.

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

A run started by a person also records **who**: the run page shows
`triggered via manual by alice`, and the same line goes to the server log along
with pauses and resumes.

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

While a run is `queued` or `running` the page refreshes itself every three
seconds, so logs appear as the task produces them — including `type: python`
tasks that call a `function:`. A long task shows progress rather than going
silent until it finishes. `piply logs --follow` tails the same output from a
terminal.

Both `print()` and the `logging` module are captured, including tracebacks from
`log.exception()`. Log records are recorded with their level, so a run log shows
`INFO Extracted 1200 rows` rather than a bare message. Task output goes to the
run log rather than the server console, which is how `type: cli` tasks have
always behaved.

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

### Why a downstream pipeline has not started

The downstream panel used to report every not-yet-started pipeline as
*pending*, which hid the ones that were never going to start on their own. It
now names the actual state, with a one-line reason:

| Chip | Meaning |
| --- | --- |
| `waiting` | This run has not finished yet |
| `skipped` | This run did not succeed, so nothing was triggered |
| `queued` | Trigger is queued; the reason it has not dispatched is shown |
| `paused` | **Will not run** until the pipeline is resumed |
| `disabled` | **Will not run**: disabled in the config |
| `unknown` | The target is no longer defined in the config |
| a run status | It ran; the chip shows how it went |

A queued chip carries the scheduler's own reason, such as *a run is already in
progress* — the same text written to the server log.

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

A read-only view of the effective runtime configuration — config and database
paths, auth state, worker defaults, heartbeat and poll intervals, queue settings,
and retention — plus the admin-only panels below.

### Database

Admins can move Piply's own metadata store without editing files or restarting.
Pick SQLite or PostgreSQL, give the path or connection URL, and press **Test and
switch**. Piply opens the target before saving, so a wrong value is refused on
the page rather than at the next restart.

Leave **Copy the current data across** ticked to bring runs, logs, and accounts
with you. The old database is never deleted, so it stays available as a rollback,
and copying refuses a target that already holds data rather than merging two
histories.

Switching is refused while any run is in progress, because an in-flight run
would be stranded between the two databases. Wait for it, or pause the schedules.

The panel is hidden when `PIPLY_DATABASE` is set as a real environment variable,
because the process environment overrides `.env` and the change could not take
effect. See [Metadata Store](DATABASE.md#52-changing-the-database-later).

### Alerts

Admin-only. Shows every declared Teams destination, whether its webhook
resolved, which pipelines use it (resolved through groups), and a log of recent
delivery attempts with the reason for each failure. **Send test** posts a card
immediately so a webhook can be verified without waiting for a run.

Destinations are declared in YAML rather than here, because a webhook URL is a
credential. See [Notifications](NOTIFICATIONS.md).

### Email and accounts

SMTP settings and account administration are also admin-only. Non-admins see the
runtime configuration alone. Teams destinations are declared in YAML rather than
here — see [Notifications](NOTIFICATIONS.md).

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
