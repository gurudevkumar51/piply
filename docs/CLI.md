# Command Line Reference

Every `piply` command, what it is for, and the flags that matter.

`--config / -c` is accepted by every command except `init`. Without it Piply
looks for `piply.yaml`, then `piply.yml`, then `piply-demo/piply.yaml`, starting
in the working directory.

---

## Contents

1. [Getting started](#1-getting-started)
2. [Running pipelines](#2-running-pipelines)
3. [Watching what happened](#3-watching-what-happened)
4. [The server](#4-the-server)
5. [Tasks](#5-tasks)
6. [Accounts](#6-accounts)
7. [Database and housekeeping](#7-database-and-housekeeping)
8. [Exit codes and scripting](#8-exit-codes-and-scripting)

---

## 1. Getting started

### `piply init [DIRECTORY]`

Scaffolds a project — `piply.yaml`, a workspace folder, and a sample pipeline.

| Flag | Purpose |
| --- | --- |
| `--force` | Overwrite files that already exist |

```bash
piply init my-project
```

### `piply validate`

Loads the config and reports what it found, or the first error with the file and
key that caused it. Runs nothing.

```bash
piply validate
```

```
Config: /srv/etl/piply.yaml
Project: Claims Platform
Pipelines: 4
  - claim_extract: 3 tasks | triggers ['claim_load']
```

Use it in CI and in a pre-commit hook — it is the cheapest way to catch a typo
before the scheduler finds it at 2am.

### `piply list`

One line per pipeline: id, schedule, task count, and enabled state.

---

## 2. Running pipelines

### `piply run PIPELINE_ID`

Runs a pipeline now.

| Flag | Purpose |
| --- | --- |
| `--param KEY=VALUE` | Run parameter. Repeatable; JSON values accepted |
| `--var NAME=VALUE` | Supply a `{placeholder}` the config leaves to run time. Repeatable |
| `--prompt` | Ask for any `{placeholder}` still missing rather than running without it |
| `--tenant TEXT` | Tenant id to attach to the run |
| `--wait` / `--detach` | Stream logs in the terminal, or return immediately. Default `--wait` |

```bash
piply run claim_extract --var run_date=2026-09-01 --param retries=3
piply run claim_extract --detach          # queue it and come back later
```

`--prompt` is the one to reach for when a pipeline is written to be filled in at
run time; without it, an unfilled `{placeholder}` reaches the command literally.

### `piply plan [PIPELINE_ID]`

Shows what a run *would* do — stages, resolved variables, and the exact command
each task would execute — without executing anything. Omit the id to preview
every pipeline.

| Flag | Purpose |
| --- | --- |
| `--param KEY=VALUE` | Preview with this parameter. Repeatable |
| `--tenant TEXT` | Resolve variables against this tenant |
| `--json` | Emit the preview as JSON |

```bash
piply plan claim_extract --tenant ecw
```

This is the fastest way to answer "why is my variable not being substituted" —
the preview shows the command after interpolation.

### `piply backfill TARGET`

Two jobs in one command:

```bash
piply backfill a1b2c3d4e5f6                          # replay one historic run
piply backfill claim_extract --from 2026-09-01 --to 2026-09-07   # queue every scheduled slot
```

Replaying a run reuses **the exact configuration that run captured**, so a
downstream pipeline can be re-run without re-running the upstream chain.

`--from` / `--to` accept `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SS`, or
`YYYY-MM-DD HH:MM:SS`.

### `piply pause PIPELINE_ID` / `piply resume PIPELINE_ID`

Stops and restarts scheduling for one pipeline without editing YAML. Manual runs
still work while paused. The state lives in the database, so it survives a
restart — which also means a paused pipeline stays paused until someone resumes
it.

---

## 3. Watching what happened

### `piply runs`

Recent runs, newest first.

| Flag | Purpose |
| --- | --- |
| `--limit N` | How many to show. Default 20 |

### `piply logs [RUN_ID]`

Prints a run's logs. Omit the run id to follow logs across every run as they
arrive — the closest thing to `tail -f` for the whole project.

| Flag | Purpose |
| --- | --- |
| `--follow` / `-f` | Stream new lines as they are written |
| `--pipeline TEXT` | Only lines from this pipeline |
| `--task TEXT` | Only lines from this task |
| `--limit N` | Recent lines to print before following. Default 200 |
| `--color` / `--no-color` | Colourise task name and stream. Default on |
| `--interval FLOAT` | Seconds between polls |

```bash
piply logs -f --pipeline claim_extract      # follow one pipeline
piply logs a1b2c3d4e5f6                     # everything one run printed
```

### `piply artifacts RUN_ID`

Lists the files a run produced — name, size, and path. See
[Artifacts](YAML_SPECIFICATION.md#6-tasks) for how a task declares them.

| Flag | Purpose |
| --- | --- |
| `--task TEXT` | Only artifacts from this task |

### `piply diagnostics`

Scheduler, worker, sensor, and reconciliation health, plus the resolved config
path and database location. The first command to run when something looks stuck.

| Flag | Purpose |
| --- | --- |
| `--json` | Emit the payload as JSON |

---

## 4. The server

### `piply start`

Starts the web UI and the scheduler.

| Flag | Purpose |
| --- | --- |
| `--host TEXT` | Bind address. Default `127.0.0.1` |
| `--port INTEGER` | Bind port. Default `8000` |
| `--reload` | Auto-reload on code changes (development only) |
| `--detach` / `-d` | Run in the background, logging to `.piply/server.log` |

```bash
piply start                          # foreground, http://127.0.0.1:8000
piply start --host 0.0.0.0 --detach  # background, reachable on the network
```

Binding to `0.0.0.0` exposes Piply to your network — turn authentication on
first, see [Authentication](AUTHENTICATION.md).

Set `PIPLY_SCHEDULER_ENABLED=false` to start the UI **without** the scheduler, so
opening a project on a laptop does not fire whatever was due overnight.

Successful polls from the UI's own scheduler chip are filtered out of the access
log, so what you see in the terminal is pipeline activity rather than a wall of
identical `200`s.

### `piply stop`

Asks a backgrounded server to shut down gracefully. It exits within a few
seconds, after in-flight tasks finish.

---

## 5. Tasks

### `piply tasks list PIPELINE_ID`

Every task in a pipeline, with its type and dependencies.

### `piply tasks run PIPELINE_ID TASK_ID`

Runs **one task** in its own run, without the rest of the pipeline. Useful for
developing a single step.

| Flag | Purpose |
| --- | --- |
| `--param KEY=VALUE` | Run parameter. Repeatable; JSON accepted |
| `--tenant TEXT` | Tenant id for this task-scoped run |
| `--wait` / `--detach` | Stream logs, or return immediately |

### `piply tasks retry RUN_ID TASK_ID`

Retries a failed run **from** a given task.

| Flag | Purpose |
| --- | --- |
| `--mode resume` | Re-run this task and everything downstream. Default |
| `--mode startover` | Re-run the whole pipeline from the beginning |
| `--wait` / `--detach` | Stream logs, or return immediately |

```bash
piply tasks retry a1b2c3d4e5f6 transform_step
```

`resume` is what you almost always want: the upstream tasks already succeeded
and re-running them wastes time and may not be idempotent.

---

## 6. Accounts

Creating the first account **switches authentication on** for the install. Until
then Piply is open to anyone who can reach the port.

### `piply users create USERNAME`

| Flag | Purpose |
| --- | --- |
| `--password TEXT` | Password. Generated and printed when omitted |
| `--role TEXT` | `admin` or `user`. Default `user` |
| `--grant PIPELINE=actions` | Grant permissions at creation. Repeatable |

```bash
piply users create alice --role admin
piply users create bob --grant "claim_extract=view,run"
```

Omitting `--password` is the better habit — Piply generates a strong one and
prints it once, and it never reaches your shell history.

### `piply users grant USERNAME PIPELINE_ID ACTIONS`

`ACTIONS` is a comma-separated list of `view`, `edit`, `run`, or `all`.
`PIPELINE_ID` may be `*` for every pipeline.

```bash
piply users grant bob "*" view
piply users grant bob claim_extract view,run
```

### The rest

| Command | Purpose |
| --- | --- |
| `piply users list` | Accounts and the permissions they hold |
| `piply users passwd USERNAME` | Set a new password |
| `piply users revoke USERNAME PIPELINE_ID` | Remove every permission on one pipeline |
| `piply users disable USERNAME` | Disable without deleting |
| `piply users delete USERNAME` | Delete the account and its permissions |

Full model in [Authentication](AUTHENTICATION.md).

---

## 7. Database and housekeeping

### `piply backup DESTINATION`

Snapshots the runtime database **while Piply keeps running** — it uses SQLite's
online backup API, so it is safe against a live server and an active
write-ahead log. `DESTINATION` may be a file or a directory to write a
timestamped file into.

```bash
piply backup /backups                    # /backups/piply-20260908-0300.db
piply backup /backups/before-deploy.db
```

### `piply restore SOURCE`

Replaces the runtime database with a backup.

**Stop the server first.** Restoring under a running scheduler leaves it holding
handles to a database that is about to be replaced.

| Flag | Purpose |
| --- | --- |
| `--yes` / `-y` | Skip the confirmation prompt |

### `piply migrate-db --to DSN`

Copies the runtime database to another backend, usually SQLite → PostgreSQL. Run
history, logs, artifacts, accounts, and schedules all move with their original
ids, so retry chains and downstream links stay intact.

The target must be empty, and the server must be stopped — migrating while runs
execute copies a moving target.

See [Metadata Store](DATABASE.md) for the full procedure.

### `piply prune`

Deletes history beyond the retention window.

| Flag | Purpose |
| --- | --- |
| `--run-days N` | Delete finished runs older than N days |
| `--log-days N` | Delete log lines older than N days |
| `--max-runs N` | Keep at most N runs per pipeline |
| `--dry-run` | Report what would be removed, delete nothing |
| `--vacuum` / `--no-vacuum` | Run SQLite `VACUUM` afterwards. Default on |
| `--yes` / `-y` | Skip the confirmation prompt |

```bash
piply prune --dry-run            # always look first
piply prune --run-days 90 --yes  # then in a cron job
```

Chatty tasks make the log table the largest thing in the database. `--dry-run`
costs nothing and tells you which knob is the one that matters.

---

## 8. Exit codes and scripting

`piply validate` and `piply run` exit non-zero on failure, so they compose with
`&&` and work as CI gates:

```bash
piply validate && piply run claim_extract --wait
```

`--json` on `piply plan` and `piply diagnostics` emits machine-readable output
for scripting:

```bash
piply diagnostics --json | jq '.scheduler.running'
piply plan claim_extract --json | jq '.tasks[].command'
```

For anything the CLI does not cover, the same operations are available over
HTTP — see the [API Reference](API.md).

---

## Related

- [YAML Specification](YAML_SPECIFICATION.md) — every config key
- [Variables](VARIABLES.md) — what `--var` and `--param` interact with
- [Environment Variables](ENVIRONMENT.md) — `PIPLY_*` settings that change CLI behaviour
- [UI Guide](UI_GUIDE.md) — the same operations in the browser
- [API Reference](API.md) — the same operations over HTTP
