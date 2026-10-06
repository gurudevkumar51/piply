# Piply Runbook

For v0.3.4, follow the [release assessment](RELEASE_CHECK.md) before rollout.
The local admission gate, native PostgreSQL, and Windows compatibility checks
pass. Linux and remote compatibility CI are still required. Fresh-install graphs
no longer depend on a CDN.

This runbook is for operating and troubleshooting a Piply installation. It
assumes the config file is `piply.yaml`; pass `--config PATH` when using a
different file.

---

## First Checks

When something looks wrong, start here:

```bash
piply validate
piply diagnostics
piply runs --limit 20
piply logs --follow
```

In the UI:

- Dashboard: current failures, active pipelines, scheduler status.
- Runs: recent history and lineage.
- Run detail: task status, logs, artifacts, retry/replay actions.
- Diagnostics: scheduler, queue, running tasks, sensors, storage.
- Settings: effective config, database, alerts, SMTP, users.

---

## Start And Stop

### Start the server

```bash
piply start --config piply.yaml
```

Use another port when needed:

```bash
piply start --config piply.yaml --port 8080
```

For a background server:

```bash
piply start --config piply.yaml --detach
```

### Stop the server

```bash
piply stop --config piply.yaml
```

Graceful shutdown stops accepting new work, cancels active runtime work, and
marks unfinished runs interrupted rather than leaving them stuck as running.

### Start UI without schedules

On a development machine, prevent scheduled work from firing:

```bash
PIPLY_SCHEDULER_ENABLED=false piply start --config piply.yaml
```

Or put this in the local `.env`:

```text
PIPLY_SCHEDULER_ENABLED=false
```

Manual runs still work.

---

## Routine Operations

### Validate config

```bash
piply validate --config piply.yaml
```

Run this after YAML edits and before deployment. It catches duplicate ids,
missing tasks, dependency cycles, bad schedules, and invalid keys.

### Preview a run

```bash
piply plan PIPELINE_ID --config piply.yaml
```

Use this before running a changed pipeline. It shows resolved variables,
expanded entities, task order, conditions, and commands.

### Run a pipeline manually

```bash
piply run PIPELINE_ID --config piply.yaml --wait
```

If placeholders are normally provided by an upstream pipeline:

```bash
piply run PIPELINE_ID --config piply.yaml --prompt
```

Or pass values directly:

```bash
piply run PIPELINE_ID --config piply.yaml --var practice=BENNETT
```

### Pause or resume scheduling

```bash
piply pause PIPELINE_ID --config piply.yaml
piply resume PIPELINE_ID --config piply.yaml
```

This controls future scheduled dispatch. It does not cancel a currently running
run.

### Pause task starts in a live run

Use the Pause control on the run page or Runs list, or:

```bash
piply pause-run RUN_ID --config piply.yaml
piply resume-run RUN_ID --config piply.yaml
```

Active tasks finish normally. Remaining tasks wait; Resume continues the same
run. The run still occupies its concurrency slot, and pipeline timeout keeps
counting wall-clock time. Cancel works while paused. This control does not hold
future schedule slots; use `piply pause PIPELINE_ID` for that.

After a restart, normal interrupted-run recovery still applies. Resume cannot
revive execution in a stopped process; retry an interrupted run.

### Inspect who changed or ran something

Open **Audit** as an administrator, or use local database access:

```bash
piply audit --since 7d --config piply.yaml
piply audit --action run.cancel --json --config piply.yaml
piply audit --target-type run --target-id RUN_ID --config piply.yaml
```

User, permission, SMTP, notification-test, database, scheduling, and run-control
actions have identifier-only summaries. Entries survive run/account deletion
and move with database migration. Audit retention follows run-retention days;
check `audit_deleted` in a retention dry run before pruning. See
[Audit History](AUDIT.md) for attribution and durability limits.

### Follow logs

```bash
piply logs --follow --config piply.yaml
piply logs --follow --pipeline PIPELINE_ID --config piply.yaml
piply logs RUN_ID --config piply.yaml
```

### Inspect artifacts

```bash
piply artifacts RUN_ID --config piply.yaml
```

Artifacts are only downloadable when the recorded path is inside an allowed
root and was declared by the run.

---

## Common Incidents

### Pipelines or task graphs are blank on a new laptop

If the page heading and controls load but the pipeline list or graph does not,
check the browser console and Network tab. A message such as "Refused to
execute script ... because its MIME type ('text/plain') is not executable"
means the browser rejected a script; it does not mean the pipelines are missing.
An `escapeHtml is not defined` error can follow because `app.js` never ran.

Piply now serves bundled `.js` files as `text/javascript` and `.css` files as
`text/css`, regardless of the laptop's MIME mappings. Do not disable
`X-Content-Type-Options: nosniff` or change the browser's security settings.

1. Install a build containing the static-asset content-type fix into the same
   Conda environment used to start Piply. For a checkout containing the fix:

   ```powershell
   conda activate YOUR_ENV
   python -m pip install --force-reinstall .
   ```

   Run the install command from the updated Piply repository, not your pipeline
   project folder. Let installation resolve declared dependencies, including
   Windows timezone data; a published package upgrade only helps once the fix
   is released.

2. Stop and restart Piply in that environment, then hard-refresh the browser
   with `Ctrl+F5`.
3. In Network, confirm `/static/app.js` and `/static/dag.js` return HTTP 200 and
   `Content-Type: text/javascript; charset=utf-8`. If they still return
   `text/plain`, check that the server uses the updated environment and that any
   reverse proxy is not replacing response headers.
4. If the console instead reports a script HTTP 404, reinstall the package and
   check that the packaged `piply/ui/static` files exist.

A Google Fonts timeout is a separate appearance issue; it does not explain
blocked local scripts. If the graph reports "Graph library not available",
verify the local `/static/vendor/` dagre and graphlib scripts return HTTP 200
with JavaScript content types. Current graphs do not load a JavaScript CDN.

### Cron schedules report Unknown timezone, including UTC

Activate the environment used by the server and install the updated package.
Windows installs now declare `tzdata`; minimal POSIX images must provide OS
timezone data or install it separately with `python -m pip install tzdata`.
Then restart Piply and validate the config. Do not replace a real regional
timezone with an arbitrary UTC offset; daylight-saving rules would be lost.

### Scheduler is offline, stale, or crashed

1. Open Diagnostics or run:

   ```bash
   piply diagnostics --config piply.yaml
   ```

2. Check:

   - scheduler state and heartbeat age
   - owner pid and whether it is alive
   - last scheduler error
   - whether `PIPLY_SCHEDULER_ENABLED=false`
   - queue depth and failed queue items

3. If the server is meant to run schedules, restart it:

   ```bash
   piply stop --config piply.yaml
   piply start --config piply.yaml
   ```

4. If config load fails, fix YAML first and rerun `piply validate`.

### A run is stuck as running

1. Check diagnostics:

   ```bash
   piply diagnostics --config piply.yaml
   ```

2. Open the run detail page and check whether logs are still arriving.

3. If the run is active but should stop, cancel it from the UI or:

   ```bash
   curl -X POST http://127.0.0.1:8000/api/runs/RUN_ID/cancel
   ```

4. If Piply was killed or crashed, restart the server. Startup recovery should
   mark orphaned work interrupted.

5. If a Python `function:` task never stops, check whether user code is holding
   the process. Prefer subprocess-style tasks for long-running or browser-based
   work.

### A pipeline did not start on schedule

Check these in order:

1. Is the scheduler live?
2. Is the pipeline disabled in YAML?
3. Is the pipeline paused in the database?
4. Does the pipeline have a valid schedule?
5. Is another trigger already queued for the same slot?
6. Is the next run after the resume floor from a previous pause?
7. Did config reload fail?

Helpful commands:

```bash
piply list --config piply.yaml
piply diagnostics --config piply.yaml
piply logs --follow --config piply.yaml
```

### A downstream pipeline did not start

Open the upstream run detail page and inspect the downstream panel. It should
show whether the downstream run is waiting, skipped, queued, paused, disabled,
unknown, or already dispatched.

Common causes:

- upstream run has not finished yet
- upstream run failed and only success triggers are configured
- downstream pipeline is paused
- downstream pipeline is disabled
- target pipeline id changed or no longer exists
- queued trigger has not been dispatched yet

### A manual downstream run asks for missing values

This is expected when values are normally inherited from an upstream deployment.

Use the UI prompt, or:

```bash
piply run PIPELINE_ID --var practice=BENNETT --config piply.yaml
```

The supplied values are captured with the run and reused by retry/backfill.

### A task failed

1. Open the run detail page.
2. Select the failed task.
3. Read task logs oldest-to-newest for the failure.
4. Check task output and artifacts.
5. If upstream tasks succeeded and only downstream work needs repair, retry from
   the failed task:

   ```bash
   piply tasks retry RUN_ID TASK_ID --mode resume --config piply.yaml
   ```

Use `--mode startover` only when all tasks should run again.

### A task timed out

Check task or pipeline timeout settings and the kill grace period in YAML. Piply
terminates the process tree for subprocess tasks.

If a Python callable task must be cancellable or strongly isolated, prefer:

```yaml
tasks:
  my_task:
    type: python
    path: scripts/my_task.py
```

or:

```yaml
tasks:
  my_task:
    type: cli
    command: python scripts/my_task.py
```

### Logs are too large or the database is growing

Preview cleanup first:

```bash
piply prune --dry-run --config piply.yaml
```

Then prune with explicit retention:

```bash
piply prune --run-days 90 --log-days 30 --max-runs 200 --yes --config piply.yaml
```

For SQLite, pruning runs `VACUUM` by default to reclaim disk.

### Alert or notification did not arrive

1. Open Settings -> Alerts.
2. Check whether the destination is configured.
3. Send a test card.
4. Review recent delivery attempts and failure detail.
5. Confirm the pipeline has a notification route for that outcome.
6. Remember that webhook URLs are credentials and are intentionally not shown.

For email tasks or SMTP:

1. Open Settings -> Email.
2. Send a test email.
3. Confirm SMTP host, port, TLS mode, username, and password source.

### Authentication problem

If accounts exist, authentication is on.

Admin CLI basics:

```bash
piply users list --config piply.yaml
piply users create alice --role admin --config piply.yaml
piply users passwd alice --config piply.yaml
piply users grant bob PIPELINE_ID=view,run --config piply.yaml
```

Repeated failed sign-ins lock a username for a short window. Wait or use an
admin account to reset the password.

### Metadata database switch failed

Switching is refused while any run is active. Wait for active runs to finish, or
pause schedules and cancel/complete work first.

If `PIPLY_DATABASE` is set as a real environment variable, Settings cannot
change it because the process environment wins over `.env`. Change it in the
service manager, compose file, or shell environment, then restart.

---

## Backup, Restore, And Migration

### SQLite backup

Safe while Piply is running:

```bash
piply backup ./backups --config piply.yaml
```

### SQLite restore

Stop the server first:

```bash
piply stop --config piply.yaml
piply restore ./backups/piply.db --config piply.yaml
piply start --config piply.yaml
```

### PostgreSQL backup

Use database-native tools such as `pg_dump` and `pg_restore`.

### Migrate SQLite to PostgreSQL

Stop the server and migrate into an empty target:

```bash
piply stop --config piply.yaml
piply migrate-db --to "postgresql://piply:secret@db:5432/piply" --config piply.yaml
piply start --config piply.yaml
```

Run ids, logs, accounts, retry chains, and downstream links are preserved.

---

## Deployment Checklist

If a direct start reports `max_concurrent_runs` or HTTP 409, check active and
paused runs in Runs. Finish, resume, or cancel existing work before retrying.
Direct requests are not queued automatically. Automatic triggers stay queued;
the queue item reports its hold reason and retains its inputs. Raise the limit
only after checking that overlapping writes are safe. It does not form a shared
resource pool across differently named pipelines.

Deploy the fixed version and matching pipeline limits to every process using
the same store; an older process does not participate in the admission gate.

Before exposing Piply beyond a local machine:

- Create at least one admin account.
- Grant users only the pipelines/actions they need.
- Set a durable metadata store path or PostgreSQL DSN.
- Confirm `.env` is not committed.
- Put secrets in environment variables or a secret backend, not plain YAML.
- Configure SMTP or Teams alerts for production pipelines.
- Run `piply validate`.
- Run `piply plan` for changed pipelines.
- Confirm `/health` and `/metrics` behavior.
- Take a database backup before risky changes.

---

## Release Or Upgrade Checklist

**Do not publish v0.3.4 while [the release assessment](RELEASE_CHECK.md) is
HOLD.** A passing local admission gate does not replace real PostgreSQL and
supported-platform checks.

For maintainers, require all configured compatibility and PostgreSQL jobs to
pass. Run `python -m tools.check_run_admission`, verify wheel/source contents
with `python tools/check_release_artifacts.py dist`, run `python -m twine check
dist/*`, and test the installed wheel in a disposable environment with
`python -I tools/release_smoke.py`. Do not run that smoke script against a
production database; it creates and tests a separate workspace.

PostgreSQL parity requires an expendable database and both
`PIPLY_TEST_POSTGRES_DSN` and `PIPLY_TEST_POSTGRES_RESET=1`. The reset opt-in
permits schema deletion; it does not detect production automatically. Use a
separate cluster/container and verify its identity before running the tests.

Push `codex/` review branches for validation only. `staging` pushes invoke the
publishing workflow, so never use staging as a test-only branch. CI runner
labels must correspond to real operating systems; the matrix checks this.

If the review push fails authentication, refresh the owner-approved Git
credential in the machine's credential manager with repository-write access.
Do not paste tokens/passwords into chat, disable authentication, or use staging
as an alternative push target. A local commit is not a remotely validated run.

1. Read `CHANGELOG.md`.
2. Back up the metadata store and configuration. Rehearse restore into an
   isolated copy; record the previous package version and environment.
3. Run tests if developing locally:

   ```bash
   python -m pytest -q --basetemp .pytest-tmp-codex -p no:cacheprovider
   ```

4. Validate production config:

   ```bash
   piply validate --config piply.yaml
   ```

5. Preview changed pipelines:

   ```bash
   piply plan PIPELINE_ID --config piply.yaml
   ```

6. Stop the old process during a quiet window, then upgrade in the same
   environment and restart. Do not run two server versions against one store.
7. Sign in again after the password-bound cookie upgrade. Check Diagnostics,
   recent runs, Audit, pipeline lists, and both pipeline/run graphs.
8. On failure, stop the new process, restore the pre-upgrade database/config
   backup and previous environment, then restart and check recovery. Do not
   assume an older package can safely consume a newer schema.

---

## Developer Notes For Fixes

Use these starting points when an incident becomes code work:

| Area | Start here |
| --- | --- |
| CLI behavior | `piply/cli/main.py` |
| UI pages | `piply/api/routes/ui.py`, `piply/ui/templates/` |
| API behavior | `piply/api/routes/` |
| Cross-surface runtime behavior | `piply/core/service.py` |
| Persistence/schema | `piply/core/store.py` |
| Scheduling | `piply/core/scheduler.py`, `piply/core/scheduling.py` |
| DAG execution | `piply/engine/local_engine.py` |
| Subprocess/Python task execution | `piply/engine/task_runner.py` |
| Auth and permissions | `piply/core/auth.py`, `piply/api/auth.py` |
| Notifications | `piply/core/notifications.py`, `piply/core/mailer.py` |
| Preview | `piply/core/preview.py` |

For async runtime tests, poll until a terminal state before asserting details.
Windows process handling is different from POSIX, so avoid shell assumptions in
task execution and tests.
