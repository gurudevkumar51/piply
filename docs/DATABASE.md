# Metadata Store

Piply keeps its own runtime state — runs, task runs, logs, artifacts, accounts,
queue, sensors — in a metadata store. This is separate from any database your
*pipelines* talk to.

- **SQLite by default.** One file, nothing to configure, no server.
- **PostgreSQL when you ask for it.** Same schema, same behaviour, state that
  survives a container redeploy.

Nothing in your `piply.yaml` changes when you switch. The choice is one
environment variable.

---

## 1. Which one should you use?

| | SQLite (default) | PostgreSQL |
| --- | --- | --- |
| Setup | none | a server plus `pip install "mr-piply[postgres]"` |
| Survives a container redeploy | only with a mounted volume | yes, it lives outside the container |
| Backup | `piply backup` | `pg_dump` |
| Concurrent Piply instances | one | still one (see §7) |
| Good for | laptops, single VMs, most installs | containers, managed databases, existing Postgres estates |

**Stay on SQLite** unless you have a reason not to. It is faster for a single
process and there is nothing to operate.

**Move to PostgreSQL** when your deployment recreates the filesystem — the usual
trigger is "we redeployed and the run history vanished".

---

## 2. Choosing the database at launch

`PIPLY_DATABASE` selects the store. A value containing `://` is treated as a
DSN; anything else is a file path.

### SQLite, default

Nothing to set. The database is created next to your config:

```
<config directory>/.piply/piply.db
```

### SQLite, explicit path

```bash
export PIPLY_DATABASE=/var/lib/piply/piply.db
piply start
```

Relative paths resolve against the config file's directory, not the working
directory, so `piply start` behaves the same from anywhere.

### PostgreSQL

```bash
pip install "mr-piply[postgres]"
export PIPLY_DATABASE="postgresql://piply:secret@db.internal:5432/piply"
piply start
```

The schema is created on first connect and migrated automatically on every
start. Both `psycopg` (v3) and `psycopg2` are supported; v3 is used when both
are installed.

Accepted URL forms — the SQLAlchemy-style driver suffix is accepted and stripped:

```
postgresql://user:password@host:5432/piply
postgres://user:password@host:5432/piply
postgresql+psycopg://user:password@host:5432/piply
postgresql+psycopg2://user:password@host:5432/piply
```

TLS and other libpq options work as normal query parameters:

```
postgresql://piply:secret@db.internal:5432/piply?sslmode=require
```

### Keeping the password out of the URL

Set the password through libpq's own environment variables, or a
`~/.pgpass` file, and leave it out of `PIPLY_DATABASE`:

```bash
export PGPASSWORD_FILE=/run/secrets/pg_password   # your process manager reads this
export PIPLY_DATABASE="postgresql://piply@db.internal:5432/piply"
```

Piply never prints the DSN with its password. The Diagnostics page, the API,
the logs, and `piply diagnostics` all show `postgresql://piply:***@host:5432/piply`.

### What is *not* accepted

MySQL, MariaDB, SQL Server, Oracle, MongoDB, and CockroachDB are rejected at
startup with an explanation rather than being misread as a file path:

```
PIPLY_DATABASE does not support 'mysql'. The metadata store is either a SQLite
file path (the default) or a PostgreSQL URL. Other databases are reached from
sql_sensor and from tasks instead.
```

Those databases are still reachable *from your pipelines* — see `connections:`
and `sql_sensor` in the [YAML Specification](YAML_SPECIFICATION.md). The
restriction is only about where Piply keeps its own state.

A `sqlite://` URL is also rejected, because the setting takes a plain path:

```bash
PIPLY_DATABASE=sqlite:///piply.db     # rejected
PIPLY_DATABASE=piply.db               # correct
```

---

## 3. Switching after the project is already running

### Empty target: just point at it

If you do not need the existing history, change `PIPLY_DATABASE` and restart.
Piply creates the schema and starts fresh. Your pipelines are defined in YAML,
so nothing about them is lost — only run history.

### Keeping the history: `piply migrate-db`

```bash
# 1. Stop the server. Migrating a live database copies a moving target.
piply stop

# 2. Copy everything across.
piply migrate-db --to "postgresql://piply:secret@db.internal:5432/piply"

# 3. Point Piply at the new store and start again.
export PIPLY_DATABASE="postgresql://piply:secret@db.internal:5432/piply"
piply start
```

Output:

```
Source: C:\work\rcm\.piply\piply.db (sqlite, 48211 rows)
Target: postgresql://piply:***@db.internal:5432/piply (postgres)
Stop the Piply server before migrating.
Copy this data to the target database? [y/N]: y
  runs                 1204
  task_runs            9663
  logs                 36894
  task_outputs         412
  users                9
  user_permissions     22
  meta                 7
Copied 48211 rows.

Point Piply at the new database and restart:
  PIPLY_DATABASE="postgresql://piply:secret@db.internal:5432/piply"
```

**What is preserved.** Every row of every table, with its original primary key.
That matters more than it sounds: run ids are referenced by `retry_of`,
`parent_run_id`, and `dispatched_run_id`, so preserving them keeps retry chains,
downstream lineage, and the trigger queue intact. Password hashes copy across
unchanged, so accounts keep working.

**Options.**

| Flag | Meaning |
| --- | --- |
| `--to <dsn or path>` | Required. The destination. |
| `--from <dsn or path>` | Source. Defaults to the configured database. |
| `--config`, `-c` | Path to `piply.yaml`. |
| `--yes`, `-y` | Skip the confirmation prompt, for scripts. |

**The target must be empty.** Merging two histories would need a policy for
duplicate run ids, and silently picking one is worse than refusing:

```
Migration refused: The target database already contains data (runs=12, logs=340).
Migrate into an empty database, or drop the existing Piply tables first.
```

To retry after a failed attempt, empty the target first:

```sql
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
```

**It works in both directions.** `--to /path/to/piply.db` copies PostgreSQL back
down to SQLite, which is useful for pulling a production snapshot onto a laptop.

**Prune first if the history is large.** Logs dominate the row count, and there
is no point copying rows you are about to delete:

```bash
piply prune --run-days 90 --log-days 30
piply migrate-db --to "postgresql://..."
```

---

## 4. Schema reference

Twelve tables. All timestamps are ISO-8601 UTC strings, stored as text in both
backends so the two stores stay byte-comparable. Booleans are integers, `0` or
`1`, for the same reason.

The schema is created and upgraded automatically; new columns are added on
start. You never run a migration by hand.

### `runs`

One row per pipeline execution.

| Column | Type | Meaning |
| --- | --- | --- |
| `id` | TEXT PK | Run id, a short hex string. Appears in URLs and the CLI. |
| `pipeline_id` | TEXT | The pipeline that ran. |
| `pipeline_title` | TEXT | Title captured at run time, so history survives a rename. |
| `status` | TEXT | `queued`, `running`, `success`, `failed`, `skipped`, `cancelled`, `interrupted`, `timed_out`. |
| `trigger` | TEXT | `manual`, `schedule`, `pipeline`, `sensor`, `task`, `retry`. A backfill is recorded as `manual`, not as a seventh value. Quoted in SQL: it is a reserved word. |
| `command` | TEXT | Representative command, for the run list. |
| `primary_entry` | TEXT | Entry point shown in the UI. |
| `created_at` | TEXT | When the run was queued. |
| `started_at` | TEXT | When execution began. Null while queued. |
| `finished_at` | TEXT | When it reached a terminal status. |
| `scheduled_for` | TEXT | The schedule slot this run fills. Used to dedupe backfills. |
| `exit_code` | INTEGER | Process exit code where meaningful. |
| `error` | TEXT | Failure summary. |
| `heartbeat_at` | TEXT | Last liveness write. Drives stale-run reconciliation. |
| `retry_of` | TEXT | The run this is a retry of. |
| `retry_mode` | TEXT | `resume` or `startover`. |
| `retry_task_id` | TEXT | Task a resume restarted from. |
| `parent_run_id` | TEXT | The upstream run that triggered this one. Drives lineage. |
| `parent_pipeline_id` | TEXT | The upstream pipeline. |
| `tenant_id` | TEXT | Optional tenant label passed at trigger time. |
| `owner_pid` | INTEGER | PID that owns the run. Lets startup recovery tell a crashed run from one owned by a live process. |
| `run_config` | TEXT | JSON snapshot of the resolved variables, environment, and settings. This is what makes a downstream run retryable without re-running its upstream. Contains credentials, so the API masks them on the way out. |

### `task_runs`

One row per task inside a run.

| Column | Type | Meaning |
| --- | --- | --- |
| `id` | identity PK | Generated by the backend. |
| `run_id` | TEXT | Owning run. `ON DELETE CASCADE`. |
| `task_id` | TEXT | Task id. For entity tasks this is the expanded form, `payment.extract`. |
| `title` | TEXT | Display name. |
| `task_type` | TEXT | `python`, `cli`, `api`, `webhook`, `email`, `ssh`. |
| `status` | TEXT | Same vocabulary as `runs.status`. |
| `position` | INTEGER | Execution order within the run. |
| `command_preview` | TEXT | Resolved command, for display. |
| `priority` | INTEGER | Effective priority, including entity `*` suffixes. Higher runs first. |
| `timeout_seconds` | INTEGER | Effective timeout. |
| `run_if` | TEXT | The condition, kept for display and debugging. |
| `depends_on` | TEXT | Comma-separated upstream task ids. |
| `started_at`, `finished_at` | TEXT | Timing. |
| `exit_code` | INTEGER | Process exit code. |
| `error` | TEXT | Failure detail. |

Unique on `(run_id, task_id)`.

### `logs`

| Column | Type | Meaning |
| --- | --- | --- |
| `id` | identity PK | Also the cursor for `--follow` and live tailing. |
| `run_id` | TEXT | Owning run. `ON DELETE CASCADE`. |
| `task_id` | TEXT | Task the line came from. Null for run-level lines. |
| `created_at` | TEXT | Timestamp. |
| `stream` | TEXT | `stdout` or `stderr`. |
| `message` | TEXT | The line. |

Usually the largest table. `piply prune --log-days` trims it.

### `task_outputs`

Captured return values, one row per task.

| Column | Type | Meaning |
| --- | --- | --- |
| `run_id`, `task_id` | TEXT | Owning task. Unique together. |
| `output_type` | TEXT | How the value was captured. |
| `preview` | TEXT | Truncated text form for the UI. |
| `is_json` | INTEGER | 1 when `json_value` is populated. |
| `json_value` | TEXT | Full JSON value, when the output was JSON. |
| `metadata_json` | TEXT | Extra capture metadata. |
| `size_bytes` | INTEGER | Size of the original output. |
| `created_at` | TEXT | Timestamp. |

### `task_artifacts`

Files a task declared through `artifacts:`.

| Column | Type | Meaning |
| --- | --- | --- |
| `id` | identity PK | |
| `run_id`, `task_id` | TEXT | Producing task. |
| `name` | TEXT | Display name, usually the basename. |
| `path` | TEXT | Absolute path on the Piply host. |
| `size_bytes` | INTEGER | Size when recorded. |
| `content_type` | TEXT | Guessed MIME type. |
| `modified_at` | TEXT | File mtime when recorded. |
| `created_at` | TEXT | When Piply recorded it. |

Unique on `(run_id, task_id, path)`, so re-recording updates rather than
duplicates. Only metadata is stored — the file stays on disk.

### `sensor_health`

One row per configured sensor.

| Column | Type | Meaning |
| --- | --- | --- |
| `sensor_key` | TEXT PK | `pipeline_id:sensor_id`. |
| `pipeline_id`, `sensor_id` | TEXT | Owning pipeline and sensor. |
| `sensor_type` | TEXT | `file_sensor`, `sql_sensor`, `api_sensor`. |
| `status` | TEXT | `healthy`, `failing`, `idle`. |
| `last_polled_at` | TEXT | Last poll. |
| `last_success_at` | TEXT | Last poll that did not error. |
| `last_event_at` | TEXT | Last poll that produced a trigger. |
| `last_error` | TEXT | Most recent error, with credentials masked. |
| `consecutive_failures` | INTEGER | Resets on success. Drives the `failing` status. |
| `poll_count`, `event_count` | INTEGER | Lifetime counters. |

### `users`

| Column | Type | Meaning |
| --- | --- | --- |
| `username` | TEXT PK | Lower-cased. Letters, digits, and `. _ - @`. |
| `password_hash` | TEXT | `pbkdf2$rounds$salt$hash`, PBKDF2-SHA256. Never reversible. |
| `role` | TEXT | `admin` or `user`. |
| `is_active` | INTEGER | 0 disables sign-in without deleting history. |
| `created_at`, `last_login_at` | TEXT | Timestamps. |

### `user_permissions`

| Column | Type | Meaning |
| --- | --- | --- |
| `username` | TEXT | Account. Part of the primary key. |
| `pipeline_id` | TEXT | Pipeline id, or `*` for every pipeline. |
| `actions` | TEXT | Comma-separated subset of `view`, `edit`, `run`. |

Primary key `(username, pipeline_id)`, so re-granting updates in place. Rows are
deleted with the account, in the same transaction.

### `pipeline_overrides`

| Column | Type | Meaning |
| --- | --- | --- |
| `pipeline_id` | TEXT PK | |
| `paused` | INTEGER | 1 when paused from the UI or CLI. Survives restarts and config reloads. |

### `meta`

Key/value runtime state. Notable keys:

| Key | Meaning |
| --- | --- |
| `scheduler_state` | `running`, `stopped`, `crashed`. |
| `scheduler_heartbeat`, `scheduler_started_at`, `scheduler_owner_pid` | Scheduler liveness. |
| `scheduler_last_error` | Written with the state in one transaction, so a reader never sees `crashed` with no reason. |
| `runtime_accepting_work` | False during graceful shutdown. |
| `runtime_last_recovery_at`, `runtime_last_recovered_runs` | Startup recovery results. |
| `session_secret` | HMAC key for session cookies. Generated on first use. Overridden by `PIPLY_SESSION_SECRET`. |
| `smtp_*` | Central SMTP settings. `smtp_password` is write-only through the API. |
| `admin_bootstrapped_at` | When the first admin was created. |
| `shutdown_requested` | Set by `piply stop` and observed by the running server. |

### `trigger_queue`

Pending work, so a trigger survives a restart.

| Column | Type | Meaning |
| --- | --- | --- |
| `id` | identity PK | |
| `pipeline_id` | TEXT | Pipeline to run. |
| `trigger` | TEXT | Why. Quoted in SQL: reserved word. |
| `status` | TEXT | `queued`, `dispatched`, `failed`. |
| `available_at` | TEXT | Earliest dispatch time. Implements retry delays. |
| `created_at`, `scheduled_for` | TEXT | Timestamps. |
| `source_key` | TEXT | Originating sensor or schedule. |
| `dedupe_key` | TEXT | Collapses duplicates via insert-or-ignore. |
| `payload_json` | TEXT | Variables and context to pass through. |
| `dispatched_at`, `dispatched_run_id` | TEXT | What the entry became. |
| `error` | TEXT | Why dispatch failed. |

### `sensor_state`

| Column | Type | Meaning |
| --- | --- | --- |
| `sensor_key` | TEXT PK | `pipeline_id:sensor_id`. |
| `state_json` | TEXT | Cursor a sensor carries between polls — last seen mtime, max id, ETag. |
| `updated_at` | TEXT | Last write. |

---

## 5. Docker: not losing your data

The most common cause of "Piply lost all my history" is a container with no
volume. The database sits at `/app/.piply/piply.db` in the writable layer, which
is destroyed on every redeploy.

### Option A — SQLite on a named volume

```yaml
services:
  piply:
    image: your-image
    environment:
      PIPLY_DATABASE: /var/lib/piply/piply.db
      PIPLY_ARTIFACTS_DIR: /var/lib/piply-artifacts
    volumes:
      - piply-state:/var/lib/piply
      - piply-artifacts:/var/lib/piply-artifacts

volumes:
  piply-state:
  piply-artifacts:
```

The paths must exist in the image, owned by the runtime user, **before** the
`USER` instruction. Docker seeds a fresh named volume from the image path
including its ownership, so a path that does not exist in the image becomes a
root-owned volume the app cannot write to:

```dockerfile
RUN mkdir -p /var/lib/piply /var/lib/piply-artifacts \
 && chown --recursive appuser:appuser /var/lib/piply /var/lib/piply-artifacts
USER appuser
```

### Option B — PostgreSQL

The state lives outside the container, so no volume is needed for it:

```yaml
services:
  piply:
    image: your-image
    environment:
      PIPLY_DATABASE: postgresql://piply:secret@postgres:5432/piply
      PIPLY_ARTIFACTS_DIR: /var/lib/piply-artifacts
    volumes:
      - piply-artifacts:/var/lib/piply-artifacts
    depends_on: [postgres]

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: piply
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: piply
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  piply-artifacts:
  pgdata:
```

Artifacts are files on disk and are **not** in the database, so they still need
a volume either way.

### Checking it worked

```bash
piply diagnostics          # shows the resolved database location and backend
```

Then redeploy and confirm your run history is still listed.

---

## 6. Backup and restore

### SQLite

```bash
piply backup /backups                              # timestamped file
piply backup /backups/piply-nightly.db             # explicit name
piply restore /backups/piply-20260804T074211Z.db   # stop the server first
```

`piply backup` uses SQLite's online backup API, so it is safe to run against a
live server with an active write-ahead log. `piply restore` keeps the displaced
database next to the target as `piply.db.replaced` rather than deleting it.

### PostgreSQL

Use your database's own tooling:

```bash
pg_dump "postgresql://piply:secret@db.internal:5432/piply" > piply.sql
psql "postgresql://piply:secret@db.internal:5432/piply" < piply.sql
```

`piply backup` and `piply restore` refuse to run against a server store, with a
message naming `pg_dump`, rather than silently producing an unusable file.

---

## 7. Operational notes

- **Run one Piply instance per database, on either backend.** The scheduler
  assumes it owns the queue. Two instances against one database will both
  dispatch the same scheduled slot. Use `PIPLY_SESSION_SECRET` if you ever put
  more than one process behind a load balancer for read traffic.
- **Retention applies to both backends.** `piply prune` deletes old runs and
  logs. `VACUUM` runs on SQLite only; PostgreSQL autovacuum handles reclamation
  and `VACUUM` cannot run inside a transaction, so it is skipped.
- **`database_size_bytes` reports 0 on PostgreSQL.** The size of a server-side
  database is not something Piply owns or can measure cheaply.
- **Schema upgrades are automatic and additive.** Columns are added on start;
  nothing is dropped or renamed, so a rollback to an earlier Piply keeps working.
- **The `psycopg` extra is optional.** Without it, a PostgreSQL DSN fails at
  startup with an install hint rather than an import traceback.

---

## 8. Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| History empty after a redeploy | No volume, SQLite in the container layer | §5 |
| `PIPLY_DATABASE does not support 'mysql'` | Unsupported backend for the metadata store | Use SQLite or PostgreSQL; reach MySQL from tasks instead |
| `PIPLY_DATABASE must be a plain file path, not a sqlite:// URL` | URL form used for SQLite | Drop the scheme |
| `psycopg` import error at startup | Extra not installed | `pip install "mr-piply[postgres]"` |
| `Migration refused: the target database already contains data` | Target not empty | Drop and recreate the schema, or migrate somewhere empty |
| Duplicate key error after a manual copy | Identity sequences not advanced | Use `piply migrate-db`, which realigns them |
| Two runs for the same scheduled slot | Two Piply instances on one database | Run one instance |

---

## Related

- [YAML Specification §11](YAML_SPECIFICATION.md#11-runtime-storage-and-external-databases) — the `PIPLY_DATABASE` setting in context
- [Authentication](AUTHENTICATION.md) — the `users` and `user_permissions` tables in use
- [Runtime Lifecycles](LIFECYCLES.md) — how the queue, heartbeat, and recovery columns are used
