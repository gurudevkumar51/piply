# Piply YAML Specification

This is the complete reference for `piply.yaml`. Every key is optional unless
marked **required**. Validate any config with:

```bash
piply validate --config piply.yaml
```

Preview what a config would actually execute, without running it:

```bash
piply plan --config piply.yaml
```

---

## 1. Document Structure

```yaml
version: "1"                  # document version, informational
title: My Workspace           # shown in the UI header
workspace: .                  # root for relative script and artifact paths
timezone: UTC                 # default timezone for all schedules

defaults: {}                  # project-wide defaults
variables: {}                 # reusable {name} values
secrets: {}                   # secret backend configuration
connections: {}               # reusable SQL connection strings
entities: {}                  # project-wide entity expansion

pipelines: {}                 # simple mode: runnable pipelines
pipeline_templates: {}        # advanced mode: reusable definitions
pipeline_deployments: {}      # advanced mode: concrete instances
```

A config must define at least one of `pipelines` or `pipeline_deployments`.
`jobs` is accepted as a legacy alias for `pipelines`.

---

## 2. Interpolation

Three substitution forms are supported everywhere a string is accepted.

| Form | Source | Example |
| --- | --- | --- |
| `{name}` | `variables`, `entities`, deployment values | `python {scripts_dir}/run.py` |
| `$NAME`, `${NAME}`, `%NAME%` | OS environment and `.env` | `${DATABASE_URL}` |
| `${secret:NAME}` | configured secret backend | `${secret:WAREHOUSE_DSN}` |

Unresolved `{name}` placeholders are left as-is rather than being blanked, so a
missing variable is visible in the command preview and reported by `piply plan`.

`run_if` is the one exception: its `{name}` placeholders are resolved at
execution time, not load time, so values are substituted as quoted literals.

---

## 3. Top-Level Keys

### `defaults`

```yaml
defaults:
  python: python                # interpreter for python script tasks
  timezone: Europe/Berlin
  variables:                    # merged under the root `variables`
    region: eu
  env:                          # env applied to every task
    PIPLY_ENV: development
```

### `variables`

```yaml
variables:
  scripts_dir: pipelines
  batch_id: demo-batch
  archive_dir: "{scripts_dir}/archive"   # earlier variables are usable later
```

### `secrets`

```yaml
secrets:
  backend: env                  # env | file
  prefix: PIPLY_SECRET_         # env backend: strip this prefix
  path: .secrets.env            # file backend: KEY=VALUE file
```

Referenced as `${secret:NAME}`.

### `connections`

```yaml
connections:
  local_sensor_db: sqlite:///sensor_demo.db
  warehouse: ${secret:WAREHOUSE_DSN}
```

Referenced from a `sql_sensor` as `connection: "@warehouse"` or
`connection_ref: warehouse`.

### `entities`

```yaml
entities:
  report: [payment, adjustment, refund]
  region:
    - eu
    - us
```

Entities expand a single task definition into one runtime task per value. A task
declared as `extract` with `entities: {report: [...]}` produces runtime task ids
of the form `payment.extract`, `adjustment.extract`, and so on. Set
`entities: false` on a task to opt it out of expansion.

---

## 4. Pipeline Keys

```yaml
pipelines:
  extract_flow:
    title: Extract Flow                  # display name
    description: Loads and validates.    # shown in the UI
    enabled: true                        # false hides it from the scheduler
    tags: [ingest, tier1]
    timezone: UTC                        # overrides the project timezone

    schedule:                            # see section 5
      every: 15m

    variables:                           # merged over project variables
      batch_id: nightly

    env:                                 # applied to every task in the pipeline
      STAGE: production
    env_file: .env.production            # single extra env file
    env_files: [.env.shared]             # several extra env files

    execution: parallel                  # or sequential, or a worker count
    max_parallel_tasks: 4
    max_concurrent_runs: 1

    timeout: 30m                         # pipeline-level execution timeout

    retry:
      attempts: 2
      mode: resume                       # resume | startover
      delay_seconds: 10

    triggers_on_success:
      - report_flow

    entities: {}                         # pipeline-scoped entity expansion
    sensors: {}                          # see section 7
    tasks: {}                            # required, see section 6
```

### Concurrency

| Key | Meaning |
| --- | --- |
| `execution: sequential` | one task at a time, dependency order |
| `execution: parallel` | run every ready task, up to `max_parallel_tasks` |
| `execution: 8` | shorthand for `max_parallel_tasks: 8` |
| `max_concurrent_runs` | how many runs of this pipeline may overlap |

Parallel execution is only used when the DAG actually branches; a purely linear
pipeline runs sequentially regardless of the setting.

### Pipeline timeout

`timeout` (alias `timeout_seconds`) bounds the whole run. When it elapses the
engine cancels in-flight tasks, marks every unfinished task `timed_out`, and
finishes the run as `timed_out`.

---

## 5. Schedules

```yaml
schedule: "0 6 * * *"            # cron shorthand

schedule:                        # cron with an explicit timezone
  cron: "0 6 * * *"
  timezone: America/New_York

schedule:                        # fixed interval
  every: 15m                     # s, m, h, d suffixes

schedule:
  interval_seconds: 900
```

Missed slots are backfilled automatically through the durable trigger queue, so
a scheduler that was offline catches up on restart rather than skipping work.

---

## 6. Tasks

Every pipeline needs a non-empty `tasks` mapping.

### Keys shared by all task types

```yaml
tasks:
  build_report:
    type: python                 # python | cli | api | webhook | email | ssh
    title: Build Report
    description: Renders the daily report.
    enabled: true

    depends_on: [extract, validate]
    on_upstream_failure: skip    # skip | fail | continue

    priority: high               # see below

    timeout: 5m                  # task execution timeout
    kill_grace_period: 5         # seconds between terminate and kill

    run_if: "{report} == 'payment'"

    artifacts:
      - "out/*.csv"
      - "out/manifest.json"

    env:
      REPORT_MODE: full
    cwd: .
```

#### `priority`

Controls which *runnable* task the scheduler picks first. Dependency order
always wins: a high-priority task still waits for its dependencies.

| Form | Value |
| --- | --- |
| `priority: 5` | explicit integer, higher runs first |
| `priority: "***"` | star shorthand, one star per level (`***` = 3) |
| `priority: lowest \| low \| normal \| high \| higher \| highest \| critical` | named levels (-2, -1, 0, 1, 2, 3, 5) |
| task id suffix `extract***:` | same as `priority: 3` on a task named `extract` |

The id-suffix form is normalised before anything else runs, so dependencies,
entity expansion, and the UI all use the clean name:

```yaml
tasks:
  extract***:                  # -> task "extract", priority 3
    type: python
    path: jobs/extract.py
  transform**:                 # -> task "transform", priority 2
    type: python
    path: jobs/transform.py
  validate*:                   # -> task "validate", priority 1
    type: cli
    depends_on: [extract]      # references the normalised id
    command: python validate.py
```

An explicit `priority:` key wins over the suffix. Ties fall back to declaration
order, so an unprioritised pipeline behaves exactly as before.

#### `timeout` and `kill_grace_period`

`timeout` accepts seconds or a duration string (`30`, `30s`, `5m`, `1h`,
`500ms`). When it elapses:

1. the reason is written to the task log,
2. the process is sent a terminate signal,
3. after `kill_grace_period` seconds (default `5`) it is killed,
4. the task ends as `timed_out` and the run ends as `timed_out`.

For `api` and `webhook` tasks the timeout is applied as the HTTP timeout. For
`python` callable tasks the call runs on a worker thread; Python cannot force a
thread to stop, so the task is marked `timed_out` and the abandoned thread is
left as a daemon.

#### `run_if`

A deliberately small conditional, not an expression language. Supported:
literals, `{placeholders}`, `==`, `!=`, `in`, `not in`, `and`, `or`, `not`, and
list/tuple literals. Anything else raises and fails the task loudly.

```yaml
run_if: "{report} == 'payment'"
run_if: "{environment} in ['staging', 'production']"
run_if: "{tenant} != 'demo' and {enabled} == 'true'"
```

A false condition marks the task `skipped`; the run can still succeed.

#### `artifacts`

Glob patterns, relative to the task working directory, describing the files the
task produces. After a successful task Piply records each match's path, size,
and mtime. Files are never copied; the browser and `piply artifacts` read them
from disk.

### `type: python`

Two shapes are supported.

```yaml
# 1. Run a script as a subprocess
extract:
  type: python
  path: pipelines/extract.py
  python: python3.12
  args: ["--records", "120"]

# 2. Import and call a function in-process
transform:
  type: python
  path: pipelines/extract.py
  function: transform_data
  kwargs:
    records: 120
```

`module: package.module` plus `function:` also works, as does
`call: package.module:function` or `call: /path/to/file.py::function`.

A callable that declares a `context` parameter receives the run context, which
contains upstream task outputs keyed by task id.

### `type: cli`

```yaml
validate:
  type: cli
  command: python {scripts_dir}/validate.py {batch_id}
  cwd: .
  shell: bash            # optional: bash, sh, zsh, powershell, pwsh, cmd
```

`path:` may be used instead of `command:`; `.bat`, `.cmd`, and `.ps1` files are
launched with the correct interpreter automatically.

### `type: api` / `type: webhook`

```yaml
ping:
  type: api
  url: https://example.com/api/ping
  method: GET
  headers:
    X-Trace: piply
  body: '{"event":"piply"}'
  token: ${secret:API_TOKEN}
  expected_status: [200, 204]
  timeout: 30s
```

`webhook` is the same operator with `POST` as the default method.

### `type: email`

```yaml
notify:
  type: email
  smtp_host: ${SMTP_HOST}
  smtp_port: 587
  smtp_user: ${SMTP_USER}
  smtp_password: ${SMTP_PASSWORD}
  to: [team@example.com]
  subject: "Batch {batch_id} finished"
  body: The nightly batch completed.
```

### `type: ssh`

```yaml
remote_check:
  type: ssh
  host: build-01.internal
  user: ${SSH_USER}
  port: 22
  key_file: ~/.ssh/id_ed25519
  command: systemctl is-active piply
  connect_timeout: 8
  ssh_binary: ssh            # override when ssh is not on PATH
```

Piply shells out to the local `ssh` client rather than bundling an SSH library,
so authentication is whatever your `ssh` config already does. `BatchMode=yes` is
always passed, meaning a key that needs an interactive passphrase will fail
rather than hang.

---

## 7. Sensors

Sensors poll external state and enqueue a pipeline trigger when it changes.

```yaml
sensors:
  inbox_files:
    type: file_sensor
    path: sensor_inbox            # or sftp://user@host/path
    pattern: "*.csv"
    recursive: false
    ignore_existing: true
    task_id: inspect_event        # optional: run one task instead of the pipeline

  inbound_rows:
    type: sql_sensor
    connection_ref: local_sensor_db
    table: inbound_events
    cursor_column: id
    where: "processed = 0"
    ignore_existing: true

  external_api:
    type: api_sensor
    url: https://example.com/api/events
    method: GET
    cursor_path: version
    expected_status: [200]
    ignore_existing: true
```

Every poll updates the sensor's health record. Failures do not raise: the error
is stored, surfaced on the Diagnostics page and `GET /api/sensors`, and exported
as the `piply_sensor_health` metric, while the other sensors keep polling.

---

## 8. Templates And Deployments

Use these when one workflow must run once per tenant, region, or environment.

```yaml
pipeline_templates:
  tenant_ingest:
    description: Shared ingest workflow.
    schedule:
      cron: "0 * * * *"
    env:
      STAGE: production
    retry:
      attempts: 2
      mode: resume
    triggers_on_success:
      - tenant_report
    tasks:
      ingest:
        type: cli
        priority: high
        timeout: 5m
        command: python ingest.py --tenant {tenant}

pipeline_deployments:
  acme_ingest:
    template: tenant_ingest
    tenant: acme
    variables:
      region: eu
    schedule:
      cron: "15 * * * *"        # overrides the template schedule

  globex_ingest:
    template: tenant_ingest
    tenant: globex
    environment: staging
```

Rules:

- A deployment is deep-merged over its template; deployment keys win.
- `tenant` / `tenant_id` populate the `{tenant}` and `{tenant_id}` variables.
- `environment` populates `{environment}`.
- Everything else — variables, env, schedule, retry, timeout, execution,
  concurrency, sensors, triggers — is inherited from the template unless the
  deployment overrides it.
- Deployment ids become ordinary pipeline ids everywhere: CLI, API, UI, and DAG.
- Entity expansion still applies inside a deployed template.
- A deployment id may not collide with a `pipelines:` id.

See [MIGRATION.md](MIGRATION.md) for moving an existing config onto templates.

---

## 9. Downstream Inheritance

When a pipeline lists `triggers_on_success`, the downstream run receives:

- the upstream pipeline's resolved **variables**,
- the upstream pipeline's shared **env** values,
- every JSON-serialisable upstream task **output**, under both the task id and
  the `upstream` key,
- the upstream `tenant_id`,
- a `parent` entry with the upstream run and pipeline id.

All of it is stored on the downstream run as a configuration snapshot, so that
run can later be retried or replayed on its own — the upstream pipeline does not
need to run again.

---

## 10. Environment Variables

Runtime behaviour is configured through the environment or a `.env` file next to
the config.

| Variable | Default | Purpose |
| --- | --- | --- |
| `PIPLY_CONFIG` | discovered | path to `piply.yaml` |
| `PIPLY_DATABASE` | `<config>/.piply/piply.db` | SQLite file path, or a PostgreSQL DSN, see §11 |
| `PIPLY_DEFAULT_MAX_PARALLEL_TASKS` | `4` | default worker count |
| `PIPLY_STALE_RUN_TIMEOUT_SECONDS` | `3600` | heartbeat age before a run is interrupted |
| `PIPLY_HEARTBEAT_INTERVAL_SECONDS` | `10` | run heartbeat cadence |
| `PIPLY_SCHEDULER_POLL_INTERVAL_SECONDS` | `10` | scheduler tick interval |
| `PIPLY_RECONCILE_INTERVAL_SECONDS` | `15` | cooldown between full stale-run scans |
| `PIPLY_QUEUE_DISPATCH_BATCH_SIZE` | `100` | trigger queue batch size |
| `PIPLY_QUEUE_DISPATCH_STALE_SECONDS` | `300` | requeue abandoned dispatches after |
| `PIPLY_UPCOMING_RUN_PREVIEW_COUNT` | `8` | upcoming slots shown in the UI |
| `PIPLY_RETENTION_RUN_DAYS` | `30` | `piply prune` run age limit |
| `PIPLY_RETENTION_LOG_DAYS` | `14` | `piply prune` log age limit |
| `PIPLY_RETENTION_MAX_RUNS_PER_PIPELINE` | `200` | `piply prune` per-pipeline cap |
| `PIPLY_ARTIFACTS_DIR` | unset | extra allowed root for artifact downloads |
| `PIPLY_METRICS_ENABLED` | `true` | serve `GET /metrics` |
| `PIPLY_AUTH_ENABLED` | `false` | require authentication |
| `PIPLY_AUTH_USERNAME` / `PIPLY_AUTH_PASSWORD` | unset | UI basic auth |
| `PIPLY_API_TOKEN` | unset | API and `/metrics` bearer token |

Setting a username/password pair or an API token enables auth implicitly.

---

## 11. Runtime Storage And External Databases

Two different things get called "the database". They are unrelated: the store
Piply keeps its *own* state in, and the databases *your* pipelines read from.

### Piply's own runtime store

Runs, task runs, logs, task outputs, artifact records, the trigger queue, sensor
cursors, and scheduler metadata live in the metadata store. Two backends are
supported, selected by `PIPLY_DATABASE`.

| `PIPLY_DATABASE` | Backend | Driver |
| --- | --- | --- |
| unset | SQLite at `<config-dir>/.piply/piply.db` | built in |
| a file path, e.g. `/var/lib/piply/piply.db` | SQLite | built in |
| `postgresql://user:pass@host:5432/piply` | PostgreSQL | `psycopg` |

**SQLite is the default and needs no configuration.** It is the right choice for
a single node: zero setup, one file to back up, and WAL mode handles the
concurrency Piply generates.

**PostgreSQL is opt-in**, for deployments that already run a managed database
and would rather keep runtime state there than on a local volume:

```bash
pip install "mr-piply[postgres]"       # or: pip install psycopg
export PIPLY_DATABASE="postgresql://piply:secret@db.internal:5432/piply"
piply start
```

```
Runtime database: postgresql://piply:***@db.internal:5432/piply  (PostgreSQL)
```

The schema is created on first connect and migrated forward on every start, the
same as SQLite. Nothing else in a config changes; pipelines, sensors, and tasks
are unaffected by the choice of backend.

Accepted DSN spellings: `postgresql://`, `postgres://`, `postgresql+psycopg://`,
`postgresql+psycopg2://`. SQLAlchemy-style driver suffixes are stripped before
the DSN reaches the driver. Any other scheme is rejected at startup with the
reason.

To use a non-default schema, pass it in the DSN:

```bash
PIPLY_DATABASE="postgresql://piply:secret@db:5432/app?options=-csearch_path%3Dpiply"
```

#### What differs between the two backends

| | SQLite | PostgreSQL |
| --- | --- | --- |
| Setup | none | server, database, and a driver |
| `piply backup` / `restore` | supported | use `pg_dump` / `pg_restore` |
| `piply prune` | deletes and runs `VACUUM` | deletes; autovacuum reclaims space |
| Database size in diagnostics | file size | reported as 0, not Piply's to measure |
| Survives a container redeploy | only on a mounted volume | yes, it is outside the container |

Everything else — run history, retries, recovery, metrics, the UI — behaves
identically. The behaviour suite is run against both backends.

#### Still one instance at a time

A PostgreSQL store does **not** yet enable running several Piply instances
against one database. Ownership is tracked per process and writes are serialised
in-process, so run exactly one instance per database either way. Multi-instance
work is a proposal, not a feature — see
[FUTURE_FEATURES.md §4.1](FUTURE_FEATURES.md).

#### Migrating an existing SQLite store

There is no automatic import. The pragmatic options are:

1. **Start fresh.** Point `PIPLY_DATABASE` at PostgreSQL and let the new store
   build itself. History stays in the old file if you keep it.
2. **Keep the old file for reference.** Run `piply backup /backups` before
   switching, so the previous history remains readable by pointing a throwaway
   `PIPLY_DATABASE` back at the snapshot.

Configuration, pipelines, and schedules need no changes; only the run history
lives in the store.

### Docker and other ephemeral filesystems

The default location is next to your config. **In a container that is inside the
image's writable layer, which is destroyed every time the container is
replaced.** A redeploy then silently starts with an empty runtime.

`piply start` prints the path it will use, so this is visible on first boot:

```
Using config: /app/piply.yaml
Runtime database: /app/.piply/piply.db  (default location; set PIPLY_DATABASE to move it)
```

The fix is a volume plus an explicit `PIPLY_DATABASE` outside the app directory:

```yaml
services:
  piply:
    image: my-org/piply-project:latest
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

If the image runs as a non-root user, create and chown those directories in the
`Dockerfile` **before** declaring the volume. Docker seeds a fresh named volume
from the image path including its ownership; if the path does not exist in the
image the volume is created root-owned and the app cannot write to it:

```dockerfile
RUN mkdir -p /var/lib/piply /var/lib/piply-artifacts     && chown --recursive appuser:appuser /var/lib/piply /var/lib/piply-artifacts
USER appuser
VOLUME ["/var/lib/piply", "/var/lib/piply-artifacts"]
```

Artifacts are recorded by path rather than copied into the database, so if any
task declares `artifacts:` those files need a durable location too.

### Backup and restore

```bash
piply backup /backups                    # timestamped file in that directory
piply backup /backups/before-deploy.db   # explicit filename
piply restore /backups/before-deploy.db
```

`backup` uses SQLite's online backup API, so it is safe against a running server
and a database with an active WAL; there is no need to stop the scheduler.
`restore` replaces the database and keeps the displaced file alongside it as
`piply.db.replaced`. Stop the server before restoring, since a running scheduler
holds handles to the file it is about to lose.

A backup before each deploy is a cheap safety net even once volumes are in
place:

```bash
piply backup /backups && docker compose up -d
```

### Your databases — reached from sensors and tasks

`sql_sensor` connects to external databases directly:

```yaml
connections:
  warehouse: ${secret:WAREHOUSE_DSN}
  reporting: postgresql://piply:${PGPASSWORD}@db.internal:5432/reporting
  local: sqlite:///app.db

pipelines:
  on_new_claims:
    sensors:
      claims:
        type: sql_sensor
        connection_ref: warehouse
        table: claims
        cursor_column: claim_id
    tasks:
      process:
        type: cli
        command: python process_claims.py
```

| Scheme | Driver |
| --- | --- |
| `sqlite`, `sqlite3`, `sqlite+pysqlite` | built in |
| `postgres`, `postgresql`, `postgresql+psycopg` | `psycopg`, falling back to `psycopg2` |
| `postgresql+psycopg2` | `psycopg2` |
| `mysql`, `mysql+pymysql`, `mariadb`, `mariadb+pymysql` | `pymysql` |
| `mysql+mysqlconnector` | `mysql-connector-python` |
| `mssql`, `mssql+pyodbc`, `sqlserver`, `odbc` | `pyodbc` |

Drivers are imported lazily and are **not** Piply dependencies. Install only
what you use:

```bash
pip install psycopg      # PostgreSQL
pip install pymysql      # MySQL / MariaDB
pip install pyodbc       # SQL Server
```

A missing driver or unsupported scheme marks that sensor `failing` with the
reason, visible on the Diagnostics page and at `GET /api/sensors`. It does not
crash the scheduler or affect other sensors.

Relative `sqlite:///` paths resolve against the workspace. Passwords are
redacted wherever a sensor is displayed or logged.

For everything other than sensing, connect from inside the task — that is where
your existing client, pooling, and migrations already live:

```yaml
secrets:
  backend: env

pipelines:
  warehouse_load:
    env:
      DATABASE_URL: ${secret:WAREHOUSE_DSN}
    tasks:
      load:
        type: python
        path: jobs/load.py        # reads os.environ["DATABASE_URL"]
```

---

## 12. Aliases And Legacy Keys

Every key below is accepted by the loader. They exist for backward
compatibility or convenience; the preferred spelling is on the left.

### Task keys

| Preferred | Also accepted | Notes |
| --- | --- | --- |
| `timeout` | `timeout_seconds` | identical behaviour |
| `kill_grace_period` | `kill_grace_period_seconds` | identical behaviour |
| `artifacts` | `artifact_paths` | identical behaviour |
| `path` | `script` | script path for `python` and `cli` tasks |
| `call` | `callable` | `module:function` or `/path.py::function` |
| `title` | `name` | display name |
| `type: python` | `type: python_call` | the two python shapes were merged |
| `on_upstream_failure: fail` | `fail_if_upstream_failed: true` | boolean form |
| `on_upstream_failure: continue` | `continue_if_upstream_failed: true` | boolean form |

Setting both boolean flags on one task is a config error. Mixing a boolean flag
with `on_upstream_failure` is allowed; the boolean wins.

### Pipeline keys

| Preferred | Also accepted | Notes |
| --- | --- | --- |
| `timeout` | `timeout_seconds` | identical behaviour |
| `title` | `name` | display name |
| `tasks` | `entrypoint` / `script` | see below |

A pipeline with no `tasks` mapping but an `entrypoint` or `script` key is
loaded as a single-task pipeline named `main`. This is the pre-multi-task
config shape and is kept working, not recommended for new configs:

```yaml
# Legacy single-task form
pipelines:
  nightly:
    script: jobs/run.py
    args: ["--full"]
    working_dir: .

# Equivalent modern form
pipelines:
  nightly:
    tasks:
      main:
        type: python
        path: jobs/run.py
        args: ["--full"]
        cwd: .
```

`entrypoint` accepts `path`, `python`, `args`, `cwd`, and `env` sub-keys.
`working_dir` is the legacy spelling of `cwd`.

### Root keys

| Preferred | Also accepted |
| --- | --- |
| `pipelines` | `jobs` |
| `template` (in a deployment) | `pipeline_template` |

### Sensor keys

| Preferred | Also accepted | Notes |
| --- | --- | --- |
| `connection` | `database_url`, `dsn` | inline connection string |
| `connection_ref` | `connection_name`, `connection_id` | name from root `connections` |
| — | `connection_env` | read the connection string from this env var |
| `url` | `endpoint` | api_sensor target |
| `title` | `name` | display name |
| `path` | `database` | sqlite file for a `sql_sensor` |

`ssh_binary` is also available on a remote `file_sensor`, with the same meaning
as on an `ssh` task.

Four ways to point a `sql_sensor` at the same database:

```yaml
connections:
  warehouse: ${secret:WAREHOUSE_DSN}

sensors:
  a: {type: sql_sensor, table: events, connection_ref: warehouse}
  b: {type: sql_sensor, table: events, connection: "@warehouse"}
  c: {type: sql_sensor, table: events, connection: "postgresql://..."}
  d: {type: sql_sensor, table: events, connection_env: WAREHOUSE_DSN}
```

---

## 13. Status Values

| Run status | Meaning |
| --- | --- |
| `queued` | created, not started |
| `running` | executing |
| `success` | every task finished acceptably |
| `failed` | at least one task failed |
| `timed_out` | a task or the pipeline exceeded its timeout |
| `cancelled` | cancelled by a user |
| `interrupted` | the owning process stopped before the run finished |

Task statuses are the same set plus `skipped` (disabled, false `run_if`, or an
unsuccessful dependency).
