# Piply Execution Examples

Runnable patterns, each with the YAML and the commands that exercise it.

---

## 1. Linear pipeline

```yaml
version: "1"
title: Daily ETL
workspace: .

pipelines:
  daily_etl:
    schedule:
      cron: "0 2 * * *"
    tasks:
      extract:
        type: python
        path: jobs/extract.py
      transform:
        type: python
        path: jobs/extract.py
        function: transform_data
        depends_on: [extract]
      load:
        type: cli
        command: python jobs/load.py
        depends_on: [transform]
```

```bash
piply plan daily_etl        # preview without running
piply run daily_etl         # run and stream logs
piply runs --limit 5
```

---

## 2. Parallel fan-out with priority

```yaml
pipelines:
  nightly:
    execution: parallel
    max_parallel_tasks: 4
    tasks:
      fetch:
        type: cli
        command: python fetch.py

      billing_report:
        type: cli
        priority: critical            # highest, runs first once ready
        depends_on: [fetch]
        command: python report.py --kind billing

      analytics_report:
        type: cli
        priority: low
        depends_on: [fetch]
        command: python report.py --kind analytics

      archive:
        type: cli
        depends_on: [billing_report, analytics_report]
        command: python archive.py
```

`fetch` runs first because of the dependency. `billing_report` beats
`analytics_report` because of priority. Priority never reorders dependencies.

---

## 3. Timeouts

```yaml
pipelines:
  bounded:
    timeout: 30m                       # whole-run ceiling
    tasks:
      slow_query:
        type: cli
        timeout: 5m                    # per-task ceiling
        kill_grace_period: 10          # terminate, wait 10s, then kill
        command: python query.py

      publish:
        type: api
        depends_on: [slow_query]
        url: https://example.com/publish
        method: POST
        timeout: 30s                   # HTTP timeout
```

An overrun writes the reason to the task log, terminates the process, and ends
the run as `timed_out`.

---

## 4. Conditional execution

```yaml
variables:
  report: payment

pipelines:
  conditional:
    tasks:
      payment_export:
        type: cli
        run_if: "{report} == 'payment'"
        command: python export.py --kind payment

      refund_export:
        type: cli
        run_if: "{report} == 'refund'"
        command: python export.py --kind refund

      staging_only:
        type: cli
        run_if: "{environment} in ['staging', 'dev']"
        command: python seed_demo_data.py
```

A false condition marks the task `skipped`; the run still succeeds.

---

## 5. Entity expansion

```yaml
pipelines:
  per_report:
    entities:
      report: [payment, adjustment, refund]
    execution: parallel
    max_parallel_tasks: 3
    tasks:
      extract:
        type: python
        path: jobs/extract.py
        function: extract_data
        kwargs:
          report: "{report}"

      validate:
        type: cli
        depends_on: [extract]
        command: python validate.py {report}

      summarize:
        type: python
        path: jobs/extract.py
        function: summarize_reports
        entities: false                # one summary, not one per report
        depends_on: [validate]
```

Runtime tasks become `payment.extract`, `adjustment.extract`, and so on.
`summarize` reads them through `context["mapped"]["validate"]`.

```bash
piply plan per_report        # shows the expanded entity set and every command
```

---

## 6. Task output passing

```yaml
pipelines:
  chained:
    tasks:
      extract:
        type: python
        path: jobs/extract.py
        function: extract_data
      transform:
        type: python
        path: jobs/extract.py
        function: transform_data
        depends_on: [extract]
```

```python
def extract_data(records: int = 100) -> dict:
    return {"records": records, "chunks": 3}


def transform_data(context: dict) -> dict:
    # Declaring `context` opts the callable in; upstream outputs are keyed
    # by task id.
    upstream = context.get("extract") or {}
    return {"records": upstream.get("records", 0) + 1}
```

Subprocess tasks receive the same data as JSON in `PIPLY_CONTEXT_JSON`, along
with `PIPLY_RUN_ID` and `PIPLY_TASK_ID`.

---

## 7. Pipeline chaining with inherited variables and env

```yaml
pipelines:
  upstream:
    variables:
      batch: batch-77
    env:
      BATCH_ENV: env-77
    triggers_on_success:
      - downstream
    tasks:
      emit:
        type: cli
        command: python emit.py

  downstream:
    variables:
      batch: unset            # overridden by the upstream value at runtime
    tasks:
      consume:
        type: python
        path: jobs/consume.py
        args: ["{batch}"]
```

`downstream` receives `batch=batch-77`, `BATCH_ENV=env-77`, and every JSON
output from `upstream`. Inheritance is transitive: a third pipeline triggered by
`downstream` receives the same values.

Because the downstream run stores that configuration, it can be repaired alone:

```bash
piply backfill <downstream-run-id>          # replays the captured config
```

or from the run page, **Replay config**. The upstream pipeline does not re-run.

---

## 8. Artifacts

```yaml
pipelines:
  reporting:
    tasks:
      build:
        type: python
        path: jobs/build.py
        cwd: .
        artifacts:
          - "out/*.csv"
          - "out/manifest.json"
```

```bash
piply run reporting
piply artifacts <run-id>
```

The run page lists each file with its size and a download link.

---

## 9. Multi-tenant deployments

```yaml
pipeline_templates:
  tenant_ingest:
    schedule:
      cron: "0 * * * *"
    env:
      STAGE: production
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
  globex_ingest:
    template: tenant_ingest
    tenant: globex
    schedule:
      cron: "30 * * * *"

pipelines:
  tenant_report:
    tasks:
      report:
        type: cli
        command: python report.py
```

Both deployments appear on the Pipelines page grouped under
`Template: tenant_ingest`. See [MIGRATION.md](MIGRATION.md).

---

## 10. Sensors

A sensor polls an external source on every scheduler tick and enqueues a run
when the source changes. All three types are shown together below; each is
broken out afterwards.

```yaml
version: "1"
title: Sensor Examples
workspace: .

connections:
  app_db: sqlite:///app.db

pipelines:
  on_file:
    sensors:
      inbox:
        type: file_sensor
        path: inbox
        pattern: "*.csv"
        ignore_existing: true
    tasks:
      load:
        type: cli
        command: python load.py

  on_row:
    sensors:
      new_rows:
        type: sql_sensor
        connection_ref: app_db
        table: inbound_events
        cursor_column: id
        where: "processed = 0"
        ignore_existing: true
    tasks:
      process:
        type: cli
        command: python process.py

  on_api:
    sensors:
      feed:
        type: api_sensor
        url: https://example.com/api/events
        cursor_path: version
        expected_status: [200]
        ignore_existing: true
    tasks:
      sync:
        type: cli
        command: python sync.py
```

Sensors only poll while the server is running:

```bash
piply start          # scheduler polls each sensor every tick
piply diagnostics    # per-sensor status, poll counts, and last error
```

`ignore_existing: true` records the current state on the first poll without
firing, so starting the server does not immediately trigger a run for data that
was already there.

### `file_sensor`

Fires when new files appear.

```yaml
sensors:
  inbox:
    type: file_sensor
    path: inbox                 # relative to workspace
    pattern: "*.csv"
    recursive: false
    ignore_existing: true
    task_id: load               # optional: run one task, not the pipeline
```

The run log names the files that triggered it:

```
Triggered by sensor 'inbox'.
Detected new files: /srv/app/inbox/orders.csv
```

Watch a remote directory over SSH with an `sftp://` path:

```yaml
sensors:
  remote_drop:
    type: file_sensor
    path: sftp://etl@sftp.partner.com/outbound
    pattern: "*.zip"
    key_file: ~/.ssh/id_ed25519
    connect_timeout: 10
```

Piply shells out to `ssh` with `BatchMode=yes`, so key auth must already work.

### `sql_sensor`

Fires when a monotonically increasing column advances.

```yaml
sensors:
  new_rows:
    type: sql_sensor
    connection_ref: app_db      # a name from the root `connections` block
    table: inbound_events
    cursor_column: id           # must be increasing, e.g. an autoincrement id
    where: "processed = 0"      # optional filter
    ignore_existing: true
```

```
Triggered by sensor 'new_rows'.
Detected new rows in inbound_events from cursor 1 to 2.
```

`table` and `cursor_column` must be plain identifiers; they are validated before
being interpolated into SQL. `where` is passed through, so keep it literal
rather than building it from user input.

External databases are supported here — see section 11.

### `api_sensor`

Fires when a cursor value in an HTTP response changes.

```yaml
sensors:
  feed:
    type: api_sensor
    url: https://example.com/api/events
    method: GET
    headers:
      Accept: application/json
    token: ${secret:API_TOKEN}    # sent as Authorization: Bearer
    cursor_path: version          # dotted path, e.g. data.meta.version
    expected_status: [200]
    ignore_existing: true
```

```
Triggered by sensor 'feed'.
Detected API sensor change at https://example.com/api/events from cursor 1 to 2.
```

Without `cursor_path`, Piply looks for a `cursor`, `version`, `updated_at`,
`last_modified`, `id`, or `count` field, and falls back to a digest of the whole
body.

### When a sensor fails

A failing poll is recorded, never raised, so one unreachable source cannot stop
the others or crash the scheduler:

```bash
piply diagnostics
```

```
Sensors     : 2 healthy, 1 failing, 0 idle
  FAILING inventory/warehouse: OperationalError: could not connect to host
```

The same data is on the Diagnostics page, at `GET /api/sensors`, and as the
`piply_sensor_health` metric (`0` when the last poll failed):

```promql
piply_sensor_health == 0
```

Passwords in connection strings and URLs are redacted everywhere the sensor is
displayed.

---

## 11. External databases

Piply keeps **its own** runtime state (runs, tasks, logs, queue) in a local
SQLite file. That is not configurable — `PIPLY_DATABASE` is a file path, and a
server URL is rejected with an explanatory error.

*Your* databases are reached from `sql_sensor` and from tasks.

### From a sensor

```yaml
connections:
  warehouse: ${secret:WAREHOUSE_DSN}
  reporting: postgresql://piply:${PGPASSWORD}@db.internal:5432/reporting

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

Supported schemes and the driver each needs:

| Scheme | Driver to install |
| --- | --- |
| `sqlite`, `sqlite3`, `sqlite+pysqlite` | built in |
| `postgres`, `postgresql`, `postgresql+psycopg` | `psycopg` (falls back to `psycopg2`) |
| `postgresql+psycopg2` | `psycopg2` |
| `mysql`, `mysql+pymysql`, `mariadb`, `mariadb+pymysql` | `pymysql` |
| `mysql+mysqlconnector` | `mysql-connector-python` |
| `mssql`, `mssql+pyodbc`, `sqlserver`, `odbc` | `pyodbc` |

Drivers are imported lazily and are **not** Piply dependencies — install only
what you use:

```bash
pip install psycopg          # PostgreSQL
pip install pymysql          # MySQL / MariaDB
pip install pyodbc           # SQL Server
```

An unsupported scheme or a missing driver marks the sensor `failing` with the
reason, rather than crashing the scheduler.

### From a task

Tasks are ordinary processes, so use whatever client you already use. Piply's
job is to supply the credentials safely:

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
      dbt_run:
        type: cli
        command: dbt run --project-dir warehouse
        depends_on: [load]
```

This is the right layer for it: your task already has the connection pooling,
migrations, and typing it needs, and Piply stays free of database drivers.

---

## 12. Retry policy

```yaml
pipelines:
  flaky:
    retry:
      attempts: 2
      mode: resume            # reuse successful tasks
      delay_seconds: 30
    tasks:
      extract:
        type: cli
        command: python extract.py
      publish:
        type: cli
        depends_on: [extract]
        command: python publish.py
```

If `publish` fails, the automatic retry reuses `extract`'s result and re-runs
only `publish`. Manual equivalents:

```bash
piply tasks retry <run-id> publish --mode resume
piply tasks retry <run-id> publish --mode startover
```

---

## 13. Backfilling a schedule window

```bash
piply backfill nightly_report \
  --from 2026-07-01T00:00:00 \
  --to   2026-07-08T00:00:00
```

One run is queued per scheduled slot in the window. Slots that already ran are
skipped by the queue's dedupe key.

---

## 14. Following logs

```bash
piply logs --follow                          # everything, live
piply logs --follow --pipeline nightly       # one pipeline
piply logs --follow --task extract           # one task across runs
piply logs <run-id> --follow --no-color      # one run, plain output
```

Each line is rendered as `[time] [pipeline] [task] message`.

---

## 15. Retention

```bash
piply prune --dry-run                        # report only
piply prune --run-days 14 --max-runs 100     # override the configured window
piply prune --yes --no-vacuum                # unattended, skip VACUUM
```

Defaults come from `PIPLY_RETENTION_*`. Active runs are never deleted.

---

## 16. Monitoring

```bash
curl -s http://127.0.0.1:8000/metrics | head -20
curl -s http://127.0.0.1:8000/api/diagnostics | jq .scheduler
```

Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: piply
    static_configs:
      - targets: ["piply.internal:8000"]
    authorization:
      type: Bearer
      credentials: <PIPLY_API_TOKEN>
```

Useful alerts:

```promql
piply_scheduler_up == 0
piply_queue_oldest_age_seconds > 900
increase(piply_runs_failure_total[1h]) > 0
piply_sensor_health == 0
```
