# Sensors

A sensor watches something outside Piply and starts a pipeline when it changes.
Schedules answer "run at 03:00"; sensors answer "run when the file lands".

Three types, all polled by the same scheduler loop with no extra process:

| Type | Watches | Fires when |
| --- | --- | --- |
| [`file_sensor`](#2-file_sensor) | a local directory or an SFTP path | new files appear |
| [`sql_sensor`](#3-sql_sensor) | a table in any reachable database | a cursor column advances |
| [`api_sensor`](#4-api_sensor) | an HTTP endpoint | the response changes |

**Read [§6 What a sensor does *not* do](#6-what-a-sensor-does-not-do) before
building on this.** There is one limitation that surprises people.

---

## 1. How polling works

Sensors are evaluated on every scheduler tick — every 10 seconds by default,
set with `PIPLY_SCHEDULER_POLL_INTERVAL_SECONDS`. There is no separate sensor
daemon and no per-sensor interval.

Each sensor keeps a **cursor** in the `sensor_state` table. A poll compares the
world to that cursor:

```
poll -> unchanged  -> update state, do nothing
poll -> changed    -> update state, enqueue a trigger
poll -> error      -> record the failure, keep the old cursor, do not fire
```

This is what makes a sensor fire on *change* rather than on every poll. It also
means state survives restarts: a file that arrived while Piply was down is still
new when it comes back.

### Sensors can live in their own file

Sensors are declared inside the pipeline they trigger, but that block can live
in a different file from the tasks:

```yaml
# piply.yaml
include: [piply_pipe.yaml, piply_sensor.yaml]
```

```yaml
# piply_pipe.yaml — what runs
pipelines:
  ingest_files:
    tasks:
      load: {type: cli, command: python load.py}
```

```yaml
# piply_sensor.yaml — what triggers it
pipelines:
  ingest_files:
    sensors:
      inbox:
        type: file_sensor
        path: /mnt/237share/inbound
        pattern: "*.csv"
```

Different *blocks* of one pipeline may come from different files. The same block
in two files — two `tasks:` for one pipeline — is still an error naming both
files. See [YAML §3](YAML_SPECIFICATION.md#include).

### Sensors do not fire on what already exists

`ignore_existing` defaults to `true`. On the very first poll the sensor records
what is already there and stays quiet, so adding a sensor to a directory holding
2,000 old files does not immediately queue a run.

Set `ignore_existing: false` when the backlog *is* the work:

```yaml
sensors:
  backfill_inbox:
    type: file_sensor
    path: inbox
    ignore_existing: false     # the first poll fires for everything present
```

### Duplicate events are suppressed

Every event carries a `source_key` — the hash of the new filenames, the SQL
cursor value, or the API cursor. The trigger queue dedupes on
`sensor:{pipeline}:{sensor}:{source_key}`, so the same change cannot enqueue two
runs even if a poll overlaps a slow dispatch.

### A failing sensor never blocks the others

Each sensor is polled inside its own error boundary. One unreachable database
does not stop the file sensor next to it; the failure is recorded against that
sensor and polling continues.

### Paused pipelines are not polled

Pausing a pipeline stops its sensors too. When you resume, the cursor is still
where it was, so changes that happened while paused fire on the next poll.

---

## 2. `file_sensor`

Watches a directory for new files.

```yaml
pipelines:
  ingest:
    sensors:
      inbox:
        type: file_sensor
        path: sensor_inbox        # relative to workspace:
        pattern: "*.csv"          # default "*"
        recursive: false          # default false
        ignore_existing: true
    tasks:
      load:
        type: python
        path: pipelines/load.py
        function: load
```

| Key | Default | Purpose |
| --- | --- | --- |
| `path` | *required* | Directory to watch, or an `sftp://` URI |
| `pattern` | `*` | Glob applied to file names |
| `recursive` | `false` | Descend into subdirectories |
| `ignore_existing` | `true` | Skip files present at first poll |
| `task_id` | — | Run only this task, not the whole pipeline |
| `enabled` | `true` | Turn the sensor off without deleting it |
| `title` | derived | Label shown in the UI |

The sensor tracks the **set of known file paths**. A file that is modified in
place does not re-fire — only a path that was not there before counts as new.

> `path` resolves against `workspace:`, not the config file. This is the same
> rule as `env_file` and the same trap. If the sensor never fires, check the
> path first — a directory that does not exist is reported as a sensor failure
> on the Diagnostics page.

### What the triggered run receives

A sensor does not just start a pipeline — it tells it what changed. Every
sensor-triggered run gets variables usable in any `command:`, and the full event
in `context["sensor"]` for Python tasks.

```yaml
    tasks:
      announce:
        type: cli
        command: echo new file found: {sensor_file_name}
      load:
        type: cli
        command: python load.py --file "{sensor_file}"
```

| Variable | From | Example |
| --- | --- | --- |
| `{sensor_id}` | all | `drop` |
| `{sensor_type}` | all | `file_sensor` |
| `{sensor_file}` | `file_sensor` | full path of the first new file |
| `{sensor_file_name}` | `file_sensor` | `claims_2026.csv` |
| `{sensor_files}` | `file_sensor` | every new path, space separated |
| `{sensor_file_count}` | `file_sensor` | `1` |
| `{sensor_table}` | `sql_sensor` | `events` |
| `{sensor_cursor_from}` / `{sensor_cursor_to}` | `sql_sensor`, `api_sensor` | `41` / `57` |
| `{sensor_row_count}` | `sql_sensor` | `16` |
| `{sensor_url}` | `api_sensor` | the polled URL |

`{sensor_file}` is deliberately singular and first, because one file at a time is
the common case. When a poll finds several, `{sensor_files}` has them all and
`{sensor_file_count}` says how many.

A Python task gets the whole event:

```python
def process(context=None):
    sensor = (context or {}).get("sensor") or {}
    for path in sensor.get("new_files", []):
        print(f"processing {path}")
```

The run log also records it without any configuration, so a run always shows
what woke it:

```
Triggered by sensor 'drop'.
Detected new files: /mnt/237share/inbound/claims_2026.csv
```

> **Quote the path.** `--file "{sensor_file}"` rather than `--file {sensor_file}`
> — a share path can contain spaces, and an unquoted one becomes two arguments.

### Using a variable for the path

`path` is interpolated like any other value, so one sensor definition can watch
a different directory per environment:

```yaml
variables:
  share:
    if: env == "prod"
    then: /mnt/237share/inbound       # absolute: used exactly as written
    else: dev_inbox                   # relative: resolved against workspace:

pipelines:
  ingest:
    sensors:
      drop:
        type: file_sensor
        path: "{share}"
        pattern: "*.csv"
    tasks:
      load:
        type: cli
        command: python load.py --from {share}
```

`env` is a built-in that falls back to `PIPLY_ENV`, so nothing needs declaring —
start the server with `PIPLY_ENV=prod` and the same config watches the share
instead of the local folder. The same variable is available to the tasks, so the
sensor and the code that reads the files cannot drift apart.

The inline ternary works too, but the whole expression must be one quoted
string — see [conditional values](YAML_SPECIFICATION.md#variables):

```yaml
  share: '"/mnt/237share/inbound" if env == "prod" else "dev_inbox"'
```

The mapping form above is preferred for paths, because a path usually needs
quoting anyway and the two sets of quotes are easy to get wrong.

### Watching a network share (CIFS/SMB, NFS)

Piply has no share-mounting feature and does not need one. Mount the share at
the **operating system** level and give the sensor the ordinary local path —
verified working with an absolute path, for both the sensor and `{variable}`
interpolation in tasks.

Mount it persistently, not with a one-off `sudo mount`. A manual mount does not
survive a reboot, and Piply will start before you notice:

```
# /etc/fstab
//10.15.51.237/Ddump$  /mnt/237share  cifs  credentials=/etc/piply/smb.cred,uid=piply,gid=piply,file_mode=0640,dir_mode=0750,iocharset=utf8,vers=3.0,_netdev,nofail  0  0
```

```
# /etc/piply/smb.cred  — chmod 600, owned by root
username=svc_piply
password=...
domain=YOURDOMAIN
```

Then use it like any other directory:

```yaml
variables:
  share: /mnt/237share

pipelines:
  vendor_ingest:
    sensors:
      drop:
        type: file_sensor
        path: /mnt/237share/inbound      # absolute paths are used as-is
        pattern: "*.csv"
    tasks:
      archive:
        type: cli
        command: cp {share}/inbound/*.csv {share}/archive/
```

Points that actually bite:

- **`uid=`/`gid=` matter.** CIFS does not map users; every file appears owned by
  whoever you specify. Set it to the account Piply runs as, or the sensor sees
  the directory and the task cannot read the files.
- **`nofail` and `_netdev`.** Without them a share that is down at boot can stop
  the machine from finishing startup.
- **A dropped mount is reported, not silent.** If the share disappears the
  sensor turns `failing` with `Watched path does not exist: /mnt/237share/...`,
  shown on the Diagnostics page and sorted to the top of the sensor list. It
  does not stop other sensors.
- **An empty mount point looks like an empty directory.** If the mount silently
  fails, `/mnt/237share` still exists — it is just empty — so the sensor reports
  healthy and never fires. Watch a subdirectory that only exists when mounted
  (`/mnt/237share/inbound`), so a failed mount shows up as a failing sensor
  rather than as silence.
- **Writes are the share's business.** A task writing to `/mnt/237share` needs
  write permission on the export *and* in `file_mode`/`dir_mode`.
- **Latency.** Every poll lists the directory over the network. On a large or
  slow share, widen the poll interval rather than watching a directory with tens
  of thousands of files.

If you cannot mount the share — no root, or a container without
`CAP_SYS_ADMIN` — use the SFTP form below instead.

### Watching a remote directory over SFTP

Give `path` an `sftp://` URI and the sensor lists the remote directory over SSH
instead. It shells out to the `ssh` binary, so it uses your existing SSH config
and keys — no new dependency.

```yaml
sensors:
  vendor_drop:
    type: file_sensor
    path: sftp://etl@sftp.vendor.com/inbound/claims
    pattern: "*.zip"
    key_file: ~/.ssh/vendor_ed25519
    connect_timeout: 8
```

Host, user, and port are taken from the URI. Override any of them explicitly
with `host`, `user`, and `port`, which win over the URI. `ssh_binary` changes
which executable is invoked.

---

## 3. `sql_sensor`

Watches a table and fires when a monotonically increasing column advances.

```yaml
connections:
  app_db: postgresql://reader:${DB_PASSWORD}@db.internal:5432/app

pipelines:
  process_events:
    sensors:
      new_events:
        type: sql_sensor
        connection: "@app_db"      # "@name" refers to the connections block
        table: events
        cursor_column: id          # default "rowid"
        where: "status = 'ready'"
    tasks:
      process: {type: cli, command: python process.py}
```

| Key | Default | Purpose |
| --- | --- | --- |
| `table` | *required* | Table to watch |
| `connection` | — | DSN, or `@name` from `connections:` |
| `database` | — | SQLite file path, instead of `connection` |
| `cursor_column` | `rowid` | Column that only increases |
| `where` | — | Extra filter applied to the cursor query |
| `ignore_existing` | `true` | Skip rows present at first poll |
| `task_id` | — | Run only this task |

You must supply either `connection` or `database`, plus `table`.

### Choosing the connection

Four equivalent forms, in the order they are resolved:

```yaml
connection: "@app_db"                     # from the connections: block
connection: postgresql://user:pw@host/db  # inline DSN
connection_env: APP_DATABASE_URL          # read from an environment variable
database: local/app.db                    # a SQLite file, workspace-relative
```

Prefer `@name` or `connection_env`. An inline DSN with a password ends up in the
config file, and while the value is masked everywhere Piply displays it, it is
still in git.

> **`@name` must resolve to a connection string, not a file path.** A common
> slip is `connections: {app_db: local/app.db}` with `connection: "@app_db"`.
> That is a path, so it has no scheme and the sensor fails with a message
> telling you to use `database:` or a `sqlite:///` DSN. For a SQLite file use
> `database: local/app.db`, or write the connection as
> `sqlite:///local/app.db`.

### What the query actually does

```sql
SELECT COALESCE(MAX(cursor_column), 0), COUNT(*) FROM table [WHERE ...]
```

Consequences worth knowing:

- **The cursor must be numeric and increasing.** `MAX()` on a UUID or a text
  column will not behave. Use an identity column or an epoch timestamp.
- **Deletes do not fire.** The cursor only moves forward, so removing rows is
  invisible. `row_count` is reported in the event but is not what triggers it.
- **An updated row does not fire** unless the update raises the cursor column.
- `table` and `cursor_column` must be plain identifiers. They are interpolated
  into SQL, so they are validated against an identifier pattern and rejected
  otherwise — this is why you cannot put an expression in `cursor_column`.
- `where` is **not** validated that way. Treat it as trusted config, like a
  `command:`, and never build it from user input.

### Which databases work

Whatever the installed driver supports — PostgreSQL, MySQL, SQL Server, Oracle,
SQLite. This is unrelated to `PIPLY_DATABASE`, which is where Piply keeps its
*own* state. See [Metadata Store](DATABASE.md) for the distinction.

---

## 4. `api_sensor`

Polls an HTTP endpoint and fires when the response changes.

```yaml
sensors:
  upstream_ready:
    type: api_sensor
    url: https://api.vendor.com/v1/batches/latest
    method: GET                    # default GET
    headers:
      Accept: application/json
    token: ${VENDOR_API_TOKEN}     # sent as Authorization: Bearer ...
    cursor_path: data.batch_id     # what to compare between polls
    expected_status: [200]         # default [200, 201, 202, 204]
```

| Key | Default | Purpose |
| --- | --- | --- |
| `url` | *required* | Endpoint to poll |
| `method` | `GET` | HTTP method |
| `headers` | `{}` | Extra request headers |
| `token` | — | Shorthand for a Bearer `Authorization` header |
| `body` | — | Request body, for `POST` polling |
| `cursor_path` | — | Dotted path into the JSON response |
| `expected_status` | `200,201,202,204` | Anything else is a sensor failure |

### How the cursor is chosen

In order:

1. **`cursor_path`**, if set and present in the JSON — `data.batch_id` reads
   `response["data"]["batch_id"]`. Always prefer this; it is the only form you
   control.
2. Otherwise the first of `cursor`, `version`, `updated_at`, `last_modified`,
   `id`, `count` found at the top level of a JSON object.
3. Otherwise a **hash of the whole response body**.

The fallback to hashing is why an endpoint returning a timestamp, a request id,
or anything else that changes every call will fire on every poll. If you see a
sensor firing constantly, set `cursor_path` at a field that only changes when
the underlying data does.

The response is read up to 1 MB. Only that prefix is hashed.

---

## 5. Scoping a sensor to one task

By default a sensor triggers the whole pipeline. `task_id` narrows it to a
single task and its upstream dependencies:

```yaml
sensors:
  inbox:
    type: file_sensor
    path: inbox
    task_id: load_raw        # runs load_raw and whatever it depends on
```

Useful when one pipeline has both a scheduled full run and an event-driven
partial one.

---

## 6. What a sensor does *not* do

**The sensor payload does not reach the task.** This is the most important
thing on this page.

A `file_sensor` knows exactly which files are new, and records them in the event
payload — but the triggered task's `context` contains only `runtime_task_id`.
The task is told *that* something changed, not *what*:

```python
def load(context):
    print(sorted(context))        # ['runtime_task_id']
    context.get("new_files")      # None
```

So write tasks that **rescan the source themselves**:

```python
def load(context):
    # Correct: find the work yourself. The sensor is a wake-up call.
    for path in sorted(pathlib.Path("inbox").glob("*.csv")):
        process(path)
        path.rename(ARCHIVE / path.name)   # so the next run does not redo it
```

This pattern is more robust anyway — a task that rescans recovers correctly from
a missed event, a crash, or a manual re-run, none of which a payload-driven task
handles.

Passing the payload through to `context` is on the
[roadmap](FUTURE_FEATURES.md).

Also not supported:

- **Per-sensor poll intervals.** Every sensor is polled on the scheduler tick.
- **Deletion or modification events.** All three types detect *additions* and
  *forward movement* only.
- **Webhook-style push.** Sensors poll. For push, have the sender call
  `POST /api/pipelines/{id}/run` — see [the API reference](API.md).

---

## 7. Monitoring sensors

Sensor health is on the **Diagnostics** page and at `GET /api/sensors`:

| Status | Meaning |
| --- | --- |
| `healthy` | Last poll succeeded |
| `failing` | Last poll raised; `consecutive_failures` counts how many in a row |
| `idle` | Configured but never polled yet |

Each sensor also reports `last_polled_at`, `last_success_at`, `last_event_at`,
`last_error`, `poll_count`, and `event_count`.

Two things to watch:

- **`poll_count` rising with `event_count` flat** is normal for a quiet sensor,
  and indistinguishable from a broken one. Check `last_event_at` against when
  you know data last arrived.
- **`event_count` rising every poll** on an `api_sensor` means the cursor is
  unstable — set `cursor_path`.

Prometheus exposes per-sensor health at `/metrics`. A sensor that has been
`failing` for more than a few polls is worth alerting on: it fails *silently* in
the sense that nothing runs, and nothing running looks exactly like nothing
happening.

Connection strings and URL credentials are masked everywhere a sensor is
displayed — the UI, the API, the logs, and the event payload.

---

## 8. Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| Never fires | `path` is workspace-relative, not config-relative — check Diagnostics for a path error |
| Never fires, path is right | `ignore_existing: true` and the files were already there |
| Fires once then stops | Working as designed: the cursor advanced. Files must be *new* paths |
| Fires on every poll (`api_sensor`) | Response contains a timestamp or request id; set `cursor_path` |
| Fires on every poll (`sql_sensor`) | `cursor_column` is not monotonic |
| `must be simple identifiers` | `table` or `cursor_column` contains an expression |
| Task cannot see the new files | Expected — see [§6](#6-what-a-sensor-does-not-do) |
| Nothing polls at all | Pipeline is paused or `enabled: false` |

To test without waiting for the scheduler:

```python
from piply.core.service import PipelineService
service = PipelineService(config_path="piply.yaml")
print(service.poll_sensors())        # events enqueued
print(service.drain_trigger_queue()) # run ids dispatched
```

---

## Related

- [Notifications](NOTIFICATIONS.md) — being told when a sensor-triggered run fails

- [YAML Specification](YAML_SPECIFICATION.md) — every sensor key in the reference table
- [Execution Examples](EXAMPLES.md) — runnable sensor projects
- [Metadata Store](DATABASE.md) — `sensor_state` and `sensor_health` tables
- [UI Guide](UI_GUIDE.md#diagnostics) — where sensor health is shown
