# Environment Variables

Windows installs include `tzdata` for cron timezones. Minimal POSIX images
without an IANA database must install OS timezone data or `python -m pip
install tzdata`; otherwise even `timezone: UTC` cron schedules cannot load.

v0.3.4 adds no required environment settings. Audit retention follows
`PIPLY_RETENTION_RUN_DAYS` (zero keeps history); run pause state is stored in the
database, not `.env`. For Conda, install and start Piply from the same activated
environment; see the [runbook](RUNBOOK.md) and [release assessment](RELEASE_CHECK.md).

Two separate things share the name "environment variable" in Piply:

1. **`${NAME}` in your config** — values you reference from YAML, usually secrets
2. **`PIPLY_*` settings** — knobs that change how Piply itself behaves

They are read from the same place and confused constantly, so this page keeps
them apart.

---

## Contents

1. [Where values come from](#1-where-values-come-from)
2. [Using `${NAME}` in config](#2-using-name-in-config)
3. [Piply settings reference](#3-piply-settings-reference)
4. [What a task sees](#4-what-a-task-sees)
5. [The service problem](#5-the-service-problem)

---

## 1. Where values come from

In order, later winning:

1. `.env` beside `piply.yaml`
2. The real process environment

A variable already exported in the process **beats** `.env`. That is deliberate —
a container or systemd unit should be able to override a file committed to the
repo — but it is also why editing `.env` sometimes appears to do nothing.

`.env` is gitignored. Keep secrets there and out of YAML; a webhook URL or
password written literally in a config is a credential in your git history.

---

## 2. Using `${NAME}` in config

```yaml
notifications:
  teams:
    oncall:
      type: channel
      webhook: ${TEAMS_ONCALL_WEBHOOK}

pipelines:
  export:
    tasks:
      push:
        type: cli
        command: "aws s3 cp out.csv s3://bucket/ --profile ${AWS_PROFILE}"
```

`${NAME}`, `$NAME`, and `%NAME%` are all accepted. An unset variable is **left in
place verbatim** rather than blanked, so the mistake is visible rather than
silent.

For config values that are not secrets, prefer `{name}` config variables — they
are scoped, can be conditional, and show up in `piply plan`. See
[Variables](VARIABLES.md).

### The `env` condition

`env` in a conditional falls back to `PIPLY_ENV`, so the common
deployment-stage check works without declaring anything:

```yaml
variables:
  workers: 8 if env == "production" else 2
```

```bash
PIPLY_ENV=production piply start
```

---

## 3. Piply settings reference

### Core

| Variable | Default | Purpose |
| --- | --- | --- |
| `PIPLY_CONFIG` | discovered | Path to `piply.yaml` |
| `PIPLY_DATABASE` | `.piply/piply.db` | SQLite path or a PostgreSQL DSN |
| `PIPLY_ARTIFACTS_DIR` | unset | Extra directory artifact downloads may read from |
| `PIPLY_ENV` | unset | Read by the `env` condition |
| `PIPLY_BASE_URL` | unset | Public URL, used to link back to a run from an alert |

`PIPLY_DATABASE` set in the process **wins over** the Settings page, which is
why changing the database in the UI can appear to do nothing. Change it in the
compose file or unit instead, then restart.

### Scheduler and execution

| Variable | Default | Purpose |
| --- | --- | --- |
| `PIPLY_SCHEDULER_ENABLED` | `true` | `false` starts the UI with no scheduler |
| `PIPLY_SCHEDULER_POLL_INTERVAL_SECONDS` | `10` | How often schedules are checked |
| `PIPLY_DEFAULT_MAX_PARALLEL_TASKS` | `4` | Default task concurrency per run |
| `PIPLY_QUEUE_DISPATCH_BATCH_SIZE` | `100` | Queue items claimed per dispatch |
| `PIPLY_QUEUE_DISPATCH_STALE_SECONDS` | `300` | When a claimed queue item is considered abandoned |
| `PIPLY_HEARTBEAT_INTERVAL_SECONDS` | `10` | Worker liveness interval |
| `PIPLY_RECONCILE_INTERVAL_SECONDS` | `15` | How often stale runs are reconciled |
| `PIPLY_STALE_RUN_TIMEOUT_SECONDS` | `3600` | When a `running` run with no heartbeat is failed |

`PIPLY_SCHEDULER_ENABLED=false` in a project `.env` is the fix for "opening my
laptop starts last night's pipelines". The UI, the API, and manual runs all keep
working; the header chip and the startup log both say the scheduler is off, so a
quiet install is not mistaken for a broken one.

### Retention

| Variable | Default | Purpose |
| --- | --- | --- |
| `PIPLY_RETENTION_RUN_DAYS` | `30` | Delete finished runs older than this |
| `PIPLY_RETENTION_LOG_DAYS` | `14` | Delete log lines older than this |
| `PIPLY_RETENTION_MAX_RUNS_PER_PIPELINE` | `200` | Cap runs kept per pipeline |

Applied by `piply prune`. Logs are usually the largest table — see
[the CLI reference](CLI.md#7-database-and-housekeeping).

### Interface

| Variable | Default | Purpose |
| --- | --- | --- |
| `PIPLY_PIPELINE_RUN_HISTORY_COUNT` | `5` (max 20) | Run dots per pipeline row |
| `PIPLY_UPCOMING_RUN_PREVIEW_COUNT` | `8` | Upcoming runs previewed |
| `PIPLY_METRICS_ENABLED` | `true` | Expose the metrics endpoint |

### Authentication

| Variable | Purpose |
| --- | --- |
| `PIPLY_AUTH_ENABLED` | Force authentication on |
| `PIPLY_AUTH_USERNAME` / `PIPLY_AUTH_PASSWORD` | Fallback admin, for installs predating accounts |
| `PIPLY_ADMIN_USERNAME` / `PIPLY_ADMIN_PASSWORD` | Bootstrap the first admin |
| `PIPLY_API_TOKEN` | Token for API clients |
| `PIPLY_SESSION_SECRET` | Signs session cookies — set this in production |

Setting any credential switches authentication on. Full model in
[Authentication](AUTHENTICATION.md).

Any of these may be given as `<NAME>_FILE` pointing at a file instead, for Docker
secrets.

### Mail

`PIPLY_SMTP_*` configures email delivery, including `PIPLY_SMTP_PASSWORD`. See
[Notifications](NOTIFICATIONS.md).

---

## 4. What a task sees

Every task process is given:

| Variable | Contents |
| --- | --- |
| `PIPLY_RUN_ID` | The id of the run it belongs to |
| `PIPLY_CONTEXT_JSON` | Run context as JSON — parameters, tenant, upstream outputs |

```python
import json, os

run_id = os.environ["PIPLY_RUN_ID"]
context = json.loads(os.environ.get("PIPLY_CONTEXT_JSON", "{}"))
```

On top of that, a task inherits — in order, later winning — the process
environment, `defaults.env`, the pipeline's `env:`, and the task's own `env:`.
See [Task environment precedence](YAML_SPECIFICATION.md#task-environment-precedence).

---

## 5. The service problem

The single most common environment issue: **a variable exported in your shell is
not visible to the scheduler.**

A pipeline works from the terminal and fails on schedule, because `piply start`
ran as a systemd unit, a Windows service, or a container — none of which inherit
the shell you exported it in.

Put it in `.env` beside `piply.yaml`, or in the unit's own `Environment=` /
the container's `environment:`. Then check what Piply actually resolved:

```bash
piply diagnostics
```

### A related trap: CA bundles

`SSL_CERT_FILE`, `SSL_CERT_DIR`, `REQUESTS_CA_BUNDLE`, and `CURL_CA_BUNDLE` are
read by Python but **ignored by `curl`**. A rebuilt conda environment leaves one
pointing at a bundle that no longer exists, and the symptom is a webhook that
works from your terminal and fails from Piply.

Piply now ignores a certificate path that does not exist, falls back to the
system trust store, and logs it once. A path that *does* exist is always
honoured, so a corporate CA bundle keeps working. See
[Notifications](NOTIFICATIONS.md#7-when-delivery-fails).

---

## Related

Development/test-only variables are not application settings:

- `PIPLY_TEST_POSTGRES_DSN` selects an expendable PostgreSQL database for parity tests.
- `PIPLY_TEST_POSTGRES_RESET=1` explicitly permits those tests to drop/recreate its
  public schema. Do not put either variable in a production service environment.
  Without a test DSN, PostgreSQL tests skip; with a DSN but no reset consent, they
  fail before connecting or deleting schema data.

See the [release assessment](RELEASE_CHECK.md) for current evidence and CI gates.

- [Variables](VARIABLES.md) — `{name}` config variables and their scopes
- [CLI Reference](CLI.md) — commands these settings affect
- [Authentication](AUTHENTICATION.md) — the credential variables in context
- [Metadata Store](DATABASE.md) — `PIPLY_DATABASE` in depth
- [Security](SECURITY.md) — what to set before exposing Piply
