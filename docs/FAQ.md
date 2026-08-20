# FAQ

Questions that come up while building pipelines with Piply, with the answers
verified against the current code rather than from memory.

If you are looking for the exhaustive key list, that is the
[YAML Specification](YAML_SPECIFICATION.md). This is the "why is it doing that"
document.

**Jump to:** [Setup](#1-setup-and-first-run) · [Config](#2-config-and-yaml) ·
[Variables and env](#3-variables-environment-and-secrets) · [Tasks](#4-tasks) ·
[Passing data](#5-dependencies-and-passing-data) · [Entities](#6-entities) ·
[Templates](#7-templates-and-deployments) ·
[Conditionals and priority](#8-conditionals-and-priority) ·
[Scheduling](#9-scheduling-sensors-and-triggers) ·
[Failures](#10-retries-timeouts-and-failure-handling) ·
[Debugging](#11-running-and-debugging) · [UI](#12-the-ui) ·
[Auth](#13-authentication) · [Deployment](#14-deployment-and-the-database) ·
[Performance](#15-performance-and-limits) ·
[Error index](#16-error-message-index) ·
[Quick answers](#17-quick-answers)

---

## 1. Setup and first run

### How do I start a new project?

```bash
piply init my-project
cd my-project
piply validate
piply start
```

`piply init` writes a working `piply.yaml`, example scripts, and a `.env`. The
generated project runs as-is, so you can change one thing at a time.

### Where does Piply look for the config?

In order: `--config`, then `PIPLY_CONFIG`, then `piply.yaml`, `piply.yml`, and
`piply-demo/piply.yaml` in the current directory. Every command accepts
`--config`/`-c`.

### Do I need a database, broker, or scheduler service?

No. Piply is one process with a SQLite file. No Redis, no Celery, no separate
scheduler daemon. PostgreSQL is available if you want the state elsewhere — see
[Metadata Store](DATABASE.md) — but it is opt-in.

### What Python version?

3.10+. Eight runtime dependencies: fastapi, uvicorn, pydantic, jinja2, pyyaml,
typer, httpx, python-dotenv.

### The port is already in use.

```bash
piply start --port 8080
piply start --host 0.0.0.0 --port 8080   # bind on all interfaces
```

Piply binds `127.0.0.1` by default. If you change that, put it behind a proxy
and turn on authentication.

### How do I stop it?

`piply stop` from the project directory, or Ctrl+C in the foreground. Both wind
running tasks down cleanly and mark them `interrupted` rather than leaving
orphaned `running` rows.

---

## 2. Config and YAML

### What is the minimum valid config?

```yaml
version: "1"
title: My Project
workspace: .

pipelines:
  hello:
    tasks:
      greet:
        type: cli
        command: echo hello
```

### What does `workspace:` actually do?

It is the base directory that **relative paths resolve against** — script paths,
`cwd`, `env_file`, artifacts. It is not the config's directory unless you set
`workspace: .`.

This is the single most common source of "my file isn't found". See
[the `env_file` question](#my-env_file-is-being-ignored) below, which is the
same problem wearing a different hat.

### Do I have to restart after editing `piply.yaml`?

No. The config is reloaded when its modification time changes. A running run
keeps the definition it started with; the next run picks up the edit.

Structural mistakes are caught at load time, so a broken edit leaves the last
good config in place rather than taking the server down.

### Can I split the config across multiple files?

Not currently. One config file per project. Use
[templates and deployments](#7-templates-and-deployments) to remove the
repetition that usually motivates splitting.

### How do I check my config without running anything?

```bash
piply validate          # structure, references, cycles
piply plan              # everything above, plus fully resolved commands
piply plan --json       # same, machine readable
```

`piply plan` is the one to reach for. It shows the exact command each task will
run with every variable substituted, and warns about unresolved placeholders.

---

## 3. Variables, environment, and secrets

This section covers the majority of real support questions.

### What is the difference between `{name}` and `${NAME}`?

| Syntax | Resolves from | When |
| --- | --- | --- |
| `{name}` | Piply `variables:`, entity values, deployment variables | config load, and at runtime for downstream values |
| `${NAME}` or `$NAME` | the process environment and `.env` | config load |
| `%NAME%` | the process environment (Windows form) | config load |

```yaml
variables:
  stage: ${PIPLY_ENV}          # from the environment
  target: "{stage}-warehouse"  # from another Piply variable
```

### An unresolved `{placeholder}` stayed in my command literally.

That is deliberate: an unknown placeholder is left alone rather than replaced
with an empty string, so the failure is visible instead of silently running the
wrong command.

`piply plan` reports them:

```
Task 'extract' still contains unresolved placeholder(s): {practice}
```

Usually it means the variable is defined on a *deployment* but you are running
the *template's* pipeline directly, or the name is misspelled.

### What is the env precedence order?

Later wins:

1. `defaults.env` (project-wide)
2. pipeline `env:`
3. pipeline `env_file:` / `env_files:`
4. task `env:`
5. the process environment, for anything not set above

> ⚠️ **`env_file` overrides inline `env:`, not the other way round.** If you set
> `DBT_TARGET: prod` under `env:` and your `.env` also has `DBT_TARGET=dev`, the
> task gets `dev`. This surprises most people. If you want the inline value to
> win, set it on the **task** instead of the pipeline.

### My `env_file` is being ignored.

Almost certainly the path. **`env_file:` resolves relative to `workspace:`, not
to the config file.**

```yaml
workspace: workspace       # <- the base for relative paths
pipelines:
  demo:
    env_file: .env         # <- looks for workspace/.env, NOT ./.env
```

A missing env file loads nothing and **does not raise** — the pipeline runs with
those variables simply absent, which is why this presents as "my credentials
aren't there" rather than as an error.

`piply validate` and `piply plan` warn about it and name the path they looked at:

```
1 warning(s):
  ! Pipeline 'demo' env_file '.env' was not found at /srv/app/workspace/.env.
    Paths resolve against workspace ('/srv/app/workspace'), not the config file.
    No variables were loaded from it.
```

It stays a warning rather than an error because an absent env file is
legitimate in some environments.

Fixes, pick one:

- move the file to the workspace directory, or
- use a path relative to the workspace: `env_file: ../.env`, or
- set `workspace: .` so the config directory is the base.

### How do I keep secrets out of the config?

Three options, in increasing order of how much you should like them:

```yaml
# 1. .env file
pipelines:
  demo:
    env_file: .env

# 2. explicit secrets block, with backends
secrets:
  backend: env
  prefix: PIPLY_SECRET_

# 3. environment only, nothing in the repo
variables:
  api_key: ${VENDOR_API_KEY}
```

For Piply's *own* credentials — API token, Basic password, bootstrap admin —
each setting has a `_FILE` variant that reads from a mounted file, which is
better than an environment variable on a server. See
[Security](SECURITY.md#4-deployment-checklist).

### Are secrets visible in the UI or logs?

Mostly no, with one thing to know:

- Sensor connection strings are masked (`postgresql://user:***@host/db`).
- The database DSN is masked everywhere it is printed.
- A run's stored configuration is masked in the API and UI.

**Masking is by variable name.** Anything containing `password`, `secret`,
`token`, `api_key`, `apikey`, `access_key`, `dsn`, `credential`, `auth`,
`private`, `connection_string`, or `signing` is hidden. A variable called
`DB_CONN` is **not** masked. Name secrets so they are recognisable.

Task *output* is never masked — if your script prints a credential to stdout, it
lands in the logs verbatim.

### How do I use a different value per environment?

```yaml
variables:
  stage: ${PIPLY_ENV}
  dbt_target: prod if stage == "prod" else dev
  threads:
    if: stage == "prod"
    then: 8
    else: 2
```

See [conditionals](#8-conditionals-and-priority).

---

## 4. Tasks

### What task types are there?

`python`, `cli`, `api`, `webhook`, `email`, `ssh`.

```yaml
tasks:
  from_script:
    type: python
    path: pipelines/extract.py
    function: extract_data     # omit to run the file as a script

  from_shell:
    type: cli
    command: python -m mypackage --tenant {tenant}
    cwd: .

  call_service:
    type: api
    url: https://api.example.com/v1/jobs
    method: POST
```

### `python` with `path` + `function`, or `cli` running `python`?

Use `type: python` when the code is yours and lives in the project — you get
return values passed to downstream tasks, and no subprocess overhead.

Use `type: cli` when you need a specific interpreter, a conda environment, or a
third-party CLI like `dbt`.

### My `cli` task can't find the command / uses the wrong shell.

Omit `shell:` and Piply uses the platform default. Only set `shell: bash` when
you genuinely need Bash syntax **and** Bash is on `PATH` — on Windows that is
often not true.

```yaml
tasks:
  load:
    type: cli
    shell: bash
    command: set -a && source .env && set +a && conda run -n py312 python job.py
```

### How do I set a working directory?

`cwd:` on the task, resolved against `workspace:`.

### Can a task run on another machine?

`type: ssh` runs a command remotely. There is no distributed executor — see
[the roadmap](ROADMAP.md) for worker processes.

---

## 5. Dependencies and passing data

### How do I order tasks?

```yaml
tasks:
  extract:
    type: python
    path: pipelines/e.py
    function: extract
  transform:
    type: python
    path: pipelines/t.py
    function: transform
    depends_on: [extract]
```

Anything without `depends_on` may run in parallel, up to `max_parallel_tasks`
(default 4, or `PIPLY_DEFAULT_MAX_PARALLEL_TASKS`).

### How does one task read another's output?

A `python` task's return value is stored and handed to downstream tasks under
its task id:

```python
def extract_data(context):
    return {"records": 120}

def transform_data(context):
    upstream = context["extract"]        # the dict above
    return {"records": upstream["records"] + 1}
```

For `cli` tasks, write to a file or print JSON — the captured stdout is stored
as the task output and is visible in the UI.

### Can I pass values into a run?

```bash
piply run nightly --param batch=2026-05-26 --param dry_run=true
```

They arrive as `context["params"]`. JSON scalars are parsed, so `true` becomes a
boolean. The UI cannot yet supply parameters — see the roadmap.

### Circular dependencies?

Rejected at load:

```
Pipeline 'x' contains a cycle at task 'transform'
```

The same applies across pipelines: `Pipeline trigger cycle detected at 'x'`.

---

## 6. Entities

### What are entities for?

Running one task template once per business value, without copy-pasting.

```yaml
pipelines:
  extract_flow:
    entities:
      report: [payment, adjustment, refund]
    tasks:
      extract:
        type: cli
        command: python extract.py --report {report}
      validate:
        type: cli
        command: python validate.py --report {report}
        depends_on: [extract]
```

That becomes six runtime tasks: `payment.extract → payment.validate`,
`adjustment.extract → adjustment.validate`, and so on. The chains are
independent, so one failing report does not block the others.

### How do I exclude one task from expansion?

`entities: false` on the task. Useful for a single setup or teardown step:

```yaml
tasks:
  login:
    type: cli
    command: python login.py
    entities: false          # runs once, not once per report
  extract:
    type: cli
    command: python extract.py --report {report}
    depends_on: [login]
```

### Can I control which entity runs first?

Yes — append `*` to the value. See
[priority](#how-do-i-make-some-entities-run-first).

---

## 7. Templates and deployments

### When should I use them?

When the same pipeline shape runs for several tenants, regions, or clients.

```yaml
pipeline_templates:
  tenant_etl:
    tasks:
      extract:
        type: cli
        command: python extract.py --client {practice}

pipeline_deployments:
  BENNETT_ETL:
    template: tenant_etl
    schedule: {cron: "0 3 * * 0"}
    variables: {practice: BENNETT}
    triggers_on_success: [Bronze_to_Silver]

  PALOS_ETL:
    template: tenant_etl
    schedule: {cron: "0 5 * * 0"}
    variables: {practice: PALOS}
```

Each deployment becomes a normal pipeline id. The scheduler, UI, CLI, and API
all treat it like any other pipeline.

### Do deployment variables reach a downstream pipeline?

Yes. A pipeline started through `triggers_on_success` inherits the parent's
variables, environment, and outputs, and the parent's values take precedence.

So `BENNETT_ETL` triggering `Bronze_to_Silver` runs it with `practice=BENNETT`.
A *manual* run of `Bronze_to_Silver` uses its own variables or the top-level
defaults instead — there is no parent to inherit from.

### Does that work more than one level deep?

Yes. `Deployment → A → B → C` propagates the whole way down, and the Runs page
shows the full chain.

### A retry of a downstream run used to lose the parent's values.

Fixed. Every run stores the exact configuration it launched with, so retrying,
resuming, or backfilling a triggered run replays those values instead of
re-deriving them. This was the cause of a downstream `dbt` run reverting
`DBT_CLIENT` to the literal `{practice}`.

### Can a deployment override tasks, not just variables?

Yes — deployment keys are deep-merged over the template, so you can override a
single task's `command` or `timeout` without restating the rest.

---

## 8. Conditionals and priority

### How do I make a task conditional?

```yaml
tasks:
  publish:
    type: cli
    command: python publish.py
    run_if: "{stage} == 'prod'"
```

### How do I make a *value* conditional?

Two forms, both fine:

```yaml
variables:
  headless: true if stage == "prod" else false

  workers:
    if: stage == "prod"
    then: 8
    else: 2
```

### What can I put in a condition?

Comparisons (`==`, `!=`, `<`, `<=`, `>`, `>=`), membership (`in`, `not in`),
boolean logic (`and`, `or`, `not`), and literals. Variables and environment
values are the operands.

**Not supported, deliberately:** function calls, attribute access, arithmetic,
indexing, or anything else. This is not an expression language and will not
become one — complex logic belongs in a Python task. Unsupported syntax raises
a clear error at load time rather than failing at 3am.

### My condition using `true`/`false` behaved oddly.

YAML's lowercase `true`/`false`/`null` are handled as booleans in conditions, so
this works as written:

```yaml
active: true if stage == "dev" else false
```

If you quote them (`"true"`) you get the *string* `"true"`, which is truthy
either way — usually not what you want.

### A sentence with "if" in it got treated as a condition.

It should not. A value only parses as a conditional if it looks like one;
prose such as `run if you can else walk` is left as a literal string. If you
have a value that genuinely looks like an expression and should stay literal,
quote it and add something that breaks the pattern, or move it to `env:`.

### How do task priorities work?

Higher runs first when several tasks are ready at once. Three equivalent ways:

```yaml
tasks:
  gate:
    priority: high        # lowest -2, low -1, normal/default/medium 0,
                          # high 1, higher 2, highest 3, critical 5
  gate2:
    priority: "***"       # star shorthand: 3
  gate3***:               # suffix on the id; the id stays "gate3"
    type: cli
```

Priority breaks ties among *ready* tasks. It does not override `depends_on` — a
dependency always wins.

### How do I make some entities run first?

Append stars to the entity value:

```yaml
entities:
  report:
    - payments_at_claim_level***    # priority 3
    - charges_at_cpt_level**        # priority 2
    - adjustments*                  # priority 1
    - next_day_appointments         # priority 0
```

The stars never reach the value — the task still receives
`report=payments_at_claim_level`. Entity priority is *added* to the task's own
priority.

This is how you make a partial extraction a useful one: if the tenant times out
halfway, you got the reports that mattered.

---

## 9. Scheduling, sensors, and triggers

### How do I schedule a pipeline?

```yaml
schedule:
  cron: "0 3 * * 0"        # Sunday 03:00
# or
schedule:
  every: 15m               # 30s, 15m, 2h
```

### What timezone do schedules use?

The server's local time. Timestamps are stored in UTC.

### How do I chain pipelines?

```yaml
pipelines:
  extract:
    triggers_on_success: [transform]
```

The downstream run inherits the upstream's variables, environment, and outputs.

### What sensors are available?

`file_sensor`, `sql_sensor`, `api_sensor`.

```yaml
sensors:
  inbox:
    type: file_sensor
    path: sensor_inbox/*.csv
    task: load

  new_rows:
    type: sql_sensor
    connection: app_db
    query: SELECT MAX(id) FROM events
```

Sensors keep a cursor between polls, so they fire on *change* rather than on
every poll. Their health is on the Diagnostics page: status, last poll,
consecutive failures, and the last error.

### How do I pause a schedule without editing YAML?

```bash
piply pause nightly
piply resume nightly
```

The pause is stored in the database and survives restarts and config reloads.

### Can two runs of the same pipeline overlap?

Not by default — `max_concurrent_runs` is 1. Raise it if overlap is safe.

---

## 10. Retries, timeouts, and failure handling

### How do I retry automatically?

```yaml
pipelines:
  nightly:
    retry:
      attempts: 2
      mode: resume          # or startover
      delay_seconds: 30
```

`resume` restarts from the failed task and keeps successful results.
`startover` re-runs everything.

Retry is currently **pipeline-level**. Per-task retry is on the roadmap.

### How do I stop a hung task?

```yaml
pipelines:
  extract:
    timeout: 4h             # ceiling for the whole run
    tasks:
      login:
        timeout: 10m        # per task
```

Durations accept `30`, `30s`, `5m`, `1h`. A timed-out task is recorded as
`timed_out`, distinct from `failed`, so you can tell a hang from an error.

### What happens to downstream tasks when one fails?

By default they are `skipped`. Change it per task:

```yaml
tasks:
  cleanup:
    on_upstream_failure: continue    # skip (default) | fail | continue
```

### A run is stuck in `running` after a crash.

It gets reconciled. Each run records a heartbeat and the pid that owns it; on
start-up, runs whose owning process is gone are marked `interrupted` with a
reason. Ctrl+C and `piply stop` do this cleanly on the way out.

If a run is genuinely stuck while the process is alive, cancel it from the UI or
`piply` and check Diagnostics for the stale-run timeout.

### How do I re-run just one task?

```bash
piply tasks retry <run_id> <task_id> --mode resume
```

Or from the run page in the UI.

### How do I re-run a historic run with its original values?

```bash
piply backfill <run_id>                                  # one run, replayed
piply backfill nightly --from 2026-07-01 --to 2026-07-08 # a schedule window
```

---

## 11. Running and debugging

### How do I see what a task will actually run, before running it?

```bash
piply plan
piply plan nightly --param batch=2026-05-26
```

This is the fastest way to debug variables. Diff the output before and after a
config change to confirm nothing moved that you did not intend:

```bash
piply plan --json > before.json
# edit
piply plan --json > after.json
diff before.json after.json
```

### How do I follow logs live?

```bash
piply logs --follow                          # everything
piply logs --follow --pipeline extract_flow  # one pipeline
piply logs <run_id>                          # one run
```

Every line carries its task name, so interleaved parallel output stays readable.

### My task fails but the log is empty.

Usually the process wrote nothing before dying. Check:

- the exit code on the run page,
- that `cwd` and any script path resolve — remember they are relative to
  `workspace:`,
- for `cli`, run the same command by hand from the workspace directory.

### How do I run one pipeline from the command line and wait?

```bash
piply run extract_flow --wait
```

Without `--wait` it queues and returns immediately.

### Where do artifacts go?

You declare them; Piply records the metadata and shows them on the run page:

```yaml
tasks:
  export:
    type: cli
    command: python export.py
    artifacts: ["out/*.csv"]
```

The files stay where they are. Downloads are restricted to paths that run
actually recorded *and* that resolve inside an allowed root.

### Can I inspect the database directly?

Yes, it is plain SQLite. `piply diagnostics` prints its location. The schema is
documented [table by table](DATABASE.md#4-schema-reference). Treat it as
read-only while the server is running.

---

## 12. The UI

### What are the coloured dots on the pipelines page?

The last five runs, newest on the right. Click any dot to open that run.

| Colour | Status |
| --- | --- |
| Green | `success` |
| Red | `failed`, `timed_out` |
| Blue | `running` |
| Amber | `queued` |
| Orange | `interrupted` |
| Purple | `cancelled` |
| Grey | `skipped` |
| Hollow | no run yet in that slot |

### How do I find one failed run among thousands?

The Runs page filters server-side, so it works past the page limit and the URL
is shareable: pipeline, status (including a *Failure-like* preset), trigger
type, date range, and seven sort orders.

### How do I tell which tenant triggered a shared downstream pipeline?

The lineage column on the Runs page shows the whole chain — `Acme Etl → silver →
gold → semantic` — and each step links to its run.

### The DAG is too small.

Collapse the metadata panel and the task-focus panel; the graph then goes
full-width and taller. Both remember their state per browser.

### Is there a dark mode?

Not yet. On the roadmap — the palette is already CSS variables, so it is a small
change.

---

## 13. Authentication

### Is Piply open by default?

Yes. With no accounts and no auth environment variables, there is no login and
everything is permitted. Authentication switches on the moment the first account
exists.

### How do I create the first user?

```bash
piply users create admin --role admin
```

On a server, use a mounted secret instead — see
[Authentication §1b](AUTHENTICATION.md#1b-creating-the-first-admin-on-a-server).

### What can I grant?

`view`, `edit`, `run`, per pipeline or against `*`. `edit` and `run` both imply
`view`.

```bash
piply users grant alice nightly=view,run
piply users grant alice '*' view
```

### Can a user with `run` execute arbitrary commands?

No. `command_overrides` — which replaces what a task executes — requires
`admin`. A `run` grant only runs the pipeline **as configured**. That
distinction is deliberate; see [Security](SECURITY.md).

### I locked myself out.

Passwords cannot be recovered, only reset. With access to the database:

```bash
piply users passwd admin
piply users create rescue --role admin
```

---

## 14. Deployment and the database

### My run history disappeared after a Docker redeploy.

The database was in the container's writable layer. Mount a volume, or use
PostgreSQL. Full setup, including the `chown`-before-`USER` detail that is easy
to miss, is in [Metadata Store §5](DATABASE.md#5-docker-not-losing-your-data).

### How do I move an existing SQLite install to PostgreSQL?

```bash
piply stop
piply migrate-db --to "postgresql://piply:secret@db:5432/piply"
```

Run ids are preserved, so retry chains, lineage, and accounts survive. The
target must be empty.

### Can I run two Piply instances against one database?

No, on either backend. The scheduler assumes it owns the queue; two instances
will both dispatch the same scheduled slot.

### How do I stop the database growing forever?

```bash
piply prune --run-days 90 --log-days 30 --max-runs 500
piply prune --dry-run                  # see what would go first
```

Or set `PIPLY_RETENTION_*` and let it happen automatically. Logs dominate the
row count.

### How do I back up?

`piply backup /backups` for SQLite — it uses the online backup API, so it is
safe while running. For PostgreSQL use `pg_dump`; `piply backup` refuses a
server store rather than writing an unusable file.

### Is there a health endpoint for a load balancer?

`GET /health`, and it is public. `GET /metrics` exposes Prometheus metrics and
accepts the API bearer token, so a scraper does not need UI credentials.

---

## 15. Performance and limits

### How many pipelines can it handle?

Hundreds are fine. A real 36-pipeline config with 8 tenant deployments lists in
3–4 SQL statements; the dashboard is 17. The listing queries are constant in the
number of pipelines, not linear.

### How many tasks run at once?

`max_parallel_tasks` per pipeline, default 4. Raise it per pipeline, or globally
with `PIPLY_DEFAULT_MAX_PARALLEL_TASKS`.

### Everything queues behind one slow pipeline.

Runs of the *same* pipeline serialise by default. If several pipelines all
trigger one shared downstream pipeline, they queue behind each other there.

Options today: raise `max_concurrent_runs` if overlap is safe, stagger the
schedules, or split the downstream stage into per-tenant deployments so the
chains are independent. Concurrency pools — expressing "the warehouse takes 3
concurrent dbt runs" directly — are on the [roadmap](ROADMAP.md).

### Is there a task or log size limit?

Task output is stored with a truncated preview plus the full value. Very chatty
tasks make the `logs` table the biggest thing in the database; that is what
retention is for.

---

## 16. Error message index

What the message means and what to do about it.

| Message | Cause | Fix |
| --- | --- | --- |
| `Could not find a Piply config file` | No `piply.yaml` in the working directory | `--config`, or `cd` to the project |
| `The root of the config file must be a mapping` | The YAML is a list, or empty | Start with `version: "1"` |
| `Config must define a 'pipelines' mapping or 'pipeline_deployments' mapping` | Neither key present | Add one |
| `Configured workspace does not exist` | `workspace:` points nowhere | Create it, or use `.` |
| `Pipeline 'x' contains a cycle at task 'y'` | `depends_on` loops | Break the loop |
| `Pipeline trigger cycle detected at 'x'` | `triggers_on_success` loops | Break the loop |
| `Pipeline 'x' triggers unknown pipeline 'y'` | Typo, or the target is a template not a deployment | Use a real pipeline id |
| `Pipeline 'x' cannot trigger itself on success` | Self-reference | Remove it |
| `... task 'y' requires command or path for cli tasks` | `type: cli` with neither | Add `command:` |
| `... task 'y' points to a missing script` | Path wrong, or relative to the wrong base | Remember paths resolve against `workspace:` |
| `... task 'y' uses unsupported type 'z'` | Typo in `type:` | One of python, cli, api, webhook, email, ssh |
| `... must be a duration such as 30, 30s, 5m, or 1h` | Bad `timeout` | Use those forms |
| `... priority must be a number, a named level, or star shorthand` | Bad `priority` | A number, `high`, or `***` |
| `Pipeline 'x' has a task id made only of '*' characters` | A task named `***` | Give it a name before the stars |
| `Pipeline deployment 'd' references unknown template 't'` | Typo in `template:` | Match a `pipeline_templates` key |
| `Pipeline deployment 'd' conflicts with an existing pipeline id` | Same name as a `pipelines:` entry | Rename one |
| `... cannot set both fail_if_upstream_failed and continue_if_upstream_failed` | Contradictory legacy keys | Use `on_upstream_failure:` |
| `PIPLY_DATABASE does not support 'mysql'` | Unsupported metadata backend | SQLite or PostgreSQL; reach MySQL from tasks |
| `PIPLY_DATABASE must be a plain file path, not a sqlite:// URL` | URL form for SQLite | Drop the scheme |
| `Migration refused: the target database already contains data` | `migrate-db` target not empty | Drop and recreate the schema |
| `This is the only active admin` | Would leave no way in | Promote another admin first |
| `No SMTP server is configured` | Email task with no central SMTP | Configure it in Settings, or give the task `smtp_host` |

---

## 17. Quick answers

| Question | Answer |
| --- | --- |
| Restart after a config edit? | No, it reloads automatically |
| Do relative paths resolve against the config file? | No — against `workspace:` |
| Does `env_file` override inline `env:`? | Yes |
| Does a missing `env_file` raise? | No — it loads nothing, but `validate` warns |
| Does an unknown `{placeholder}` become empty? | No, it stays literal |
| Do deployment variables reach downstream pipelines? | Yes, at any depth |
| Can I retry a downstream run without the upstream? | Yes, `piply backfill <run_id>` |
| Does `priority` override `depends_on`? | No |
| Do entity stars appear in the value? | No |
| Is auth on by default? | No, until the first account exists |
| Can `run` permission execute arbitrary commands? | No, that needs `admin` |
| Is `/metrics` public? | No, but it accepts the API token |
| Two instances on one database? | No |
| Is `piply backup` safe while running? | Yes, on SQLite |
| Can I use MySQL as the metadata store? | No — SQLite or PostgreSQL |

---

## Related

- [YAML Specification](YAML_SPECIFICATION.md) — every key, with defaults
- [Execution Examples](EXAMPLES.md) — runnable patterns
- [Metadata Store](DATABASE.md) — backends, migration, schema
- [Security](SECURITY.md) — trust model and deployment checklist
- [UI Guide](UI_GUIDE.md) — every page and what it answers
- [Roadmap](ROADMAP.md) — what is planned next
