# Changelog

Notable changes per release. Dates are release dates.

Piply follows semantic versioning loosely while pre-1.0: the YAML contract is
kept backward compatible, but security defaults may tighten in a minor release.
Anything that changes observable behaviour is listed under **Behaviour changes**
rather than buried in the feature list.

---

## 0.3.0 — 2026-08-26

A hardening and operability release. Every existing `piply.yaml` keeps working
untouched.

### Fixed — upgrade recommended

- **Newly created users could not sign in.** Creating the *first* account
  switches authentication on, which locked out the very page that created it:
  every following request returned 401, so the *next* account was silently never
  created. The person then could not sign in as an account that did not exist.
  The session that creates the first admin is now signed in as it.
- **`logging` output never reached the run log.** Only `print()` was captured
  from a `type: python` task using `function:`. A `StreamHandler` binds
  `sys.stderr` when it is *constructed*, so a module calling
  `logging.basicConfig()` at import time — how most production code is written —
  wrote straight past the capture. Every `log.info(...)` from an extraction was
  missing from the run page and went to the server console instead. Existing
  handlers are now pointed at the capture for the duration of the task and
  handed back exactly as they were found, so `logging`, `log.exception()`
  tracebacks, `print()`, and direct `sys.stderr` writes all land in the run log,
  each line separately and with its level.

- **Python callables showed no output until they finished.** A `type: python`
  task using `function:` buffered everything it printed and flushed it only when
  the callable returned, so a long extraction was indistinguishable from a hung
  one — nothing to watch, no way to tell progress from a stall. Output now
  streams line by line, as `type: cli` and script tasks always have, so the run
  page (which polls every 3s) and `piply logs --follow` tail it live. stdout and
  stderr are now interleaved in the order the task produced them, rather than
  stdout-then-stderr at the end.

- **Parallel Python tasks logged against the wrong task.** Output capture swapped
  the process-global `sys.stdout`, so with `max_parallel_tasks` above one the
  tasks' enter/exit order interleaved. In a two-task reproduction, 29 of the
  first task's 30 lines were recorded against the second. Capture is now scoped
  to the thread running the task.

  The same bug left `sys.stdout` pointing at a discarded buffer once a run
  finished, so **everything the server printed afterwards disappeared** —
  uvicorn's access log included. A task that runs past its timeout no longer
  holds the streams either.

- **A `sql_sensor` given a file path where a DSN belongs said only
  `Unsupported sql_sensor connection scheme '<none>'`.** It now names the value
  and says to use `database:` or a `sqlite:///` DSN. The sensor already showed
  as `failing` on Diagnostics with the error attached; only the wording was
  unhelpful.

- **Downstream pipelines reported `pending` when they were never going to run.**
  The run page now names the real state — `paused`, `disabled`, `queued`,
  `waiting` — with the reason.

- **The server could not start on a clean install.** The sign-in form required
  `python-multipart`, which `pip install mr-piply` never installed — `fastapi`
  only declares it under its `standard` extra. Installs that happened to have it
  from another package worked, which is why it went unnoticed. Sign-in now parses
  its form with the standard library, so nothing extra is needed and the runtime
  dependency count stays at **8**.
- `starlette` was imported directly but never declared. Both middlewares now use
  FastAPI's own `@app.middleware("http")`, so Piply imports only what it declares.

### Behaviour changes

Read this section before upgrading a multi-user install.

- **Authorization is now enforced on every endpoint.** Permissions previously
  covered the pipeline and run APIs but not diagnostics, dashboard, log search,
  the execution matrix, artifacts, preview, backfill, or prune. Any authenticated
  account could reach them regardless of its grants.

  Accounts that relied on that access will now receive `403`. Log search, the
  dashboard, and the matrix are **filtered rather than refused** — a restricted
  user still sees their own pipelines. Diagnostics and prune are now admin-only.

- **`command_overrides` requires `admin`.** Overriding the command a task runs
  turned a `run` grant into arbitrary code execution as the Piply process.
  Triggering a pipeline *as configured* still only needs `run`, so per-tenant
  delegation is unaffected.

- **Run configuration masks credentials.** `GET /api/runs/{id}/config` redacts
  values whose name looks like a secret (`password`, `token`, `api_key`, `dsn`,
  and similar), for every caller including admins. The stored snapshot keeps the
  real values, so retry and backfill still work. Name secrets recognisably —
  `DB_CONN` is **not** masked, `DB_CONNECTION_STRING` is.

- **Sign-in is throttled.** Eight failures within five minutes locks that
  username out for five minutes. Counters are in memory and clear on restart.

- **Security headers on every response**, including a Content-Security-Policy.
  If you serve Piply inside an iframe, `X-Frame-Options: DENY` will now block it.

### Added

- **`piply validate` warns when a project-level entity expands a pipeline that
  never uses it.** A top-level `entities:` block applies to every pipeline, so a
  nightly cleanup job beside one entity-driven pipeline quietly ran three times
  and a summary email was sent three times — identical tasks, nothing failing,
  nobody noticing. The docs now recommend scoping entities to the pipeline or
  template that uses them.

- **`entities:` on a task accepts a list of dimension names**, selecting which
  of the pipeline's entities it expands over instead of repeating their values.
  A per-practice `login` beside per-practice-per-report tasks is now
  `entities: [practice]` on that one task, with every other task needing no
  entity declaration at all.

- **A task can expand over fewer entity dimensions than its dependents.**
  Declaring `practice` at project level and `report` on one task gives a
  per-practice `login` feeding per-practice-per-report `extract` tasks, with
  each extract waiting only for *its own* practice's login. Previously every
  extract depended on every login, so one practice failing stalled all of them.
  Dependencies now match on entity values, and fall back to fanning in when the
  match is ambiguous.

- **`include:` splits `piply.yaml` across files.** A production config had
  reached 974 lines, so adding a tenant meant editing one enormous file and two
  people touching unrelated tenants conflicted in git for no reason. The root
  file can now pull in others by path or glob — the suggested split keeps
  project settings and deployments in the master, with pipelines and alerts in
  their own files. A repeated pipeline id is an **error naming both files**,
  never last-wins, and every included file is watched so edits take effect
  without a restart. Purely additive: a config with no `include:` is unchanged.
  Different *blocks* of one pipeline may come from different files, so sensors
  can live in `piply_sensor.yaml` while the tasks stay in `piply_pipe.yaml`;
  the same block in two files is still an error.
- **An Alerts panel under Settings**, admin-only. Shows every declared
  destination, whether its webhook resolved, which pipelines use it — resolved
  through groups — and a log of recent delivery attempts with the reason for
  each failure. **Send test** posts a card immediately so a webhook can be
  checked without waiting for a run. Every attempt is recorded, including the
  case that used to be completely silent: a pipeline with only `on_failure`
  that succeeds now records `nothing configured` rather than nothing at all,
  because silence was indistinguishable from a delivery that failed.
- **Sensor-triggered runs receive what changed.** A file sensor now hands its
  tasks `{sensor_file}`, `{sensor_file_name}`, `{sensor_files}`, and
  `{sensor_file_count}`; SQL and API sensors pass their table, cursors, and row
  count. Python tasks get the whole event as `context["sensor"]`. Previously the
  filenames were logged but unreachable, so every task had to re-scan the
  directory and guess which file it had been woken for.
- **The task graph uses the full page width**, with the task panel opening when
  a node is clicked rather than permanently occupying a third of the screen.
- **Microsoft Teams notifications.** Declare reusable destinations — channels
  and group chats — plus named groups, then reference them per pipeline under
  `on_failure` / `on_success`. Destinations are posted concurrently, each with
  its own timeout. Webhook URLs come from the environment or a secrets file and
  are never written to a log or an API response, because a Teams webhook URL is
  itself the credential. Delivery lives outside pipeline execution, so a failed
  or timed-out notification is recorded against the run and never changes its
  status.
- **First-run database setup.** A brand-new install opens a setup page instead
  of the dashboard and asks where Piply should keep its own data — a SQLite file
  or PostgreSQL. The choice is validated by opening the database before anything
  is saved, written to `.env`, and applied without a restart. The scheduler is
  held back until you choose, so nothing is written into a database you may be
  about to replace. Existing installs are never redirected, and the page cannot
  repoint a system that is already configured.
- **An optional first-admin step after database setup.** Piply is open to
  anyone who can reach it until an account exists, which is easy to miss when
  deploying to a server. Setup now offers to create the first administrator and
  signs you in as it, so the account that switches authentication on cannot lock
  out the page that created it. Skipping is one click and leaves the old
  behaviour. The step closes permanently once any account exists.
- **Admins can change the database from Settings.** Move Piply's own metadata
  store to another SQLite file or to PostgreSQL, optionally copying runs, logs,
  and accounts across with their ids intact — no file editing and no restart.
  The target is opened before anything is saved, the old database is left as a
  rollback, and copying refuses a non-empty target. Admin-only, and refused
  outright when `PIPLY_DATABASE` comes from the process environment, where
  writing `.env` would silently do nothing, and while any run is in flight,
  which would otherwise strand that run between the two databases.
- **Actor attribution.** Runs record the account that started them, shown on the
  run page and returned as `actor` by the API. Pausing, resuming, and manual runs
  are logged as `Pipeline 'x' paused by alice`. Schedule and sensor runs have no
  actor rather than an invented one.
- **Queued triggers explain themselves.** A trigger that cannot run yet records
  why — `Skipping trigger for 'x': pipeline is paused` — logged once per change
  rather than on every ten-second tick, and kept on the queue row.
- **Interactive runtime inputs.** Starting a pipeline by hand that normally
  receives variables from an upstream trigger now prompts for the missing
  `{placeholder}` values instead of running a command containing them literally.
  Available in the UI, as `piply run --var NAME=VALUE`, `--prompt`, and through
  `GET /api/pipelines/{id}/runtime-inputs`. Values are stored with the run, so a
  retry or backfill reuses them.
- **Collapsible template groups on the pipelines page.** With one template
  deployed per tenant the list becomes a wall of near-identical rows. Groups
  collapse individually or in bulk, keep their running / failed / paused counts
  visible while closed, persist per browser, and open automatically while
  searching so a match cannot hide behind a collapsed heading.
- **DAG nodes no longer overflow with long task names.** Entity expansion
  produces names like `payer_claim_status_dashboard / Load Bronze`, which used
  to paint outside the node box and across the edges. Labels are now measured
  and shortened from the middle, with the full value on hover, and nodes are
  wide enough that the status line always fits.
- **`piply migrate-db --to <dsn>`** copies a SQLite runtime onto PostgreSQL with
  ids intact, so retry chains, lineage, and accounts survive.
- **`GET /health`** — a public liveness probe for load balancers and container
  healthchecks. Returns 503 only when the metadata store is unreachable.
- **Secret files.** `PIPLY_AUTH_PASSWORD`, `PIPLY_API_TOKEN`, and
  `PIPLY_ADMIN_PASSWORD` each accept a `_FILE` variant that reads from a mounted
  file — the Docker and Kubernetes convention, and safer than an environment
  variable on a server.
- A supplied bootstrap admin password is no longer echoed to the startup log.
- `piply validate` and `piply plan` warn when a declared `env_file` does not
  resolve. It previously loaded nothing silently, which surfaced much later as
  missing credentials.

### Documentation

New: [FAQ](docs/FAQ.md) with an error-message index,
[Notifications](docs/NOTIFICATIONS.md), [Security](docs/SECURITY.md),
[Metadata Store](docs/DATABASE.md) with a table-by-table schema reference, and
[Roadmap](docs/ROADMAP.md).

The notifications guide covers Teams and email together. They were previously
documented in separate places, so "how do I get alerted when this fails" had no
single answer.

Two long-standing behaviours are now written down explicitly, because both cost
real debugging time: `env_file` paths resolve against `workspace:` rather than
the config file, and `env_file` values override an inline `env:` block.

### Internal

- `tests/test_packaging.py` guards the class of bug above: every imported
  package must be declared, the dependency count is asserted so changing it is
  deliberate, and the app is booted in a subprocess with `multipart` hidden.
- The CSP test scans every template and script for remote origins rather than
  checking a hand-maintained list, after the policy silently blocked the DAG
  layout libraries.

---

## 0.2.1

- Pipeline run-history dots on the pipelines page, each linking to its run
- Runs page filtering, sorting, and multi-level trigger lineage
- Conditional values in YAML (`true if stage == "dev" else false`)
- Entity task priority via `*` suffixes on entity values
- Collapsible metadata and task-focus panels on the pipeline graph
- Accounts, roles, and per-pipeline `view` / `edit` / `run` permissions
- Central SMTP under Settings, reused by email tasks and run notifications
- Optional PostgreSQL metadata store behind the `[postgres]` extra

**Behaviour change:** `smtp_host` no longer defaults to `localhost`. A task that
relied on an implicit local mail server must now set it explicitly. Without this
the default overrode central SMTP settings on every task.

---

## 0.2.0

Pipeline templates and deployments, entity expansion, task and pipeline
timeouts, dry-run preview, declared artifacts, backfill and replay, retention
and pruning, Prometheus metrics, diagnostics, and sensor health.
