# Changelog

Notable changes per release. Dates are release dates.

Piply follows semantic versioning loosely while pre-1.0: the YAML contract is
kept backward compatible, but security defaults may tighten in a minor release.
Anything that changes observable behaviour is listed under **Behaviour changes**
rather than buried in the feature list.

---

## 0.3.3 — 2026-09-06

Notification routing, tags you can filter by, and four fixes — all prompted by a
live install. Every 0.3.2 config keeps working untouched: every key added here
is opt-in and defaults to the old behaviour.

### Added

- **`notifications.defaults` alerts every pipeline without a block on each
  one.** Declaring `on_failure:` per pipeline meant editing 29 places to change
  the on-call channel, and any pipeline added later was silent until someone
  remembered. A project-level default now applies to every pipeline that does
  not state its own:

  ```yaml
  notifications:
    teams: { ... }
    defaults:
      on_failure: [critical]
  ```

  A pipeline overrides it **per outcome**, so narrowing failures never silently
  changes successes; `on_success: []` silences one outcome and
  `notifications: false` opts out entirely. Defaults are resolved into each
  pipeline at load time, so the run record, the UI's "used by" panel, and
  `piply validate` all show who will actually be told. A typo in a default is a
  load error rather than 29 silent pipelines.

- **File-scoped variables.** A top-level `variables:` block merges across every
  included file, so two teams could not both define `batch_size` — the second
  was a duplicate-key error, and one of them had to rename. Declaring them under
  a file's `pipeline_defaults.variables` makes the name private to that file:

  ```yaml
  # claims/piply_claims.yaml        # reports/piply_reports.yaml
  pipeline_defaults:                pipeline_defaults:
    variables:                        variables:
      batch_size: 500                   batch_size: 50
  ```

  Precedence is pipeline → file → project, and each layer sees the one above it,
  so a file variable can interpolate a project one. Global variables still
  belong in `piply.yaml`; top-level `variables:` behaves exactly as before.

  Notification **destinations** stay project-wide on purpose — a destination is
  a real channel, and one name meaning two channels would make the settings page
  and the delivery history unable to say where an alert went. Notification
  *routing* is already file-scoped. A duplicate-key error for either now names
  the fix that actually applies to it.

- **`pipeline_defaults` gives one config file its own tags and destinations.**
  With the config split one file per team or tenant, every pipeline in a file
  usually shares both. Stating them once at the top of the file removes the
  duplication the split was meant to remove in the first place:

  ```yaml
  # claims/piply_claims.yaml
  pipeline_defaults:
    tags: [claims, prod]
    notifications:
      on_failure: [claims_oncall]
  ```

  Tags are **added** to each pipeline's own; destinations **replace** them, per
  outcome. Precedence is pipeline → file → project. Templates are excluded on
  purpose — their deployments usually live elsewhere, so covering both would
  apply the defaults twice.

- **Tags are now a filter, not just a label.** Clicking a tag on the pipelines
  page filters to it, with a visible chip to clear it; matching is exact, so
  `prod` no longer also matches `prod_backup`. The runs page gained a tag
  dropdown, and `/runs?tag=claims` works directly. The tag resolves to pipeline
  ids inside the query rather than filtering afterwards, so `limit` still means
  "this many matching runs".

- **`allow_failure: true` marks a task best-effort.** Previously any failed task
  failed its run — `on_upstream_failure` only governed what downstream tasks
  did — so there was no way to say "this sync is optional". The task still
  records its own failure; only the run's status stops depending on it. Paired
  with `alert_on_failure: true` on the same task, somebody is still told, which
  is the one failure a pipeline-level alert cannot report.

- **A bell on the pipelines listing shows which pipelines alert someone**, with
  the destinations in its tooltip. With project and file defaults in play, a
  pipeline's own YAML no longer answers that question.

### Performance

- **The CLI starts about twice as fast.** `uvicorn`, `httpx` and `asyncio` were
  imported at module scope but are only needed by `piply start` and by actually
  sending a card. Deferring them to their point of use cut `import
  piply.cli.main` from ~470ms to ~215ms, which every command that is not the
  server — `validate`, `runs`, `tasks retry` — was paying on every invocation.
  A packaging test now fails if one of them is imported at module scope again.

- **A login attempt no longer stalls the whole server.** Verifying a password is
  ~240k PBKDF2 rounds, about 100ms, and it ran on the event loop because the
  login route must be `async def` to read its form. Four concurrent attempts
  made an unrelated page take **817ms** instead of 5ms — and a login page is
  exactly what gets hit repeatedly when someone is guessing. Hashing now runs in
  a threadpool, for both login and first-run admin creation.

### Changed

- **The runs list refreshes itself** every 10 seconds instead of needing a
  reload to notice a run starting or finishing. It re-fetches the same
  server-rendered page and swaps the table, so there is one renderer rather than
  a JS copy of every row, and it holds off while a log drawer is open, while a
  filter has focus, or while the tab is in the background.

- **The scheduler chip stops polling a tab nobody is looking at**, and its
  successful poll no longer prints an access-log line. An open dashboard was
  filing a request every 5 seconds in every tab, and the resulting wall of
  identical 200s buried the lines that matter in `piply start`'s output. A poll
  that fails is still logged — that is when it is worth having.

### Fixed

- **A webhook that worked from `curl` could fail from Piply.** `curl` uses its
  own certificate store; Python reads `SSL_CERT_FILE`, and a rebuilt conda
  environment leaves it pointing at a bundle that is gone — so every HTTPS post
  raised `FileNotFoundError` before reaching the network. A certificate path
  that does not exist is now ignored in favour of the system trust store,
  logged once. Verification still happens, and a path that *does* exist is
  always honoured, so a corporate CA bundle is unaffected.
- **The run-history tooltip on the pipelines page was cut off and covered its
  own label.** The dots table clipped its contents, so hovering a dot on the
  first or last row — the two most likely to be hovered — showed a tooltip with
  its edges sliced away. It also opened upward, directly over the "Last 5 runs"
  label it was describing. It now opens downward and is no longer clipped, and
  the run id and full timestamp were moved into it, replacing the duplicate
  browser tooltip that used to appear alongside.
- **A duplicate-key error across two included files with the same name was
  unactionable.** Splitting a config per tenant gives every folder its own
  `piply_template.yaml`, and the error printed bare filenames — `'…' is defined
  in more than one config file: 'piply_template.yaml' and
  'piply_template.yaml'` — which reads like Piply is rejecting the *name*. It
  never was: only the keys inside a file can clash. Included files are now
  named by their path relative to the project, so the folders tell them apart.
- **`HTTP 401 AuthorizationFailed` from a Teams alert gave no way forward.** The
  credential is the webhook URL itself, so a 401 means its `sig` is stale or
  truncated — never that the card or the account is wrong, which is what the
  bare message suggests. The failure now says so, and reports the signature's
  length, which is what separates a truncated URL from one invalidated by
  re-saving the flow. The URL is still never logged.

---

## 0.3.2 — 2026-09-06

A fix release. Three of these were reported from a live install, and every
0.3.1 config keeps working untouched.

### Added

- **`PIPLY_SCHEDULER_ENABLED=false` stops schedules without stopping Piply.**
  Opening a project on a laptop otherwise starts whatever was due, which is
  usually last night's pipelines. The UI, the API, and manual runs keep working;
  the startup log and the header chip both say the scheduler is off, so a quiet
  install is not mistaken for a broken one. `.env` is now gitignored — it holds
  the database URL, the SMTP password, and the Teams webhooks, and only luck had
  been keeping it out of commits.

- **The runs list answers "why did that fail?" in place.** Clicking a run's
  status opens its full log in a drawer beside the table, oldest first so a
  traceback reads the right way up, without losing your filters or scroll
  position. Finished runs also gain a **Re-run** action, so starting one again
  no longer means opening it first.

### Fixed

- **Alerts are sent as Adaptive Cards, not MessageCards.** Microsoft retired
  Office 365 connectors, and the Power Automate "Workflows" endpoints that
  replace them reject the older `MessageCard` payload Piply was sending — so a
  webhook that worked with `curl` produced nothing from Piply. Destinations now
  default to the Adaptive Card envelope, fall back to `messagecard` for URLs on
  `webhook.office.com`, and accept an explicit `format:` either way.

- **Cancelling a run did not stop it.** `terminate()` signals only the direct
  child, and CLI tasks run through a shell by default — so the shell died while
  the real process kept going. Worse, the orphan held the stdout pipe open, so
  the runner waited for output that never ended and the run never left
  `running`: the task node kept its moving dot indefinitely. The whole process
  tree is now stopped, on Windows and POSIX. A `type: python` task using
  `function:` still cannot be interrupted — Python has no safe way to stop a
  thread — but cancelling now says so in the run log instead of appearing to do
  nothing.

- **A failed Python task said almost nothing about why.** Only `str(exc)` was
  logged, and for the most common failures that is meaningless on its own — a
  `KeyError` logged just `'pre-flight'`, with no type, no file, and no line. The
  run log now carries the traceback and the run's error names the exception
  type, so the same failure reads as `KeyError: 'pre-flight'` with the file and
  line that raised it. Piply's own dispatch frames are trimmed, and the
  traceback is stored as one entry so a newest-first log does not render it
  upside down.

- **A Teams delivery failure could arrive as a bare `[Errno 2] No such file or
  directory`.** A `FileNotFoundError` from a missing TLS certificate bundle is
  not an `httpx.HTTPError`, so it escaped the transport handler and was reported
  with no exception type and no cause. Failures now name the exception type and,
  where the environment suggests one, the reason — a `SSL_CERT_FILE` pointing at
  a path that does not exist, or a configured proxy. The failure is also logged
  server-side, so the console shows more than `502 Bad Gateway`.

- **A pipeline's `enabled:` ignored conditionals and was always true.** It was
  read with `bool(...)` and without evaluating the condition, so both
  `enabled: {if: env == "dev", then: false, else: true}` and a bare
  `enabled: "false"` left the pipeline scheduled in every environment — a
  non-empty mapping is truthy, and so is the string `"false"`. Conditionals are
  now evaluated, false-looking values are honoured, and anything that is
  neither raises a config error instead of being silently treated as true.

- **`SyntaxWarning: invalid escape sequence` on every config reload.** Any value
  that looks like a ternary is speculatively parsed, so a Windows path such as
  `D:\Dumps` reached Python's parser as data and warned about the backslash.
  The warning was about the user's data, not their code.

---

## 0.3.1 — 2026-09-05

Follow-on release to 0.3.0. Every 0.3.0 config keeps working untouched.

### Added

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

- **`entities:` on a task accepts a list of dimension names**, selecting which
  of the pipeline's entities it expands over instead of repeating their values.
  A per-practice `login` beside per-practice-per-report tasks is now
  `entities: [practice]` on that one task, with every other task needing no
  entity declaration at all.

- **The task graph uses the full page width**, with the task panel opening when
  a node is clicked rather than permanently occupying a third of the screen.
  The panel closes from its own **Close** button, with `Escape`, or by clicking
  the same node again; closing clears the node selection so the two cannot
  disagree.

### Fixed

- **A task can expand over fewer entity dimensions than its dependents.**
  Declaring `practice` on the pipeline and `report` on one task gives a
  per-practice `login` feeding per-practice-per-report `extract` tasks, with
  each extract waiting only for *its own* practice's login. Previously every
  extract depended on every login, so one practice failing stalled all of them.
  Dependencies now match on entity values, and fall back to fanning in when the
  match is ambiguous.

- **`piply validate` warns when a project-level entity expands a pipeline that
  never uses it.** A top-level `entities:` block applies to every pipeline, so a
  nightly cleanup job beside one entity-driven pipeline quietly ran three times
  and a summary email was sent three times — identical tasks, nothing failing,
  nobody noticing. The docs now recommend scoping entities to the pipeline or
  template that uses them.

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
