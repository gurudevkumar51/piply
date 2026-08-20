# Changelog

Notable changes per release. Dates are release dates.

Piply follows semantic versioning loosely while pre-1.0: the YAML contract is
kept backward compatible, but security defaults may tighten in a minor release.
Anything that changes observable behaviour is listed under **Behaviour changes**
rather than buried in the feature list.

---

## 0.2.2 — 2026-08-20

A hardening and operability release. Every existing `piply.yaml` keeps working
untouched.

### Fixed — upgrade recommended

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

- **Interactive runtime inputs.** Starting a pipeline by hand that normally
  receives variables from an upstream trigger now prompts for the missing
  `{placeholder}` values instead of running a command containing them literally.
  Available in the UI, as `piply run --var NAME=VALUE`, `--prompt`, and through
  `GET /api/pipelines/{id}/runtime-inputs`. Values are stored with the run, so a
  retry or backfill reuses them.
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
[Security](docs/SECURITY.md), [Metadata Store](docs/DATABASE.md) with a
table-by-table schema reference, and [Roadmap](docs/ROADMAP.md).

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
