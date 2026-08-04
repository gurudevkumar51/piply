# Piply Future Features

Ideas that are **not built**. Everything here is a proposal, ordered by the
value it would add against the cost of building it.

Two constraints shape every entry:

1. **Stay lightweight.** No broker, no external database, no daemon zoo. A new
   runtime dependency needs to earn its place; anything that can be optional
   should be an extra, not a default.
2. **Stay backward compatible.** An existing `piply.yaml` must keep working
   untouched. New capability arrives as new optional keys.

Legend: **Effort** S (days) / M (1–2 weeks) / L (a month+).

---

## Tier 1 — highest value for the cost

### 1.1 Alerting on failure

**Problem.** Today you learn a pipeline failed by opening the UI. There is no
push.

**Proposal.** A pipeline-level `on_failure` block reusing the existing operator
implementations, so nothing new has to be written to send the message.

```yaml
pipelines:
  nightly:
    on_failure:
      - type: email
        to: [oncall@example.com]
        subject: "{pipeline_id} failed: {error}"
      - type: webhook
        url: ${SLACK_WEBHOOK}
        body: '{"text": "{pipeline_id} run {run_id} failed"}'
    on_success:
      - type: webhook
        url: ${SLACK_WEBHOOK}
        body: '{"text": "{pipeline_id} finished in {duration}"}'
```

Add `on_timeout` and `on_sla_miss` later using the same shape.

**Why it matters.** This is the single largest gap between Piply and a tool a
team can rely on unattended. Everything needed already exists — the operators,
the run record, the success/failure callbacks.

**Effort:** S · **Deps:** none

---

### 1.2 SLA / expected-duration tracking

**Problem.** A pipeline that normally takes 10 minutes and is now at 55 is
invisible until it hits its hard timeout.

**Proposal.**

```yaml
pipelines:
  nightly:
    sla:
      expected_duration: 15m      # warn past this
      must_finish_by: "06:00"     # wall-clock deadline
```

Surface as a `piply_run_sla_breached` metric, an amber row on the listing page,
and an `on_sla_miss` hook. The store already records durations, so the baseline
can also be computed automatically (e.g. p95 of the last 20 runs) and offered as
a suggestion rather than requiring a hand-tuned number.

**Why it matters.** Slow is the failure mode that hard timeouts miss.

**Effort:** M · **Deps:** none

---

### 1.3 Run parameters as a first-class form

**Problem.** `--param key=value` exists on the CLI but the UI cannot supply
parameters, and nothing declares what a pipeline accepts.

**Proposal.** Declare the contract, then render it.

```yaml
pipelines:
  backfill_report:
    params:
      start_date:
        type: date
        required: true
      tenant:
        type: enum
        values: [acme, globex]
        default: acme
      dry_run:
        type: bool
        default: true
```

The Run button opens a generated form; values land in `context["params"]` and in
the run configuration snapshot, so a parameterised run can be replayed exactly.
Validation happens before the run is created rather than inside task code.

**Why it matters.** Turns Piply into something a non-author can safely operate.

**Effort:** M · **Deps:** none

---

### 1.4 Log persistence outside SQLite

**Problem.** Every log line is a row. A chatty dbt run can add tens of thousands
of rows per execution, which is why `piply prune` is necessary at all.

**Proposal.** Keep the last N lines in SQLite for the UI's live tail, and stream
the full log to a per-run file under `.piply/logs/<run_id>.log`. Serve old logs
from the file. Optional gzip on completion.

**Why it matters.** Directly attacks the main growth driver of the database, and
makes retention a file-deletion problem instead of a `DELETE` + `VACUUM` one.

**Effort:** M · **Deps:** none

---

### 1.5 Task-level retry

**Problem.** `retry` is pipeline-level. One flaky API call forces a whole-run
retry.

**Proposal.**

```yaml
tasks:
  call_vendor_api:
    type: api
    url: https://vendor/api
    retry:
      attempts: 3
      delay_seconds: 5
      backoff: exponential        # fixed | linear | exponential
      retry_on: [timeout, 5xx]
```

Retries happen inside the task slot, so the DAG never sees the failure.

**Why it matters.** Most real flakiness is one task, not one pipeline.

**Effort:** S · **Deps:** none

---

## Tier 2 — clear value, more work

### 2.1 Per-tenant downstream chains

**Problem.** When N deployments all trigger one shared downstream pipeline, the
downstream serialises across tenants and carries whichever tenant triggered it.
A stuck tenant delays everyone.

**Proposal.** Let a trigger fan out into a tenant-scoped instance.

```yaml
pipeline_deployments:
  acme_ingest:
    template: tenant_ingest
    tenant: acme
    triggers_on_success:
      - pipeline: silver_template
        as_deployment: "{tenant}_silver"   # isolated per tenant
```

**Why it matters.** The shared-downstream pattern is common in multi-tenant
setups and its blast radius is currently all tenants.

**Effort:** L · **Deps:** none

---

### 2.2 Data-aware scheduling (assets)

**Problem.** Triggers are pipeline-to-pipeline. If two upstreams both feed one
downstream, there is no way to say "run when *both* have refreshed."

**Proposal.** Declare what a pipeline produces and consumes.

```yaml
pipelines:
  extract_a:
    produces: [bronze.appointments]
  extract_b:
    produces: [bronze.charges]
  build_silver:
    consumes: [bronze.appointments, bronze.charges]   # waits for both
```

**Why it matters.** Removes hand-maintained trigger graphs, which drift.

**Effort:** L · **Deps:** none

---

### 2.3 Pipeline-level concurrency pools

**Problem.** `max_parallel_tasks` is per pipeline. Nothing caps a *shared*
resource across pipelines, so eight concurrent runs can open eight database
connections to the same warehouse.

**Proposal.**

```yaml
pools:
  warehouse: 2
  ecw_sessions: 1

tasks:
  load_bronze:
    pool: warehouse
```

**Why it matters.** The usual reason for staggered cron schedules is really an
undeclared resource limit. This makes it explicit.

**Effort:** M · **Deps:** none

---

### 2.4 Task result caching

**Proposal.** Skip a task whose inputs have not changed.

```yaml
tasks:
  expensive_transform:
    cache:
      key: ["{tenant}", "{run_date}"]
      ttl: 24h
```

Reuses the existing `task_outputs` table for the cached value.

**Why it matters.** Makes re-running a partially failed pipeline cheap.
Complements the existing resume-mode retry.

**Effort:** M · **Deps:** none

---

### 2.5 Plugin hooks

**Proposal.** Entry-point discovery for third-party operators and sensors.

```toml
[project.entry-points."piply.operators"]
databricks = "my_pkg.operators:DatabricksOperator"

[project.entry-points."piply.sensors"]
s3 = "my_pkg.sensors:S3Sensor"
```

**Why it matters.** Lets the core stay small while teams add what they need.
The operator surface is already narrow enough to be a stable contract.

**Effort:** M · **Deps:** none

---

## Tier 3 — user experience

### 3.1 Run comparison

Diff two runs side by side: task durations, statuses, outputs, and the
configuration snapshots. Answers "what changed between the run that worked and
the one that didn't." The snapshots already exist.

**Effort:** M

### 3.2 Gantt / timeline view

The DAG shows structure but not time. A timeline of task start/end makes the
critical path and the idle gaps obvious. Duration data is already recorded.

**Effort:** M

### 3.3 Log search across the whole history

Current log search is a `LIKE` scan. SQLite FTS5 is built into the stdlib
sqlite3 module, so full-text search costs one virtual table and no dependency.

**Effort:** S

### 3.4 Config editing in the UI

Edit a pipeline's YAML in the browser with validation and a `piply plan` preview
before saving. Needs care: file writes, concurrent edits, and an audit trail.

**Effort:** L

### 3.5 Dark mode

The stylesheet already uses CSS custom properties, so this is largely a second
variable block plus a toggle.

**Effort:** S

### 3.6 Keyboard navigation

`/` to search, `g p` for pipelines, `g r` for runs, `r` to re-run the focused
run. Cheap, and a real speed-up for daily operators.

**Effort:** S

---

## Tier 4 — scale and operations

### 4.1 Multiple worker processes

**Problem.** Piply is single-process by design. `owner_pid` recovery and the
in-process write lock both assume it. This is still true with the PostgreSQL
backend: a shared database does not by itself make the runtime multi-instance.

**Proposal.** Optional worker processes claiming queue items with an atomic
`UPDATE ... WHERE status='queued'`. The queue already has claim/requeue
semantics; the hard parts are the write lock and log streaming.

**Cost.** This is the change most likely to compromise "lightweight." Worth
doing only when a real workload proves one process is not enough.

**Effort:** L

### 4.2 Optional PostgreSQL backend — **shipped**

Implemented behind `piply/core/dialects.py`. Set `PIPLY_DATABASE` to a
PostgreSQL DSN; SQLite remains the zero-config default. See
[YAML Specification §11](YAML_SPECIFICATION.md#11-runtime-storage-and-external-databases).

Not yet covered: sharing one database between several Piply instances, which
needs §4.1 first.

### 4.3 Structured JSON logging

`--log-format json` for the server, so runtime logs land cleanly in Loki,
CloudWatch, or Datadog. Task output stays raw.

**Effort:** S

### 4.4 OpenTelemetry traces

One span per run, one per task, with the existing run/task ids as attributes.
Strictly an optional extra.

**Effort:** M · **Deps:** `opentelemetry-sdk` as an extra

### 4.5 Read-only role

Today auth is all-or-nothing. A viewer role that can browse but not trigger,
cancel, or delete is a common requirement once more than one team can see the UI.

**Effort:** M

---

## Deliberately not planned

Recorded so the reasoning is not relitigated.

| Idea | Why not |
| --- | --- |
| Full expression language for `run_if` | The whole point of `run_if` is that it is *not* one. Complex logic belongs in a Python task. |
| Built-in Kubernetes executor | Contradicts local-first. A `type: cli` task running `kubectl` covers it. |
| Bundled Airflow/Prefect compatibility layer | Doubles the runtime surface to serve migrations that happen once. |
| Web-based YAML DAG builder | Enormous surface area, and the YAML is already the source of truth in git. |
| `prometheus_client` dependency | `/metrics` is ~150 lines of string formatting. Not worth a dependency. |
| Replacing SQLite by default | Zero-config setup is a core promise. Postgres is available behind an extra instead. |

---

## Suggested order

If picking up this list, this order front-loads value and keeps each step small:

1. **1.1 Alerting** — biggest gap, smallest cost
2. **1.5 Task-level retry** — small, removes most spurious full-run retries
3. **3.3 FTS log search** + **3.5 Dark mode** + **3.6 Keyboard nav** — cheap wins
4. **1.4 Log persistence** — before the database becomes the problem
5. **1.3 Run parameters** — unlocks non-author operators
6. **1.2 SLA tracking** — pairs naturally with alerting
7. **2.3 Concurrency pools** — replaces staggered-cron workarounds
8. **2.5 Plugin hooks** — lets the core stop growing
9. Everything else, driven by real demand rather than this list


### Suggestion by Guru:

1. UI: Pipeline page: every pipeline show last 5 run in Dot format, every DOT will represent a Run & on click on dot redirect to that particular run page.
