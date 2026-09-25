# Roadmap

What is worth doing next, and when. This is release-oriented: **0.3** is what
the current codebase is close enough to finish; later sections are directional.

[Future Features](FUTURE_FEATURES.md) is the wider catalogue of ideas with the
reasoning behind each. This document is the subset with a date attached.

Every item is measured against the four things Piply is trying to be:

> **lightweight** · **fast** · **secure** · **easy to use** — a YAML-based
> low-code platform that a data engineer can adopt in an afternoon.

An item that costs one of those to buy another is called out as such.

---

## Where things stand

Shipped in 0.2.x, so the roadmap does not relitigate them:

pipeline templates and deployments · entity expansion · conditional values ·
task and entity priority · timeouts and kill grace · dry-run preview · artifacts
· backfill and replay · retention and pruning · Prometheus metrics ·
diagnostics · sensor health · run-history dots · run filtering, sorting, and
lineage · collapsible graph panels · accounts with per-pipeline permissions ·
central SMTP and notifications · optional PostgreSQL · SQLite-to-PostgreSQL
migration.

Runtime dependencies: **8**. That number is the budget. Anything new should be
an optional extra.

---

## 0.3 — Operability

**Theme: know what happened, and who did it — and keep the config workable as
it grows.**

The security audit closed the "who can do what" question. The unanswered half is
"who *did* what", the reliability gap is that one flaky task still costs a whole
run, and the authoring gap is that a real tenant rollout produces a config file
nobody wants to edit.

### 0.3.0 Split `piply.yaml` across files — **shipped in 0.3.0**

A production config has reached 974 lines: 29 deployments, 8 pipelines, and 4
templates in one file. Adding a tenant means editing it, and two people touching
unrelated tenants conflict in git for no reason.

An `include:` list in the root file, with the master file keeping the deployment
inventory and the volatile definitions moving out:

```yaml
# piply.yaml keeps project settings + all 29 deployments
include:
  - config/templates/*.yaml
  - config/pipelines/*.yaml
```

Edit history decided that split: across 35 commits, `pipelines` was touched
alone 12 times and deployments only 4, while templates and their deployments
changed together just 6 times. The churn belongs in its own files; the stable
inventory belongs in the master.

Purely additive — a config with no `include:` behaves exactly as it does now.
The parts that need care are duplicate detection across files (an error, never
last-wins), file provenance in every error message, and reload watching all
included files rather than one. Full design, including the rules table, is in
[Future Features §1.7](FUTURE_FEATURES.md#17-splitting-piplyyaml-across-files).

Shipped as `include:`, with duplicate detection across files, both file
names in every conflict error, and reload watching every included file.

### 0.3.1 Audit log — **highest value**

There is currently no record of who triggered, retried, cancelled, deleted, or
reconfigured anything. For a tool sitting in front of healthcare or financial
data, that is the most conspicuous remaining gap, and it is now cheap: accounts
already exist, and every action already passes a permission check.

One table, written from the same place permissions are enforced:

| Column | Meaning |
| --- | --- |
| `at` | timestamp |
| `username` | actor, or `system` for the scheduler |
| `action` | `run`, `retry`, `cancel`, `delete`, `pause`, `grant`, `smtp_update` |
| `target` | pipeline or run id |
| `detail` | JSON: overrides, permissions granted, previous value |

Surfaced as an admin-only page and `piply audit --since 7d`. Retained by the
existing prune settings.

**Cost:** S · no new dependency · one table

### 0.3.2 Task-level retry

Pipeline-level `retry:` means one flaky vendor API forces a whole-run retry.
Most real flakiness is one task.

```yaml
tasks:
  call_vendor_api:
    retry:
      attempts: 3
      delay_seconds: 5
      backoff: exponential
```

Retries happen inside the task slot, so the DAG never sees the failure.

**Cost:** S · no new dependency

### 0.3.3 Session and token management

Follow-through on the auth work:

- invalidate sessions on password change (currently they survive up to 12 hours)
- named API tokens per account, replacing the single global `PIPLY_API_TOKEN`
- show and revoke active sessions from the admin page

Token callers are currently treated as admin, which is the bluntest edge left in
the permission model.

**Cost:** M · no new dependency

### 0.3.4 SLA tracking

A pipeline that normally takes 10 minutes and is now at 55 is invisible until
its hard timeout. `expected_duration: 15m` plus a warning state, reusing the
`notify:` channel that shipped in 0.2.1.

**Cost:** S

### 0.3.5 Polish

- **Dark mode.** The palette is already CSS variables; this is one media query
  and a toggle.
- **Keyboard navigation.** `/` to search, `g p` for pipelines, `r` to re-run.
- **Structured JSON logging** behind `PIPLY_LOG_FORMAT=json`, so the server's
  own logs land in an aggregator.

**Cost:** S each

---

## 0.4 — Scale

**Theme: stop being one process, without becoming a distributed system.**

### 0.4.1 Concurrency pools

The one that matters most for the RCM-style workload. Eight tenant deployments
that all trigger the same downstream pipeline serialise behind each other, and
staggered cron times are a workaround for it.

```yaml
pools:
  warehouse: {max_concurrent: 3}

pipelines:
  Bronze_to_Silver:
    pool: warehouse
```

Expresses the real constraint — how many concurrent dbt runs the warehouse can
take — rather than encoding it in cron offsets.

**Cost:** M · no new dependency · the queue table already exists

### 0.4.2 Multiple worker processes

Today one process runs everything. A worker pool that claims queue rows would
lift the ceiling. The queue already has the dedupe and dispatch columns needed;
what is missing is row claiming (`SELECT ... FOR UPDATE SKIP LOCKED` on
PostgreSQL, a claim column on SQLite).

**Tension:** this is where "lightweight" starts to strain. It should stay
opt-in, single-process by default, and must not require a broker.

**Cost:** L

### 0.4.3 Log persistence outside the metadata store

Logs dominate row count. Streaming them to files or object storage with only an
index row in the database would keep the database small and make retention
cheap.

**Cost:** M

### 0.4.4 Secret-manager backends

The `secrets:` block already abstracts backends. Adding Vault, AWS Secrets
Manager, and Azure Key Vault as **optional extras** would remove the last reason
to keep credentials in `.env` on the host.

**Cost:** M · optional extras only, never a default dependency

---

## 0.5 and beyond — directional

Real demand should decide these, not this list.

| Idea | Note |
| --- | --- |
| Run parameters as a declared form | `params:` schema with types, defaults, and validation, rendered as a UI form. Manual runs already prompt for *undeclared* `{placeholder}` values; this would add declared, typed inputs |
| Data-aware scheduling | "run when this table updates" rather than a clock |
| Task result caching | Skip a task whose inputs did not change |
| Plugin hooks | Lets the core stop growing; third parties add task types |
| Run comparison | Diff two runs' durations, outputs, and configs |
| Gantt / timeline view | Where time actually went inside a run |
| OpenTelemetry traces | Optional extra, for shops that already collect traces |
| Config editing in the UI | Real tension with "YAML is the source of truth in git" |

---

## Still deliberately not planned

Recorded so it is not relitigated. Full reasoning in
[Future Features](FUTURE_FEATURES.md).

| Idea | Why not |
| --- | --- |
| A full expression language for `run_if` | The point of `run_if` is that it is *not* one. Complex logic belongs in a Python task. |
| Built-in Kubernetes executor | Contradicts local-first. `type: cli` running `kubectl` covers it. |
| Airflow/Prefect compatibility layer | Doubles the runtime surface for a migration that happens once. |
| Web-based DAG builder | Enormous surface, and the YAML is already the source of truth. |
| `prometheus_client` dependency | `/metrics` is ~150 lines of string formatting. |
| Replacing SQLite by default | Zero-config setup is a core promise. PostgreSQL is the opt-in. |
| Task sandboxing | Running your commands *is* the product. Isolation belongs to the container or host. |

---

## How this maps to the four goals

| Goal | Biggest remaining gap | Addressed by |
| --- | --- | --- |
| Lightweight | Nothing pressing — 8 runtime dependencies | Hold the line: new backends stay extras |
| Fast | Log table growth on long-lived installs | 0.4.3 log persistence, existing prune |
| Secure | No audit trail; global API token | 0.3.1 audit log, 0.3.3 token management |
| Easy to use | UI cannot supply run parameters | 0.5 declared `params:` |

If only one thing gets built next, make it **0.3.1 audit logging**. It is small,
needs no new dependency, and it is the question every reviewer asks first once
more than one person can press Run.
