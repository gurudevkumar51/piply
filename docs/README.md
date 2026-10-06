# Piply Documentation

These guides describe the v0.3.4 candidate. Start with the
[release assessment](RELEASE_CHECK.md) for the go/no-go decision, tested
environments, known limits, and publishing checklist.

Start here. Every page below is self-contained; follow the links rather than
reading in order.

**Guides teach a topic. The [YAML Specification](YAML_SPECIFICATION.md) is the
exhaustive reference** — when a guide says "every key is in the spec", that is
where it means. Keeping enumeration in one place is what stops the two drifting
apart.

---

## New here?

1. [README](../README.md) — what Piply is and a five-minute install
2. [Pipelines](PIPELINES.md) — the unit you will write first
3. [Tasks](TASKS.md) — what goes inside one
4. [CLI Reference](CLI.md) — `piply validate`, `piply run`, `piply plan`
5. [Examples](EXAMPLES.md) — runnable patterns to copy

---

## Writing config

| Page | Covers |
| --- | --- |
| [Pipelines](PIPELINES.md) | Schedules, concurrency, retries, chaining, tags, splitting the config |
| [Tasks](TASKS.md) | The six task types, dependencies, failure behaviour, artifacts, logs |
| [Variables](VARIABLES.md) | `{name}`, the four scopes, precedence, conditionals, run-time values |
| [Environment Variables](ENVIRONMENT.md) | `${NAME}`, `.env`, every `PIPLY_*` setting, what a task sees |
| [Templates and Deployments](TEMPLATES_AND_DEPLOYMENTS.md) | One pipeline shape, many tenants |
| [Sensors](SENSORS.md) | Triggering on a file, a SQL query, or an endpoint |
| [Notifications](NOTIFICATIONS.md) | Teams and email, routing at every level, delivery failures |
| [YAML Specification](YAML_SPECIFICATION.md) | **Reference:** every key, default, and alias |

---

## Operating

| Page | Covers |
| --- | --- |
| [CLI Reference](CLI.md) | Every command and flag |
| [UI Guide](UI_GUIDE.md) | Every page and the question it answers |
| [API Reference](API.md) | Every HTTP route and the permission it needs |
| [Runbook](RUNBOOK.md) | Operating procedures, incident checks, backup/restore, deployment checklist |
| [Release Assessment](RELEASE_CHECK.md) | v0.3.4 blockers, test evidence, environment gaps, reproducible release gates |
| [Metadata Store](DATABASE.md) | SQLite, PostgreSQL, backup, migration, the schema |
| [Authentication](AUTHENTICATION.md) | Accounts, roles, per-pipeline permissions |
| [Security](SECURITY.md) | Trust model and a deployment checklist |
| [Audit History](AUDIT.md) | Actor attribution, action history, admin queries, retention and migration |
| [Runtime Lifecycles](LIFECYCLES.md) | Scheduler, run, task, retry, recovery, retention |

---

## When something is wrong

| Page | Covers |
| --- | --- |
| [FAQ](FAQ.md) | "Why is it doing that", plus an **error-message index** |
| [Migration Guide](MIGRATION.md) | Moving onto templates and deployments |

Fastest first steps:

```bash
piply validate                  # config errors, with the file and key
piply plan claim_extract        # what a run would do, after interpolation
piply diagnostics               # scheduler, worker, and sensor health
```

---

## Project direction

| Page | Covers |
| --- | --- |
| [Changelog](../CHANGELOG.md) | What changed in each release |
| [Backlog](BACKLOG.md) | Consolidated open work, priorities, acceptance criteria, likely code touch points |
| [Roadmap](ROADMAP.md) | What is planned next |
| [Future Features](FUTURE_FEATURES.md) | Proposed ideas, ranked by value vs cost — and what is deliberately *not* planned |
| [Technical Architecture](architecture/technical_architecture.md) | Maintainer guide to the whole system |

---

## Common questions, answered fast

| Question | Page |
| --- | --- |
| Why is `{my_var}` not being replaced? | [Variables §7](VARIABLES.md#7-debugging) |
| Two files both need a `batch_size` | [Variables §2](VARIABLES.md#2-where-to-declare-a-variable) |
| How do I stop schedules while developing? | [Environment](ENVIRONMENT.md#3-piply-settings-reference) — `PIPLY_SCHEDULER_ENABLED=false` |
| A task may fail without failing the run | [Tasks §3](TASKS.md#3-when-a-task-fails) — `allow_failure` |
| Alert everything without editing every pipeline | [Notifications](NOTIFICATIONS.md) — `notifications.defaults` |
| My webhook works in `curl` but not in Piply | [Environment §5](ENVIRONMENT.md#5-the-service-problem) — CA bundles |
| It works in my shell but not on schedule | [Environment §5](ENVIRONMENT.md#5-the-service-problem) |
| Where did my run history go after redeploy? | [Metadata Store](DATABASE.md#6-docker-not-losing-your-data) |
| Same pipeline for 29 tenants | [Templates and Deployments](TEMPLATES_AND_DEPLOYMENTS.md) |
