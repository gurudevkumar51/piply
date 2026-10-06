# Pipelines

v0.3.4 separates schedule pause from run pause: `piply pause PIPELINE_ID` holds
automatic triggers; `piply pause-run RUN_ID` holds new tasks in one run. The
run-admission limit is shared across direct starts and queued triggers. See the
[release assessment](RELEASE_CHECK.md) for remaining deployment sign-off.

A pipeline is a named group of tasks with a schedule, a retry policy, and a list
of who to tell when it finishes.

Cron schedules use IANA timezone data. The package installs `tzdata` on Windows;
minimal POSIX images must supply OS timezone data or install `tzdata` separately.

For the exhaustive key list see
[YAML Specification §4](YAML_SPECIFICATION.md#4-pipeline-keys).

---

## Contents

1. [The shape of one](#1-the-shape-of-one)
2. [Schedules](#2-schedules)
3. [Concurrency](#3-concurrency)
4. [Retries](#4-retries)
5. [Chaining pipelines](#5-chaining-pipelines)
6. [Tags](#6-tags)
7. [Enabling, disabling, pausing](#7-enabling-disabling-pausing)
8. [Splitting the config](#8-splitting-the-config)

---

## 1. The shape of one

```yaml
pipelines:
  claim_extract:
    title: Claim Extract              # display name
    description: Pulls claims nightly.
    tags: [claims, prod]

    schedule:
      cron: "0 2 * * *"
    timezone: UTC

    max_concurrent_runs: 1
    max_parallel_tasks: 4
    timeout: 2h

    retry:
      attempts: 3
      delay: 30s

    notifications:
      on_failure: [claims_oncall]

    triggers_on_success: [claim_load]

    tasks:
      extract: { type: cli, command: ./extract.sh }
```

Only `tasks:` is required. Everything else has a default.

---

## 2. Schedules

```yaml
schedule:
  cron: "0 2 * * *"       # 02:00 daily
```

```yaml
schedule:
  every: 15m              # 30s, 15m, 2h, 1d
```

No `schedule:` means manual only. `timezone:` on the pipeline overrides the
project timezone — set it explicitly for anything that must land at a local hour,
or daylight saving will move it.

The scheduler polls every 10s by default
(`PIPLY_SCHEDULER_POLL_INTERVAL_SECONDS`), so a schedule fires within about that
window rather than on the exact second.

To develop without schedules firing, set `PIPLY_SCHEDULER_ENABLED=false` in the
project `.env` — see [Environment Variables](ENVIRONMENT.md).

---

## 3. Concurrency

Two independent limits:

| Key | Controls |
| --- | --- |
| `max_concurrent_runs` | How many runs of **this pipeline** overlap. Default 1 |
| `max_parallel_tasks` | How many **tasks within one run** execute at once. Default 4 |

`max_concurrent_runs` counts queued and running records, including paused runs.
Capacity is reserved atomically when the run and its tasks are created, across
services sharing the same metadata store. Limits are per pipeline, not per
tenant or selected task. Different deployments with different pipeline ids have
separate limits; sharing a warehouse does not create a cross-pipeline pool.

At capacity, direct manual/task/retry/chain/backfill requests reject without
creating another run: the API returns 409 and the CLI exits 1 with a limit
message. They are not automatically queued, even with `--wait`. Automatic
schedule/sensor/retry/downstream triggers remain queued in dispatch order until
capacity opens. A limit of two admits two runs rather than serializing them.
Pausing a run does not release its slot. Repeating an existing schedule slot
returns its original run instead of reserving another slot.

---

## 4. Retries

```yaml
retry:
  attempts: 3
  delay: 30s
  mode: resume      # the default
```

The policy is per pipeline. A failed run is retried up to `attempts` times.

| Mode | Behaviour |
| --- | --- |
| `resume` | **Default.** Keeps the tasks that already succeeded; re-runs the failed one and everything downstream of it |
| `startover` | Re-runs every task, including the ones that passed |

`resume` is the default because re-running a task that already succeeded costs
time and repeats whatever it wrote. On a parallel pipeline that matters a lot:
one failing task should not drag two long, successful extracts through a second
execution.

Reach for `startover` when a partial run has to be discarded before retrying — a
non-idempotent load, or one that leaves half-written output behind.

A retry always waits for the **whole run** to finish, not just the task that
failed. A sibling still executing is never overlapped by its own retry.

To retry after the fact, from a specific task rather than the start:

```bash
piply tasks retry RUN_ID TASK_ID
```

For a task whose failure should not fail the run at all, see
[`allow_failure`](TASKS.md#allow_failure).

---

## 5. Chaining pipelines

```yaml
pipelines:
  claim_extract:
    triggers_on_success: [claim_load]
```

`claim_load` starts when `claim_extract` succeeds. The chain is validated at
load: a cycle, a self-reference, or a target that does not exist are all errors.

The downstream run **inherits the parent run's variables and parameters**, so a
`{run_date}` supplied once flows through the whole chain. A manual run has no
parent and falls back to its own variables — see
[Downstream Inheritance](YAML_SPECIFICATION.md#9-downstream-inheritance).

The run detail page shows both directions of the chain, with links.

---

## 6. Tags

```yaml
pipelines:
  claim_extract:
    tags: [claims, prod]
```

Tags are labels you can filter by. On the pipelines page, click one to filter to
it; on the runs page there is a tag dropdown, and `?tag=claims` works directly in
the URL.

A file can tag everything it declares at once:

```yaml
# claims/piply_claims.yaml
pipeline_defaults:
  tags: [claims]
```

File tags are **added to** a pipeline's own rather than replacing them, because
"this lives in the claims file" stays true whatever else the pipeline calls
itself.

---

## 7. Enabling, disabling, pausing

| Mechanism | Where it lives | Use for |
| --- | --- | --- |
| `enabled: false` | YAML | Permanently off, in version control |
| `piply pause` / the UI | Database | Temporarily off, without a deploy |

`enabled:` accepts a conditional, which is how one config serves two
environments:

```yaml
enabled:
  if: env == "dev"
  then: false
  else: true
```

A paused pipeline stays paused across restarts — the state is in the database,
not the config. It also still accepts manual runs; pausing stops the *schedule*.

**Resuming is not retroactive.** Slots that fell while a pipeline was paused are
not replayed: pause three days of a nightly job and resuming gives you one run
at the next scheduled time, not four at once. A slot that passed earlier on the
day you resume is gone too — resuming picks up from the moment you resume.

That is different from an **outage**. If the server was simply down over a
scheduled window, those slots *are* backfilled when it comes back, because
nobody asked for them to be skipped.

| Situation | On the way back |
| --- | --- |
| Pipeline was paused | Nothing replayed; next slot runs normally |
| Server was down | Missed slots backfilled |

---

## 8. Splitting the config

One 900-line `piply.yaml` makes every change a merge conflict. `include:` splits
it:

```yaml
# piply.yaml
version: "1"
title: Claims Platform
workspace: .
include:
  - "*/piply_*.yaml"
```

Each file can then set defaults for the pipelines it declares:

```yaml
# claims/piply_claims.yaml
pipeline_defaults:
  tags: [claims]
  variables:
    source_system: ECW
  notifications:
    on_failure: [claims_oncall]

pipelines:
  claim_extract: { ... }
  claim_load:    { ... }
```

There is deliberately **no last-wins**: the same pipeline in two files is an
error naming both, because silently preferring one would mean editing a pipeline
and watching nothing change.

Every included file is watched, so editing one takes effect without a restart.

Details in [YAML Specification §3](YAML_SPECIFICATION.md#3-top-level-keys).

---

## Related

- [Tasks](TASKS.md) — what goes inside a pipeline
- [Templates and Deployments](TEMPLATES_AND_DEPLOYMENTS.md) — one pipeline, many tenants
- [Variables](VARIABLES.md) — scoping and precedence
- [Notifications](NOTIFICATIONS.md) — who hears about a run
- [Sensors](SENSORS.md) — triggering on a file, a query, or an endpoint
- [Runtime Lifecycles](LIFECYCLES.md) — what the scheduler actually does
