# Tasks

A task is one executable unit inside a pipeline. This page covers the six task
types and the keys that control how a task runs, fails, and reports.

For the exhaustive key list with every alias, see
[YAML Specification §6](YAML_SPECIFICATION.md#6-tasks).

---

## Contents

1. [The six types](#1-the-six-types)
2. [Dependencies and order](#2-dependencies-and-order)
3. [When a task fails](#3-when-a-task-fails)
4. [Timeouts and cancellation](#4-timeouts-and-cancellation)
5. [Conditional tasks](#5-conditional-tasks)
6. [Artifacts](#6-artifacts)
7. [Environment](#7-environment)
8. [Logs](#8-logs)

---

## 1. The six types

| Type | Runs | Key fields |
| --- | --- | --- |
| `python` | A Python script or callable | `path`, `call`, `args`, `kwargs`, `python` |
| `cli` | A shell command | `command`, `path`, `shell`, `cwd` |
| `api` | An HTTP request | `url`, `method`, `headers`, `body`, `expected_status` |
| `webhook` | An outbound webhook | `url`, `method`, `headers`, `body` |
| `email` | Sends mail via the central SMTP settings | `to`, `subject`, `body` |
| `ssh` | A command on a remote host | `host`, `command` |

```yaml
tasks:
  extract:
    type: cli
    command: "python extract.py --date {run_date}"

  transform:
    type: python
    path: transform.py
    depends_on: [extract]

  publish:
    type: api
    url: "https://api.example.com/refresh"
    method: POST
    expected_status: [200, 202]
    depends_on: [transform]
```

Paths resolve against `workspace:`, not the working directory. A task that
"points to a missing script" is nearly always relative to the wrong base.

---

## 2. Dependencies and order

`depends_on` builds a DAG. Tasks with no unmet dependency run in parallel, up to
the pipeline's `max_parallel_tasks`.

```yaml
tasks:
  extract_a: { type: cli, command: ./a.sh }
  extract_b: { type: cli, command: ./b.sh }     # runs alongside extract_a
  combine:
    type: cli
    command: ./combine.sh
    depends_on: [extract_a, extract_b]          # waits for both
```

A cycle is a load error naming the task it was found at.

`priority` orders tasks that are otherwise ready at the same time — a number, a
named level (`high`), or star shorthand (`***`).

---

## 3. When a task fails

Three separate questions, three separate keys. Mixing them up is the most common
source of "why did my run fail / not fail".

| Question | Key |
| --- | --- |
| Should **downstream** tasks still run? | `on_upstream_failure` |
| Should the **run** still be a success? | `allow_failure` |
| Should **anyone be told**? | `alert_on_failure` |

### `on_upstream_failure`

What a task does when something it depends on did not succeed.

| Value | Behaviour |
| --- | --- |
| `skip` | Default. This task is skipped |
| `continue` | Run anyway |
| `fail` | Fail this task immediately |

### `allow_failure`

By default **any** failed task fails its run. `allow_failure: true` marks a task
best-effort: it still records its own `failed` status and exit code, but the run
finishes `success`.

```yaml
tasks:
  optional_sync:
    type: cli
    command: ./sync.sh
    allow_failure: true       # a refresh that is nice to have

  main_load:
    type: cli
    command: ./load.sh
    depends_on: [optional_sync]
    on_upstream_failure: continue   # run even though the sync failed
```

| You want | Set |
| --- | --- |
| Downstream runs anyway, run still fails | `on_upstream_failure: continue` |
| Run stays green, downstream skipped | `allow_failure: true` |
| Both | Both keys |

### `alert_on_failure`

A tolerated failure is silent by design — that is the point of tolerating it.
When you still want to know, pair the two:

```yaml
optional_sync:
  type: cli
  command: ./sync.sh
  allow_failure: true
  alert_on_failure: true    # tell the pipeline's on_failure destinations
```

The card is sent **only** when the run itself succeeded; a failed run already has
its own alert. See [Notifications](NOTIFICATIONS.md).

### Retries

Retry policy is set per pipeline, not per task — see
[Pipelines](PIPELINES.md#4-retries). An automatic retry defaults to `resume`, so
tasks that already succeeded are **not** run again; only the failed task and
whatever depends on it. To re-run a single task from a failed run:

```bash
piply tasks retry RUN_ID TASK_ID           # resume: this task and downstream
piply tasks retry RUN_ID TASK_ID --mode startover
```

---

## 4. Timeouts and cancellation

```yaml
tasks:
  long_extract:
    type: cli
    command: ./extract.sh
    timeout: 5m              # 30, 30s, 5m, 1h
    kill_grace_period: 5     # seconds between terminate and kill
```

On timeout or cancellation Piply terminates the **whole process tree**, not just
the direct child — a shell script that spawned `python` does not leave it
running.

---

## 5. Conditional tasks

`run_if` decides at run time whether a task executes:

```yaml
tasks:
  send_report:
    type: email
    run_if: "{report_type} == 'payment'"
```

It is evaluated against the run's resolved variables, so it can depend on a
value supplied with `--var`. See [Variables](VARIABLES.md).

A task can also be switched off outright with `enabled: false`, which accepts a
conditional.

---

## 6. Artifacts

A task declares glob patterns; Piply records where the produced files live, how
large they are, and when they were written, so the UI can list and download them.

```yaml
tasks:
  extract:
    type: cli
    command: ./extract.sh
    artifacts:
      - "out/*.csv"
      - "out/manifest.json"
```

Files are **not copied** into the database — only their paths and metadata, up to
200 files per task. Two consequences:

- The files need somewhere durable to live. In Docker that means a volume, and
  `PIPLY_ARTIFACTS_DIR` must be in the download allow-list. See
  [Metadata Store](DATABASE.md#6-docker-not-losing-your-data).
- Paths are stored absolute, so records from before a move point at the old
  location.

```bash
piply artifacts RUN_ID
```

---

## 7. Environment

A task's environment is layered, later winning:

1. The Piply process environment
2. `defaults.env` (project)
3. The pipeline's `env:`
4. The task's `env:`

Plus `PIPLY_RUN_ID` and `PIPLY_CONTEXT_JSON`, which every task process receives.
See [Environment Variables](ENVIRONMENT.md#4-what-a-task-sees).

---

## 8. Logs

Task stdout and stderr are captured **as they are produced**, not buffered until
the task ends — a long extract streams into the UI and `piply logs -f` while it
runs.

Capture is thread-scoped, so parallel tasks never interleave into each other's
logs.

When a task raises, the traceback is recorded as a single log entry with Piply's
own frames trimmed, so a newest-first view does not show it upside down.

```bash
piply logs -f --task transform
```

---

## Related

- [Pipelines](PIPELINES.md) — the container a task lives in
- [YAML Specification §6](YAML_SPECIFICATION.md#6-tasks) — every key and alias
- [Variables](VARIABLES.md) — what `{...}` in a command resolves to
- [Notifications](NOTIFICATIONS.md) — `alert_on_failure` in context
- [Examples](EXAMPLES.md) — runnable patterns per task type
