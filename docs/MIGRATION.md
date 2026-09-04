# Migrating To Pipeline Templates And Deployments

Templates and deployments are **optional**. Existing configs keep working
untouched — this guide is only for teams that have copy-pasted the same pipeline
once per tenant, region, or environment.

---

## When It Is Worth It

Use templates when you recognise this shape:

```yaml
pipelines:
  acme_ingest:
    schedule: {cron: "0 * * * *"}
    tasks:
      ingest:
        type: cli
        command: python ingest.py --tenant acme

  globex_ingest:            # identical except for one word
    schedule: {cron: "0 * * * *"}
    tasks:
      ingest:
        type: cli
        command: python ingest.py --tenant globex
```

Stay with plain `pipelines:` when each pipeline is genuinely different. A
template that is deployed once adds indirection without removing duplication.

---

## Compatibility Guarantees

| Guarantee | Detail |
| --- | --- |
| Backward compatible | `pipelines:` needs no change; both forms can coexist in one file |
| Same runtime | deployments become ordinary `PipelineDefinition` records after loading |
| Same ids | the deployment id *is* the pipeline id in the CLI, API, UI, and DAG |
| Entity expansion | still applies inside a deployed template |
| Run history | pipeline ids are unchanged if you keep the deployment id the same |

That last row is the one that matters most: **name each deployment after the
pipeline it replaces** and the existing run history, pause state, and schedule
slots stay attached.

---

## Step-by-step

### 1. Find the shared shape

Take the duplicated pipelines and note every value that differs between them.
Those become variables; everything else becomes the template.

### 2. Extract the template

```yaml
pipeline_templates:
  tenant_ingest:
    description: Hourly ingest for one tenant.
    schedule:
      cron: "0 * * * *"
    env:
      STAGE: production
    retry:
      attempts: 2
      mode: resume
    timeout: 30m
    max_parallel_tasks: 2
    triggers_on_success:
      - tenant_report
    tasks:
      ingest:
        type: cli
        priority: high
        timeout: 5m
        command: python ingest.py --tenant {tenant}
      verify:
        type: cli
        depends_on: [ingest]
        command: python verify.py --tenant {tenant}
```

Use `{tenant}` (and any other variable) wherever the copies differed.

### 3. Declare the deployments

Keep the original pipeline ids as the deployment ids:

```yaml
pipeline_deployments:
  acme_ingest:
    template: tenant_ingest
    tenant: acme

  globex_ingest:
    template: tenant_ingest
    tenant: globex
    environment: staging
    schedule:
      cron: "30 * * * *"       # this one runs on the half hour instead
```

### 4. Delete the originals

Remove the now-duplicated entries from `pipelines:`. A deployment id may not
collide with a `pipelines:` id, so the loader will tell you if you miss one.

### 5. Verify before you commit

```bash
piply validate --config piply.yaml
piply plan --config piply.yaml
```

`piply plan` prints, per deployment, the resolved variables, expanded entities,
execution order, and the fully interpolated command for every task. Diff that
output against the same command from before the migration — it should match
exactly.

---

## What A Deployment Inherits

Everything on the template, unless the deployment overrides it. **Every pipeline
key is valid in both places** — there is no template-only key. What differs is
*how* an override combines with the template's value:

| Key | When the deployment also sets it |
| --- | --- |
| `tasks` | **merged** — override one key of one task without restating it |
| `variables` | **merged** — the template's other variables survive |
| `env`, `env_file`, `env_files` | **merged** |
| `retry`, `notify` | **merged** — set `attempts` alone and keep `mode` |
| `timeout`, `execution`, `max_parallel_tasks`, `max_concurrent_runs` | replaced |
| `description`, `enabled`, `title` | replaced |
| `tags`, `triggers_on_success` | **replaced wholesale** — lists do not append |
| `schedule` | merged, but see the warning below |
| `sensors`, `entities` | merged by key |

> ⚠️ **`schedule` merges, and `cron` beats `every`.** A template with `cron:` and
> a deployment with `every:` produces a mapping holding both, and the parser
> picks `cron` — so that deployment silently keeps the template's cron. The
> reverse (`every:` on the template, `cron:` on the deployment) works fine.
> Check with `piply plan`, which prints each deployment's effective schedule.

Deployment-only keys:

| Key | Effect |
| --- | --- |
| `template` (**required**) | which template to expand |
| `tenant` / `tenant_id` | sets the `{tenant}` and `{tenant_id}` variables |
| `environment` | sets the `{environment}` variable |
| `title` | defaults to a title-cased deployment id |

Merging is recursive: a deployment can override a single key inside a single
task without restating the whole task.

```yaml
pipeline_deployments:
  acme_ingest:
    template: tenant_ingest
    tenant: acme
    tasks:
      verify:
        timeout: 15m           # only this key changes
```

---

## Rollback

The migration is a pure config change. To roll back, restore the previous
`pipelines:` block and delete the template and deployment sections. Because run
history keys off the pipeline id, keeping the ids stable means nothing in the
database needs to change in either direction.

---

## Common Problems

**"Pipeline deployment 'x' conflicts with an existing pipeline id"**
The old `pipelines:` entry is still there. Delete it.

**"Pipeline deployment 'x' references unknown template 'y'"**
Check the `template:` spelling against the `pipeline_templates:` key.

**A command still contains a literal `{tenant}`**
The deployment did not set `tenant`. Unresolved placeholders are left visible on
purpose; `piply plan` also reports them as a warning.

**A downstream pipeline gets the wrong tenant's values**
Downstream runs inherit the upstream deployment's variables and env. If the
downstream is also deployed per tenant, give it its own deployment so its own
values win where they are explicitly set.

**A deployment's `schedule` seems to be ignored**
See the `cron` / `every` warning above. `piply plan` prints the effective
schedule for each deployment.

**A deployment's `tags` lost the template's tags**
Lists are replaced, not appended. Restate the full list on the deployment.

**A downstream pipeline ignores the parent's `max_parallel_tasks` or `timeout`**
That is by design. A child inherits *data* — variables, env, outputs — but keeps
its own execution settings, so a shared downstream stage behaves the same
whichever tenant triggered it. See
[YAML Specification §9](YAML_SPECIFICATION.md#9-downstream-inheritance).
