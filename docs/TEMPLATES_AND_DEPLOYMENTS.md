# Templates and Deployments

One pipeline shape, run for many tenants, without copying it 29 times.

- A **template** describes the work. It is not runnable on its own.
- A **deployment** runs that template with its own variables.

For the exhaustive key list see
[YAML Specification §8](YAML_SPECIFICATION.md#8-templates-and-deployments).

---

## Contents

1. [The problem it solves](#1-the-problem-it-solves)
2. [The basic pattern](#2-the-basic-pattern)
3. [What a deployment overrides](#3-what-a-deployment-overrides)
4. [Merge rules](#4-merge-rules)
5. [Where to put notifications](#5-where-to-put-notifications)
6. [Per-tenant files](#6-per-tenant-files)
7. [When not to use a template](#7-when-not-to-use-a-template)

---

## 1. The problem it solves

Twenty-nine tenants running the same extract means twenty-nine near-identical
pipeline blocks. Changing the retry policy means twenty-nine edits, and the
diff is unreviewable.

A template inverts that: the shape is written once, and each tenant contributes
only what makes it different.

---

## 2. The basic pattern

```yaml
pipeline_templates:
  extraction:
    description: Nightly claim extract.
    schedule:
      cron: "0 2 * * *"
    retry:
      attempts: 3
    tasks:
      extract:
        type: cli
        command: "./extract.sh --tenant {tenant} --source {source_system}"
      load:
        type: cli
        command: "./load.sh --tenant {tenant}"
        depends_on: [extract]

pipeline_deployments:
  ecw_extract:
    template: extraction
    tenant: ecw
    variables:
      source_system: ECW

  athena_extract:
    template: extraction
    tenant: athena
    variables:
      source_system: ATHENA
```

Two runnable pipelines, `ecw_extract` and `athena_extract`, from one definition.

A deployment id must not collide with a pipeline id — that is a load error.

---

## 3. What a deployment overrides

Anything the template sets. Common ones:

```yaml
pipeline_deployments:
  slow_tenant:
    template: extraction
    tenant: slowco
    schedule:
      cron: "0 4 * * *"       # this tenant runs later
    max_parallel_tasks: 2      # and gently
    enabled: false             # and is currently off
```

### Free variables

A deployment gets these without declaring them:

| Variable | From |
| --- | --- |
| `{tenant}`, `{tenant_id}` | `tenant:` or `tenant_id:` |
| `{environment}` | `environment:` |

```yaml
pipeline_deployments:
  ecw_extract:
    template: extraction
    tenant: ecw            # -> {tenant} is "ecw" everywhere in the template
```

---

## 4. Merge rules

| Kind | Rule |
| --- | --- |
| Mapping (`variables`, `env`, `tasks`) | Merged key by key |
| Scalar (`schedule`, `timeout`, `enabled`) | Replaced |
| **List** (`tags`, `triggers_on_success`) | **Replaced wholesale** |

The list rule surprises people: `tags: [gamma]` on a deployment **discards** the
template's tags rather than appending to them. To keep both, list both.

Because mappings merge, a deployment can change one field of one task without
restating the task:

```yaml
pipeline_deployments:
  big_tenant:
    template: extraction
    tenant: bigco
    tasks:
      extract:
        timeout: 4h      # only this; the command comes from the template
```

---

## 5. Where to put notifications

Pick the **widest** level where the answer is the same for everything under it.

| Level | Use when |
| --- | --- |
| Project (`notifications.defaults`) | The same people hear about everything |
| File (`pipeline_defaults`) | One config file per team or tenant |
| **Template** | Every deployment alerts the same people |
| Deployment | One tenant needs different destinations |

**Start at the template.** With one deployment per tenant, putting it on each
deployment means editing 29 places when the on-call channel changes.

```yaml
pipeline_templates:
  extraction:
    notifications:
      on_failure: [ops]        # every deployment inherits this

pipeline_deployments:
  beta_extract:
    template: extraction
    notifications:
      on_failure: [data_team]  # -> data_team INSTEAD of ops
```

Remember the list rule: the deployment **replaces**. To alert both, list both, or
use a group. Full detail in [Notifications](NOTIFICATIONS.md).

---

## 6. Per-tenant files

Templates and deployments combine naturally with `include:`. A common layout:

```
piply.yaml                    include: ["*/piply_*.yaml"]
templates/piply_template.yaml pipeline_templates: { extraction: ... }
ecw/piply_ecw.yaml            pipeline_defaults + pipeline_deployments
athena/piply_athena.yaml      pipeline_defaults + pipeline_deployments
```

```yaml
# ecw/piply_ecw.yaml
pipeline_defaults:
  tags: [ecw]
  variables:
    source_system: ECW
  notifications:
    on_failure: [ecw_oncall]

pipeline_deployments:
  ecw_extract:    { template: extraction, tenant: ecw }
  ecw_reconcile:  { template: extraction, tenant: ecw }
```

Two notes on this layout:

- **Filenames never matter**, only the keys inside them. Every tenant folder may
  hold a file called `piply_template.yaml`; what must be unique is the template
  and deployment **ids**.
- `pipeline_defaults` applies to `pipelines:` and `pipeline_deployments:` in that
  file, **not** to templates. A template is not runnable and usually lives in a
  different file, so covering both would apply the defaults twice with no
  obvious winner. Tag the deployments.

---

## 7. When not to use a template

If you have one of something, write a plain pipeline. Templates pay off at three
or more deployments; below that they add a layer of indirection for nothing.

### Entities are not a substitute

`entities:` expands **tasks within one pipeline** across a set of values. It is
not a way to run one pipeline per tenant.

Declaring entities at the project level applies them to every pipeline, which
silently multiplies unrelated work — a summary email task becomes three summary
emails. `piply validate` warns about a global entity that a pipeline never
selects.

If you want one run per tenant, that is a deployment. If you want one task per
region *inside* a run, that is an entity.

---

## Related

- [Pipelines](PIPELINES.md) — the keys a template can set
- [Migration Guide](MIGRATION.md) — moving existing pipelines onto templates
- [Variables](VARIABLES.md) — where `{tenant}` fits in precedence
- [Notifications](NOTIFICATIONS.md) — routing across the levels
- [YAML Specification §8](YAML_SPECIFICATION.md#8-templates-and-deployments) — every key
