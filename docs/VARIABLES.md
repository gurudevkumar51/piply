# Variables

How a value gets into a command, and which value wins when more than one is
defined.

Piply has one interpolation syntax and four places a value can come from. Most
confusion is really a scope question — "why is this empty?" is almost always
"that name is not visible here."

---

## Contents

1. [The two syntaxes](#1-the-two-syntaxes)
2. [Where to declare a variable](#2-where-to-declare-a-variable)
3. [Precedence](#3-precedence)
4. [Conditional values](#4-conditional-values)
5. [Values supplied at run time](#5-values-supplied-at-run-time)
6. [Built-in variables](#6-built-in-variables)
7. [Debugging](#7-debugging)

---

## 1. The two syntaxes

| Syntax | Reads from | Use it for |
| --- | --- | --- |
| `{name}` | Config variables | Anything you declare in YAML |
| `${NAME}` | The environment | Secrets, per-machine paths |

```yaml
variables:
  region: eu-west-1

pipelines:
  export:
    tasks:
      push:
        type: cli
        command: "aws s3 cp out.csv s3://{region}-bucket/ --profile ${AWS_PROFILE}"
```

`{region}` is resolved from the config. `${AWS_PROFILE}` is resolved from the
environment — see [Environment Variables](ENVIRONMENT.md).

Keep secrets in `${...}`. A webhook URL or password written as a literal in YAML
is a credential in your git history.

A name that cannot be resolved is **left in place verbatim** rather than replaced
with an empty string. `echo {typo}` stays `echo {typo}`, so a mistake is visible
in the command preview instead of silently producing an empty argument.

---

## 2. Where to declare a variable

| Scope | Declare it in | Visible to |
| --- | --- | --- |
| **Global** | top-level `variables:` in `piply.yaml` | Every pipeline in the project |
| **One file** | `pipeline_defaults.variables` in that file | Only pipelines declared in that file |
| **One pipeline** | `variables:` on the pipeline | That pipeline |
| **Run time** | `--var` / `--param`, or the UI prompt | That run |

### Global

```yaml
# piply.yaml
variables:
  region: eu-west-1
  stage: production
```

Top-level `variables:` **merge across every included file**. That is what makes
them global — and it is also why two files cannot both declare `batch_size`:
the second one is a duplicate-key error naming both files.

### One file

When the config is split one file per team or tenant, the values usually differ
per file. `pipeline_defaults.variables` keeps the name private to that file:

```yaml
# claims/piply_claims.yaml
pipeline_defaults:
  variables:
    source_system: ECW
    batch_size: 500

pipelines:
  claim_extract:
    tasks:
      extract:
        type: cli
        command: "./pull.sh {source_system} {batch_size}"
```

```yaml
# reports/piply_reports.yaml
pipeline_defaults:
  variables:
    source_system: ATHENA
    batch_size: 50        # same name, different file, no collision
```

It applies to the `pipelines:` and `pipeline_deployments:` declared in that same
file. A name defined in one file is simply unknown in the next.

### One pipeline

```yaml
pipelines:
  claim_extract:
    variables:
      batch_size: 1000     # this pipeline only
```

---

## 3. Precedence

Narrowest wins:

```
run time  →  pipeline  →  file  →  project  →  environment
```

Each layer **sees** the one above it, so a later value can build on an earlier
one:

```yaml
# piply.yaml
variables:
  stage: production

# claims/piply_claims.yaml
pipeline_defaults:
  variables:
    label: "{stage}-claims"     # -> "production-claims"
```

Ordering matters *within* a block too: a variable can use one declared above it,
but not one declared below.

```yaml
variables:
  base: /data
  inbox: "{base}/inbox"        # works
```

---

## 4. Conditional values

A value may be conditional, inline or as a mapping:

```yaml
variables:
  # inline
  workers: 8 if stage == "production" else 2

  # explicit
  sensor_path:
    if: stage == "dev"
    then: ./sample_data
    else: /mnt/prod/incoming
```

Conditions see the environment plus every variable declared **before** them.

This is how one config serves dev and prod without duplicating a pipeline — see
[Examples](EXAMPLES.md).

---

## 5. Values supplied at run time

Some values only exist when the run starts — a date, a batch id. Leave the
placeholder in the config and supply it per run:

```bash
piply run claim_extract --var run_date=2026-09-01
piply run claim_extract --prompt          # asks for whatever is still missing
```

In the UI, a run with unfilled placeholders asks for them rather than running the
command literally — see
[Missing runtime values](UI_GUIDE.md#missing-runtime-values).

`--param KEY=VALUE` is the related flag for structured run parameters, and
accepts JSON values.

**Downstream pipelines inherit the parent run's values.** A manual run has no
parent, so it falls back to the pipeline's own variables and the project
defaults — see [Downstream Inheritance](YAML_SPECIFICATION.md#9-downstream-inheritance).

---

## 6. Built-in variables

A deployment gets these for free from its own declaration:

| Variable | Set from |
| --- | --- |
| `{tenant}`, `{tenant_id}` | `tenant:` on the deployment |
| `{environment}` | `environment:` on the deployment |

```yaml
pipeline_deployments:
  ecw_extract:
    template: extraction
    tenant: ecw              # -> {tenant} is "ecw"
```

See [Templates and Deployments](TEMPLATES_AND_DEPLOYMENTS.md).

Entity expansion introduces its own per-task values — see
[Entities](YAML_SPECIFICATION.md#3-top-level-keys).

---

## 7. Debugging

`piply plan` shows the command **after** interpolation, which answers almost
every variable question in one step:

```bash
piply plan claim_extract --tenant ecw
piply plan claim_extract --json | jq '.tasks[].command'
```

| Symptom | Usual cause |
| --- | --- |
| `{name}` appears literally in the command | Not defined in this scope — check which file declares it |
| Value is the wrong one | Something narrower overrides it; check the pipeline block |
| `'variables.x' is defined in more than one config file` | Top-level `variables:` are global; move it to `pipeline_defaults.variables` |
| `${VAR}` appears literally | The environment variable is unset where Piply runs, not where you tested |

The last one catches people out most: the scheduler runs as a service, which does
not inherit the shell you exported the variable in. See
[Environment Variables](ENVIRONMENT.md).

---

## Related

- [Environment Variables](ENVIRONMENT.md) — `${...}`, `.env`, and `PIPLY_*` settings
- [YAML Specification](YAML_SPECIFICATION.md) — the exhaustive key reference
- [Templates and Deployments](TEMPLATES_AND_DEPLOYMENTS.md) — where `{tenant}` comes from
- [Examples](EXAMPLES.md) — runnable patterns, including dev/prod conditionals
