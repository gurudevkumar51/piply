# Notifications

How Piply tells you a run finished, and what happens when it cannot.

Two channels, configured independently:

| Channel | Pipeline key | Where delivery is configured |
| --- | --- | --- |
| **Email** | `notify:` | Central SMTP, under Settings or environment variables |
| **Microsoft Teams** | `notifications:` | A `notifications:` block of webhook destinations |

They are separate on purpose. Email suits an on-call rota and an audit trail;
Teams suits the channel a team already watches all day. Most projects use one,
some use both, and a failure in one never affects the other — or the run.

Sending a notification is **not** part of executing a pipeline. A run that
succeeded succeeded, whether or not the alert was delivered.

---

## 1. Which one do I want?

Use **Teams** if your team lives in Teams and you want failures visible where
people already are. It is the lower-friction option: one webhook per channel, no
mail server.

Use **email** if you need alerts to reach people outside the workspace, want
them in an inbox for record-keeping, or already run SMTP.

Use **both** for pipelines where a missed failure is expensive: Teams for
immediacy, email for the paper trail.

---

## 2. Email

### Configure delivery once

Under **Settings → Email (SMTP)**, or with environment variables:

| Variable | Meaning |
| --- | --- |
| `PIPLY_SMTP_HOST` | Server hostname |
| `PIPLY_SMTP_PORT` | Port, usually 587 |
| `PIPLY_SMTP_USER` | Username |
| `PIPLY_SMTP_PASSWORD` | Password. `PIPLY_SMTP_PASSWORD_FILE` reads it from a mounted file |
| `PIPLY_SMTP_FROM` | From address |

The password is **write-only**: it is never returned by the API or shown in the
UI. Leave the field blank when saving to keep the stored value.

### Say who to tell

```yaml
# Shorthand: a bare list means "on failure", which is what people want.
notify: [oncall@example.com]

# Explicit
notify:
  on_failure: [oncall@example.com, sre@example.com]
  on_success: [team@example.com]
```

A pipeline lists *who* to tell, never *how* to reach the mail server. If no SMTP
server is configured the run log says so and the run still succeeds.

---

## 3. Microsoft Teams

### Declare destinations once

Usually in their own file — see [splitting the config](YAML_SPECIFICATION.md#include):

```yaml
# piply_alert.yaml
notifications:
  teams:
    production_alerts:
      type: channel                     # channel | chat
      webhook: ${TEAMS_PROD_WEBHOOK}
    data_engineering:
      type: chat
      webhook: ${TEAMS_DATA_CHAT_WEBHOOK}
      timeout_seconds: 15               # optional, default 10

  groups:                               # reusable bundles
    critical:
      - production_alerts
      - data_engineering
```

| Key | Required | Meaning |
| --- | --- | --- |
| `type` | no | `channel` for a channel connector, `chat` for a group chat. Default `channel`. |
| `webhook` | **yes** | Incoming webhook URL. Must resolve to `https://`. |
| `timeout_seconds` | no | Per-request timeout. Default `10`, must be greater than zero. |
| `format` | no | `adaptive` or `messagecard`. Guessed from the URL when omitted. |

### Two wire formats, and why it matters

Microsoft **retired Office 365 connectors**. New webhooks are created through
Power Automate ("Workflows"), and the two accept different payloads:

| `format` | For | Shape |
| --- | --- | --- |
| `adaptive` *(default)* | Power Automate Workflows | Adaptive Card inside `{"type": "message", "attachments": [...]}` |
| `messagecard` | Legacy connector URLs | The older `MessageCard` |

**They are not interchangeable** — a Workflows endpoint rejects a MessageCard.
Piply guesses from the host: a URL on `webhook.office.com` gets `messagecard`,
anything else gets `adaptive`. Set `format:` explicitly to override the guess.

If a curl like this works but Piply's alert does not, the format is the reason:

```bash
curl -H "Content-Type: application/json"   -d '{"type":"message","attachments":[{"contentType":"application/vnd.microsoft.card.adaptive","content":{"type":"AdaptiveCard","body":[{"type":"TextBlock","text":"hello"}],"version":"1.4"}}]}'   "$WEBHOOK_URL"
```

That is the `adaptive` shape, and it is what Piply now sends by default.

### Getting a webhook URL

**Channel** — in Teams, open the channel, **⋯ → Connectors → Incoming Webhook**,
name it, and copy the URL. Some tenants have connectors disabled by policy; if
the option is missing, that is why.

**Group chat** — chats do not expose connectors directly. Use a Power Automate
*"When a Teams webhook request is received"* flow that posts into the chat, and
give Piply that flow's HTTP URL. It behaves the same from Piply's side.

### Wire it to a pipeline

```yaml
# piply_pipe.yaml
pipelines:
  claim_pipeline:
    notifications:
      on_failure:
        - production_alerts
        - data_engineering
      on_success:
        - data_engineering
    tasks:
      extract: {type: python, path: extract.py, function: run}
```

A bare list means **on failure**, matching `notify:`:

```yaml
    notifications: [critical]     # same as on_failure: [critical]
```

A group name works anywhere a destination name does. Naming both a group and one
of its members notifies that member **once**, not twice.

### Which level should it go on?

`notifications:` is accepted project-wide, on a template, on a deployment, and
on a pipeline. Pick the *widest* level where the answer is the same for
everything under it.

| Level | Use it when | Behaviour |
| --- | --- | --- |
| **Project** (`notifications.defaults`) | The same people should hear about everything | Applied to every pipeline that does not state its own |
| **File** (`pipeline_defaults`) | One config file per team or tenant | Applied to the pipelines declared **in that file**; beats the project default |
| **Template** | Every deployment should alert the same people | Inherited by all of them — write it once |
| **Deployment** | One tenant needs different destinations | **Replaces** the template's list for that deployment |
| **Pipeline** | One pipeline differs from everything above | **Replaces** the inherited value, per outcome |

Precedence runs narrowest-wins: **pipeline → file → project**.

### Alerting on everything, without repeating yourself

`notifications.defaults` sits beside the destinations, so on a split config it
lives in `piply_alert.yaml` with everything else about alerting:

```yaml
notifications:
  teams:
    production_alerts: { type: channel, webhook: "${TEAMS_PROD_WEBHOOK}" }
    data_engineering:  { type: chat,    webhook: "${TEAMS_DE_WEBHOOK}" }
  groups:
    critical: [production_alerts, data_engineering]

  defaults:
    on_failure: [critical]          # every pipeline, no per-pipeline block
    on_success: [data_engineering]  # omit this line to alert only on failure
```

That is the whole setup. Any pipeline that says nothing now alerts `critical`
on failure.

A pipeline overrides the default **per outcome**, so narrowing one never
silently changes the other:

```yaml
pipelines:
  noisy_import:
    notifications:
      on_success: []              # stop the success card; failures still go to critical
    tasks: { ... }

  chatty_probe:
    notifications: false          # opt out of the defaults entirely
    tasks: { ... }

  billing_export:
    notifications:
      on_failure: [finance_oncall]  # replaces critical for failures only;
    tasks: { ... }                  # on_success still inherits data_engineering
```

The override **replaces** rather than merges. Merging would mean a pipeline
could never narrow who gets paged, and "why is this channel still being alerted"
is the harder question to answer at 3am. To alert the default *and* someone
else, name both: `on_failure: [critical, finance_oncall]`.

A name that does not exist in `defaults` fails at load, not at send time — one
typo there would otherwise break alerting for every pipeline at once.

### Routing per file

With the config split one file per team, the on-call channel is usually the same
for everything in a file. `pipeline_defaults` says it once, at the top of that
file:

```yaml
# claims/piply_claims.yaml
pipeline_defaults:
  tags: [claims, prod]          # see the tags guide
  notifications:
    on_failure: [claims_oncall]  # beats the project default for this file only

pipelines:
  claim_extract: { ... }
  claim_load:    { ... }
```

It applies to the `pipelines:` and `pipeline_deployments:` declared in that same
file. Templates are excluded on purpose — a template is not runnable, and its
deployments usually live in a different file, so applying it to both would apply
it twice with no obvious winner.

#### Destination *names* stay project-wide

Only the **routing** is file-scoped. The destinations themselves — the entries
under `notifications.teams` — are global, and two files declaring the same name
is an error:

```
'notifications.teams.oncall' is defined in more than one config file:
'claims.yaml' and 'reports.yaml'. Destination names are project-wide. Give them
distinct names (claims_oncall, reports_oncall) and route per file with
'pipeline_defaults.notifications.on_failure'.
```

This is deliberate, and the opposite of how
[variables](YAML_SPECIFICATION.md#pipeline_defaults--settings-for-one-file)
work. A variable is a config value, so two teams having their own `batch_size`
is harmless. A destination is a **real channel**: if `oncall` meant one thing in
`claims.yaml` and another in `reports.yaml`, the settings page could no longer
tell you where an alert actually went, and neither could the delivery history.

So name them for what they are and route per file:

```yaml
# piply_alert.yaml — every destination, named unambiguously
notifications:
  teams:
    claims_oncall:  { type: channel, webhook: "${TEAMS_CLAIMS_WEBHOOK}" }
    reports_oncall: { type: channel, webhook: "${TEAMS_REPORTS_WEBHOOK}" }

# claims/piply_claims.yaml — this file's pipelines page the claims channel
pipeline_defaults:
  notifications:
    on_failure: [claims_oncall]
```

### Seeing who gets told

Once defaults are in play, a pipeline's own YAML no longer answers "does anyone
hear about this?". The pipelines page puts a 🔔 on every pipeline that alerts
someone, and its tooltip names the destinations — the resolved answer, after
project and file defaults. No bell means nobody is told.

### Alerting on a task that is allowed to fail

A failed task normally fails its run, so the pipeline alert covers it. A task
marked `allow_failure: true` is the exception: it is best-effort, the run stays
green, and nothing would otherwise report it. Pair the two:

```yaml
tasks:
  optional_sync:
    type: cli
    command: ./sync.sh
    allow_failure: true      # a failure here does not fail the run
    alert_on_failure: true   # ...but still tell the on-call channel
```

The card goes to the pipeline's `on_failure` destinations and says the run
succeeded but the task did not. It is sent **only** when the run itself
succeeded — a failed run already has its own card, and a second would be noise.

```yaml
pipeline_templates:
  scrape:
    notifications:
      on_failure: [ops]            # every deployment inherits this
    tasks: { ... }

pipeline_deployments:
  alpha_scrape:
    template: scrape               # -> alerts ops
  beta_scrape:
    template: scrape
    notifications:
      on_failure: [data_team]      # -> alerts data_team INSTEAD of ops
```

The deployment **replaces** rather than adds, following the ordinary
[list-replaces rule](YAML_SPECIFICATION.md#8-templates-and-deployments). To
alert both, list both: `on_failure: [ops, data_team]`, or use a group.

**Start at the template.** With one deployment per tenant, putting it on each
deployment means editing 29 places when the on-call channel changes.

---

## 4. Never put a webhook in YAML

A Teams webhook URL **is** the credential. Anyone holding it can post to the
channel as your integration.

- Write `webhook: ${TEAMS_PROD_WEBHOOK}` and set the variable in the
  environment, `.env`, or a [`secrets:`](YAML_SPECIFICATION.md#secrets) file.
- Piply never writes a webhook URL to a log, an error message, or an API
  response. A delivery failure names the **destination**, never the URL.
- A literal `https://...` in YAML is accepted — Piply cannot tell it apart from
  a resolved value — but it will be committed to git. Do not do it.
- Rotate by deleting the connector in Teams and issuing a new one; the old URL
  stops working immediately.

An unresolved variable is a **warning**, not a load error, so a developer
without the production secret can still run pipelines locally:

```
$ piply validate
2 warning(s):
  ! notifications.teams.production_alerts: webhook '${TEAMS_PROD_WEBHOOK}' did not
    resolve to a value, so this destination will be skipped.
```

---

## 5. What the alert looks like

One standardised card per run, colour-coded by status — green `success`, red
`failed`, amber `timed_out`, grey `cancelled` — carrying:

| Field | Example |
| --- | --- |
| Pipeline | `Claim Pipeline (claim_pipeline)` |
| Status | `failed` |
| Run | `880617da766c` |
| Trigger | `manual`, `schedule`, `sensor`, `upstream` |
| Tasks | `0/1 succeeded` |
| Duration | `12.4s` |
| Error | present only on failure |

A long error is **truncated rather than dropped**, because Teams rejects an
oversized card and a shortened alert beats no alert.

Set `PIPLY_BASE_URL` to add an **Open run in Piply** button linking straight to
the run page:

```
PIPLY_BASE_URL=https://piply.internal
```

---

## 6. Seeing what was sent

**Settings → Alerts (Microsoft Teams)**, admin only.

It answers the question a run log cannot: *was anything even attempted?*

- **Every declared destination**, its type, and whether its webhook resolved
- **Used by** — which pipelines reference it, resolved **through groups**, so a
  destination reached only via a group is not reported as unused
- **Send test** — posts a card immediately, so a webhook can be checked without
  waiting for a run to fail
- **Recent deliveries** — pipeline, run, destination, outcome, and the reason

Webhook URLs never appear on this page. Destinations are declared in YAML, not
here, because the URL is a credential and belongs in the environment.

The outcome column includes **`nothing configured`**, which exists because
silence is the hardest case to debug: a pipeline with only `on_failure` that
succeeds sends nothing, and without this row that is indistinguishable from a
delivery that failed silently.

---

## 7. When delivery fails

Nothing happens to the run. Every outcome is recorded in the run log instead:

| Situation | Run status | Logged against the run |
| --- | --- | --- |
| Delivered | unchanged | `Teams notification sent to production_alerts.` |
| Webhook returns 4xx/5xx | unchanged | `Teams notification to 'x' failed: HTTP 500: ...` |
| Host unreachable or slow | unchanged | `Teams notification to 'x' failed: timed out after 10s` |
| Destination name is a typo | unchanged | `Unknown notification destination 'x'. Known destinations: ...` |
| `${VAR}` never resolved | unchanged | `Teams notification skipped for 'x': its webhook is not configured.` |
| No `notifications:` block at all | unchanged | `Teams notification skipped: no 'notifications:' destinations are declared.` |

A typo in a destination name is deliberately **not** a load error — one mistyped
name should not stop every pipeline in the project from loading. It is reported
against the run that tried to use it. A typo inside a `groups:` list *is* caught
at load time, because that is a static reference Piply can check.

---

## 8. How delivery works

- Destinations are posted **concurrently** with `httpx.AsyncClient`, each with
  its own timeout, so four destinations cost one timeout rather than four.
- Delivery happens **after** the run is recorded, never inside task execution.
- Timeout defaults to 10 seconds. A notification is not worth holding a run's
  completion path open for.
- No new dependency: `httpx` is already one of Piply's eight.

---

## 9. Troubleshooting

**The alert never arrives, and there is nothing in the run log.**
Open **Settings → Alerts** first — it records outcomes a run log does not,
including `nothing configured`. The usual causes are a pipeline with no
`notifications:` block at all, or a run whose outcome does not match the list
you filled in: `on_success` and `on_failure` are separate.

**`its webhook is not configured`.**
The `${VAR}` did not resolve. `piply validate` warns about this at load time.
Check the variable is set in the environment Piply actually runs in — a
`systemd` unit does not inherit your shell.

**`curl` works but Piply cannot reach the same webhook.**
`curl` uses its own certificate store and ignores `SSL_CERT_FILE`; Python reads
it. If that variable — or `SSL_CERT_DIR`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`
— points at a file that no longer exists, every HTTPS post used to fail with
`FileNotFoundError: [Errno 2] No such file or directory` before reaching the
network. A rebuilt conda environment is the usual cause, because `conda
activate` exports the variable into the environment.

Piply now **ignores a certificate path that does not exist** and verifies
against the system trust store, logging this once:

```
Ignoring a certificate-authority path that does not exist
(SSL_CERT_FILE=/…/envs/py313/ssl/cacert.pem) and verifying against the system
trust store instead. Fix or unset it to silence this.
```

Verification still happens — only the missing override is dropped. A path that
*does* exist is always honoured, so a corporate CA bundle keeps working. Fix the
environment anyway:

```bash
echo $SSL_CERT_FILE                                            # is it really there?
unset SSL_CERT_FILE                                            # or
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt   # or your platform's bundle
```

**`request failed (ConnectError)`.**
DNS or the network, not Teams. If a proxy is set in the server's environment the
message says so — the webhook host has to be reachable from the machine Piply
runs on, which is often not the machine you are browsing from.

**`HTTP 400` from Teams.**
Usually a revoked or mistyped connector URL. Recreate the connector.

**`HTTP 429`.**
Teams is rate-limiting the webhook. Reduce how many pipelines point at one
destination, or notify only on failure.

**Alerts arrive twice.**
A pipeline naming both a group and one of its members is de-duplicated, so check
for two pipelines both alerting — an upstream and its downstream both configured
with `on_failure` will each send.

---

## Related

- [YAML Specification](YAML_SPECIFICATION.md#13-notifications) — the key reference
- [Sensors](SENSORS.md) — what triggers the runs you are being alerted about
- [Security](SECURITY.md) — how Piply handles secrets generally
- [FAQ](FAQ.md) — short answers to the questions above
