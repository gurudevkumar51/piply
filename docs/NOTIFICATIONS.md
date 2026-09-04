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

Both kinds accept the same payload, so a channel and a group chat are configured
identically apart from `type`.

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

## 6. When delivery fails

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

## 7. How delivery works

- Destinations are posted **concurrently** with `httpx.AsyncClient`, each with
  its own timeout, so four destinations cost one timeout rather than four.
- Delivery happens **after** the run is recorded, never inside task execution.
- Timeout defaults to 10 seconds. A notification is not worth holding a run's
  completion path open for.
- No new dependency: `httpx` is already one of Piply's eight.

---

## 8. Troubleshooting

**The alert never arrives, and there is nothing in the run log.**
The pipeline has no `notifications:` block, or the run's outcome does not match
the list you filled in — `on_success` and `on_failure` are separate.

**`its webhook is not configured`.**
The `${VAR}` did not resolve. `piply validate` warns about this at load time.
Check the variable is set in the environment Piply actually runs in — a
`systemd` unit does not inherit your shell.

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
