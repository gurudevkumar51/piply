# Authentication And Authorization

Piply ships with accounts, roles, and per-pipeline permissions. All of it is
optional: an install with no accounts behaves exactly as it did before this
existed — no login page, no restrictions.

---

## 1. Turning it on

Authentication switches on the moment **the first account exists**. There is no
separate flag to remember.

### Create the first admin from the CLI

```bash
piply users create admin --role admin
```

```
Created admin 'admin'.
Password: 7Qf2mKp0xVdA3nLs
Store it now. It is hashed and cannot be shown again.
```

Omit `--password` and Piply generates one. It is hashed with PBKDF2-SHA256
(240,000 rounds, per-user salt) and cannot be recovered — only reset.

### Or let the server bootstrap one

Set `PIPLY_AUTH_ENABLED=true` and start the server with no accounts present:

```
====================================================================
Piply created an initial administrator account:
    username: admin
    password: 7Qf2mKp0xVdA3nLs
Store it now. It cannot be shown again.
====================================================================
```

Override the generated values with `PIPLY_ADMIN_USERNAME` and
`PIPLY_ADMIN_PASSWORD`.

Bootstrap is skipped when `PIPLY_AUTH_USERNAME` / `PIPLY_AUTH_PASSWORD` are set,
because those already define an administrator.

---

## 2. Roles

| Role | What it means |
| --- | --- |
| `admin` | Every pipeline, every action, plus user and SMTP management |
| `user` | Only what an explicit grant allows |

There is deliberately no third role. Anything finer is expressed as grants.

---

## 3. Pipeline permissions

Three actions, granted per pipeline:

| Action | Allows |
| --- | --- |
| `view` | See the pipeline, its runs, its logs, and its artifacts |
| `edit` | Change or delete the pipeline, pause and resume its schedule |
| `run` | Trigger, retry, and cancel runs |

`edit` and `run` both imply `view` — being able to act on something invisible
would be useless, so the grant is normalised automatically.

```bash
# One pipeline
piply users grant alice nightly_etl view,run

# Every pipeline
piply users grant alice '*' view

# Everything on everything (short for view,edit,run)
piply users grant alice '*' all

# Take it away
piply users revoke alice nightly_etl
```

A user with no grants can sign in and sees an empty pipeline list.

### What enforcement actually covers

Both the API and the rendered pages are checked, against the pipeline that owns
the object:

| Request | Requires |
| --- | --- |
| `GET /api/pipelines`, `/pipelines` | `view`; the list is filtered to what you may see |
| `GET /api/pipelines/{id}`, `/pipelines/{id}` | `view` on that pipeline |
| `POST /api/pipelines/{id}/run` | `run` |
| `POST /api/pipelines/{id}/tasks/{task}/run` | `run` |
| `POST /api/pipelines/{id}/pause` and `/resume` | `edit` |
| `DELETE /api/pipelines/{id}` | `edit` |
| `GET /api/runs`, `/runs` | `view`; filtered to visible pipelines |
| `GET /api/runs/{id}` and its logs, tasks, outputs | `view` on the run's pipeline |
| `POST /api/runs/{id}/retry`, `/cancel` | `run` on the run's pipeline |
| `DELETE /api/runs/{id}` | `edit` on the run's pipeline |
| `/api/users*`, `/api/settings/smtp*` | `admin` |

A run inherits the permissions of the pipeline that produced it, so granting
`view` on a pipeline grants its history too.

---

## 4. Managing accounts

### From the CLI

```bash
piply users list
piply users create alice --role user --grant nightly=view,run --grant weekly=view
piply users passwd alice                 # generates and prints a new password
piply users passwd alice --password ...  # or set one
piply users grant alice reports all
piply users revoke alice reports
piply users disable alice                # keep the account, block sign-in
piply users delete alice
```

### From the UI

**Settings → Users and permissions**, visible to admins only. Create accounts,
grant pipeline actions, and delete accounts from the same table.

### From the API

```http
GET    /api/users
POST   /api/users                       {"username", "password", "role", "permissions"}
PATCH  /api/users/{username}            {"password"?, "role"?, "is_active"?}
DELETE /api/users/{username}
POST   /api/users/{username}/permissions {"pipeline_id", "actions": ["view","run"]}
GET    /api/permissions                 describes the vocabulary
GET    /api/me                          the current account; answers anonymously
```

### The last-admin guard

Piply refuses any change that would leave no way in — deleting, demoting, or
disabling the only active admin all fail with:

```
This is the only active admin. Promote another admin first.
```

---

## 5. How a request is authenticated

Tried in order:

1. **Session cookie** — set by the login form. Signed with HMAC-SHA256 using a
   per-install secret, valid 12 hours, `HttpOnly`, `SameSite=Lax`, and `Secure`
   when served over HTTPS. A tampered cookie is rejected, not trusted.
2. **HTTP Basic** — against a stored account, or the legacy
   `PIPLY_AUTH_USERNAME` / `PIPLY_AUTH_PASSWORD` pair.
3. **Bearer token** — `PIPLY_API_TOKEN`, for machine access. Token callers are
   treated as admin, matching the behaviour before permissions existed.

The signing secret comes from `PIPLY_SESSION_SECRET` if set, otherwise a random
value generated on first use and stored in the metadata table. Setting it
explicitly keeps sessions valid across a database reset and is required if you
ever run more than one instance.

Authentication failures are constant-time: a wrong username costs the same as a
wrong password, so accounts cannot be enumerated by timing.

---

## 6. Backward compatibility

| Existing setup | What happens now |
| --- | --- |
| No auth configured, no accounts | Unchanged: no login, everything permitted |
| `PIPLY_AUTH_USERNAME`/`PASSWORD` only | Unchanged: HTTP Basic challenge, treated as admin. The login form also accepts these credentials |
| `PIPLY_API_TOKEN` only | Unchanged: bearer token works on `/api/*` and `/metrics` |
| Accounts created | Login page appears; permissions enforced |

The browser Basic-auth challenge is kept while no accounts exist, so a legacy
install is never redirected to a login form it has no credentials for.

---

## 7. Environment variables

| Variable | Purpose |
| --- | --- |
| `PIPLY_AUTH_ENABLED` | Require authentication even with no accounts; enables bootstrap |
| `PIPLY_ADMIN_USERNAME` | Username for the bootstrapped admin (default `admin`) |
| `PIPLY_ADMIN_PASSWORD` | Password for the bootstrapped admin (default: generated) |
| `PIPLY_AUTH_USERNAME` / `PIPLY_AUTH_PASSWORD` | Legacy single-admin Basic credentials |
| `PIPLY_API_TOKEN` | Bearer token for machine access |
| `PIPLY_SESSION_SECRET` | Session signing key; generated and stored if unset |

---

## 8. Operational notes

- **Serve over HTTPS.** Basic credentials and session cookies are only as
  private as the transport. The `Secure` cookie flag is set automatically when
  the request scheme is HTTPS.
- **Behind a proxy**, forward the `Authorization` header and `X-Forwarded-Proto`.
- **Rotate the API token** by changing `PIPLY_API_TOKEN` and restarting.
- **Password resets** are admin-driven; there is no email reset flow.
- Deleting a user removes every grant it held.
