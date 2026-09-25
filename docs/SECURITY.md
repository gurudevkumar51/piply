# Security

Piply is a pipeline runner: its whole job is executing commands you configured.
That makes the trust boundary unusually blunt, so it is worth stating plainly.

**Anyone who can edit `piply.yaml`, or who holds an `admin` account, can run
arbitrary code as the Piply process.** That is not a flaw to be fixed; it is
what the product does. Everything below is about limiting who reaches that
position, and about making sure a *non*-admin cannot get there by accident.

---

## 1. The trust model

| Who | Can do |
| --- | --- |
| Anyone who can write `piply.yaml` | Anything the Piply process can do |
| `admin` account | Every pipeline, every action, user and SMTP management, command overrides |
| `user` with `run` on a pipeline | Trigger, retry, cancel **that pipeline as configured** — not arbitrary commands |
| `user` with `edit` | Change, delete, pause, resume that pipeline |
| `user` with `view` | Read that pipeline, its runs, logs, and artifacts |
| Anonymous, no accounts | Everything — auth is off until you turn it on |

Piply does not sandbox tasks. A task runs as the Piply user with its
environment and filesystem access. Run Piply as a dedicated unprivileged user,
not root.

---

## 2. What was hardened, and what it means for you

The following were found by audit and fixed. If you are upgrading, these are
behaviour changes worth knowing about.

### Authorization now covers every endpoint

Permissions were previously enforced on the pipeline and run APIs but **not** on
the operations, dashboard, log-search, matrix, or diagnostics endpoints. Any
authenticated account — including one granted `view` on a single unrelated
pipeline — could reach them.

Now enforced:

| Endpoint | Requires |
| --- | --- |
| `GET /api/runs/{id}/config` | `view` on the run's pipeline, **and** credentials are masked |
| `GET /api/runs/{id}/artifacts`, `/artifacts/download` | `view` on the run's pipeline |
| `GET`/`POST /api/pipelines/{id}/preview` | `view` on that pipeline |
| `POST /api/runs/{id}/backfill` | `run` on the run's pipeline |
| `POST /api/pipelines/{id}/backfill` | `run` on that pipeline |
| `POST /api/maintenance/prune` | `admin` |
| `GET /api/diagnostics`, `/diagnostics` | `admin` |
| `GET /api/logs`, `/api/logs/stream`, `/logs` | `view`; results filtered to visible pipelines |
| `GET /api/execution-matrix`, `/execution-matrix` | `view`; filtered |
| `GET /api/dashboard`, `/` | `view`; pipeline and run lists filtered |
| `GET /api/sensors` | `view`; filtered |
| `GET /api/metrics`, `/metrics` | `view` |

Log search and the dashboard are **filtered, not refused** — a restricted user
sees their own pipelines, just never anyone else's.

### Run configuration no longer leaks credentials

`runs.run_config` stores the fully resolved environment so a downstream run can
be replayed without re-running its upstream. That includes database passwords
and API keys.

The API now masks credential-looking values on the way out, for every caller
including admins:

```json
{ "env": { "DB_PASSWORD": "***", "API_KEY": "***", "DBT_CLIENT": "BENNETT" } }
```

Masking is by **variable name**, not value: anything containing `password`,
`passwd`, `secret`, `token`, `apikey`, `api_key`, `accesskey`, `access_key`,
`private`, `credential`, `auth`, `dsn`, `connection_string`, `conn_str`,
`sasl`, `session_key`, or `signing` is hidden. Deliberately broad — a false
positive only hides a value from a debugging view, a false negative leaks a
credential.

The **stored** snapshot keeps the real values, so replay still works.

> **Name your secrets accordingly.** A variable called `DB_CONN` will not be
> masked. Prefer `DB_CONNECTION_STRING`, `DB_PASSWORD`, or `..._SECRET`.

### `command_overrides` is admin-only

The run and preview APIs accept `command_overrides`, which replaces the command
a task executes. Under a plain `run` grant this turned "may run this one
pipeline" into "may execute any command as the Piply process".

It now requires `admin`. Triggering a pipeline **as configured** still only
needs `run`, so per-tenant delegation is unaffected.

### Malformed credentials return 401, not 500

`hmac.compare_digest` raises `TypeError` on non-ASCII strings, so a login
attempt with a non-ASCII username produced an unhandled 500. Session cookies
had the same problem. Both now reject cleanly.

### Sign-in is throttled

Password verification is PBKDF2 with 240,000 rounds — deliberately slow, which
makes an unthrottled login endpoint both a guessing risk and a way to burn CPU.
Eight failures within five minutes locks that username out for five minutes.

The counter is in memory, matching the single-process design. A restart clears
it, which is acceptable for a lockout measured in minutes.

### Redirects cannot leave the site

`/login?next=//evil.example.com` previously redirected an already-signed-in
user off-site. Only same-site absolute paths are accepted now.

### Hardening headers on every response

`X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`,
`Referrer-Policy: same-origin`, and a Content-Security-Policy. Applied by the
outermost middleware, so they are present on 401 and login-redirect responses
too.

The policy is same-origin apart from three CDN origins the bundled UI loads:

```
default-src 'self';
script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net;
style-src 'self' 'unsafe-inline' https://fonts.googleapis.com;
font-src 'self' https://fonts.gstatic.com; img-src 'self' data:;
connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'
```

| Origin | Loaded for |
| --- | --- |
| `fonts.googleapis.com` | the web-font stylesheet in `base.html` |
| `fonts.gstatic.com` | the font files themselves |
| `cdn.jsdelivr.net` | `dagre` and `graphlib`, the DAG layout libraries |

A test scans every template and script for `https://` references and asserts
the policy permits each one, because the failure mode here is silent: a blocked
CDN means the graph or the typography stops rendering with nothing in the
server log.

`'unsafe-inline'` is required for scripts and styles because pages bootstrap
their state from inline `<script>` blocks. Values interpolated into those
blocks go through Jinja's `tojson`, which escapes `<`, `>`, `&`, and `'`, so a
`</script>` cannot be injected through data.

> **Air-gapped or privacy-sensitive installs.** Those three are the only remote
> origins Piply's UI touches, and no data is sent to them — they serve static
> assets. To remove them entirely, vendor the two jsDelivr scripts into
> `piply/ui/static/`, drop the font `<link>` from `base.html`, and tighten the
> policy to `'self'`. The cost is web fonts, which fall back to system fonts.
> The DAG will not render without the layout libraries, so those must be
> vendored rather than simply removed.

---

## 3. What is deliberately *not* protected

Being explicit about the gaps is more useful than implying full coverage.

- **No sandboxing.** Tasks run with the Piply process's privileges. This is the
  product's purpose.
- **No CSRF tokens.** Session cookies are `SameSite=Lax`, which blocks
  cross-site `POST` with cookies attached. State-changing endpoints are all
  POST/PATCH/DELETE. `GET /logout` is the one state-changing GET; the worst a
  cross-site link achieves is signing you out.
- **No audit log.** There is no record of who triggered or deleted what. Run
  history shows *that* something ran, not *who* asked. See the roadmap.
- **Sessions are not revoked on password change.** Changing a password does not
  invalidate existing session cookies; they expire after 12 hours. Disabling an
  account *does* take effect immediately, because the user is re-read on every
  request. To force a global sign-out, rotate `PIPLY_SESSION_SECRET`.
- **No rate limiting outside login.** A valid account can hammer the API.
- **Artifacts are metadata only.** Piply records paths; file permissions are
  the operating system's business. Download is restricted to paths a run
  actually recorded *and* that resolve inside an allowed root, so a crafted
  path cannot escape the workspace.
- **YAML is trusted input.** `piply.yaml` is code. Treat write access to it the
  same as commit access to the repository.

---

## 4. Deployment checklist

Roughly in order of how much it matters.

1. **Serve over HTTPS.** Basic credentials and session cookies are only as
   private as the transport. The `Secure` cookie flag is set automatically when
   the request scheme is HTTPS. Behind a proxy, forward `X-Forwarded-Proto`.
2. **Create accounts.** Auth is off until the first account exists. See
   [Authentication](AUTHENTICATION.md).
3. **Run as an unprivileged user.** Not root. Give it only the filesystem and
   network access your tasks genuinely need.
4. **Keep secrets out of environment variables where you can.** Every
   credential setting accepts a `_FILE` variant that reads from a mounted file —
   `PIPLY_AUTH_PASSWORD_FILE`, `PIPLY_API_TOKEN_FILE`,
   `PIPLY_ADMIN_PASSWORD_FILE`. An environment variable is readable through
   `docker inspect`, `/proc/<pid>/environ`, and most crash reporters; a mounted
   file is not.
5. **Set `PIPLY_SESSION_SECRET`.** Otherwise it is generated and stored in the
   metadata database, and resetting the database signs everyone out.
6. **Grant narrowly.** Per-pipeline `view`/`run` rather than `*`. Reserve
   `admin` for the people who already have YAML write access — an admin can
   override commands, so the two are equivalent in power.
7. **Name secret variables recognisably** so masking catches them (§2).
8. **Set retention.** `piply prune` or `PIPLY_RETENTION_*`. Logs accumulate
   task output, which frequently contains more than you would choose to keep.
9. **Rotate the API token** by changing `PIPLY_API_TOKEN` and restarting. Token
   callers are treated as admin.
10. **Restrict network exposure.** Piply binds `127.0.0.1` by default. If you
    change that, put it behind a proxy you control.

---

## 5. Reporting a problem

Security issues are best reported privately to the maintainers rather than in a
public issue.

---

## Related

- [Authentication](AUTHENTICATION.md) — accounts, roles, permissions, sessions
- [Metadata Store](DATABASE.md) — what is stored, including `run_config`
- [Roadmap](ROADMAP.md) — audit logging and secret-manager integration
