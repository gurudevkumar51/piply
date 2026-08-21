# HTTP API Reference

Every route Piply serves, with the permission each one requires.

The UI is a client of this API — anything the UI does can be scripted. Routes
are grouped by what they are for rather than by which module implements them.

**Permission column:**

| Value | Meaning |
| --- | --- |
| *public* | Reachable without credentials, even when auth is on |
| `view` / `edit` / `run` | Requires that action on the pipeline named in the path |
| `view` (any) | Requires the action on *some* pipeline; results are filtered to what you may see |
| `admin` | Administrator only |

When no accounts exist and no auth environment variables are set, authentication
is off entirely and every route is open. See
[Authentication](AUTHENTICATION.md).

---

## 1. Authenticating a request

Three mechanisms, tried in this order:

```bash
# 1. Session cookie, from the login form (what the UI uses)
curl -c jar -X POST http://localhost:8000/login \
     -d 'username=admin&password=secret'
curl -b jar http://localhost:8000/api/pipelines

# 2. HTTP Basic, against a stored account or the legacy env-var pair
curl -u admin:secret http://localhost:8000/api/pipelines

# 3. Bearer token, for machine integrations
curl -H "Authorization: Bearer $PIPLY_API_TOKEN" http://localhost:8000/api/pipelines
```

A bearer-token caller is treated as an administrator, matching the behaviour
from before per-user permissions existed.

### Status codes

| Code | Meaning |
| --- | --- |
| `401` | No valid credentials |
| `403` | Authenticated, but the account lacks the required permission |
| `404` | Unknown pipeline, run, or task |
| `400` | Invalid payload — the body explains what |
| `413` / `415` | Sign-in body too large, or wrong content type |
| `502` | An outbound call failed, e.g. the SMTP test |

---

## 2. Pipelines

| Method | Path | Permission | Purpose |
| --- | --- | --- | --- |
| `GET` | `/api/pipelines` | `view` (any) | List pipelines, filtered to what you may see |
| `GET` | `/api/pipelines/{id}` | `view` | One pipeline with tasks and recent runs |
| `DELETE` | `/api/pipelines/{id}` | `edit` | Delete the definition and its history |
| `POST` | `/api/pipelines/{id}/pause` | `edit` | Pause the schedule |
| `POST` | `/api/pipelines/{id}/resume` | `edit` | Resume the schedule |
| `GET` | `/api/pipelines/{id}/tasks/{task_id}` | `view` | One task definition |

### Running

| Method | Path | Permission | Purpose |
| --- | --- | --- | --- |
| `POST` | `/api/pipelines/{id}/run` | `run` | Trigger a manual run |
| `POST` | `/api/pipelines/{id}/tasks/{task_id}/run` | `run` | Run one task and its dependencies |
| `POST` | `/api/pipelines/{id}/chain/{target_id}` | `run` on **both** | Trigger a downstream pipeline with parent context |
| `GET` | `/api/pipelines/{id}/runtime-inputs` | `run` | What a manual run still needs before it can start |

Request body for all three run endpoints:

```json
{
  "variables":         {"practice": "BENNETT"},
  "params":            {"batch": "2026-05-26"},
  "tenant_id":         "bennett",
  "command_overrides": {"task_id": "echo replaced"}
}
```

- `variables` fill `{placeholder}` values the config leaves to run time. They are
  applied exactly as an upstream pipeline's variables would be.
- `params` land in `context["params"]`.
- **`command_overrides` requires `admin`.** It replaces what a task executes, so
  allowing it under `run` would turn "may run this pipeline" into "may execute
  any command". Everything else here only needs `run`.

Checking what a manual run needs, before starting it:

```bash
curl -u admin:secret \
  http://localhost:8000/api/pipelines/Bronze_to_Silver/runtime-inputs
```

```json
{
  "pipeline_id": "Bronze_to_Silver",
  "ready": false,
  "triggered_by": ["BENNETT_ETL_Flow", "PALOS_ETL_Flow"],
  "required": [{"name": "practice", "tasks": ["dbt"]}]
}
```

Add `?task_id=dbt` to scope it to one task. See
[UI Guide](UI_GUIDE.md#missing-runtime-values).

---

## 3. Runs

Runs inherit the permissions of the pipeline that produced them.

| Method | Path | Permission | Purpose |
| --- | --- | --- | --- |
| `GET` | `/api/runs` | `view` (any) | List runs, filtered |
| `GET` | `/api/runs/{run_id}` | `view` | One run with task runs and logs |
| `DELETE` | `/api/runs/{run_id}` | `edit` | Delete a finished run |
| `POST` | `/api/runs/{run_id}/retry` | `run` | Retry, `{"mode": "resume"\|"startover"}` |
| `POST` | `/api/runs/{run_id}/cancel` | `run` | Cancel a queued or running run |
| `POST` | `/api/runs/{run_id}/backfill` | `run` | Re-execute using the run's captured config |
| `GET` | `/api/runs/{run_id}/logs` | `view` | Paginated logs, `?limit=&offset=` |
| `GET` | `/api/runs/{run_id}/tasks/{task_id}` | `view` | One task run with logs and output |
| `GET` | `/api/runs/{run_id}/tasks/{task_id}/output` | `view` | Captured task output |
| `POST` | `/api/runs/{run_id}/tasks/{task_id}/retry` | `run` | Resume from that task |
| `GET` | `/api/runs/{run_id}/config` | `view` | Runtime configuration snapshot |
| `GET` | `/api/runs/{run_id}/artifacts` | `view` | Files the run recorded |
| `GET` | `/api/runs/{run_id}/artifacts/download` | `view` | Download one, `?path=` |

`GET /api/runs` accepts `pipeline_id`, `status`, `tenant`, and `limit`.

> **Credentials are masked in `/config`.** Values whose name looks like a secret
> are returned as `***`, for every caller including admins. The stored snapshot
> keeps the real values so replay still works. See
> [Security](SECURITY.md#run-configuration-no-longer-leaks-credentials).

Artifact downloads are doubly constrained: the path must be one that run
actually recorded, **and** must resolve inside an allowed root.

---

## 4. Maintenance

| Method | Path | Permission | Purpose |
| --- | --- | --- | --- |
| `GET` | `/api/pipelines/{id}/preview` | `view` | Dry run using configured values |
| `POST` | `/api/pipelines/{id}/preview` | `view`, `admin` for overrides | Dry run with supplied values |
| `POST` | `/api/pipelines/{id}/backfill` | `run` | Queue one run per scheduled slot in a window |
| `POST` | `/api/maintenance/prune` | `admin` | Delete history past the retention window |

Prune is admin-only because it is installation-wide and irreversible. Send
`{"dry_run": true}` to see what would go.

---

## 5. Observability

| Method | Path | Permission | Purpose |
| --- | --- | --- | --- |
| `GET` | `/health` | **public** | Liveness probe |
| `GET` | `/metrics` | `view` (any) | Prometheus text format |
| `GET` | `/api/metrics` | `view` (any) | Queue and worker metrics as JSON |
| `GET` | `/api/diagnostics` | `admin` | Scheduler, workers, sensors, store |
| `GET` | `/api/sensors` | `view` (any) | Sensor health, filtered |
| `GET` | `/api/dashboard` | `view` (any) | Dashboard payload, filtered |
| `GET` | `/api/dashboard/scheduler` | `view` (any) | Scheduler status only |
| `GET` | `/api/execution-matrix` | `view` | Task-by-run grid, filtered |
| `GET` | `/api/logs` | `view` | Search logs, filtered |
| `GET` | `/api/logs/stream` | `view` | Lines after a cursor, for tailing |

`/health` is public so a load balancer needs no credentials. It reveals only
status, version, scheduler state, and whether the process is accepting work —
no pipeline names or paths. It returns `503` only when the metadata store is
unreachable, which is the condition a restart would fix.

`/metrics` accepts the bearer token, so a Prometheus scraper does not need UI
credentials. Set `PIPLY_METRICS_ENABLED=false` to disable it.

Diagnostics is admin-only because it names filesystem paths, the config
location, the process id, and the metadata store.

---

## 6. Accounts and settings

| Method | Path | Permission | Purpose |
| --- | --- | --- | --- |
| `GET` | `/login` | **public** | Sign-in form |
| `POST` | `/login` | **public** | Submit credentials, sets the session cookie |
| `GET` | `/logout` | **public** | Clear the session |
| `GET` | `/api/me` | **public** | Current account; answers anonymously |
| `GET` | `/api/permissions` | `view` (any) | Describes the permission vocabulary |
| `GET` | `/api/users` | `admin` | List accounts |
| `POST` | `/api/users` | `admin` | Create an account |
| `PATCH` | `/api/users/{username}` | `admin` | Change password, role, or active flag |
| `DELETE` | `/api/users/{username}` | `admin` | Delete an account |
| `POST` | `/api/users/{username}/permissions` | `admin` | Grant or clear pipeline actions |
| `GET` | `/api/settings/smtp` | `admin` | Central SMTP; password never returned |
| `PUT` | `/api/settings/smtp` | `admin` | Update SMTP; blank password keeps the stored one |
| `POST` | `/api/settings/smtp/test` | `admin` | Send one test message |

`/api/me` is public so a client can ask whether it is signed in without
triggering a challenge:

```json
{"authenticated": false, "auth_required": true}
```

The login form is `application/x-www-form-urlencoded`. Other content types are
refused with `415`, and bodies over 8 KB with `413`.

Repeated failed sign-ins lock a username out for five minutes. See
[Security](SECURITY.md#sign-in-is-throttled).

---

## 7. Rendered pages

Server-rendered HTML, same permissions as the APIs behind them. Listed for
completeness — there is no separate "UI API".

| Path | Permission |
| --- | --- |
| `/` | `view` (any), filtered |
| `/pipelines`, `/pipelines/{id}` | `view` |
| `/runs`, `/runs/{run_id}` | `view` |
| `/logs`, `/execution-matrix` | `view`, filtered |
| `/settings` | `view` (any) |
| `/diagnostics` | `admin` |

See the [UI Guide](UI_GUIDE.md) for what each page answers.

---

## 8. Interactive schema

FastAPI serves generated documentation from the running app:

- `/docs` — Swagger UI
- `/redoc` — ReDoc
- `/openapi.json` — the raw schema

These follow the same authentication rules as everything else.

---

## Related

- [Authentication](AUTHENTICATION.md) — accounts, roles, and grants
- [Security](SECURITY.md) — trust model and what is deliberately not protected
- [UI Guide](UI_GUIDE.md) — the pages these routes serve
- [YAML Specification](YAML_SPECIFICATION.md) — the config these routes operate on
