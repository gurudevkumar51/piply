# Audit History

Implemented in the v0.3.4 candidate. Audit entries are application-managed
history, not a compliance-grade or crash-atomic ledger. The current release
decision and verification gaps are in the [release assessment](RELEASE_CHECK.md).

Audit history records administrative and execution actions in the metadata
database. Existing databases gain the `audit_events` table at startup; no YAML
changes are required. History begins after upgrading and is not reconstructed
from older runs.

## Recorded Actions

Each entry contains an increasing id, UTC timestamp, actor, action, target type,
target id, and a JSON detail summary.

| Action | Target / summary |
| --- | --- |
| `run.create` | New run; pipeline, trigger, selected task, parent run |
| `run.retry` | Retry run; original run, retry mode, selected task |
| `run.pause`, `run.resume`, `run.cancel`, `run.delete` | Run and pipeline id |
| `pipeline.pause`, `pipeline.resume`, `pipeline.delete` | Pipeline id |
| `schedule.backfill` | Pipeline, window, number of slots queued |
| `user.create`, `user.update`, `user.delete` | Account, role/active changes, password-change flag |
| `permission.grant`, `permission.revoke` | Account, pipeline, normalized actions |
| `smtp.update` | Names of submitted SMTP fields, not their values |
| `smtp.test`, `notification.test` | Delivery outcome; notification destination names only |
| `database.switch`, `database.migrate` | Source/target backend names; migration flag for switches |
| `history.prune` | Retention rules and deletion counts |

Creation and cancellation entries record accepted actions, not completion or
delivery guarantees. Run and task history still provides execution outcomes.
Rejected actions that make no changes are not recorded as successful changes.
Audit queries and retention dry runs do not generate new events.

## Actors And Access

Authenticated UI/API actions use the account username. Legacy Basic credentials
use `env-admin`; the legacy global bearer token uses `api-token`. Anonymous
local-mode requests use `anonymous`; CLI commands use `cli`. Direct service
calls use `system` unless an actor is supplied. Automatic dispatch uses
`system:scheduler`, `system:sensor`, `system:pipeline`, or `system:retry`;
initial automatic account creation uses `system:bootstrap`.

`/audit` and `GET /api/audit` require an administrator when authentication is
enabled. The navigation link is hidden from non-admins. As with other admin
features, an installation with authentication disabled permits local users.
The CLI reads the database directly and relies on operating-system access; it
does not authenticate against an account.

The detail summaries deliberately exclude passwords/hashes, API tokens,
webhook URLs, database connection strings, SMTP values, submitted commands,
environment values, run parameters, and exception text. Identifiers and actor
names remain visible, so protect database files and backups accordingly.

## Query History

Open **Audit** in the UI. Filters are exact matches for actor, action, target
type, and target id. `since` accepts an ISO timestamp or a positive relative
interval such as `7d`, `12h`, or `30m`; timestamps without an offset mean UTC.
Use **Older** for the next page and **Latest** to return to the newest matches.

```bash
piply audit --since 7d
piply audit --actor ops --action run.cancel --json
piply audit --target-type run --target-id RUN_ID
piply audit --before-id 123 --limit 100
```

```bash
curl -u admin:password "http://127.0.0.1:8000/api/audit?since=7d&limit=50"
```

The API returns a JSON array ordered by descending event id. Pass the last
entry's id as `before_id` for older records. Pagination remains stable when new
events arrive. Limits are 1-500; invalid date/interval filters return HTTP 400.
There is no API for editing or individually deleting entries.

## Retention And Migration

Audit history is independent of runs and accounts: deleting either does not
erase their events. SQLite backups and SQLite/PostgreSQL migrations include
audit history and preserve event ids. A UI database switch records a summary
in both the old and new stores, even without migration.

Audit age retention follows the existing run-retention setting,
`PIPLY_RETENTION_RUN_DAYS`, and the `piply prune --run-days` override. A value of
zero keeps audit entries indefinitely; `--max-runs` and log retention do not
remove audit events. Prune summaries include `audit_deleted`; dry runs preview
the count without changing history.

Audit entries are append-only through Piply's normal application interfaces,
apart from retention. This is not a tamper-proof external ledger: an operating
system/database administrator or configured task with database access can
modify the database. A change and its audit append use separate transactions;
a process crash or storage failure between them can leave a gap. Use database
backups and external monitoring where stronger guarantees are required.
