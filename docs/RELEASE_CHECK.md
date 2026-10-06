# v0.3.4 Release Assessment

Assessment date: 2026-10-06. **Decision: HOLD; do not publish yet.**
This is an evidence record, not a claim of exhaustive testing or a release date.
No release was published or tagged. The user authorized a validation-only
`codex/release-v0.3.4-validation` review branch; no merging or staging publication
is authorized. Protected branches remain untouched.

## Release Blockers

1. **Linux and remote CI sign-off is incomplete.** Windows Python 3.10-3.14
   and native PostgreSQL 16 are now tested locally. Docker Desktop startup
   timed out and its engine is still unavailable, so local Linux testing is
   not possible. The authorized review branch runs validation only; the real
   Gitea compatibility/PostgreSQL jobs must pass. Configuration is not evidence.

P1.3's initial four-path failure is corrected locally. The unchanged
`tools/check_run_admission.py` now admits one active run for limit-one direct
paths and two for limit-two queue dispatch. Keep it as a publishing gate.
SQLite service/process races and API/CLI rejection checks pass. Real PostgreSQL
admission cases now pass against real PostgreSQL locally; Linux CI is still
required to verify service-container and runner integration.

## Verification Matrix

| Area | Evidence / status |
| --- | --- |
| Windows Python 3.14 baseline | 331 passed, 20 PostgreSQL skips; 2 SQLite ResourceWarnings |
| Baseline combined line/branch coverage | 70%; subprocess CLI paths and optional drivers are not fully captured |
| New release guards | 9 passed: error-text escaping, local graph assets/licenses, package discovery, backup/restore closure, CLI callable-log routing |
| Conda Python 3.13.16 before admission follow-up | 341 passed, 20 PostgreSQL skips, 1 test-client deprecation warning; pytest 9.1.1; 519.72 seconds |
| Standard Windows Python 3.14 before admission follow-up | 341 passed, 20 PostgreSQL skips, 1 test-client deprecation warning; pytest 9.1.1; 582.06 seconds |
| Admission follow-up | Four-path gate and 34 admission guards pass; a 36-check focused run includes both adapted overlap fixtures |
| Conda Python 3.13.16 admission suite | 375 passed, 25 PostgreSQL skips, 1 test-client warning; 325.32 seconds; final API-preview fixture isolation included |
| Windows Python 3.14 admission suite | 375 passed, 25 PostgreSQL skips, 1 test-client warning; 373.78 seconds; subsequent API-preview fixture isolation and documentation checks: 10 passed |
| Windows Python 3.10.21 | 378 passed, 25 PostgreSQL skips, 1 test-client warning; 416.06 seconds; includes explicit schema-reset guards |
| Windows Python 3.11.16 | 378 passed, 25 PostgreSQL skips, 1 test-client warning; 398.82 seconds |
| Windows Python 3.12.14 | 378 passed, 25 PostgreSQL skips, 1 test-client warning; 406.24 seconds |
| Final pause/resume correction | 27 focused checks passed; deterministic pre-fix failure and both final full-suite passes; installed-wheel parallel browser run completed 3/3 after pause/resume |
| Wheel + source archive | Built; content/metadata/secret-exclusion checks passed: 80 runtime files, 31 project Markdown files, and 2 CI workflows; Twine metadata checks passed |
| Installed-wheel smoke | Python 3.10-3.14 wheels pass CLI initialization/execution, UI/API, assets/MIME, authentication/session invalidation, audit, cron timezones, and the four-path admission gate outside the checkout |
| SQLite upgrade/restore/rollback | Published 0.3.3 -> candidate 0.3.4 preserved historical run/tasks and test account; new execution/audit, upgraded backup restore, and rollback from the pre-upgrade snapshot passed |
| Dependency vulnerability check | Conda runtime/test and Python 3.14 QA environments: no known findings after pytest/pip upgrades; graph-library npm audit: zero findings |
| Browser desktop/mobile | Installed-wheel pipelines, pipeline/run graphs, upcoming schedules, pause/resume, audit filtering and mobile containment passed; no browser warnings/errors captured |
| PostgreSQL integration | 25 passed on native PostgreSQL 16.15, psycopg 3.3.6, Windows Conda Python 3.13.16; 195.26 seconds; no skips; dedicated password-protected cluster/database |
| Latest CI/static guards | 22 release/documentation checks passed on each Python 3.10-3.14 environment; validation-only branch trigger, publication isolation, PostgreSQL reset opt-in and compatible Ubuntu runner image covered |
| Linux | Not executed locally; actual Gitea matrix/service-container verification required |
| Live SMTP, Teams, SSH/SFTP and non-SQLite sensor databases | No external credentials/services used; covered by local/mocked tests where present |

Full tests cover config/includes/templates/entities, runtime inputs and replay,
sequential/parallel execution, priorities, schedules/sensors, cancellation,
timeouts, retry, pause gates, shutdown/recovery, outputs/artifacts/logs,
retention, auth/permissions, setup, settings, and audit queries. The admission
follow-up adds shared-store/process/dispatcher races, paused capacity, failure
rollback, API 409 and CLI exit-1 behavior. PostgreSQL SQL/isolation contract
checks are not substitutes for executing the real database tests.

The Conda coverage run before the final race correction measured **77% combined
line/branch coverage** (80% statements, 68% branches), not exhaustive coverage.
A clean standard Windows
venv initially failed five cron-related tests because `tzdata` was undeclared;
the corrected installed wheel passes UTC, Asia/Kolkata, and America/New_York
smoke checks. The next full Python 3.14 run exposed a real parallel pause/resume
race: resume between two pause-state reads could report success with a downstream
task still queued. A deterministic regression failed before the correction;
the engine now retries ready work instead of deciding completion from a second
pause read. The fake-task fixture was also made independent of a five-second
worker deadline, with unconditional cleanup. Both final full reruns passed after
the engine correction. Browser testing also verified two active tasks finishing
while paused, the third task remaining queued, and same-run resume completing
all three. No runtime failures or SQLite ResourceWarnings remain in these reruns.

The upgrade rehearsal used the actual public 0.3.3 wheel and the local candidate
in a separate environment, a two-task SQLite pipeline, and a disposable admin
account. It did not exercise a production database, PostgreSQL upgrade, or live
external integrations. Final JUnit results are stored locally in
`reports/release-v0.3.4-final313.xml` and `reports/release-v0.3.4-final314.xml`.

Admission follow-up results use `reports/release-v0.3.4-admission313.xml` and
`reports/release-v0.3.4-admission314.xml`. Two older identity/authorization
fixtures now explicitly allow their two intended accepted launches rather than
depending on a bypass of the default limit. An API override/schedule-preview
test now disables live scheduler execution: a saved failed-test database
confirmed a schedule trigger had legitimately occupied its single slot.
Scheduler/admission races remain exercised by dedicated admission tests.

The rebuilt-wheel browser check showed an overlap attempt refused with HTTP
409 while a run was paused, no extra run created, the Run button reenabled,
and same-run resume completing 3/3 tasks. The preview database has no active
runs after this check. Browser coverage remains Chromium only, not all browsers.

## Fixes Made During Assessment

- Bundled the existing pinned MIT-licensed dagre/graphlib browser distributions,
  verified their npm archive SHA-512 integrity, and retained their licenses.
  Graphs no longer need a CDN; optional web fonts still have system fallbacks.
- Escaped browser error messages/source text before inserting the error banner.
- Closed SQLite destination handles after backup/restore instead of relying on GC.
- Prevented Python callable logs from recursively entering task capture when
  CLI callbacks echo them. A fresh Conda install exposed this with current Typer.
- Fixed parallel resume prematurely completing a run with ready tasks still
  queued; the new regression checks execution and persisted task statuses.
- Made run/task insertion and per-pipeline capacity reservation atomic. Direct
  launches reject at capacity; automatic triggers retain payload/order and use
  all configured slots. Existing schedule slots remain idempotent. Failure
  rollback and dispatch cleanup prevent abandoned reservations from errors.
- Added narrow ignore exceptions for graph distributions hidden by `dist/`.
  These assets must be included when committing the candidate, not just exist
  on a developer's machine.
- Excluded demo workspaces from package discovery; added docs and QA tools to the
  source archive, including the changelog caught by archive verification. There
  are eight common runtime dependencies plus Windows-only
  `tzdata`, added after a clean venv exposed missing cron timezone data. See
  [Python's timezone guidance](https://docs.python.org/3/library/zoneinfo.html#data-sources).
- Added Python 3.10's TOML test parser as a conditional test-only dependency, so
  packaging checks no longer silently skip on the minimum supported version.
- Raised the test-only pytest minimum to 9.0.3 after the dependency audit found
  CVE-2025-71176 in 8.4.2. The [upstream fix](https://github.com/pytest-dev/pytest/releases/tag/9.0.3)
  addresses insecure temporary-directory handling; runtime dependency count is unchanged.
- Formatted release-changed Python files; aligned Ruff's hook and CI versions.
- Updated the isolated QA installer after its audit found old pip advisories;
  wheel-check CI now updates pip before installing the candidate.
- Added real regression/compatibility and PostgreSQL CI gates, archive validation,
  installed-wheel smoke testing, and the required run-admission gate.
- Required `PIPLY_TEST_POSTGRES_RESET=1` before the PostgreSQL fixture drops its
  schema. The target must still be an expendable database; this is a second
  explicit opt-in, not automatic production-database detection.
- Corrected the PostgreSQL job container to an Ubuntu 22.04 Actions image.
  The [Python setup action](https://github.com/actions/setup-python) selects
  platform-specific builds; its current 3.13 manifest lists Ubuntu versions,
  not Debian 12. A Node-enabled Debian container alone was insufficient.
- Added review-branch push validation and OS/interpreter assertions so a
  mislabeled runner cannot count as a successful Windows compatibility run.
- Included CI workflows in source archives and archive-content verification.

Current CI integration uses the authenticated Gitea 1.27.1 repository. Actions
are enabled and the default branch is staging; only the authorized `codex/`
review branch may be pushed for validation. Staging pushes would publish.
- Refreshed all Markdown documentation for implemented versus planned behavior.

## Accepted Limitations And Follow-Up

- Audited changes and event appends are separate transactions (P1.4); a crash or
  storage failure can leave a history gap. Database/OS administrators can edit
  history. This is not a tamper-proof external ledger.
- Python callables run in-process threads and cannot be forcibly stopped; a timed
  out/cancelled callable may still finish external work.
- The global bearer token is admin-shaped. Named account tokens and individual
  session listing/revocation remain P1.2. Cookies are invalidated on password change.
- No CSRF tokens or Origin enforcement are implemented. SameSite cookies mitigate
  cookie-based cross-site POSTs, but cached browser Basic credentials are ambient
  credentials and need additional protection. Keep deployment behind trusted
  network/proxy boundaries; see [Security](SECURITY.md).
- Builds currently warn about legacy MIT license metadata/classifiers. Migrate to
  SPDX metadata with a deliberate build-backend minimum; this is not a runtime
  failure and was not silently changed during QA.
- Current Starlette emits a test-client deprecation warning about `httpx` versus
  `httpx2`. It is not a pipeline or browser failure. Audit results cover the
  resolved versions in these environments, not every older permitted version;
  the unpublished Piply candidate itself is not in PyPI advisory data.

## Reproduce And Sign Off

Use disposable data. Never run the admission check against a production database.
It creates its own temporary stores and does not execute pipeline commands.

```bash
python -m pip install -e ".[dev,test,build,postgres]" "ruff==0.15.4"
python -m ruff check piply tests tools
python -m ruff format --check piply tests tools
python -m pytest -q
python -m tools.check_run_admission
python -m build
python tools/check_release_artifacts.py dist
python -m twine check dist/*
```

Install only the built wheel in a clean environment, then run
`python -I tools/release_smoke.py`. `-I` prevents an import from the checkout or
user-site packages from masking missing packaged files/dependencies. Use that
environment's interpreter, not another `pip` on PATH.

For PostgreSQL set `PIPLY_TEST_POSTGRES_DSN` to an expendable test database and
`PIPLY_TEST_POSTGRES_RESET=1`, then run `tests/test_postgres_store.py`. Its fixture
resets the schema: **never point it at production**. CI service-container and Windows jobs require suitable
registered runner labels and Docker support; verify those on the actual
[Gitea runner](https://docs.gitea.com/runner/2/), not just YAML parsing.

Before approval, require all blockers cleared, final wheel/source checks and
clean-install smoke tests green, the supported-version/OS matrix and PostgreSQL
gate green, an upgrade/backup/restore rehearsal, and release-owner review of the
remaining limitations. Confirm the version is unused on PyPI at publish time.
Local metadata inspection found 0.3.3 current and 0.3.4 absent on 2026-10-06.
Use the existing release workflow only after explicit approval to publish.
