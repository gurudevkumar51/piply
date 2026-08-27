"""First-run database setup.

Without `PIPLY_DATABASE`, Piply falls back to a file under the config
directory. That default is fine for a laptop and usually wrong on a server: in
a container it is the writable layer, which is wiped on every redeploy, and
losing run history that way is only discovered after it happens.

So a fresh install is sent here to choose deliberately. The chosen setting is
validated by actually opening the database before anything is written, then
saved to the project `.env` and applied to the running process.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import parse_qsl

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from piply.core.dialects import is_postgres_dsn
from piply.core.scheduler import PipelineScheduler
from piply.core.sql_adapters import mask_connection_secret
from piply.core.store import RunStore
from piply.settings import SettingsError, load_settings

router = APIRouter(tags=["setup"])

SETUP_PATH = "/setup"
SETUP_ADMIN_PATH = "/setup/admin"

#: Shortest first password accepted here. Long enough to discourage "admin",
#: short enough not to fight someone on a laptop who will change it later.
_MIN_PASSWORD_LENGTH = 8

#: Generous for a DSN, small enough that an unauthenticated caller cannot make
#: the server buffer something large.
_MAX_SETUP_BODY_BYTES = 8 * 1024


#: Tables that mean a database is in use. `meta` is written the moment any
#: service starts, so it never indicates use.
_IN_USE_TABLES = ("runs", "users", "pipeline_overrides")


def is_first_run(settings, service) -> bool:
    """Return whether this looks like a brand-new install.

    Deliberately evaluated **once at startup**, before the scheduler runs.
    `piply init` generates scheduled pipelines, so within seconds of booting the
    scheduler creates a run and the database stops looking empty — asking this
    question later would race it and answer "no" on a genuinely fresh install.

    An install that never set `PIPLY_DATABASE` but has been happily using the
    default file for months is *not* a first run: it has history, so sending it
    to a setup page would be a regression rather than a feature.
    """
    if settings is None or service is None or settings.database_configured:
        return False
    try:
        counts = service.store.row_counts()
    except Exception:  # noqa: BLE001 - an unreadable store is not a setup prompt
        return False
    return not any(counts.get(table, 0) for table in _IN_USE_TABLES)


def setup_required(request: Request) -> bool:
    """Return whether this install still has to choose a metadata store."""
    return bool(getattr(request.app.state, "setup_required", False))


def _config_path(request: Request) -> Path:
    """Return the loaded config file path."""
    return Path(request.app.state.service.config_path)


async def _read_form(request: Request) -> dict[str, str]:
    """Parse the setup form without a third-party parser.

    Mirrors the sign-in form: `application/x-www-form-urlencoded` only, size
    capped, and decoded with the standard library so no extra dependency is
    needed for the one form on this page.
    """
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/x-www-form-urlencoded":
        raise HTTPException(status_code=415, detail="Setup expects an HTML form submission.")
    body = await request.body()
    if len(body) > _MAX_SETUP_BODY_BYTES:
        raise HTTPException(status_code=413, detail="Setup request was too large.")
    try:
        decoded = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail="Setup request was not valid UTF-8.") from exc
    return dict(parse_qsl(decoded, keep_blank_values=True))


#: Seconds to wait when testing a PostgreSQL DSN during setup.
# The driver's default is over two minutes, which on this page means a typo in
# the hostname leaves someone staring at a form that appears to have hung.
_PROBE_CONNECT_TIMEOUT = 5


def _with_connect_timeout(dsn: str) -> str:
    """Return the DSN with a short connect timeout, unless one was given."""
    if "connect_timeout" in dsn:
        return dsn
    separator = "&" if "?" in dsn else "?"
    return f"{dsn}{separator}connect_timeout={_PROBE_CONNECT_TIMEOUT}"


def validate_database_choice(backend: str, sqlite_path: str, dsn: str, *, base_dir: Path) -> tuple[str, str]:
    """Return the `PIPLY_DATABASE` value for a choice, or raise ValueError.

    Validation opens the database rather than only inspecting the string: a DSN
    that parses but points at an unreachable host would otherwise be accepted
    here and fail at the next restart, when the reason is much less obvious.

    Returns the value to persist plus a credential-free description for the UI.
    """
    if backend == "postgres":
        candidate = (dsn or "").strip()
        if not candidate:
            raise ValueError("Enter a PostgreSQL connection URL.")
        if not is_postgres_dsn(candidate):
            raise ValueError("That is not a PostgreSQL URL. It should start with postgresql:// or postgres://.")
    elif backend == "sqlite":
        raw = (sqlite_path or "").strip() or ".piply/piply.db"
        if "://" in raw:
            raise ValueError("Enter a file path for SQLite, not a URL.")
        resolved = Path(raw)
        if not resolved.is_absolute():
            resolved = base_dir / resolved
        try:
            resolved.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ValueError(f"Cannot create the folder for that file: {exc}") from exc
        candidate = str(resolved)
    else:
        raise ValueError("Choose SQLite or PostgreSQL.")

    # The real check: open it, create the schema, and read something back.
    try:
        probe = _with_connect_timeout(candidate) if is_postgres_dsn(candidate) else Path(candidate)
        store = RunStore(probe)
        store.row_counts()
    except Exception as exc:  # noqa: BLE001 - any failure is a bad configuration
        # Includes a missing driver: the store already raises a RuntimeError
        # naming both `pip install psycopg` and the `[postgres]` extra, so there
        # is nothing to add here.
        raise ValueError(f"Could not open that database: {exc}") from exc

    return candidate, (mask_connection_secret(candidate) or candidate) if is_postgres_dsn(candidate) else candidate


def database_is_env_managed() -> bool:
    """Return whether `PIPLY_DATABASE` comes from the process environment.

    `load_settings` lets the real environment win over `.env`, so when the
    variable is set by a compose file, systemd unit, or Kubernetes manifest,
    writing `.env` changes nothing. Saying so is better than appearing to
    succeed and silently doing nothing.
    """
    return bool(os.environ.get("PIPLY_DATABASE"))


def persist_database_setting(config_path: Path, value: str) -> Path:
    """Write `PIPLY_DATABASE` into the project `.env`, replacing any existing line."""
    env_path = config_path.parent / ".env"
    lines: list[str] = []
    if env_path.exists():
        lines = [
            line
            for line in env_path.read_text(encoding="utf-8").splitlines()
            if not line.strip().startswith("PIPLY_DATABASE=")
        ]
    lines.append(f"PIPLY_DATABASE={value}")
    env_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return env_path


def apply_database_setting(request: Request) -> None:
    """Point the running process at the new database without a restart.

    The service and scheduler were built against the fallback store, so both are
    replaced. Doing this in place is what lets the user continue straight into
    Piply instead of being told to restart the server.
    """
    app = request.app
    # Nothing is written to os.environ: `load_settings` already reads the `.env`
    # beside the config, and mutating the process environment would leak into
    # anything else sharing it.
    old_scheduler = getattr(app.state, "scheduler", None)
    if old_scheduler is not None:
        old_scheduler.stop()

    from piply.api.app import _build_service

    config_path = str(app.state.service.config_path)
    try:
        settings = load_settings(config_path)
        service = _build_service(config_path, settings)
        scheduler = PipelineScheduler(service)
    except Exception:
        # The old scheduler is already stopped, so a failure here would leave the
        # server up with schedules and sensors silently dead. Put the previous
        # one back before reporting the failure.
        if old_scheduler is not None and not app.state.setup_required:
            old_scheduler.start()
        raise

    app.state.settings = settings
    app.state.service = service
    app.state.scheduler = scheduler
    # Setup is done, so this install is no longer a first run and the scheduler
    # can start against the database the operator actually chose.
    app.state.setup_required = False
    scheduler.start()


def admin_bootstrap_available(request: Request) -> bool:
    """Return whether the first admin can still be created from the setup page.

    True only while the install has **no accounts at all**. Once one exists the
    page closes permanently, so this is never a way in to a running system — it
    is the same open-install bootstrap that `POST /api/users` already allows,
    just offered where someone new will actually find it.
    """
    service = getattr(request.app.state, "service", None)
    if service is None or setup_required(request):
        return False
    try:
        return not service.list_users()
    except Exception:  # noqa: BLE001 - an unreadable store is not an invitation
        return False


def _render_admin_page(request: Request, error: str | None = None, submitted: dict | None = None, status_code: int = 200):
    """Render the optional first-admin step."""
    return request.app.state.templates.TemplateResponse(
        request,
        "setup_admin.html",
        {
            "project": request.app.state.service.project,
            "error": error,
            "submitted": submitted or {},
            "scheduler": {"state": "", "running": False, "label": "", "config_path": ""},
            "page": "setup",
        },
        status_code=status_code,
    )


@router.get(SETUP_ADMIN_PATH, response_class=HTMLResponse)
def setup_admin_page(request: Request):
    """Offer to create the first admin account after the database is chosen."""
    if not admin_bootstrap_available(request):
        return RedirectResponse(url="/", status_code=303)
    return _render_admin_page(request)


@router.post(SETUP_ADMIN_PATH)
async def submit_setup_admin(request: Request):
    """Create the first admin account and sign this session in as it."""
    if not admin_bootstrap_available(request):
        return RedirectResponse(url="/", status_code=303)

    form = await _read_form(request)
    username = form.get("username", "").strip()
    password = form.get("password", "")
    confirm = form.get("confirm", "")

    if not username:
        return _render_admin_page(request, "Enter a username.", form, 400)
    if len(password) < _MIN_PASSWORD_LENGTH:
        return _render_admin_page(request, f"Use at least {_MIN_PASSWORD_LENGTH} characters.", form, 400)
    if password != confirm:
        return _render_admin_page(request, "The two passwords do not match.", form, 400)

    service = request.app.state.service
    try:
        user = service.create_user(username, password, role="admin")
    except Exception as exc:  # noqa: BLE001 - shown verbatim on the form
        return _render_admin_page(request, str(exc), form, 400)

    response = RedirectResponse(url="/", status_code=303)
    # Creating the first account switches authentication on, which would lock
    # out the very page that created it. Sign this session in as the new admin.
    from piply.api.routes.accounts import _set_session_cookie

    _set_session_cookie(request, response, user.username)
    return response


@router.get(SETUP_PATH, response_class=HTMLResponse)
def setup_page(request: Request, error: str | None = None):
    """Render the first-run database chooser."""
    if not setup_required(request):
        # Never offer to repoint a configured install: that would let anyone who
        # can reach the page swap the database of a running system.
        return RedirectResponse(url="/", status_code=303)

    service = request.app.state.service
    return request.app.state.templates.TemplateResponse(
        request,
        "setup.html",
        {
            "project": service.project,
            "error": error,
            "default_sqlite_path": str(Path(service.config_path).parent / ".piply" / "piply.db"),
            "scheduler": {"state": "", "running": False, "label": "", "config_path": ""},
            "page": "setup",
        },
    )


@router.post(SETUP_PATH)
async def submit_setup(request: Request):
    """Validate the chosen database, persist it, and continue into Piply."""
    if not setup_required(request):
        return RedirectResponse(url="/", status_code=303)

    form = await _read_form(request)
    try:
        value, _description = validate_database_choice(
            form.get("backend", "sqlite").strip().lower(),
            form.get("sqlite_path", ""),
            form.get("dsn", ""),
            base_dir=_config_path(request).parent,
        )
    except (ValueError, SettingsError) as exc:
        return request.app.state.templates.TemplateResponse(
            request,
            "setup.html",
            {
                "project": request.app.state.service.project,
                "error": str(exc),
                "default_sqlite_path": str(_config_path(request).parent / ".piply" / "piply.db"),
                "submitted": form,
                "scheduler": {"state": "", "running": False, "label": "", "config_path": ""},
                "page": "setup",
            },
            status_code=400,
        )

    persist_database_setting(_config_path(request), value)
    apply_database_setting(request)
    if admin_bootstrap_available(request):
        return RedirectResponse(url=SETUP_ADMIN_PATH, status_code=303)
    return RedirectResponse(url="/", status_code=303)
