"""Authentication and authorization for the Piply UI and API.

Three ways to authenticate, in the order they are tried:

1. a session cookie, set by the login form,
2. HTTP Basic, against a stored account or the legacy env-var credentials,
3. an API bearer token, for machine access.

Authorization is per pipeline. The middleware attaches the resolved user to the
request; routes then ask ``require_permission`` before doing anything.
"""

from __future__ import annotations

import base64
import secrets
from urllib.parse import quote

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, RedirectResponse, Response

from piply.core.auth import ALL_PIPELINES, User, constant_time_equals, read_session
from piply.settings import PiplySettings

SESSION_COOKIE = "piply_session"

#: Paths reachable without authentication.
# /api/me must answer anonymously so a client can ask whether it is signed in.
PUBLIC_PATHS = frozenset({"/login", "/logout", "/health", "/api/me"})
PUBLIC_PREFIXES = ("/static",)

#: A token-authenticated caller is a machine integration and is treated as an
#: admin, matching the behaviour before per-user permissions existed.
_TOKEN_USER = User(username="api-token", role="admin", is_active=True)
_LEGACY_USER = User(username="env-admin", role="admin", is_active=True)


def _decode_basic_credentials(header_value: str) -> tuple[str, str] | None:
    """Decode an HTTP Basic auth header into username and password."""
    if not header_value.startswith("Basic "):
        return None
    encoded = header_value[6:].strip()
    try:
        decoded = base64.b64decode(encoded).decode("utf-8")
    except Exception:
        return None
    if ":" not in decoded:
        return None
    username, password = decoded.split(":", 1)
    return username, password


def _valid_legacy_basic(header_value: str, settings: PiplySettings) -> bool:
    """Return whether the request matches the env-var admin credentials.

    Kept so an install that predates user accounts keeps working.
    """
    credentials = _decode_basic_credentials(header_value)
    if credentials is None:
        return False
    username, password = credentials
    return bool(
        settings.auth_username is not None
        and settings.auth_password is not None
        and constant_time_equals(username, settings.auth_username)
        and constant_time_equals(password, settings.auth_password)
    )


def _valid_bearer_token(header_value: str, settings: PiplySettings) -> bool:
    """Return whether the request includes the configured API bearer token."""
    if settings.api_token is None or not header_value.startswith("Bearer "):
        return False
    token = header_value[7:].strip()
    return secrets.compare_digest(token, settings.api_token)


def _is_api_path(path: str) -> bool:
    """Return whether a path is machine-facing rather than a rendered page."""
    return path.startswith("/api") or path == "/metrics"


def _challenge(path: str, *, has_accounts: bool) -> Response:
    """Return the right unauthenticated response for the path.

    Without stored accounts the install is in legacy env-var mode, where the
    only credentials are HTTP Basic. Redirecting those users to a login form
    they cannot use would lock them out, so keep the browser challenge until
    accounts exist.
    """
    if _is_api_path(path):
        return JSONResponse(
            status_code=401,
            content={"detail": "Authentication required."},
            headers={"WWW-Authenticate": 'Basic realm="Piply", Bearer'},
        )
    if not has_accounts:
        return Response(status_code=401, headers={"WWW-Authenticate": 'Basic realm="Piply"'})
    return RedirectResponse(url=f"/login?next={quote(path, safe='/')}", status_code=303)


def get_service(request: Request):
    """Return the shared PipelineService attached to the app."""
    return request.app.state.service


def resolve_user(request: Request) -> User | None:
    """Return the authenticated user attached by the middleware."""
    return getattr(request.state, "user", None)


def require_permission(request: Request, action: str, pipeline_id: str | None = None) -> User | None:
    """Raise 403 unless the current user may perform an action.

    Returns the user so callers can branch on role. When authentication is
    switched off entirely, returns None and permits everything, which keeps
    single-user installs frictionless.
    """
    service = request.app.state.service
    if not service.auth_required:
        return None

    user = resolve_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required.")
    if not user.can(action, pipeline_id):
        target = f" on pipeline '{pipeline_id}'" if pipeline_id else ""
        raise HTTPException(status_code=403, detail=f"Your account cannot {action}{target}.")
    return user


def require_admin(request: Request, detail: str = "Only administrators can perform this action.") -> User | None:
    """Raise unless the caller is an administrator.

    Used for installation-wide actions — retention, diagnostics, SMTP, and
    anything whose blast radius is not limited to a single pipeline.
    """
    service = request.app.state.service
    if not service.auth_required:
        return None
    user = resolve_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required.")
    if not user.is_admin:
        raise HTTPException(status_code=403, detail=detail)
    return user


def guard_run(request: Request, run_id: str, action: str):
    """Check a permission against the pipeline that owns a run.

    Runs inherit their pipeline's permissions, so every run-scoped endpoint
    has to resolve the owning pipeline before deciding. Returns the run so the
    caller does not have to load it twice.
    """
    service = request.app.state.service
    run = service.store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Unknown run '{run_id}'")
    require_permission(request, action, run.pipeline_id)
    return run


def visible_pipeline_ids(request: Request) -> set[str] | None:
    """Return the pipeline ids the caller may see, or None when unrestricted.

    None means "no filtering needed", which lets callers skip the work
    entirely on single-user installs.
    """
    service = request.app.state.service
    if not service.auth_required:
        return None
    user = resolve_user(request)
    if user is None:
        return set()
    if user.is_admin or user.permissions.get(ALL_PIPELINES):
        return None
    return {pipeline_id for pipeline_id, actions in user.permissions.items() if actions}


def filter_by_pipeline(request: Request, items: list, attribute: str = "pipeline_id") -> list:
    """Drop items belonging to pipelines the caller may not see."""
    allowed = visible_pipeline_ids(request)
    if allowed is None:
        return items
    return [item for item in items if getattr(item, attribute, None) in allowed]


def visible_pipelines(request: Request, summaries: list) -> list:
    """Filter a list of pipeline summaries to what the current user may see."""
    service = request.app.state.service
    if not service.auth_required:
        return summaries
    user = resolve_user(request)
    if user is None:
        return []
    if user.is_admin or user.permissions.get(ALL_PIPELINES):
        return summaries
    allowed = {pipeline_id for pipeline_id, actions in user.permissions.items() if actions}
    return [item for item in summaries if item.pipeline_id in allowed]


#: Headers applied to every response. Chosen to be safe for a self-contained,
#: server-rendered app with no third-party embeds and no CDN assets.
SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "same-origin",
    # Everything is same-origin except three CDN origins the bundled UI loads:
    # Google Fonts (stylesheet plus font files) and jsDelivr, which serves the
    # dagre/graphlib layout libraries the DAG view needs. `UI_REMOTE_ORIGINS`
    # below is the single list; a test keeps it in step with the markup, because
    # a policy that blocks the app's own assets is worse than no policy.
    #
    # 'unsafe-inline' is required because pages bootstrap their state from
    # inline <script> blocks; values interpolated into those blocks go through
    # Jinja's `tojson`, which escapes the characters needed to break out of one.
    "Content-Security-Policy": (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "img-src 'self' data:; "
        "connect-src 'self'; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self'"
    ),
}


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Attach hardening headers to every response."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        for header, value in SECURITY_HEADERS.items():
            response.headers.setdefault(header, value)
        return response


class AuthMiddleware(BaseHTTPMiddleware):
    """Resolve the caller's identity and reject anonymous requests."""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        service = getattr(request.app.state, "service", None)
        settings: PiplySettings = request.app.state.settings

        if any(path.startswith(prefix) for prefix in PUBLIC_PREFIXES) or path in PUBLIC_PATHS:
            request.state.user = self._identify(request, service, settings)
            return await call_next(request)

        auth_required = bool(settings.auth_enabled)
        if service is not None:
            auth_required = service.auth_required

        user = self._identify(request, service, settings)
        request.state.user = user

        if not auth_required:
            return await call_next(request)
        if user is None:
            has_accounts = service is not None and service.store.count_users() > 0
            return _challenge(path, has_accounts=has_accounts)
        return await call_next(request)

    def _identify(self, request: Request, service, settings: PiplySettings) -> User | None:
        """Return the user behind this request, or None."""
        if service is not None:
            token = request.cookies.get(SESSION_COOKIE)
            username = read_session(service.store, token)
            if username:
                user = service.get_user(username)
                if user is not None and user.is_active:
                    return user
                # A session issued for the env-var admin has no stored account.
                if settings.auth_username and username == settings.auth_username:
                    return _LEGACY_USER

        authorization = request.headers.get("Authorization", "")
        if authorization.startswith("Basic ") and service is not None:
            credentials = _decode_basic_credentials(authorization)
            if credentials is not None:
                user = service.authenticate(*credentials)
                if user is not None:
                    return user
        if _valid_legacy_basic(authorization, settings):
            return _LEGACY_USER
        if _valid_bearer_token(authorization, settings):
            return _TOKEN_USER
        return None
