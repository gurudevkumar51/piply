"""Sign-in, sign-out, and user administration."""

from __future__ import annotations

from urllib.parse import parse_qsl

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field

from piply.api.auth import SESSION_COOKIE, get_service, require_permission, resolve_user
from piply.core.auth import PERMISSIONS, AuthError, constant_time_equals, issue_session

router = APIRouter(tags=["accounts"])

#: Enough for any sign-in, small enough that an unauthenticated caller cannot
#: make the server buffer something large.
_MAX_LOGIN_BODY_BYTES = 8 * 1024


async def _read_login_form(request: Request) -> dict[str, str]:
    """Parse the sign-in form body without a third-party parser.

    FastAPI's ``Form(...)`` and Starlette's ``request.form()`` both require the
    ``python-multipart`` package. The sign-in form is the only form in the app
    and browsers always send it as ``application/x-www-form-urlencoded``, which
    the standard library decodes correctly — including ``+`` for space and
    percent-escapes. Doing it here keeps the runtime dependency count at eight
    and keeps a third-party parser off the one endpoint that is reachable
    without credentials.
    """
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/x-www-form-urlencoded":
        raise HTTPException(status_code=415, detail="Sign in expects an HTML form submission.")

    body = await request.body()
    if len(body) > _MAX_LOGIN_BODY_BYTES:
        raise HTTPException(status_code=413, detail="Sign-in request was too large.")

    try:
        decoded = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail="Sign-in request was not valid UTF-8.") from exc

    # keep_blank_values so an empty password stays present and is rejected as a
    # bad credential, rather than vanishing and looking like a malformed form.
    return dict(parse_qsl(decoded, keep_blank_values=True))


def _user_payload(user) -> dict[str, object]:
    """Return one account as JSON, never including secrets."""
    return {
        "username": user.username,
        "role": user.role,
        "is_active": user.is_active,
        "created_at": user.created_at,
        "last_login_at": user.last_login_at,
        "permissions": {pipeline_id: sorted(actions) for pipeline_id, actions in user.permissions.items()},
    }


def _set_session_cookie(request: Request, response, username: str) -> None:
    """Attach a signed session cookie for `username` to a response.

    Shared by sign-in and first-admin creation so the two cannot drift apart on
    flags like `secure` or `samesite`.
    """
    response.set_cookie(
        SESSION_COOKIE,
        issue_session(get_service(request).store, username),
        httponly=True,
        samesite="lax",
        secure=request.url.scheme == "https",
        path="/",
    )


def _safe_next(target: str | None) -> str:
    """Return a redirect target that cannot leave this site.

    Only same-site absolute paths are allowed. A protocol-relative value such
    as ``//evil.example`` is a URL to another host, so it is rejected along
    with anything that is not a plain path.
    """
    if not target or not target.startswith("/") or target.startswith("//") or target.startswith("/\\"):
        return "/"
    return target


def _require_admin(request: Request):
    """Raise unless the caller is an administrator."""
    service = get_service(request)
    if not service.auth_required:
        return None
    user = resolve_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required.")
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Only administrators can manage users.")
    return user


# --- Sign in and out ---------------------------------------------------------


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request, next: str = "/", error: str | None = None) -> HTMLResponse:
    """Render the sign-in form."""
    service = get_service(request)
    if resolve_user(request) is not None:
        return RedirectResponse(url=_safe_next(next), status_code=303)
    return request.app.state.templates.TemplateResponse(
        request,
        "login.html",
        {
            "project": service.project,
            "next": next or "/",
            "error": error,
            "scheduler": {"state": "", "running": False, "label": "", "config_path": ""},
            "page": "login",
        },
    )


@router.post("/login")
async def login_submit(request: Request):
    """Verify credentials and start a session."""
    form = await _read_login_form(request)
    username = form.get("username", "")
    password = form.get("password", "")
    next = form.get("next", "/")

    service = get_service(request)
    if service.login_retry_after(username):
        return RedirectResponse(
            url="/login?error=Too+many+failed+attempts.+Try+again+in+a+few+minutes.",
            status_code=303,
        )
    user = service.authenticate(username, password)
    session_name = None if user is None else user.username

    if user is None:
        # Fall back to the env-var admin so an install that enabled auth before
        # user accounts existed can still sign in through the form.
        settings = request.app.state.settings
        if (
            settings.auth_username
            and settings.auth_password
            and constant_time_equals(username, settings.auth_username)
            and constant_time_equals(password, settings.auth_password)
        ):
            session_name = settings.auth_username
    if session_name is None:
        return RedirectResponse(url="/login?error=Invalid+username+or+password", status_code=303)

    response = RedirectResponse(url=_safe_next(next), status_code=303)
    _set_session_cookie(request, response, session_name)
    return response


@router.get("/logout")
def logout(request: Request):
    """Clear the session cookie and return to the sign-in form."""
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(SESSION_COOKIE, path="/")
    return response


@router.get("/api/me", response_model=dict[str, object])
def whoami(request: Request) -> dict[str, object]:
    """Return the current account and what it may do."""
    service = get_service(request)
    user = resolve_user(request)
    if user is None:
        return {"authenticated": False, "auth_required": service.auth_required}
    return {"authenticated": True, "auth_required": service.auth_required, **_user_payload(user)}


# --- User administration -----------------------------------------------------


class CreateUserRequest(BaseModel):
    """Payload for creating an account."""

    username: str
    password: str = Field(min_length=8)
    role: str = "user"
    permissions: dict[str, list[str]] = Field(default_factory=dict)


class UpdateUserRequest(BaseModel):
    """Payload for changing an account."""

    password: str | None = Field(default=None, min_length=8)
    role: str | None = None
    is_active: bool | None = None


class PermissionRequest(BaseModel):
    """Payload for granting pipeline actions."""

    pipeline_id: str
    actions: list[str] = Field(default_factory=list)


@router.get("/api/users", response_model=list[dict[str, object]])
def list_users(request: Request) -> list[dict[str, object]]:
    """List every account."""
    _require_admin(request)
    return [_user_payload(item) for item in get_service(request).list_users()]


@router.post("/api/users", response_model=dict[str, object])
def create_user(request: Request, payload: CreateUserRequest, response: Response) -> dict[str, object]:
    """Create one account.

    Creating the *first* account switches authentication on, which would
    otherwise lock out the very session that just created it: the page has no
    session cookie, so every following request returns 401. The usual symptom is
    an admin reporting that accounts they created cannot sign in — because the
    creation silently failed with 401 and the account never existed.

    So when this call is what enabled authentication, the caller is signed in as
    the account they just made. That grants nothing new: authentication was off
    at the moment of the request, so this caller could already do everything.
    """
    service = get_service(request)
    _require_admin(request)
    was_open = not service.auth_required
    try:
        user = service.create_user(
            payload.username,
            payload.password,
            role=payload.role,
            permissions=dict(payload.permissions),
        )
    except AuthError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if was_open and service.auth_required and user.is_admin:
        _set_session_cookie(request, response, user.username)
    return _user_payload(user)


@router.patch("/api/users/{username}", response_model=dict[str, object])
def update_user(request: Request, username: str, payload: UpdateUserRequest) -> dict[str, object]:
    """Change one account's password, role, or active flag."""
    _require_admin(request)
    try:
        user = get_service(request).update_user(
            username,
            password=payload.password,
            role=payload.role,
            is_active=payload.is_active,
        )
    except AuthError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _user_payload(user)


@router.delete("/api/users/{username}")
def delete_user(request: Request, username: str) -> dict[str, str]:
    """Delete one account."""
    _require_admin(request)
    try:
        get_service(request).delete_user(username)
    except AuthError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "deleted", "username": username}


@router.post("/api/users/{username}/permissions", response_model=dict[str, object])
def set_permission(request: Request, username: str, payload: PermissionRequest) -> dict[str, object]:
    """Grant or clear pipeline actions for a user."""
    _require_admin(request)
    try:
        user = get_service(request).grant_permission(username, payload.pipeline_id, payload.actions)
    except AuthError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _user_payload(user)


@router.get("/api/permissions", response_model=dict[str, object])
def describe_permissions(request: Request) -> dict[str, object]:
    """Describe the permission vocabulary, for building UIs."""
    require_permission(request, "view")
    return {
        "actions": list(PERMISSIONS),
        "wildcard_pipeline": "*",
        "notes": {
            "view": "See the pipeline, its runs, and its logs.",
            "edit": "Change or delete the pipeline. Implies view.",
            "run": "Trigger, retry, and cancel runs. Implies view.",
        },
    }
