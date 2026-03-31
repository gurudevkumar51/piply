"""Authentication helpers for the Piply UI and API."""

from __future__ import annotations

import base64
import secrets

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response

from piply.settings import PiplySettings


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


def _valid_basic_auth(header_value: str, settings: PiplySettings) -> bool:
    """Return whether the request includes valid UI basic auth credentials."""
    credentials = _decode_basic_credentials(header_value)
    if credentials is None:
        return False
    username, password = credentials
    return bool(
        settings.auth_username is not None
        and settings.auth_password is not None
        and secrets.compare_digest(username, settings.auth_username)
        and secrets.compare_digest(password, settings.auth_password)
    )


def _valid_bearer_token(header_value: str, settings: PiplySettings) -> bool:
    """Return whether the request includes the configured API bearer token."""
    if settings.api_token is None or not header_value.startswith("Bearer "):
        return False
    token = header_value[7:].strip()
    return secrets.compare_digest(token, settings.api_token)


def _challenge_response(path: str) -> Response:
    """Return the correct authentication challenge for UI or API routes."""
    if path.startswith("/api"):
        return JSONResponse(
            status_code=401,
            content={"detail": "Authentication required."},
            headers={"WWW-Authenticate": 'Basic realm="Piply", Bearer'},
        )
    return Response(
        status_code=401,
        headers={"WWW-Authenticate": 'Basic realm="Piply"'},
    )


class AuthMiddleware(BaseHTTPMiddleware):
    """Protect the Piply UI and API using lightweight Basic and Bearer auth."""

    async def dispatch(self, request: Request, call_next):
        settings: PiplySettings = request.app.state.settings
        if not settings.auth_enabled or request.url.path.startswith("/static"):
            return await call_next(request)

        authorization = request.headers.get("Authorization", "")
        is_ui_path = not request.url.path.startswith("/api")
        basic_valid = _valid_basic_auth(authorization, settings)
        bearer_valid = _valid_bearer_token(authorization, settings)

        if basic_valid or (not is_ui_path and bearer_valid):
            return await call_next(request)

        return _challenge_response(request.url.path)
