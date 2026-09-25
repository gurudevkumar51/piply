"""Users, passwords, and pipeline-level permissions.

Deliberately small and dependency-free: passwords are hashed with PBKDF2 from
the standard library, and sessions are signed cookies rather than server-side
state. That keeps the single-process, single-file philosophy intact.

Two roles exist:

* ``admin`` — every pipeline, every action, plus user management.
* ``user``  — only what an explicit grant allows.

Permissions are granted per pipeline, or against ``*`` for every pipeline.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import threading
import time
from dataclasses import dataclass, field

#: Actions that can be granted on a pipeline. `edit` implies being able to
#: change or delete it; `run` implies triggering, retrying, and cancelling.
PERMISSIONS = ("view", "edit", "run")
#: A grant against this pipeline id applies to every pipeline.
ALL_PIPELINES = "*"

ROLES = ("admin", "user")

_PBKDF2_ROUNDS = 240_000
_SESSION_TTL_SECONDS = 12 * 60 * 60


class AuthError(ValueError):
    """Raised when a user or permission operation is invalid."""


@dataclass(slots=True, frozen=True)
class User:
    """One account."""

    username: str
    role: str = "user"
    is_active: bool = True
    created_at: str | None = None
    last_login_at: str | None = None
    #: pipeline id (or "*") -> granted actions
    permissions: dict[str, frozenset[str]] = field(default_factory=dict)

    @property
    def is_admin(self) -> bool:
        """Return whether this account bypasses per-pipeline grants."""
        return self.role == "admin"

    def can(self, action: str, pipeline_id: str | None = None) -> bool:
        """Return whether this user may perform an action.

        Admins may do anything. For everyone else a grant on the specific
        pipeline or on ``*`` is required. ``edit`` and ``run`` both imply
        ``view``, because being able to act on something you cannot see would
        be useless.
        """
        if not self.is_active:
            return False
        if self.is_admin:
            return True
        if action not in PERMISSIONS:
            return False

        granted: set[str] = set(self.permissions.get(ALL_PIPELINES, frozenset()))
        if pipeline_id is not None:
            granted |= set(self.permissions.get(pipeline_id, frozenset()))
        elif not granted:
            # No pipeline named and no global grant: fall back to "any pipeline",
            # which is what list views need in order to show a filtered page.
            granted = {item for actions in self.permissions.values() for item in actions}

        if action == "view":
            return bool(granted)
        return action in granted

    def visible_pipeline_ids(self, all_pipeline_ids: list[str]) -> list[str]:
        """Return the pipelines this user may see, in the given order."""
        if self.is_admin:
            return list(all_pipeline_ids)
        if self.permissions.get(ALL_PIPELINES):
            return list(all_pipeline_ids)
        allowed = {pipeline_id for pipeline_id, actions in self.permissions.items() if actions}
        return [pipeline_id for pipeline_id in all_pipeline_ids if pipeline_id in allowed]


def hash_password(password: str, *, salt: str | None = None) -> str:
    """Return a ``pbkdf2$rounds$salt$hash`` string for storage."""
    if not password:
        raise AuthError("Password cannot be empty.")
    salt_value = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt_value.encode("utf-8"), _PBKDF2_ROUNDS)
    return f"pbkdf2${_PBKDF2_ROUNDS}${salt_value}${base64.b64encode(digest).decode('ascii')}"


def verify_password(password: str, stored: str | None) -> bool:
    """Check a password against a stored hash in constant time."""
    if not stored or not password:
        return False
    try:
        algorithm, rounds_text, salt, encoded = stored.split("$", 3)
    except ValueError:
        return False
    if algorithm != "pbkdf2":
        return False
    try:
        rounds = int(rounds_text)
    except ValueError:
        return False
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), rounds)
    return hmac.compare_digest(base64.b64encode(digest).decode("ascii"), encoded)


def generate_password(length: int = 18) -> str:
    """Return a readable random password for the bootstrap admin account."""
    # url-safe avoids characters that get mangled when copied out of a terminal.
    return secrets.token_urlsafe(length)[:length]


def normalize_username(value: str) -> str:
    """Return a validated, lower-cased username."""
    username = (value or "").strip().lower()
    if not username:
        raise AuthError("Username cannot be empty.")
    if not all(character.isalnum() or character in "._-@" for character in username):
        raise AuthError("Usernames may contain letters, numbers, and . _ - @ only.")
    if len(username) > 64:
        raise AuthError("Usernames must be 64 characters or fewer.")
    return username


def normalize_permissions(values: object) -> frozenset[str]:
    """Return a validated permission set from a list or comma-separated string."""
    if values in (None, "", False):
        return frozenset()
    if isinstance(values, str):
        items = [item.strip() for item in values.split(",")]
    else:
        items = [str(item).strip() for item in values]  # type: ignore[union-attr]

    cleaned = {item.lower() for item in items if item}
    if "all" in cleaned:
        cleaned = set(PERMISSIONS)
    unknown = cleaned - set(PERMISSIONS)
    if unknown:
        raise AuthError(f"Unknown permission(s): {', '.join(sorted(unknown))}. Valid: {', '.join(PERMISSIONS)}.")
    # Acting on a pipeline implies being able to see it.
    if cleaned & {"edit", "run"}:
        cleaned.add("view")
    return frozenset(cleaned)


# --- Session cookies ---------------------------------------------------------


#: Attribute the resolved secret is memoised under, on the store itself.
# Every request validates a session, so re-reading from the database each time
# would add a query per request for a value that cannot change while the
# process runs. Caching on the instance (rather than in a module dict keyed by
# id) means the entry cannot outlive the store it belongs to.
_SECRET_ATTRIBUTE = "_piply_session_secret"
_SECRET_LOCK = threading.Lock()


def _session_secret(store) -> bytes:
    """Return the signing secret, creating one on first use.

    A per-install random secret means sessions survive restarts but cannot be
    forged from a value that ships with the source.
    """
    configured = os.environ.get("PIPLY_SESSION_SECRET")
    if configured:
        return configured.encode("utf-8")

    cached = getattr(store, _SECRET_ATTRIBUTE, None)
    if cached is not None:
        return cached
    with _SECRET_LOCK:
        stored = store.get_meta("session_secret")
        if not stored:
            stored = secrets.token_hex(32)
            store.set_meta("session_secret", stored)
        secret = stored.encode("utf-8")
        setattr(store, _SECRET_ATTRIBUTE, secret)
        return secret


def issue_session(store, username: str, *, ttl_seconds: int = _SESSION_TTL_SECONDS) -> str:
    """Return a signed session token for a username."""
    payload = {"u": username, "exp": int(time.time()) + ttl_seconds}
    raw = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8")).decode("ascii")
    signature = hmac.new(_session_secret(store), raw.encode("ascii"), hashlib.sha256).hexdigest()
    return f"{raw}.{signature}"


def read_session(store, token: str | None) -> str | None:
    """Return the username in a valid, unexpired session token, else None.

    The token comes straight from a cookie, so every step has to treat it as
    hostile: a malformed, non-ASCII, or truncated value must be rejected rather
    than raise.
    """
    if not token or "." not in token:
        return None
    raw, _, signature = token.rpartition(".")
    try:
        raw_bytes = raw.encode("ascii")
    except UnicodeEncodeError:
        return None

    expected = hmac.new(_session_secret(store), raw_bytes, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        payload = json.loads(base64.urlsafe_b64decode(raw_bytes))
        if not isinstance(payload, dict):
            return None
        if int(payload.get("exp", 0)) < int(time.time()):
            return None
    except (ValueError, TypeError):
        return None
    username = payload.get("u")
    return str(username) if username else None


class LoginThrottle:
    """In-memory lockout for repeated failed sign-ins.

    Password verification is deliberately expensive, which makes unthrottled
    login attempts both a guessing risk and a way to burn server CPU. Keeping
    the counters in memory matches the single-process design; a restart simply
    clears them, which is acceptable for a lockout measured in minutes.
    """

    def __init__(self, *, max_attempts: int = 8, window_seconds: int = 300, lockout_seconds: int = 300) -> None:
        self.max_attempts = max_attempts
        self.window_seconds = window_seconds
        self.lockout_seconds = lockout_seconds
        self._failures: dict[str, list[float]] = {}
        self._locked_until: dict[str, float] = {}
        self._lock = threading.Lock()

    def retry_after(self, key: str) -> int:
        """Return remaining lockout seconds for a key, or 0 when allowed."""
        with self._lock:
            until = self._locked_until.get(key, 0.0)
            remaining = until - time.time()
            if remaining <= 0:
                self._locked_until.pop(key, None)
                return 0
            return int(remaining) + 1

    def record_failure(self, key: str) -> None:
        """Count one failed attempt and lock the key out once over the limit."""
        now = time.time()
        with self._lock:
            recent = [item for item in self._failures.get(key, []) if now - item < self.window_seconds]
            recent.append(now)
            self._failures[key] = recent
            if len(recent) >= self.max_attempts:
                self._locked_until[key] = now + self.lockout_seconds
                self._failures[key] = []

    def record_success(self, key: str) -> None:
        """Clear the history for a key after a successful sign-in."""
        with self._lock:
            self._failures.pop(key, None)
            self._locked_until.pop(key, None)


def constant_time_equals(left: str | None, right: str | None) -> bool:
    """Compare two strings in constant time, tolerating any Unicode input.

    ``hmac.compare_digest`` rejects non-ASCII ``str`` with a ``TypeError``, so
    comparing a user-supplied username directly would turn a failed login into
    a 500. Encoding to UTF-8 first keeps the comparison total and still
    constant time.
    """
    if left is None or right is None:
        return False
    return hmac.compare_digest(left.encode("utf-8"), right.encode("utf-8"))
