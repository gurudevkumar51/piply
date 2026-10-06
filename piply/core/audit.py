"""Audit actor attribution shared by requests, CLI commands, and services."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone

from .scheduling import ensure_aware_utc, parse_interval

AUDIT_ACTOR: ContextVar[str | None] = ContextVar("piply_audit_actor", default=None)


@contextmanager
def audit_actor(actor: str):
    """Attribute actions in this context without changing a shared service."""
    token = AUDIT_ACTOR.set(actor)
    try:
        yield
    finally:
        AUDIT_ACTOR.reset(token)


def current_actor(explicit: str | None = None) -> str:
    return explicit or AUDIT_ACTOR.get() or "system"


def trigger_actor(trigger: str) -> str:
    return {
        "schedule": "system:scheduler",
        "sensor": "system:sensor",
        "pipeline": "system:pipeline",
        "retry": "system:retry",
    }.get(trigger, "system")


def parse_audit_since(value: str | None, *, now: datetime | None = None) -> datetime | None:
    """Accept a positive relative interval or an ISO timestamp (UTC if naive)."""
    if not value:
        return None
    text = value.strip()
    try:
        return ensure_aware_utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        try:
            seconds = parse_interval(text)
            return ensure_aware_utc(now or datetime.now(timezone.utc)) - timedelta(seconds=seconds)
        except (ValueError, OverflowError) as exc:
            raise ValueError("since must be an ISO timestamp or a positive interval such as 7d or 12h.") from exc
