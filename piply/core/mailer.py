"""Centralised SMTP configuration and delivery.

One SMTP server is configured once, in Settings, and reused by every email task
and every pipeline notification. A task may still override any field inline, so
existing per-task configuration keeps working unchanged.

The password is never returned by the API or rendered in the UI. Prefer setting
``PIPLY_SMTP_PASSWORD`` in the environment over storing it in the database; the
stored value is only a fallback for installs without a secret store.
"""

from __future__ import annotations

import os
import smtplib
from dataclasses import dataclass, replace
from email.message import EmailMessage

#: meta keys the settings are stored under.
_META_PREFIX = "smtp_"
_FIELDS = ("host", "port", "username", "password", "from_address", "use_tls", "use_ssl", "timeout_seconds")


@dataclass(slots=True, frozen=True)
class SmtpSettings:
    """Resolved SMTP configuration."""

    host: str = ""
    port: int = 587
    username: str = ""
    password: str = ""
    from_address: str = ""
    use_tls: bool = True
    use_ssl: bool = False
    timeout_seconds: int = 30

    @property
    def configured(self) -> bool:
        """Return whether enough is set to attempt a send."""
        return bool(self.host)

    @property
    def sender(self) -> str:
        """Return the From address, falling back to the login user."""
        return self.from_address or self.username or "piply@localhost"

    def public_dict(self) -> dict[str, object]:
        """Return a payload safe to send to the UI and API.

        The password is reported only as a boolean; its value never leaves the
        process.
        """
        return {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "from_address": self.from_address,
            "use_tls": self.use_tls,
            "use_ssl": self.use_ssl,
            "timeout_seconds": self.timeout_seconds,
            "password_set": bool(self.password),
            "configured": self.configured,
        }


def _as_bool(value: str | None, default: bool) -> bool:
    """Parse a stored boolean flag."""
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _as_int(value: str | None, default: int) -> int:
    """Parse a stored integer with a safe fallback."""
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def load_smtp_settings(store) -> SmtpSettings:
    """Load central SMTP settings, letting the environment win.

    Environment variables take precedence so a deployment can supply the
    password without it ever being written to the database.
    """
    stored = {field: store.get_meta(f"{_META_PREFIX}{field}") for field in _FIELDS}

    def pick(field: str) -> str | None:
        return os.environ.get(f"PIPLY_SMTP_{field.upper()}") or stored.get(field)

    return SmtpSettings(
        host=(pick("host") or "").strip(),
        port=_as_int(pick("port"), 587),
        username=(pick("username") or "").strip(),
        password=pick("password") or "",
        from_address=(pick("from_address") or "").strip(),
        use_tls=_as_bool(pick("use_tls"), True),
        use_ssl=_as_bool(pick("use_ssl"), False),
        timeout_seconds=_as_int(pick("timeout_seconds"), 30),
    )


def save_smtp_settings(store, values: dict[str, object], *, keep_password: bool = True) -> SmtpSettings:
    """Persist central SMTP settings.

    An omitted or blank password keeps the stored one, so an admin can edit the
    host without having to retype the secret into a form that never shows it.
    """
    payload: dict[str, str] = {}

    for field in _FIELDS:
        if field == "password":
            continue
        if field in values and values[field] is not None:
            value = values[field]
            payload[f"{_META_PREFIX}{field}"] = (
                ("true" if value else "false") if isinstance(value, bool) else str(value).strip()
            )

    password = values.get("password")
    if password:
        payload[f"{_META_PREFIX}password"] = str(password)
    elif not keep_password:
        payload[f"{_META_PREFIX}password"] = ""

    if payload:
        store.set_meta_many(payload)
    return load_smtp_settings(store)


def resolve_for_task(settings: SmtpSettings, task) -> SmtpSettings:
    """Overlay a task's inline SMTP fields on the central configuration.

    Inline values win, so a pipeline that already carried its own SMTP block
    behaves exactly as it did before central settings existed.
    """
    overrides: dict[str, object] = {}
    if task.smtp_host:
        overrides["host"] = task.smtp_host
        # A task that names its own host also owns the port, otherwise it would
        # inherit a port belonging to a different server.
        overrides["port"] = task.smtp_port or 587
    if task.smtp_user:
        overrides["username"] = task.smtp_user
    if task.smtp_password:
        overrides["password"] = task.smtp_password
    return replace(settings, **overrides) if overrides else settings


def build_message(settings: SmtpSettings, *, to: list[str], subject: str, body: str) -> EmailMessage:
    """Build one plain-text message."""
    message = EmailMessage()
    message.set_content(body or "")
    message["Subject"] = subject or "Piply Notification"
    message["From"] = settings.sender
    message["To"] = ", ".join(to)
    return message


def send_message(settings: SmtpSettings, message: EmailMessage) -> None:
    """Deliver one message, raising on failure."""
    if not settings.configured:
        raise RuntimeError("No SMTP server is configured. Set one under Settings, or give the task its own smtp_host.")

    if settings.use_ssl:
        with smtplib.SMTP_SSL(settings.host, settings.port, timeout=settings.timeout_seconds) as server:
            if settings.username and settings.password:
                server.login(settings.username, settings.password)
            server.send_message(message)
        return

    with smtplib.SMTP(settings.host, settings.port, timeout=settings.timeout_seconds) as server:
        if settings.use_tls:
            server.starttls()
        if settings.username and settings.password:
            server.login(settings.username, settings.password)
        server.send_message(message)
