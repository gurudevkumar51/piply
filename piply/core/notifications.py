"""Reusable outbound notifications, currently Microsoft Teams.

Kept deliberately separate from pipeline execution. A run finishing is one
event; who hears about it is a routing question, and mixing the two is how
notification code ends up duplicated across every execution path and impossible
to test without running a pipeline.

Webhook URLs are secrets. They are never written into YAML literally — a
destination declares `webhook: ${TEAMS_PROD_WEBHOOK}` and the value is resolved
from the environment or a secrets file at load time — and they are never written
to a log, because a Teams webhook URL is itself the credential: anyone holding
it can post to the channel.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

import httpx

#: Teams rejects a card larger than this, and a truncated alert is far more
#: useful than a delivery failure nobody sees.
_MAX_FIELD_CHARS = 800

#: Deliberately short. A notification is not worth holding a run's completion
#: path open for, and Teams either accepts a card quickly or not at all.
DEFAULT_TIMEOUT_SECONDS = 10.0

#: Teams' own accent colours, so failures actually look like failures.
_STATUS_COLOURS = {
    "success": "2EB886",
    "failed": "D93025",
    "timed_out": "E8A317",
    "cancelled": "8A8F98",
}

VALID_TEAMS_TYPES = ("channel", "chat")


#: Loopback only. A Teams webhook is always https in production — it carries the
#: credential in the URL — but refusing plain http outright makes the feature
#: impossible to exercise against a local stub.
_LOCAL_PREFIXES = ("http://127.0.0.1", "http://localhost", "http://[::1]")


def is_valid_webhook(value: str) -> bool:
    """Whether a webhook URL is acceptable to post to."""
    return value.startswith("https://") or value.startswith(_LOCAL_PREFIXES)


class NotificationError(ValueError):
    """Raised when a notification block is misconfigured."""


@dataclass(slots=True, frozen=True)
class TeamsDestination:
    """One Teams webhook, either a channel or a group chat."""

    name: str
    destination_type: str
    webhook: str
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS

    @property
    def configured(self) -> bool:
        """Whether the webhook resolved to something usable."""
        return is_valid_webhook(self.webhook)


@dataclass(slots=True)
class NotificationSettings:
    """Every declared destination, plus named groups of them."""

    destinations: dict[str, TeamsDestination] = field(default_factory=dict)
    groups: dict[str, tuple[str, ...]] = field(default_factory=dict)

    @property
    def configured(self) -> bool:
        """Whether anything at all is declared."""
        return bool(self.destinations)

    def resolve(self, names: Iterable[str]) -> list[TeamsDestination]:
        """Expand names — destinations or groups — into unique destinations.

        Order is preserved and duplicates removed, so a pipeline naming both a
        group and one of its members is notified once rather than twice.
        """
        resolved: list[TeamsDestination] = []
        seen: set[str] = set()

        def _add(name: str, trail: tuple[str, ...]) -> None:
            if name in self.destinations:
                if name not in seen:
                    seen.add(name)
                    resolved.append(self.destinations[name])
                return
            if name in self.groups:
                if name in trail:
                    raise NotificationError(
                        f"Notification group '{name}' includes itself: {' -> '.join([*trail, name])}"
                    )
                for member in self.groups[name]:
                    _add(member, (*trail, name))
                return
            known = sorted([*self.destinations, *self.groups])
            hint = f" Known destinations: {', '.join(known)}." if known else ""
            raise NotificationError(f"Unknown notification destination '{name}'.{hint}")

        for name in names:
            _add(str(name), ())
        return resolved


def parse_notifications(
    raw_value: Any, env_values: dict[str, str] | None = None
) -> tuple[NotificationSettings, list[str]]:
    """Parse the project-level `notifications:` block.

    Returns the settings plus any warnings. An unresolved webhook is a warning
    rather than an error on purpose: a developer without the production secret
    should still be able to load the project and run pipelines locally. The
    destination is then skipped at send time, with a log line saying which one.
    """
    settings = NotificationSettings()
    warnings: list[str] = []
    if raw_value in (None, "", False):
        return settings, warnings
    if not isinstance(raw_value, dict):
        raise NotificationError("'notifications' must be a mapping")

    for key in raw_value:
        if key not in ("teams", "groups"):
            raise NotificationError(f"Unsupported notification channel '{key}'. Supported: teams.")

    raw_teams = raw_value.get("teams") or {}
    if not isinstance(raw_teams, dict):
        raise NotificationError("'notifications.teams' must be a mapping of destination names")

    for name, raw_destination in raw_teams.items():
        label = f"notifications.teams.{name}"
        if not isinstance(raw_destination, dict):
            raise NotificationError(f"{label} must be a mapping")

        destination_type = str(raw_destination.get("type") or "channel").strip().lower()
        if destination_type not in VALID_TEAMS_TYPES:
            raise NotificationError(f"{label}.type must be one of: {', '.join(VALID_TEAMS_TYPES)}")

        webhook = _resolve_secret(raw_destination.get("webhook"), env_values)
        if not webhook:
            raise NotificationError(f"{label} needs a 'webhook'")
        if not is_valid_webhook(webhook):
            if _looks_unresolved(webhook):
                warnings.append(
                    f"{label}: webhook '{webhook}' did not resolve to a value, " "so this destination will be skipped."
                )
            else:
                raise NotificationError(f"{label}.webhook must be an https URL (a Teams webhook always is)")

        timeout = raw_destination.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
        try:
            timeout_seconds = float(timeout)
        except (TypeError, ValueError) as exc:
            raise NotificationError(f"{label}.timeout_seconds must be a number") from exc
        if timeout_seconds <= 0:
            raise NotificationError(f"{label}.timeout_seconds must be greater than zero")

        settings.destinations[str(name)] = TeamsDestination(
            name=str(name),
            destination_type=destination_type,
            webhook=webhook,
            timeout_seconds=timeout_seconds,
        )

    raw_groups = raw_value.get("groups") or {}
    if not isinstance(raw_groups, dict):
        raise NotificationError("'notifications.groups' must be a mapping of group names")
    for name, members in raw_groups.items():
        if isinstance(members, str):
            members = [members]
        if not isinstance(members, list):
            raise NotificationError(f"notifications.groups.{name} must be a list of destination names")
        settings.groups[str(name)] = tuple(str(item) for item in members)

    # Fail loudly here rather than at 3am when the alert does not arrive.
    for name in settings.groups:
        settings.resolve([name])

    return settings, warnings


def _looks_unresolved(value: str) -> bool:
    """Whether a webhook still contains an un-expanded placeholder."""
    return "${" in value or value.startswith("$") or ("%" in value and value.count("%") >= 2)


def _resolve_secret(value: Any, env_values: dict[str, str] | None) -> str:
    """Return the webhook as configured, without expanding it here.

    Expansion is the loader's job — it already understands `${VAR}`, `$VAR`, and
    secrets files — so this only normalises whitespace and rejects a literal URL
    being written into YAML by mistake.
    """
    if value in (None, "", False):
        return ""
    return str(value).strip()


def parse_pipeline_notifications(raw_value: Any, pipeline_id: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Parse a pipeline's `notifications:` block into failure and success names.

    A bare list means "on failure", matching `notify:`, because that is what
    people overwhelmingly want to be told about.
    """
    if raw_value in (None, "", False):
        return (), ()

    label = f"Pipeline '{pipeline_id}' notifications"

    def _names(value: Any, where: str) -> tuple[str, ...]:
        if value in (None, "", False):
            return ()
        items = value if isinstance(value, list) else [value]
        names: list[str] = []
        for item in items:
            name = str(item).strip()
            if not name:
                continue
            if name not in names:
                names.append(name)
        if not names and value:
            raise NotificationError(f"{where} lists no destination names")
        return tuple(names)

    if isinstance(raw_value, list | str):
        return _names(raw_value, label), ()
    if not isinstance(raw_value, dict):
        raise NotificationError(f"{label} must be a list of destinations or a mapping")

    for key in raw_value:
        if key not in ("on_failure", "on_success"):
            raise NotificationError(f"{label} supports only 'on_failure' and 'on_success', not '{key}'")

    return (
        _names(raw_value.get("on_failure"), f"{label}.on_failure"),
        _names(raw_value.get("on_success"), f"{label}.on_success"),
    )


def _truncate(value: str) -> str:
    """Keep a card within Teams' size limit without dropping the alert."""
    text = str(value)
    if len(text) <= _MAX_FIELD_CHARS:
        return text
    return text[: _MAX_FIELD_CHARS - 1] + "…"


def build_alert(
    *,
    title: str,
    pipeline_id: str,
    status: str,
    run_id: str,
    trigger: str,
    tasks: str,
    duration: str,
    error: str | None = None,
    run_url: str | None = None,
) -> dict[str, Any]:
    """Build one standardised Teams card.

    MessageCard rather than an Adaptive Card because it is the format both
    incoming-webhook kinds accept — channel connectors and group chats — so one
    payload works for every destination.
    """
    facts = [
        {"name": "Pipeline", "value": f"{title} ({pipeline_id})"},
        {"name": "Status", "value": status},
        {"name": "Run", "value": run_id},
        {"name": "Trigger", "value": trigger},
        {"name": "Tasks", "value": tasks},
        {"name": "Duration", "value": duration},
    ]
    if error:
        facts.append({"name": "Error", "value": _truncate(error)})

    payload: dict[str, Any] = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "summary": f"Piply: {title} {status}",
        "themeColor": _STATUS_COLOURS.get(status, "8A8F98"),
        "title": f"{title} — {status}",
        "sections": [{"facts": facts, "markdown": False}],
    }
    if run_url:
        payload["potentialAction"] = [
            {
                "@type": "OpenUri",
                "name": "Open run in Piply",
                "targets": [{"os": "default", "uri": run_url}],
            }
        ]
    return payload


async def _post_one(
    client: httpx.AsyncClient, destination: TeamsDestination, payload: dict[str, Any]
) -> tuple[str, bool, str]:
    """Post one card, converting every failure into a reportable result."""
    try:
        response = await client.post(
            destination.webhook,
            json=payload,
            timeout=destination.timeout_seconds,
        )
    except httpx.TimeoutException:
        return destination.name, False, f"timed out after {destination.timeout_seconds:g}s"
    except httpx.HTTPError as exc:
        # str(exc) can contain the URL, and the URL is the credential.
        return destination.name, False, f"request failed ({type(exc).__name__})"

    if response.status_code >= 400:
        detail = response.text.strip()[:200] or "no response body"
        return destination.name, False, f"HTTP {response.status_code}: {detail}"
    return destination.name, True, ""


async def _post_all(destinations: list[TeamsDestination], payload: dict[str, Any]) -> list[tuple[str, bool, str]]:
    """Post to every destination concurrently."""
    async with httpx.AsyncClient() as client:
        return list(await asyncio.gather(*(_post_one(client, item, payload) for item in destinations)))


def send_alert(
    destinations: list[TeamsDestination],
    payload: dict[str, Any],
    *,
    on_log: Callable[[str, bool], None] | None = None,
) -> list[tuple[str, bool, str]]:
    """Deliver one alert to every destination, never raising.

    A notification failure must never change a run's outcome — a pipeline that
    succeeded did succeed, whether or not Teams accepted the card. Every result
    is reported through `on_log` instead, which receives the message and whether
    it represents a failure.
    """
    if not destinations:
        return []

    usable = [item for item in destinations if item.configured]
    for skipped in (item for item in destinations if not item.configured):
        if on_log:
            on_log(
                f"Teams notification skipped for '{skipped.name}': its webhook is not configured.",
                True,
            )
    if not usable:
        return []

    try:
        results = _run_async(_post_all(usable, payload))
    except Exception as exc:  # noqa: BLE001 - delivery must never fail a run
        if on_log:
            on_log(f"Teams notification failed: {type(exc).__name__}: {exc}", True)
        return [(item.name, False, str(exc)) for item in usable]

    if on_log:
        delivered = [name for name, ok, _ in results if ok]
        if delivered:
            on_log(f"Teams notification sent to {', '.join(delivered)}.", False)
        for name, ok, detail in results:
            if not ok:
                on_log(f"Teams notification to '{name}' failed: {detail}", True)
    return results


def _run_async(coroutine) -> list[tuple[str, bool, str]]:
    """Run the delivery coroutine from Piply's synchronous engine.

    Runs are executed on worker threads with no event loop, so `asyncio.run` is
    the normal path. The fallback covers a caller that already has a loop
    running on this thread, where `asyncio.run` would raise.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coroutine)

    result: list[list[tuple[str, bool, str]]] = []
    import threading

    def _worker() -> None:
        result.append(asyncio.run(coroutine))

    thread = threading.Thread(target=_worker, name="piply-notify", daemon=True)
    thread.start()
    thread.join()
    return result[0] if result else []
