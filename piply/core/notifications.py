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

import logging
import os
import ssl
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

if TYPE_CHECKING:  # pragma: no cover - import cost is the point
    import httpx

#: Teams rejects a card larger than this, and a truncated alert is far more
#: useful than a delivery failure nobody sees.
_MAX_FIELD_CHARS = 800

#: Deliberately short. A notification is not worth holding a run's completion
#: path open for, and Teams either accepts a card quickly or not at all.
DEFAULT_TIMEOUT_SECONDS = 10.0

#: Adaptive Card colour names, which are keywords rather than hex values.
_ADAPTIVE_COLOURS = {
    "success": "Good",
    "failed": "Attention",
    "timed_out": "Warning",
    "cancelled": "Default",
}

#: How the card is shaped on the wire.
#:
#: `adaptive` is the Power Automate "Workflows" format, which is what Microsoft
#: directs you to now that Office 365 connectors are retired. `messagecard` is
#: the older connector format. They are not interchangeable: a Workflows
#: endpoint rejects a MessageCard.
VALID_CARD_FORMATS = ("adaptive", "messagecard")

#: Hosts that serve the legacy connector endpoints.
_MESSAGECARD_HOSTS = ("webhook.office.com", "outlook.office.com", "outlook.office365.com")

#: Teams' own accent colours, so failures actually look like failures.
_STATUS_COLOURS = {
    "success": "2EB886",
    "failed": "D93025",
    "timed_out": "E8A317",
    "cancelled": "8A8F98",
}

VALID_TEAMS_TYPES = ("channel", "chat")

_LOGGER = logging.getLogger("piply.notifications")


#: Loopback only. A Teams webhook is always https in production — it carries the
#: credential in the URL — but refusing plain http outright makes the feature
#: impossible to exercise against a local stub.
_LOCAL_PREFIXES = ("http://127.0.0.1", "http://localhost", "http://[::1]")


def detect_card_format(webhook: str) -> str:
    """Guess the payload shape a webhook expects, from its host.

    Legacy connector URLs live on `webhook.office.com`; everything else is
    assumed to be a Workflows endpoint, because that is what Microsoft issues
    now. An explicit `format:` always wins over this guess.
    """
    host = urlparse(webhook).hostname or ""
    return "messagecard" if any(host.endswith(name) for name in _MESSAGECARD_HOSTS) else "adaptive"


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
    card_format: str = "adaptive"

    @property
    def configured(self) -> bool:
        """Whether the webhook resolved to something usable."""
        return is_valid_webhook(self.webhook)


@dataclass(slots=True)
class NotificationSettings:
    """Every declared destination, plus named groups of them."""

    destinations: dict[str, TeamsDestination] = field(default_factory=dict)
    groups: dict[str, tuple[str, ...]] = field(default_factory=dict)
    #: Applied to every pipeline that does not state its own. Resolved into each
    #: pipeline at load time rather than consulted at send time, so the run
    #: record, the UI's "used by" panel, and `piply validate` all agree on who
    #: gets told without any of them knowing defaults exist.
    default_on_failure: tuple[str, ...] = ()
    default_on_success: tuple[str, ...] = ()

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
        if key not in ("teams", "groups", "defaults"):
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
                    f"{label}: webhook '{webhook}' did not resolve to a value, so this destination will be skipped."
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

        card_format = raw_destination.get("format")
        if card_format in (None, ""):
            card_format = detect_card_format(webhook)
        card_format = str(card_format).strip().lower()
        if card_format not in VALID_CARD_FORMATS:
            raise NotificationError(f"{label}.format must be one of: {', '.join(VALID_CARD_FORMATS)}")

        settings.destinations[str(name)] = TeamsDestination(
            name=str(name),
            destination_type=destination_type,
            webhook=webhook,
            timeout_seconds=timeout_seconds,
            card_format=card_format,
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

    raw_defaults = raw_value.get("defaults") or {}
    if not isinstance(raw_defaults, dict):
        raise NotificationError("'notifications.defaults' must be a mapping of 'on_failure' and/or 'on_success'")
    for key in raw_defaults:
        if key not in ("on_failure", "on_success"):
            raise NotificationError(f"notifications.defaults supports only 'on_failure' and 'on_success', not '{key}'")
    settings.default_on_failure = _destination_names(
        raw_defaults.get("on_failure"), "notifications.defaults.on_failure"
    )
    settings.default_on_success = _destination_names(
        raw_defaults.get("on_success"), "notifications.defaults.on_success"
    )
    # A typo in a default would otherwise surface on every pipeline at once, at
    # send time, long after the edit that caused it.
    settings.resolve([*settings.default_on_failure, *settings.default_on_success])

    return settings, warnings


def _destination_names(value: Any, where: str) -> tuple[str, ...]:
    """Read a list of destination or group names, deduplicated, order kept."""
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


def parse_pipeline_notifications(
    raw_value: Any,
    pipeline_id: str,
    defaults: tuple[tuple[str, ...], tuple[str, ...]] = ((), ()),
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Parse a pipeline's `notifications:` block into failure and success names.

    A bare list means "on failure", matching `notify:`, because that is what
    people overwhelmingly want to be told about.

    `defaults` are the project's `notifications.defaults`, applied per outcome:
    stating `on_failure:` replaces the default for failures but leaves successes
    inherited, so silencing one outcome never silently silences the other. The
    override is a replacement rather than a merge — a merge would mean a
    pipeline could never *narrow* who gets told, and "why is this channel still
    being paged" is the harder question to answer.

    `notifications: false` opts out of the defaults entirely, which is the only
    way to say "this pipeline alerts nobody" once a default exists.
    """
    if raw_value is False:
        return (), ()
    if raw_value in (None, ""):
        return defaults

    label = f"Pipeline '{pipeline_id}' notifications"
    default_failure, default_success = defaults

    if isinstance(raw_value, list | str):
        # A bare list has always meant on_failure; successes still inherit.
        return _destination_names(raw_value, label), default_success
    if not isinstance(raw_value, dict):
        raise NotificationError(f"{label} must be a list of destinations or a mapping")

    for key in raw_value:
        if key not in ("on_failure", "on_success"):
            raise NotificationError(f"{label} supports only 'on_failure' and 'on_success', not '{key}'")

    return (
        _destination_names(raw_value["on_failure"], f"{label}.on_failure")
        if "on_failure" in raw_value
        else default_failure,
        _destination_names(raw_value["on_success"], f"{label}.on_success")
        if "on_success" in raw_value
        else default_success,
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
    card_format: str = "adaptive",
) -> dict[str, Any]:
    """Build one standardised Teams card in the requested wire format.

    `adaptive` is an Adaptive Card wrapped in the `{"type": "message",
    "attachments": [...]}` envelope that Power Automate Workflows expects — the
    replacement for the retired Office 365 connectors. `messagecard` is the old
    connector shape. A Workflows endpoint rejects a MessageCard, so the two are
    not interchangeable and the format is chosen per destination.
    """
    fields = [
        ("Pipeline", f"{title} ({pipeline_id})"),
        ("Status", status),
        ("Run", run_id),
        ("Trigger", trigger),
        ("Tasks", tasks),
        ("Duration", duration),
    ]
    if error:
        fields.append(("Error", _truncate(error)))

    if card_format == "adaptive":
        body: list[dict[str, Any]] = [
            {
                "type": "TextBlock",
                "text": f"{title} — {status}",
                "weight": "Bolder",
                "size": "Medium",
                "color": _ADAPTIVE_COLOURS.get(status, "Default"),
                "wrap": True,
            },
            {"type": "FactSet", "facts": [{"title": name, "value": value} for name, value in fields]},
        ]
        content: dict[str, Any] = {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": body,
        }
        if run_url:
            content["actions"] = [{"type": "Action.OpenUrl", "title": "Open run in Piply", "url": run_url}]
        return {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "contentUrl": None,
                    "content": content,
                }
            ],
        }

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


#: Environment variables that redirect TLS verification at a file or directory.
#: A stale one — a removed conda environment, a service account that cannot see
#: the path — is the usual cause of a FileNotFoundError from an HTTPS post.
_CA_BUNDLE_VARS = ("SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE")


#: Warned about once per process rather than once per alert.
_ca_fallback_warned = False


def _broken_ca_variables() -> list[str]:
    """Return CA-bundle variables whose path does not exist."""
    return [
        f"{name}={value}" for name in _CA_BUNDLE_VARS if (value := os.environ.get(name)) and not os.path.exists(value)
    ]


def _verify_option() -> Any:
    """Return what httpx should verify against.

    `SSL_CERT_FILE` and friends are read by Python's TLS stack but ignored by
    `curl`, which is why a webhook can work from a terminal and fail here. When
    one of them points at a file that no longer exists — a conda environment
    that was rebuilt, most often — every HTTPS post raises `FileNotFoundError`
    before it reaches the network.

    A path that does not exist cannot be a deliberate choice, so it is dropped
    in favour of the platform's own trust store. Verification still happens;
    only the missing override is ignored, and it is reported once so the
    environment still gets fixed.

    Built from the standard library rather than `certifi`: certifi arrives only
    as a dependency of httpx, and importing something Piply does not declare is
    exactly the packaging bug its own tests guard against.
    """
    global _ca_fallback_warned
    broken = _broken_ca_variables()
    if not broken:
        return True

    if not _ca_fallback_warned:
        _ca_fallback_warned = True
        _LOGGER.warning(
            "Ignoring a certificate-authority path that does not exist (%s) and verifying "
            "against the system trust store instead. Fix or unset it to silence this.",
            "; ".join(broken),
        )
    try:
        # `create_default_context` tolerates the bogus path — the hard failure
        # came from httpx calling `load_verify_locations` on it explicitly.
        return ssl.create_default_context()
    except Exception:  # noqa: BLE001 - fall back to httpx's own default
        return True


def _transport_hint() -> str:
    """Return a pointer at the likely cause, when the environment suggests one."""
    broken = [
        f"{name}={value}" for name in _CA_BUNDLE_VARS if (value := os.environ.get(name)) and not os.path.exists(value)
    ]
    if broken:
        return (
            "A certificate-authority path in this process's environment does not exist: "
            + "; ".join(broken)
            + ". Fix or unset it, then retry."
        )
    if any(os.environ.get(name) for name in ("HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY")):
        return "A proxy is configured in this process's environment; check it can reach the webhook host."
    return "Check the host is reachable from the server Piply runs on."


def _rejection_hint(status_code: int, webhook: str) -> str:
    """Turn an auth rejection from the webhook host into something actionable.

    A Workflows URL carries its credential in the `sig` query parameter, so a
    401 is almost never about the card: the signature is absent, truncated, or
    no longer current. Re-saving a flow in Power Automate issues a fresh URL and
    silently invalidates the old one, which looks exactly like this — and so
    does a very long URL that got line-wrapped on its way into `.env`.

    The signature's *length* is reported because that is what distinguishes a
    truncated URL from a stale one. A length is not a credential; the URL itself
    is still never logged.
    """
    if status_code not in (401, 403):
        return ""
    signature = (parse_qs(urlparse(webhook).query).get("sig") or [""])[0]
    if not signature:
        return (
            " This URL has no 'sig' parameter, so it is incomplete — copy the whole URL from "
            "the flow's trigger, query string included."
        )
    return (
        f" The URL's signature ({len(signature)} characters) was rejected. Copy the current URL "
        "from the flow's trigger — re-saving a flow issues a new one — and check it did not get "
        "truncated or line-wrapped in .env."
    )


async def _post_one(
    client: httpx.AsyncClient, destination: TeamsDestination, payload: dict[str, Any]
) -> tuple[str, bool, str]:
    """Post one card, converting every failure into a reportable result."""
    import httpx

    try:
        response = await client.post(
            destination.webhook,
            json=payload,
            timeout=destination.timeout_seconds,
        )
    except httpx.TimeoutException:
        return destination.name, False, f"timed out after {destination.timeout_seconds:g}s"
    except httpx.HTTPError as exc:
        # str(exc) can contain the URL, and the URL is the credential, so only
        # the class name is reported. It is enough to tell DNS from TLS.
        return destination.name, False, f"request failed ({type(exc).__name__}): {_transport_hint()}"
    except OSError as exc:
        # Not an httpx error, so it used to escape to the generic handler and
        # arrive as a bare "[Errno 2] No such file or directory" with no clue
        # what was missing. Overwhelmingly this is a CA bundle that is not there.
        return destination.name, False, f"{type(exc).__name__}: {exc}. {_transport_hint()}"

    if response.status_code >= 400:
        detail = response.text.strip()[:200] or "no response body"
        hint = _rejection_hint(response.status_code, destination.webhook)
        return destination.name, False, f"HTTP {response.status_code}: {detail}{hint}"
    return destination.name, True, ""


async def _post_all(
    destinations: list[TeamsDestination],
    build: Callable[[TeamsDestination], dict[str, Any]],
) -> list[tuple[str, bool, str]]:
    """Post to every destination concurrently, each in its own format.

    `httpx` is imported here rather than at module scope. It costs ~130ms and is
    only needed when a card is actually sent, so `piply validate` and every
    other command stopped paying for it.
    """
    import asyncio

    import httpx

    async with httpx.AsyncClient(verify=_verify_option()) as client:
        return list(await asyncio.gather(*(_post_one(client, item, build(item)) for item in destinations)))


def send_alert(
    destinations: list[TeamsDestination],
    payload: dict[str, Any] | Callable[[TeamsDestination], dict[str, Any]],
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

    # Destinations can want different wire formats, so the payload is built per
    # destination rather than shared.
    build = payload if callable(payload) else (lambda _destination: payload)
    try:
        results = _run_async(_post_all(usable, build))
    except Exception as exc:  # noqa: BLE001 - delivery must never fail a run
        # Naming the type matters: a bare `str(exc)` on an OSError is just
        # "[Errno 2] No such file or directory", which says nothing at all.
        detail = f"{type(exc).__name__}: {exc}. {_transport_hint()}"
        if on_log:
            on_log(f"Teams notification failed: {detail}", True)
        return [(item.name, False, detail) for item in usable]

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

    `asyncio` is imported here rather than at module scope: it costs ~90ms and
    nothing outside actually sending a card needs it.
    """
    import asyncio

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
