from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class ScheduleError(ValueError):
    """Raised when a schedule expression cannot be parsed."""


def ensure_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_timezone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ScheduleError(f"Unknown timezone '{timezone_name}'") from exc


def parse_interval(value: str) -> int:
    value = value.strip().lower()
    if not value:
        raise ScheduleError("Interval value cannot be empty")

    multiplier_map = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    unit = value[-1]
    if unit not in multiplier_map:
        raise ScheduleError(
            "Interval values must end with s, m, h, or d (for example: 30s, 15m, 2h)"
        )

    try:
        quantity = int(value[:-1])
    except ValueError as exc:
        raise ScheduleError(f"Invalid interval '{value}'") from exc

    if quantity <= 0:
        raise ScheduleError("Interval quantity must be greater than zero")

    return quantity * multiplier_map[unit]


def _expand_part(part: str, minimum: int, maximum: int) -> set[int]:
    result: set[int] = set()
    for chunk in part.split(","):
        chunk = chunk.strip()
        if not chunk:
            raise ScheduleError("Cron expression contains an empty field")

        step = 1
        base = chunk
        if "/" in chunk:
            base, raw_step = chunk.split("/", 1)
            try:
                step = int(raw_step)
            except ValueError as exc:
                raise ScheduleError(f"Invalid cron step '{raw_step}'") from exc
            if step <= 0:
                raise ScheduleError("Cron step must be greater than zero")

        if base == "*":
            start, end = minimum, maximum
        elif "-" in base:
            raw_start, raw_end = base.split("-", 1)
            start, end = int(raw_start), int(raw_end)
        else:
            start = end = int(base)

        if start < minimum or end > maximum or start > end:
            raise ScheduleError(f"Cron value '{chunk}' is out of range")

        result.update(range(start, end + 1, step))

    return result


@dataclass(frozen=True, slots=True)
class CronSchedule:
    expression: str
    timezone_name: str = "UTC"

    def __post_init__(self) -> None:
        parts = self.expression.split()
        if len(parts) != 5:
            raise ScheduleError(
                "Cron expressions must have 5 fields: minute hour day month weekday"
            )
        object.__setattr__(self, "_parts", tuple(parts))
        object.__setattr__(self, "_timezone", parse_timezone(self.timezone_name))
        object.__setattr__(self, "_minutes", _expand_part(parts[0], 0, 59))
        object.__setattr__(self, "_hours", _expand_part(parts[1], 0, 23))
        object.__setattr__(self, "_days", _expand_part(parts[2], 1, 31))
        object.__setattr__(self, "_months", _expand_part(parts[3], 1, 12))
        weekdays = _expand_part(parts[4].replace("7", "0"), 0, 6)
        object.__setattr__(self, "_weekdays", weekdays)

    def describe(self) -> str:
        return f"Cron {self.expression} ({self.timezone_name})"

    def matches(self, local_dt: datetime) -> bool:
        weekday = local_dt.isoweekday() % 7
        return (
            local_dt.minute in self._minutes
            and local_dt.hour in self._hours
            and local_dt.day in self._days
            and local_dt.month in self._months
            and weekday in self._weekdays
        )

    def current_slot(self, now_utc: datetime) -> datetime | None:
        localized = ensure_aware_utc(now_utc).astimezone(self._timezone)
        candidate = localized.replace(second=0, microsecond=0)
        if self.matches(candidate):
            return candidate.astimezone(timezone.utc)
        return None

    def next_after(self, now_utc: datetime) -> datetime | None:
        localized = ensure_aware_utc(now_utc).astimezone(self._timezone)
        candidate = localized.replace(second=0, microsecond=0) + timedelta(minutes=1)
        for _ in range(366 * 24 * 60):
            if self.matches(candidate):
                return candidate.astimezone(timezone.utc)
            candidate += timedelta(minutes=1)
        return None


@dataclass(frozen=True, slots=True)
class IntervalSchedule:
    seconds: int
    timezone_name: str = "UTC"

    def __post_init__(self) -> None:
        if self.seconds <= 0:
            raise ScheduleError("Interval must be greater than zero seconds")

    def describe(self) -> str:
        if self.seconds % 3600 == 0:
            unit_value = self.seconds // 3600
            unit_label = "hour" if unit_value == 1 else "hours"
        elif self.seconds % 60 == 0:
            unit_value = self.seconds // 60
            unit_label = "minute" if unit_value == 1 else "minutes"
        else:
            unit_value = self.seconds
            unit_label = "second" if unit_value == 1 else "seconds"
        return f"Every {unit_value} {unit_label}"

    def current_slot(self, now_utc: datetime) -> datetime | None:
        current = ensure_aware_utc(now_utc)
        epoch = int(current.timestamp())
        slot = epoch - (epoch % self.seconds)
        return datetime.fromtimestamp(slot, tz=timezone.utc)

    def next_after(self, now_utc: datetime) -> datetime | None:
        slot = self.current_slot(now_utc)
        if slot is None:
            return None
        return slot + timedelta(seconds=self.seconds)
