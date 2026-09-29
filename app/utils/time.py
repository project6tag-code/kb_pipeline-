"""UTC time helpers. The runtime stores and compares only timezone-aware UTC."""

from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    """The current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def ensure_utc(value: datetime) -> datetime:
    """Return ``value`` in UTC; naive datetimes are rejected, never guessed."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("naive datetime: the runtime requires timezone-aware UTC timestamps")
    return value.astimezone(timezone.utc)
