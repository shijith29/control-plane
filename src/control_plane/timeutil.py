"""Timestamps: always UTC, always millisecond precision, rendered as ``...123Z``."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

from pydantic import PlainSerializer


def now_ms() -> datetime:
    """Current UTC time truncated to milliseconds (what we store and what we render)."""
    now = datetime.now(UTC)
    return now.replace(microsecond=(now.microsecond // 1000) * 1000)


def iso_ms(value: datetime) -> str:
    """``2026-10-21T17:03:12.481Z`` — ISO 8601, milliseconds, ``Z`` suffix."""
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


#: Datetime field that serialises in the contract format.
UtcMillis = Annotated[datetime, PlainSerializer(iso_ms, return_type=str)]
