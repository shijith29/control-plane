"""Opaque keyset cursors: base64url(JSON{"c": created_at, "i": id})."""

from __future__ import annotations

import base64
import binascii
import json
from datetime import datetime
from uuid import UUID

from control_plane.domain.errors import InvalidCursor
from control_plane.timeutil import iso_ms

DEFAULT_LIMIT = 50
MAX_LIMIT = 200


def encode_cursor(cursor: tuple[datetime, UUID] | None) -> str | None:
    if cursor is None:
        return None
    created_at, row_id = cursor
    raw = json.dumps({"c": iso_ms(created_at), "i": str(row_id)}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_cursor(token: str | None) -> tuple[datetime, UUID] | None:
    if token is None:
        return None
    try:
        padded = token + "=" * (-len(token) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded.encode()))
        created_at = datetime.fromisoformat(data["c"].replace("Z", "+00:00"))
        if created_at.tzinfo is None:
            raise ValueError("naive timestamp")
        return created_at, UUID(data["i"])
    except (ValueError, KeyError, TypeError, AttributeError, binascii.Error) as exc:
        raise InvalidCursor() from exc
