from __future__ import annotations

import base64
import uuid
from datetime import UTC, datetime

import pytest

from control_plane.api.pagination import decode_cursor, encode_cursor
from control_plane.backoff import Backoff
from control_plane.domain.errors import InvalidCursor
from control_plane.timeutil import iso_ms, now_ms


def test_cursor_round_trip() -> None:
    cursor = (datetime(2026, 10, 21, 17, 3, 12, 481000, tzinfo=UTC), uuid.uuid4())
    token = encode_cursor(cursor)
    assert token is not None and "=" not in token
    assert decode_cursor(token) == cursor


def test_no_cursor() -> None:
    assert encode_cursor(None) is None
    assert decode_cursor(None) is None


@pytest.mark.parametrize(
    "token",
    [
        "not-base64!!",
        base64.urlsafe_b64encode(b"not json").decode(),
        base64.urlsafe_b64encode(b'{"c": "2026-10-21T17:03:12.481Z"}').decode(),  # no id
        base64.urlsafe_b64encode(b'{"c": "yesterday", "i": "x"}').decode(),
        base64.urlsafe_b64encode(
            b'{"c": "2026-10-21T17:03:12", "i": "' + str(uuid.uuid4()).encode() + b'"}'
        ).decode(),  # naive
        base64.urlsafe_b64encode(b"[1, 2]").decode(),
    ],
)
def test_tampered_cursor_is_rejected(token: str) -> None:
    with pytest.raises(InvalidCursor):
        decode_cursor(token)


def test_timestamps_are_milliseconds_with_z() -> None:
    value = datetime(2026, 10, 21, 17, 3, 12, 481999, tzinfo=UTC)
    assert iso_ms(value) == "2026-10-21T17:03:12.481Z"


def test_now_is_truncated_to_milliseconds() -> None:
    now = now_ms()
    assert now.tzinfo is UTC
    assert now.microsecond % 1000 == 0


def test_backoff_grows_and_is_capped() -> None:
    backoff = Backoff(base=1.0, cap=4.0, factor=2.0)
    for _ in range(10):
        assert 0 <= backoff.next_delay() <= 4.0
    assert backoff.attempt == 10
    backoff.reset()
    assert backoff.attempt == 0
    assert backoff.next_delay() <= 1.0
