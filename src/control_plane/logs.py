"""Minimal structured (JSON-lines) logging with a per-request id carried in a contextvar."""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from typing import Any

from control_plane.timeutil import iso_ms, now_ms

request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)

_RESERVED = set(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": iso_ms(now_ms()),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if (rid := request_id_var.get()) is not None:
            payload["request_id"] = rid
        # anything passed via `extra={...}` becomes a top-level field
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    # uvicorn's access log duplicates our request logging and is not JSON
    logging.getLogger("uvicorn.access").disabled = True
    for noisy in ("aiormq", "aio_pika"):
        logging.getLogger(noisy).setLevel("WARNING")
