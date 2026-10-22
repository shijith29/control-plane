"""Transactional-outbox relay: drains committed outbox rows to the broker.

Guarantees (README §5):
* never publishes before commit — it can only see committed rows;
* never loses an event — a row stays pending until the broker *confirms* it;
* at-least-once — a crash between confirm and ``published_at`` re-publishes, which every
  consumer tolerates by design.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass
from typing import Protocol

from aio_pika.abc import AbstractChannel
from sqlalchemy.ext.asyncio import AsyncEngine

from control_plane.backoff import Backoff
from control_plane.db import tasks_repo
from control_plane.messaging.broker import Broker
from control_plane.messaging.topology import EXCHANGE, persistent_json
from control_plane.timeutil import now_ms

log = logging.getLogger(__name__)


class Publisher(Protocol):
    async def publish(self, routing_key: str, body: bytes, *, message_id: str) -> None:
        """Return only once the broker has confirmed the message; raise otherwise."""


class AmqpPublisher:
    """Publishes on a confirm-mode channel; reopens the channel if it was closed."""

    def __init__(self, broker: Broker) -> None:
        self._broker = broker
        self._channel: AbstractChannel | None = None

    async def publish(self, routing_key: str, body: bytes, *, message_id: str) -> None:
        if self._channel is None or self._channel.is_closed:
            self._channel = await self._broker.channel(publisher_confirms=True)
        exchange = await self._channel.get_exchange(EXCHANGE, ensure=False)
        # with publisher confirms, publish() awaits the broker ack and raises on nack/return
        await exchange.publish(
            persistent_json(body, message_id=message_id),
            routing_key=routing_key,
            mandatory=True,
            timeout=10,
        )


@dataclass(frozen=True)
class BatchResult:
    published: int
    error: Exception | None = None


class OutboxRelay:
    def __init__(
        self,
        engine: AsyncEngine,
        publisher: Publisher,
        *,
        batch_size: int = 100,
        poll_interval_s: float = 0.2,
    ) -> None:
        self._engine = engine
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval_s = poll_interval_s
        self._backoff = Backoff(base=0.2, cap=30.0)

    async def publish_batch(self) -> BatchResult:
        """Publish up to ``batch_size`` pending rows in id order, in one DB transaction.

        Stops at the first failure so events of one tenant are never reordered; the failed
        row records ``attempts``/``last_error`` and is retried on the next pass.
        """
        published: list[int] = []
        error: Exception | None = None
        async with self._engine.begin() as conn:
            rows = await tasks_repo.claim_pending_outbox(conn, self._batch_size)
            for row in rows:
                try:
                    await self._publisher.publish(
                        row["routing_key"],
                        json.dumps(row["payload"], separators=(",", ":")).encode(),
                        message_id=str(row["event_id"]),
                    )
                except Exception as exc:
                    error = exc
                    await tasks_repo.mark_failed(conn, row["id"], repr(exc))
                    break
                published.append(row["id"])
            await tasks_repo.mark_published(conn, published, now_ms())
        if published:
            log.info("outbox published", extra={"count": len(published)})
        return BatchResult(published=len(published), error=error)

    async def run(self, stop: asyncio.Event) -> None:
        """Poll forever; back off exponentially while the broker or DB is failing."""
        log.info("outbox relay started")
        while not stop.is_set():
            try:
                result = await self.publish_batch()
            except Exception as exc:  # DB unavailable etc. — never let the loop die
                result = BatchResult(published=0, error=exc)
            if result.error is not None:
                delay = self._backoff.next_delay()
                log.warning(
                    "outbox publish failed, backing off",
                    extra={"error": repr(result.error), "retry_in_s": round(delay, 2)},
                )
            else:
                self._backoff.reset()
                # a full batch means there is probably more waiting: go again immediately
                delay = 0.0 if result.published == self._batch_size else self._poll_interval_s
            if delay:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay)
        log.info("outbox relay stopped")
