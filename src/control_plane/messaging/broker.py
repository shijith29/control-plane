"""Broker connection that never blocks the API.

The control plane must keep accepting writes while RabbitMQ is down (the outbox holds the
events), so the connection is established in the background with backoff and components ask
for a channel only when they need one.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractRobustConnection

from control_plane.backoff import Backoff
from control_plane.messaging.topology import declare_topology

log = logging.getLogger(__name__)


class Broker:
    def __init__(self, url: str) -> None:
        self._url = url
        self._connection: AbstractRobustConnection | None = None
        self._ready = asyncio.Event()

    async def connect(self, stop: asyncio.Event) -> bool:
        """Retry until connected (and topology declared) or ``stop`` is set."""
        backoff = Backoff(base=0.5, cap=10.0)
        while not stop.is_set():
            try:
                self._connection = await aio_pika.connect_robust(self._url, timeout=10)
                async with self._connection.channel() as channel:
                    await declare_topology(channel)
                self._ready.set()
                log.info("broker connected")
                return True
            except Exception as exc:
                delay = backoff.next_delay()
                log.warning(
                    "broker unavailable, retrying",
                    extra={"error": repr(exc), "retry_in_s": round(delay, 2)},
                )
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay)
        return False

    async def wait_ready(self) -> None:
        await self._ready.wait()

    @property
    def is_connected(self) -> bool:
        # a robust connection is not "closed" while it is reconnecting, so ask whether it is
        # actually connected right now
        return self._connection is not None and self._connection.connected.is_set()

    async def channel(self, *, publisher_confirms: bool = False) -> AbstractChannel:
        if self._connection is None:
            raise RuntimeError("broker not connected")
        return await self._connection.channel(
            publisher_confirms=publisher_confirms, on_return_raises=publisher_confirms
        )

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
