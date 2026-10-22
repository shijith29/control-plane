"""Worker simulator: consumes ``tasks``, pretends to provision, reports progress.

For every task it publishes two ``TaskUpdate`` messages — ``in_progress`` then ``done`` or
``failed`` — with *deterministic* update ids (see ``deterministic_update_id``). The incoming
task is acked only after both updates are confirmed by the broker, so a crash mid-task leads
to a redelivery and a replay the control plane deduplicates.

Knobs for negative scenarios: ``fail_rate``, delays, ``duplicate_rate`` (publish each update
twice) and ``reorder_rate`` (send the terminal update *before* ``in_progress``).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from dataclasses import dataclass

import aio_pika
from aio_pika.abc import AbstractExchange, AbstractIncomingMessage
from pydantic import ValidationError

from control_plane.backoff import Backoff
from control_plane.messaging.contracts import (
    TaskEvent,
    TaskUpdate,
    UpdateError,
    deterministic_update_id,
)
from control_plane.messaging.topology import (
    EXCHANGE,
    RK_TASK_UPDATE,
    TASKS_QUEUE,
    declare_topology,
    persistent_json,
)
from control_plane.timeutil import now_ms

log = logging.getLogger("worker")


@dataclass(frozen=True)
class WorkerConfig:
    fail_rate: float = 0.0
    min_delay_ms: int = 500
    max_delay_ms: int = 3000
    duplicate_rate: float = 0.0
    reorder_rate: float = 0.0
    prefetch: int = 4

    def __post_init__(self) -> None:
        for name in ("fail_rate", "duplicate_rate", "reorder_rate"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1, got {value}")
        if self.min_delay_ms < 0 or self.max_delay_ms < self.min_delay_ms:
            raise ValueError("require 0 <= min_delay_ms <= max_delay_ms")
        if self.prefetch < 1:
            raise ValueError("prefetch must be >= 1")


@dataclass(frozen=True)
class Plan:
    """What the worker will do for one task — computed up front so it is unit-testable."""

    updates: tuple[TaskUpdate, ...]
    delay_s: float
    duplicate: bool


def plan_for(event: TaskEvent, config: WorkerConfig, rng: random.Random) -> Plan:
    task_id = event.task.id
    failed = rng.random() < config.fail_rate
    terminal = "failed" if failed else "done"
    error = (
        UpdateError(reason=f"simulated failure (fail-rate={config.fail_rate})") if failed else None
    )

    in_progress = TaskUpdate(
        update_id=deterministic_update_id(task_id, "in_progress"),
        task_id=task_id,
        status="in_progress",
        occurred_at=now_ms(),
    )
    final = TaskUpdate(
        update_id=deterministic_update_id(task_id, terminal),
        task_id=task_id,
        status=terminal,
        error=error,
        occurred_at=now_ms(),
    )
    reorder = rng.random() < config.reorder_rate
    return Plan(
        updates=(final, in_progress) if reorder else (in_progress, final),
        delay_s=rng.uniform(config.min_delay_ms, config.max_delay_ms) / 1000,
        duplicate=rng.random() < config.duplicate_rate,
    )


class Worker:
    def __init__(self, url: str, config: WorkerConfig, rng: random.Random | None = None) -> None:
        self._url = url
        self._config = config
        # simulated outcomes only, not cryptography
        self._rng = rng or random.Random()  # nosec B311
        self._exchange: AbstractExchange | None = None
        self.processed = 0

    async def _publish(self, update: TaskUpdate) -> None:
        if self._exchange is None:
            raise RuntimeError("worker is not connected")
        body = update.model_dump_json(exclude_none=True).encode()
        await self._exchange.publish(
            persistent_json(body, message_id=str(update.update_id)),
            routing_key=RK_TASK_UPDATE,
            mandatory=True,
        )

    async def on_task(self, message: AbstractIncomingMessage) -> None:
        try:
            event = TaskEvent.model_validate_json(message.body)
        except ValidationError:
            log.warning("unparseable task event rejected to tasks.dlq")
            await message.reject(requeue=False)
            return

        plan = plan_for(event, self._config, self._rng)
        log.info(
            "task received",
            extra={
                "task_id": str(event.task.id),
                "type": event.task.type,
                "tenant": event.tenant.slug,
                "plan": [u.status for u in plan.updates],
                "delay_s": round(plan.delay_s, 2),
                "duplicate": plan.duplicate,
            },
        )
        try:
            first, second = plan.updates
            for update in self._fanout(first, plan.duplicate):
                await self._publish(update)
            await asyncio.sleep(plan.delay_s)
            for update in self._fanout(second, plan.duplicate):
                await self._publish(update)
            await message.ack()
            self.processed += 1
            outcome = next(u.status for u in plan.updates if u.status != "in_progress")
            log.info("task finished", extra={"task_id": str(event.task.id), "status": outcome})
        except Exception as exc:
            # let the broker redeliver; the deterministic update ids make the replay harmless
            log.warning("task processing failed, requeueing", extra={"error": repr(exc)})
            with contextlib.suppress(Exception):  # channel gone: the broker redelivers anyway
                await message.nack(requeue=True)

    @staticmethod
    def _fanout(update: TaskUpdate, duplicate: bool) -> tuple[TaskUpdate, ...]:
        return (update, update) if duplicate else (update,)

    async def run(self, stop: asyncio.Event) -> None:
        backoff = Backoff(base=0.5, cap=10.0)
        while not stop.is_set():
            try:
                connection = await aio_pika.connect_robust(self._url, timeout=10)
                break
            except Exception as exc:
                delay = backoff.next_delay()
                log.warning("broker unavailable, retrying", extra={"error": repr(exc)})
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay)
        else:
            return

        async with connection:
            channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
            await channel.set_qos(prefetch_count=self._config.prefetch)
            # consume on the declared queue so the robust channel re-subscribes on reconnect
            queue = (await declare_topology(channel))[TASKS_QUEUE]
            self._exchange = await channel.get_exchange(EXCHANGE, ensure=False)
            tag = await queue.consume(self.on_task)
            log.info("worker started", extra={"config": self._config.__dict__})
            await stop.wait()
            await queue.cancel(tag)
        log.info("worker stopped", extra={"processed": self.processed})
