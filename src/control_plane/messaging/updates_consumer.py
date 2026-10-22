"""Inbound consumer for worker progress updates (README §5, consumer decision flow).

Split in two so the correctness logic is testable without a broker:

* ``UpdateApplier.apply``  — one DB transaction: claim idempotency key, lock task, apply a
  forward transition (and, if terminal, move the tenant). Returns what happened.
* ``UpdatesConsumer.handle`` — parse / retry / decide ack, reject-to-DLQ or requeue.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from enum import StrEnum

from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine

from control_plane.backoff import Backoff
from control_plane.db import tasks_repo, tenants_repo
from control_plane.domain.states import (
    IN_FLIGHT_STATUS,
    TaskStatus,
    TaskType,
    is_forward,
    is_terminal,
    tenant_status_after,
)
from control_plane.messaging.broker import Broker
from control_plane.messaging.contracts import TaskUpdate
from control_plane.messaging.topology import UPDATES_QUEUE, declare_topology
from control_plane.timeutil import now_ms

log = logging.getLogger(__name__)


class Outcome(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"  # update_id already processed — not recorded again
    STALE = "stale"  # would not move the task forward (late / out of order / terminal)
    UNKNOWN_TASK = "unknown_task"


class Disposition(StrEnum):
    ACK = "ack"
    REJECT = "reject"  # requeue=False -> dead-letter queue
    REQUEUE = "requeue"  # nack(requeue=True); the quorum delivery-limit caps this


class UpdateApplier:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def apply(self, update: TaskUpdate) -> Outcome:
        new_status = update.task_status
        async with self._engine.begin() as conn:
            # 1. idempotency: claim the key first; a concurrent duplicate waits here
            claimed = await tasks_repo.record_update(
                conn, update_id=update.update_id, task_id=update.task_id, status=update.status
            )
            if not claimed:
                return Outcome.DUPLICATE

            # 2. serialise all updates for this task
            task = await tasks_repo.get_task(conn, update.task_id, for_update=True)
            if task is None:
                await tasks_repo.set_update_outcome(conn, update.update_id, Outcome.UNKNOWN_TASK)
                return Outcome.UNKNOWN_TASK

            # 3. order tolerance: only strictly forward transitions are applied
            current = TaskStatus(task["status"])
            if not is_forward(current, new_status):
                await tasks_repo.set_update_outcome(conn, update.update_id, Outcome.STALE)
                return Outcome.STALE

            now = now_ms()
            error = (
                update.error.reason if (update.error and new_status is TaskStatus.FAILED) else None
            )
            await tasks_repo.set_task_status(
                conn, task_id=update.task_id, status=new_status, error=error, now=now
            )

            # 4. a terminal outcome moves the tenant (guarded on its in-flight status)
            if is_terminal(new_status):
                task_type = TaskType(task["type"])
                version = await tenants_repo.apply_terminal_outcome(
                    conn,
                    tenant_id=task["tenant_id"],
                    expected_status=IN_FLIGHT_STATUS[task_type],
                    new_status=tenant_status_after(task_type, new_status),
                    now=now,
                )
                if version is None:  # should be impossible: one open task per tenant
                    log.error(
                        "tenant not in expected in-flight status; task updated, tenant left as is",
                        extra={"task_id": str(update.task_id), "tenant_id": str(task["tenant_id"])},
                    )
            return Outcome.APPLIED


class UpdatesConsumer:
    def __init__(
        self,
        applier: UpdateApplier,
        *,
        max_retries: int = 5,
        retry_base_s: float = 0.1,
        retry_cap_s: float = 3.0,
    ) -> None:
        self._applier = applier
        self._max_retries = max_retries
        self._retry_base_s = retry_base_s
        self._retry_cap_s = retry_cap_s

    async def handle(self, body: bytes) -> Disposition:
        """Decide what to do with one raw message. Never raises."""
        try:
            update = TaskUpdate.model_validate_json(body)
        except ValidationError as exc:
            log.warning(
                "poison message rejected to dead-letter queue",
                extra={
                    "error": exc.errors(include_url=False)[:3],
                    "body": body[:200].decode(errors="replace"),
                },
            )
            return Disposition.REJECT

        backoff = Backoff(base=self._retry_base_s, cap=self._retry_cap_s)
        for attempt in range(1, self._max_retries + 1):
            try:
                outcome = await self._applier.apply(update)
            except Exception as exc:  # transient DB errors, deadlock victim, ...
                if attempt == self._max_retries:
                    log.error(
                        "update failed after retries; requeueing",
                        extra={"update_id": str(update.update_id), "error": repr(exc)},
                    )
                    return Disposition.REQUEUE
                delay = backoff.next_delay()
                log.warning(
                    "update failed, retrying",
                    extra={
                        "update_id": str(update.update_id),
                        "attempt": attempt,
                        "error": repr(exc),
                    },
                )
                await asyncio.sleep(delay)
                continue
            log.info(
                f"task update {outcome}",
                extra={
                    "update_id": str(update.update_id),
                    "task_id": str(update.task_id),
                    "status": update.status,
                    "outcome": str(outcome),
                },
            )
            return Disposition.ACK
        return Disposition.REQUEUE  # pragma: no cover - loop always returns

    async def on_message(self, message: AbstractIncomingMessage) -> None:
        disposition = await self.handle(message.body)
        try:
            if disposition is Disposition.ACK:
                await message.ack()
            elif disposition is Disposition.REJECT:
                await message.reject(requeue=False)
            else:
                await message.nack(requeue=True)
        except Exception as exc:  # channel gone: the broker will redeliver; dedupe handles it
            log.warning("could not settle message", extra={"error": repr(exc)})

    async def run(self, broker: Broker, stop: asyncio.Event, prefetch: int) -> None:
        await broker.wait_ready()
        channel = await broker.channel()
        await channel.set_qos(prefetch_count=prefetch)
        # Consume on the *declared* queue: only declared queues are re-subscribed by the
        # robust channel after a reconnect (a get_queue(ensure=False) handle is not).
        queue = (await declare_topology(channel))[UPDATES_QUEUE]
        tag = await queue.consume(self.on_message)
        log.info("updates consumer started", extra={"queue": UPDATES_QUEUE})
        try:
            await stop.wait()
        finally:
            with contextlib.suppress(Exception):  # best effort during shutdown
                await queue.cancel(tag)
            log.info("updates consumer stopped")
