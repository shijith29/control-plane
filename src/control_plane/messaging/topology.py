"""Broker topology, declared idempotently by both the control plane and the worker.

exchange provisioning (direct)
  task.created -> tasks          (quorum, delivery-limit 5, DLX -> tasks.dlq)
  task.update  -> task_updates   (quorum, delivery-limit 5, DLX -> task_updates.dlq)
exchange provisioning.dlx (direct)
  tasks        -> tasks.dlq
  task_updates -> task_updates.dlq
"""

from __future__ import annotations

from typing import Any

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue

EXCHANGE = "provisioning"
DLX = "provisioning.dlx"

TASKS_QUEUE = "tasks"
UPDATES_QUEUE = "task_updates"
TASKS_DLQ = "tasks.dlq"
UPDATES_DLQ = "task_updates.dlq"

RK_TASK_CREATED = "task.created"
RK_TASK_UPDATE = "task.update"

DELIVERY_LIMIT = 5

ALL_QUEUES = (TASKS_QUEUE, UPDATES_QUEUE, TASKS_DLQ, UPDATES_DLQ)


def _work_queue_args(queue: str) -> dict[str, Any]:
    return {
        "x-queue-type": "quorum",
        "x-delivery-limit": DELIVERY_LIMIT,
        "x-dead-letter-exchange": DLX,
        "x-dead-letter-routing-key": queue,
    }


async def declare_topology(channel: AbstractChannel) -> dict[str, AbstractQueue]:
    exchange = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True)
    dlx = await channel.declare_exchange(DLX, aio_pika.ExchangeType.DIRECT, durable=True)

    queues: dict[str, AbstractQueue] = {}
    for work_queue, dlq, routing_key in (
        (TASKS_QUEUE, TASKS_DLQ, RK_TASK_CREATED),
        (UPDATES_QUEUE, UPDATES_DLQ, RK_TASK_UPDATE),
    ):
        dead = await channel.declare_queue(dlq, durable=True)
        await dead.bind(dlx, routing_key=work_queue)
        queue = await channel.declare_queue(
            work_queue, durable=True, arguments=_work_queue_args(work_queue)
        )
        await queue.bind(exchange, routing_key=routing_key)
        queues[work_queue] = queue
        queues[dlq] = dead
    return queues


async def get_exchange(channel: AbstractChannel) -> AbstractExchange:
    return await channel.get_exchange(EXCHANGE, ensure=False)


def persistent_json(body: bytes, *, message_id: str) -> aio_pika.Message:
    return aio_pika.Message(
        body,
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        message_id=message_id,
    )
