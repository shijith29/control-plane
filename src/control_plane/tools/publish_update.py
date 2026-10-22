"""Hand-craft a TaskUpdate (or any raw text) onto the broker — for negative scenarios.

python -m control_plane.tools.publish_update --task-id <uuid> --status done [--update-id <uuid>]
python -m control_plane.tools.publish_update --raw 'not json at all'
"""

from __future__ import annotations

import argparse
import asyncio
import os
import uuid

import aio_pika

from control_plane.messaging.contracts import TaskUpdate, UpdateError
from control_plane.messaging.topology import (
    EXCHANGE,
    RK_TASK_UPDATE,
    declare_topology,
    persistent_json,
)
from control_plane.timeutil import now_ms


async def publish(url: str, body: bytes, message_id: str) -> None:
    connection = await aio_pika.connect(url)
    async with connection:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        await declare_topology(channel)
        exchange = await channel.get_exchange(EXCHANGE, ensure=False)
        await exchange.publish(
            persistent_json(body, message_id=message_id), routing_key=RK_TASK_UPDATE, mandatory=True
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--rabbitmq-url", default=os.environ.get("RABBITMQ_URL"))
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--task-id", type=uuid.UUID)
    group.add_argument("--raw", help="publish this exact text (e.g. a poison message)")
    parser.add_argument("--status", choices=["in_progress", "done", "failed"])
    parser.add_argument(
        "--update-id",
        type=uuid.UUID,
        default=None,
        help="idempotency key; reuse one to simulate a duplicate (default: random)",
    )
    parser.add_argument("--reason", default="manually published failure")
    args = parser.parse_args()
    if not args.rabbitmq_url:
        parser.error("--rabbitmq-url or RABBITMQ_URL is required")

    if args.raw is not None:
        body, message_id = args.raw.encode(), str(uuid.uuid4())
    else:
        if args.status is None:
            parser.error("--status is required with --task-id")
        update = TaskUpdate(
            update_id=args.update_id or uuid.uuid4(),
            task_id=args.task_id,
            status=args.status,
            occurred_at=now_ms(),
            error=UpdateError(reason=args.reason) if args.status == "failed" else None,
        )
        body, message_id = update.model_dump_json(exclude_none=True).encode(), str(update.update_id)

    asyncio.run(publish(args.rabbitmq_url, body, message_id))
    print(f"published message_id={message_id}: {body.decode(errors='replace')}")


if __name__ == "__main__":
    main()
