"""SQL for tasks, the outbox and the processed-updates inbox."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import and_, case, desc, insert, select, tuple_, update
from sqlalchemy.dialects.postgresql import distinct_on
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection

from control_plane.db.tables import outbox, processed_updates, tasks
from control_plane.domain.states import OPEN_TASK_STATUSES, TaskStatus, TaskType

# ----------------------------------------------------------------------------------- tasks


async def insert_task(
    conn: AsyncConnection, *, task_id: UUID, tenant_id: UUID, task_type: TaskType, now: datetime
) -> RowMapping:
    result = await conn.execute(
        insert(tasks)
        .values(
            id=task_id,
            tenant_id=tenant_id,
            type=task_type,
            status=TaskStatus.ACCEPTED,
            created_at=now,
            updated_at=now,
        )
        .returning(*tasks.c)
    )
    return result.mappings().one()


async def get_task(
    conn: AsyncConnection, task_id: UUID, *, for_update: bool = False
) -> RowMapping | None:
    stmt = select(tasks).where(tasks.c.id == task_id)
    if for_update:
        stmt = stmt.with_for_update()
    return (await conn.execute(stmt)).mappings().one_or_none()


async def set_task_status(
    conn: AsyncConnection, *, task_id: UUID, status: TaskStatus, error: str | None, now: datetime
) -> None:
    await conn.execute(
        update(tasks)
        .where(tasks.c.id == task_id)
        .values(status=status, error=error, updated_at=now)
    )


def _current_first() -> Any:
    """Ordering that puts a tenant's open task first, then its most recent one."""
    return (
        desc(case((tasks.c.status.in_(list(OPEN_TASK_STATUSES)), 1), else_=0)),
        desc(tasks.c.created_at),
        desc(tasks.c.id),
    )


async def current_tasks(
    conn: AsyncConnection, tenant_ids: Sequence[UUID]
) -> dict[UUID, RowMapping]:
    """The open task of each tenant, or its latest task if none is open."""
    if not tenant_ids:
        return {}
    stmt = (
        select(tasks)
        .where(tasks.c.tenant_id.in_(list(tenant_ids)))
        .order_by(tasks.c.tenant_id, *_current_first())
        .ext(distinct_on(tasks.c.tenant_id))
    )
    rows = (await conn.execute(stmt)).mappings().all()
    return {row["tenant_id"]: row for row in rows}


async def list_tasks(
    conn: AsyncConnection,
    *,
    tenant_id: UUID | None,
    status: TaskStatus | None,
    task_type: TaskType | None,
    after: tuple[datetime, UUID] | None,
    limit: int,
) -> list[RowMapping]:
    conditions = []
    if tenant_id is not None:
        conditions.append(tasks.c.tenant_id == tenant_id)
    if status is not None:
        conditions.append(tasks.c.status == status)
    if task_type is not None:
        conditions.append(tasks.c.type == task_type)
    if after is not None:
        conditions.append(tuple_(tasks.c.created_at, tasks.c.id) > tuple_(*after))
    stmt = (
        select(tasks)
        .where(and_(True, *conditions))
        .order_by(tasks.c.created_at, tasks.c.id)
        .limit(limit + 1)
    )
    return list((await conn.execute(stmt)).mappings().all())


# ---------------------------------------------------------------------------------- outbox


async def insert_outbox(
    conn: AsyncConnection,
    *,
    event_id: UUID,
    routing_key: str,
    payload: dict[str, Any],
    now: datetime,
) -> None:
    await conn.execute(
        insert(outbox).values(
            event_id=event_id, routing_key=routing_key, payload=payload, created_at=now
        )
    )


async def claim_pending_outbox(conn: AsyncConnection, limit: int) -> list[RowMapping]:
    """Lock the oldest unpublished rows; rows locked by another relay are skipped."""
    stmt = (
        select(outbox.c.id, outbox.c.event_id, outbox.c.routing_key, outbox.c.payload)
        .where(outbox.c.published_at.is_(None))
        .order_by(outbox.c.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return list((await conn.execute(stmt)).mappings().all())


async def mark_published(conn: AsyncConnection, ids: Sequence[int], now: datetime) -> None:
    if ids:
        await conn.execute(
            update(outbox)
            .where(outbox.c.id.in_(list(ids)))
            .values(published_at=now, attempts=outbox.c.attempts + 1, last_error=None)
        )


async def mark_failed(conn: AsyncConnection, outbox_id: int, error: str) -> None:
    await conn.execute(
        update(outbox)
        .where(outbox.c.id == outbox_id)
        .values(attempts=outbox.c.attempts + 1, last_error=error[:2000])
    )


# ------------------------------------------------------------------------- processed updates


async def record_update(
    conn: AsyncConnection, *, update_id: UUID, task_id: UUID, status: str
) -> bool:
    """Claim the idempotency key. False => this update_id was already processed.

    A concurrent transaction inserting the same key makes this statement wait until that
    transaction finishes, so two deliveries of one update can never both proceed.
    """
    result = await conn.execute(
        pg_insert(processed_updates)
        .values(update_id=update_id, task_id=task_id, status=status, outcome="applied")
        .on_conflict_do_nothing(index_elements=[processed_updates.c.update_id])
        .returning(processed_updates.c.update_id)
    )
    return result.one_or_none() is not None


async def set_update_outcome(conn: AsyncConnection, update_id: UUID, outcome: str) -> None:
    await conn.execute(
        update(processed_updates)
        .where(processed_updates.c.update_id == update_id)
        .values(outcome=outcome)
    )
