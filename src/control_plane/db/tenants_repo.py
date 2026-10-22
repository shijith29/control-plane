"""SQL for tenants. Every concurrency-sensitive write is ONE conditional statement.

All functions take an open ``AsyncConnection`` inside a transaction owned by the caller
(``service.py``) so the tenant, its task and the outbox row commit — or roll back — together.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import and_, insert, select, tuple_, update
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection

from control_plane.db.tables import tenants
from control_plane.domain.states import TenantStatus


async def insert_tenant(
    conn: AsyncConnection, *, tenant_id: UUID, slug: str, name: str, now: datetime
) -> RowMapping:
    """Raises IntegrityError(tenants_slug_key) when the slug is taken — including by a
    concurrent transaction, which Postgres makes us wait for before deciding."""
    result = await conn.execute(
        insert(tenants)
        .values(
            id=tenant_id,
            slug=slug,
            name=name,
            status=TenantStatus.PROVISIONING,
            version=1,
            created_at=now,
            updated_at=now,
        )
        .returning(*tenants.c)
    )
    return result.mappings().one()


async def get_tenant(
    conn: AsyncConnection, tenant_id: UUID, *, for_update: bool = False
) -> RowMapping | None:
    stmt = select(tenants).where(tenants.c.id == tenant_id)
    if for_update:
        stmt = stmt.with_for_update()
    return (await conn.execute(stmt)).mappings().one_or_none()


async def start_update(
    conn: AsyncConnection,
    *,
    tenant_id: UUID,
    expected_version: int,
    name: str | None,
    now: datetime,
) -> RowMapping | None:
    """``active -> updating`` iff the caller saw the current version. None = guard failed."""
    values: dict[str, Any] = {
        "status": TenantStatus.UPDATING,
        "version": tenants.c.version + 1,
        "updated_at": now,
    }
    if name is not None:
        values["name"] = name
    result = await conn.execute(
        update(tenants)
        .where(
            tenants.c.id == tenant_id,
            tenants.c.version == expected_version,
            tenants.c.status == TenantStatus.ACTIVE,
        )
        .values(**values)
        .returning(*tenants.c)
    )
    return result.mappings().one_or_none()


async def start_destroy(
    conn: AsyncConnection, *, tenant_id: UUID, allowed_from: Sequence[TenantStatus], now: datetime
) -> RowMapping | None:
    """``active|failed -> destroying``. None = tenant missing or in the wrong status."""
    result = await conn.execute(
        update(tenants)
        .where(tenants.c.id == tenant_id, tenants.c.status.in_(list(allowed_from)))
        .values(
            status=TenantStatus.DESTROYING,
            version=tenants.c.version + 1,
            updated_at=now,
        )
        .returning(*tenants.c)
    )
    return result.mappings().one_or_none()


async def apply_terminal_outcome(
    conn: AsyncConnection,
    *,
    tenant_id: UUID,
    expected_status: TenantStatus,
    new_status: TenantStatus,
    now: datetime,
) -> int | None:
    """Consumer-side transition, guarded on the in-flight status. Returns the new version."""
    result = await conn.execute(
        update(tenants)
        .where(tenants.c.id == tenant_id, tenants.c.status == expected_status)
        .values(status=new_status, version=tenants.c.version + 1, updated_at=now)
        .returning(tenants.c.version)
    )
    row = result.one_or_none()
    return None if row is None else int(row[0])


async def list_tenants(
    conn: AsyncConnection,
    *,
    status: TenantStatus | None,
    slug: str | None,
    after: tuple[datetime, UUID] | None,
    limit: int,
) -> list[RowMapping]:
    """Keyset page ordered by (created_at, id). Fetches ``limit + 1`` to detect a next page."""
    conditions = []
    if status is not None:
        conditions.append(tenants.c.status == status)
    if slug is not None:
        conditions.append(tenants.c.slug == slug)
    if after is not None:
        conditions.append(tuple_(tenants.c.created_at, tenants.c.id) > tuple_(*after))
    stmt = (
        select(tenants)
        .where(and_(True, *conditions))
        .order_by(tenants.c.created_at, tenants.c.id)
        .limit(limit + 1)
    )
    return list((await conn.execute(stmt)).mappings().all())
