"""Use cases. Each mutating method is exactly one transaction that writes the tenant, its new
task and the outbox row together; the event is published later by the relay, strictly after
this transaction commits (transactional outbox — README §5)."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from control_plane.db import tasks_repo, tenants_repo
from control_plane.db.engine import violated_constraint
from control_plane.db.tables import TENANT_SLUG_UNIQUE
from control_plane.domain.errors import (
    DomainError,
    TaskNotFound,
    TenantAlreadyExists,
    TenantNotFound,
    TenantUpdateNotAllowed,
    TenantVersionConflict,
)
from control_plane.domain.states import (
    DELETABLE_FROM,
    UPDATABLE_FROM,
    TaskStatus,
    TaskType,
    TenantStatus,
)
from control_plane.messaging.contracts import EventTask, EventTenant, TaskEvent
from control_plane.messaging.topology import RK_TASK_CREATED
from control_plane.timeutil import now_ms

log = logging.getLogger(__name__)

Cursor = tuple[datetime, UUID]


def _tenant_view(tenant: RowMapping, task: RowMapping | None) -> dict[str, Any]:
    view = dict(tenant)
    view["current_task"] = (
        None if task is None else {"id": task["id"], "type": task["type"], "status": task["status"]}
    )
    return view


class TenantService:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    # ------------------------------------------------------------------------------ writes

    async def create_tenant(self, *, slug: str, name: str) -> dict[str, Any]:
        now = now_ms()
        try:
            async with self._engine.begin() as conn:
                tenant = await tenants_repo.insert_tenant(
                    conn, tenant_id=uuid.uuid4(), slug=slug, name=name, now=now
                )
                task = await self._start_task(conn, tenant, TaskType.DEPLOY, now)
        except IntegrityError as exc:
            if violated_constraint(exc) == TENANT_SLUG_UNIQUE:
                raise TenantAlreadyExists(slug) from None
            raise
        log.info(
            "tenant created", extra={"tenant_id": str(tenant["id"]), "task_id": str(task["id"])}
        )
        return _tenant_view(tenant, task)

    async def update_tenant(
        self, tenant_id: UUID, *, expected_version: int, name: str | None
    ) -> dict[str, Any]:
        now = now_ms()
        async with self._engine.begin() as conn:
            tenant = await tenants_repo.start_update(
                conn, tenant_id=tenant_id, expected_version=expected_version, name=name, now=now
            )
            if tenant is None:
                raise await self._update_rejection(conn, tenant_id, expected_version)
            task = await self._start_task(conn, tenant, TaskType.UPDATE, now)
        log.info(
            "tenant update started", extra={"tenant_id": str(tenant_id), "task_id": str(task["id"])}
        )
        return _tenant_view(tenant, task)

    async def delete_tenant(self, tenant_id: UUID) -> dict[str, Any]:
        now = now_ms()
        async with self._engine.begin() as conn:
            tenant = await tenants_repo.start_destroy(
                conn, tenant_id=tenant_id, allowed_from=sorted(DELETABLE_FROM), now=now
            )
            if tenant is None:
                current = await tenants_repo.get_tenant(conn, tenant_id)
                if current is None:
                    raise TenantNotFound(tenant_id)
                raise TenantUpdateNotAllowed("delete", current["status"], sorted(DELETABLE_FROM))
            task = await self._start_task(conn, tenant, TaskType.DESTROY, now)
        log.info(
            "tenant destroy started",
            extra={"tenant_id": str(tenant_id), "task_id": str(task["id"])},
        )
        return _tenant_view(tenant, task)

    async def _start_task(
        self, conn: AsyncConnection, tenant: RowMapping, task_type: TaskType, now: datetime
    ) -> RowMapping:
        """Insert the task and its outbox event in the caller's transaction."""
        task = await tasks_repo.insert_task(
            conn, task_id=uuid.uuid4(), tenant_id=tenant["id"], task_type=task_type, now=now
        )
        event = TaskEvent(
            event_id=task["id"],
            occurred_at=now,
            task=EventTask(
                id=task["id"], type=task_type, tenant_id=tenant["id"], status=TaskStatus.ACCEPTED
            ),
            tenant=EventTenant(slug=tenant["slug"], name=tenant["name"]),
        )
        await tasks_repo.insert_outbox(
            conn,
            event_id=task["id"],
            routing_key=RK_TASK_CREATED,
            payload=event.model_dump(mode="json"),
            now=now,
        )
        return task

    @staticmethod
    async def _update_rejection(
        conn: AsyncConnection, tenant_id: UUID, expected_version: int
    ) -> DomainError:
        """The conditional UPDATE matched nothing; re-read to report the precise reason.

        Precedence: not_found > version_conflict > update_not_allowed (README §7).
        """
        current = await tenants_repo.get_tenant(conn, tenant_id)
        if current is None:
            return TenantNotFound(tenant_id)
        if current["version"] != expected_version:
            return TenantVersionConflict(expected_version, current["version"])
        return TenantUpdateNotAllowed("update", current["status"], sorted(UPDATABLE_FROM))

    # ------------------------------------------------------------------------------- reads

    async def get_tenant(self, tenant_id: UUID) -> dict[str, Any]:
        async with self._engine.connect() as conn:
            tenant = await tenants_repo.get_tenant(conn, tenant_id)
            if tenant is None:
                raise TenantNotFound(tenant_id)
            task = (await tasks_repo.current_tasks(conn, [tenant_id])).get(tenant_id)
        return _tenant_view(tenant, task)

    async def list_tenants(
        self, *, status: TenantStatus | None, slug: str | None, after: Cursor | None, limit: int
    ) -> tuple[list[dict[str, Any]], Cursor | None]:
        async with self._engine.connect() as conn:
            rows = await tenants_repo.list_tenants(
                conn, status=status, slug=slug, after=after, limit=limit
            )
            page, next_cursor = _split_page(rows, limit)
            current = await tasks_repo.current_tasks(conn, [r["id"] for r in page])
        return [_tenant_view(r, current.get(r["id"])) for r in page], next_cursor

    async def get_task(self, task_id: UUID) -> dict[str, Any]:
        async with self._engine.connect() as conn:
            task = await tasks_repo.get_task(conn, task_id)
        if task is None:
            raise TaskNotFound(task_id)
        return dict(task)

    async def list_tasks(
        self,
        *,
        tenant_id: UUID | None,
        status: TaskStatus | None,
        task_type: TaskType | None,
        after: Cursor | None,
        limit: int,
        require_tenant: bool = False,
    ) -> tuple[list[dict[str, Any]], Cursor | None]:
        async with self._engine.connect() as conn:
            if (
                require_tenant
                and tenant_id is not None
                and await tenants_repo.get_tenant(conn, tenant_id) is None
            ):
                raise TenantNotFound(tenant_id)
            rows = await tasks_repo.list_tasks(
                conn,
                tenant_id=tenant_id,
                status=status,
                task_type=task_type,
                after=after,
                limit=limit,
            )
        page, next_cursor = _split_page(rows, limit)
        return [dict(r) for r in page], next_cursor


def _split_page(rows: list[RowMapping], limit: int) -> tuple[list[RowMapping], Cursor | None]:
    if len(rows) <= limit:
        return rows, None
    page = rows[:limit]
    last = page[-1]
    return page, (last["created_at"], last["id"])
