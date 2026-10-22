"""Tenant use cases against a real Postgres: atomic writes, guards, error precedence."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from control_plane.domain.errors import (
    TenantAlreadyExists,
    TenantNotFound,
    TenantUpdateNotAllowed,
    TenantVersionConflict,
)
from control_plane.service import TenantService
from tests.conftest import fetch_all

pytestmark = pytest.mark.db


async def set_status(db: AsyncEngine, tenant_id: uuid.UUID, status: str) -> None:
    """Simulate the worker having moved the tenant (and closed its open task)."""
    async with db.begin() as conn:
        await conn.execute(
            text(
                "UPDATE tasks SET status = 'done' "
                "WHERE tenant_id = :t AND status IN ('accepted','in_progress')"
            ),
            {"t": tenant_id},
        )
        await conn.execute(
            text("UPDATE tenants SET status = :s, version = version + 1 WHERE id = :t"),
            {"s": status, "t": tenant_id},
        )


async def test_create_writes_tenant_task_and_outbox_atomically(
    service: TenantService, db: AsyncEngine
) -> None:
    tenant = await service.create_tenant(slug="acme", name="Acme")

    assert tenant["status"] == "provisioning"
    assert tenant["version"] == 1
    assert tenant["current_task"]["type"] == "deploy"
    assert tenant["current_task"]["status"] == "accepted"

    tasks = await fetch_all(db, "SELECT * FROM tasks")
    outbox = await fetch_all(db, "SELECT * FROM outbox")
    assert len(tasks) == 1 and len(outbox) == 1
    assert outbox[0]["event_id"] == tasks[0]["id"]
    assert outbox[0]["published_at"] is None
    assert outbox[0]["payload"]["task"] == {
        "id": str(tasks[0]["id"]),
        "type": "deploy",
        "tenant_id": str(tenant["id"]),
        "status": "accepted",
    }


async def test_duplicate_slug_is_rejected_and_nothing_is_written(
    service: TenantService, db: AsyncEngine
) -> None:
    await service.create_tenant(slug="acme", name="Acme")
    with pytest.raises(TenantAlreadyExists):
        await service.create_tenant(slug="acme", name="Other")
    assert len(await fetch_all(db, "SELECT 1 FROM tasks")) == 1
    assert len(await fetch_all(db, "SELECT 1 FROM outbox")) == 1


async def test_slug_of_destroyed_tenant_cannot_be_reused(
    service: TenantService, db: AsyncEngine
) -> None:
    tenant = await service.create_tenant(slug="acme", name="Acme")
    await set_status(db, tenant["id"], "destroyed")
    with pytest.raises(TenantAlreadyExists):
        await service.create_tenant(slug="acme", name="Again")


async def test_update_from_active(service: TenantService, db: AsyncEngine) -> None:
    tenant = await service.create_tenant(slug="acme", name="Acme")
    await set_status(db, tenant["id"], "active")  # version 2

    updated = await service.update_tenant(tenant["id"], expected_version=2, name="Acme Corp")

    assert updated["status"] == "updating"
    assert updated["version"] == 3
    assert updated["name"] == "Acme Corp"
    assert updated["current_task"]["type"] == "update"
    assert len(await fetch_all(db, "SELECT 1 FROM outbox")) == 2


async def test_update_precedence_not_found(service: TenantService) -> None:
    with pytest.raises(TenantNotFound):
        await service.update_tenant(uuid.uuid4(), expected_version=1, name="x")


async def test_update_precedence_version_conflict_before_not_allowed(
    service: TenantService,
) -> None:
    tenant = await service.create_tenant(slug="acme", name="Acme")  # provisioning, v1
    with pytest.raises(TenantVersionConflict) as exc:
        await service.update_tenant(tenant["id"], expected_version=7, name="x")
    assert exc.value.details == {"expected_version": 7, "current_version": 1}


@pytest.mark.parametrize(
    "status", ["provisioning", "updating", "destroying", "destroyed", "failed"]
)
async def test_update_not_allowed_unless_active(
    service: TenantService, db: AsyncEngine, status: str
) -> None:
    tenant = await service.create_tenant(slug="acme", name="Acme")
    await set_status(db, tenant["id"], status)  # v2
    with pytest.raises(TenantUpdateNotAllowed) as exc:
        await service.update_tenant(tenant["id"], expected_version=2, name="x")
    assert exc.value.details["current_status"] == status


@pytest.mark.parametrize("status", ["active", "failed"])
async def test_delete_from_active_or_failed(
    service: TenantService, db: AsyncEngine, status: str
) -> None:
    tenant = await service.create_tenant(slug="acme", name="Acme")
    await set_status(db, tenant["id"], status)

    deleted = await service.delete_tenant(tenant["id"])

    assert deleted["status"] == "destroying"
    assert deleted["version"] == 3
    assert deleted["current_task"]["type"] == "destroy"


@pytest.mark.parametrize("status", ["provisioning", "updating", "destroying", "destroyed"])
async def test_delete_not_allowed(service: TenantService, db: AsyncEngine, status: str) -> None:
    tenant = await service.create_tenant(slug="acme", name="Acme")
    if status != "provisioning":
        await set_status(db, tenant["id"], status)
    with pytest.raises(TenantUpdateNotAllowed):
        await service.delete_tenant(tenant["id"])


async def test_delete_unknown(service: TenantService) -> None:
    with pytest.raises(TenantNotFound):
        await service.delete_tenant(uuid.uuid4())


async def test_rejected_mutations_do_not_write(service: TenantService, db: AsyncEngine) -> None:
    tenant = await service.create_tenant(slug="acme", name="Acme")
    for attempt in (
        service.update_tenant(tenant["id"], expected_version=1, name="x"),
        service.delete_tenant(tenant["id"]),
    ):
        with pytest.raises(TenantUpdateNotAllowed):
            await attempt
    rows = await fetch_all(db, "SELECT version, name FROM tenants")
    assert rows == [{"version": 1, "name": "Acme"}]
    assert len(await fetch_all(db, "SELECT 1 FROM outbox")) == 1


async def test_one_open_task_safety_net(service: TenantService, db: AsyncEngine) -> None:
    """Even a buggy code path could not create a second open task for one tenant."""
    tenant = await service.create_tenant(slug="acme", name="Acme")
    with pytest.raises(IntegrityError, match="tasks_one_open_per_tenant"):
        async with db.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO tasks (id, tenant_id, type, status, created_at, updated_at) "
                    "VALUES (gen_random_uuid(), :t, 'update', 'accepted', now(), now())"
                ),
                {"t": tenant["id"]},
            )
