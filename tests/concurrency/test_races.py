"""Race conditions, fired with real parallel transactions (README §8).

Each test launches N coroutines at once on separate pooled connections and asserts that
exactly one wins — the database, not Python, is what serialises them.
"""

from __future__ import annotations

import asyncio
import random
import uuid
from collections import Counter

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from control_plane.domain.errors import (
    DomainError,
    TenantAlreadyExists,
    TenantUpdateNotAllowed,
    TenantVersionConflict,
)
from control_plane.messaging.contracts import TaskUpdate
from control_plane.messaging.updates_consumer import Outcome, UpdateApplier
from control_plane.service import TenantService
from tests.conftest import fetch_all

pytestmark = pytest.mark.db

N = 50


async def gather_outcomes(coros: list) -> Counter[str]:  # type: ignore[type-arg]
    results = await asyncio.gather(*coros, return_exceptions=True)
    counter: Counter[str] = Counter()
    for r in results:
        if isinstance(r, DomainError):
            counter[r.code] += 1
        elif isinstance(r, BaseException):
            raise r
        else:
            counter["ok"] += 1
    return counter


async def make_active(service: TenantService, db: AsyncEngine, slug: str = "race") -> uuid.UUID:
    tenant = await service.create_tenant(slug=slug, name="Race")
    await UpdateApplier(db).apply(
        TaskUpdate(update_id=uuid.uuid4(), task_id=tenant["current_task"]["id"], status="done")
    )
    return uuid.UUID(str(tenant["id"]))  # active, version 2


async def test_concurrent_creates_same_slug_exactly_one_wins(
    service: TenantService, db: AsyncEngine
) -> None:
    outcomes = await gather_outcomes(
        [service.create_tenant(slug="contested", name=f"n{i}") for i in range(N)]
    )
    assert outcomes == Counter({"ok": 1, TenantAlreadyExists.code: N - 1})
    assert len(await fetch_all(db, "SELECT 1 FROM tenants")) == 1
    assert len(await fetch_all(db, "SELECT 1 FROM tasks")) == 1
    assert len(await fetch_all(db, "SELECT 1 FROM outbox")) == 1  # losers published nothing


async def test_concurrent_patches_same_version_exactly_one_wins(
    service: TenantService, db: AsyncEngine
) -> None:
    tenant_id = await make_active(service, db)

    outcomes = await gather_outcomes(
        [service.update_tenant(tenant_id, expected_version=2, name=f"n{i}") for i in range(N)]
    )

    # the winner moves the tenant to `updating` v3; every loser then sees a stale version
    assert outcomes == Counter({"ok": 1, TenantVersionConflict.code: N - 1})
    tenant = await service.get_tenant(tenant_id)
    assert (tenant["status"], tenant["version"]) == ("updating", 3)
    tasks = await fetch_all(db, "SELECT type FROM tasks WHERE tenant_id = :t", t=tenant_id)
    assert sorted(t["type"] for t in tasks) == ["deploy", "update"]


async def test_patch_racing_delete_has_exactly_one_winner(
    service: TenantService, db: AsyncEngine
) -> None:
    for round_ in range(10):  # repeat to vary which one wins
        tenant_id = await make_active(service, db, slug=f"race-{round_}")
        coros = [
            service.update_tenant(tenant_id, expected_version=2, name="x"),
            service.delete_tenant(tenant_id),
        ]
        random.shuffle(coros)
        outcomes = await gather_outcomes(coros)

        assert outcomes["ok"] == 1
        assert sum(outcomes.values()) == 2
        assert set(outcomes) <= {"ok", TenantUpdateNotAllowed.code, TenantVersionConflict.code}
        open_tasks = await fetch_all(
            db,
            "SELECT 1 FROM tasks WHERE tenant_id = :t AND status IN ('accepted','in_progress')",
            t=tenant_id,
        )
        assert len(open_tasks) == 1  # still at most one open task per tenant


async def test_same_update_delivered_concurrently_is_applied_once(
    service: TenantService, db: AsyncEngine
) -> None:
    tenant = await service.create_tenant(slug="dup", name="Dup")
    update = TaskUpdate(update_id=uuid.uuid4(), task_id=tenant["current_task"]["id"], status="done")
    applier = UpdateApplier(db)

    outcomes = Counter(await asyncio.gather(*[applier.apply(update) for _ in range(N)]))

    assert outcomes == Counter({Outcome.APPLIED: 1, Outcome.DUPLICATE: N - 1})
    tenant_after = await service.get_tenant(tenant["id"])
    assert (tenant_after["status"], tenant_after["version"]) == ("active", 2)


async def test_out_of_order_concurrent_updates_never_regress(
    service: TenantService, db: AsyncEngine
) -> None:
    applier = UpdateApplier(db)
    for i in range(20):
        tenant = await service.create_tenant(slug=f"order-{i:02d}", name="x")
        task_id = tenant["current_task"]["id"]
        updates = [
            TaskUpdate(update_id=uuid.uuid4(), task_id=task_id, status=s)
            for s in ("in_progress", "done", "in_progress", "done")  # dupes with fresh ids too
        ]
        random.shuffle(updates)
        await asyncio.gather(*[applier.apply(u) for u in updates])

        final = await service.get_tenant(tenant["id"])
        assert (final["status"], final["current_task"]["status"]) == ("active", "done")
        assert final["version"] == 2  # exactly one terminal transition was applied


async def test_api_level_create_race(client: httpx.AsyncClient) -> None:
    responses = await asyncio.gather(
        *[client.post("/v1/tenants", json={"slug": "http-race", "name": "x"}) for _ in range(20)]
    )
    codes = Counter(r.status_code for r in responses)
    assert codes == Counter({201: 1, 409: 19})
    assert {r.json()["error"]["code"] for r in responses if r.status_code == 409} == {
        "tenant_already_exists"
    }


async def test_api_level_patch_race(client: httpx.AsyncClient, db: AsyncEngine) -> None:
    created = (await client.post("/v1/tenants", json={"slug": "http-patch", "name": "x"})).json()
    async with db.begin() as conn:
        await conn.execute(
            text("UPDATE tasks SET status='done' WHERE tenant_id=:t"), {"t": created["id"]}
        )
        await conn.execute(
            text("UPDATE tenants SET status='active', version=2 WHERE id=:t"), {"t": created["id"]}
        )
    url = f"/v1/tenants/{created['id']}"
    responses = await asyncio.gather(
        *[client.patch(url, json={"version": 2, "name": f"n{i}"}) for i in range(20)]
    )
    assert Counter(r.status_code for r in responses) == Counter({202: 1, 409: 19})
