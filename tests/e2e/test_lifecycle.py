"""Full stack: HTTP API + outbox relay + RabbitMQ + worker simulator + updates consumer.

Everything is real except the process boundary: the control plane app and the worker run in
this test process, talking to a real RabbitMQ and a real Postgres.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from asgi_lifespan import LifespanManager
from sqlalchemy.ext.asyncio import AsyncEngine

from control_plane.main import create_app
from control_plane.messaging.contracts import deterministic_update_id
from control_plane.messaging.topology import UPDATES_DLQ
from tests.conftest import AmqpHelper, eventually, fetch_all, make_settings
from worker.simulator import Worker, WorkerConfig

pytestmark = [pytest.mark.db, pytest.mark.broker]

FAST = {"min_delay_ms": 20, "max_delay_ms": 60}


class Stack:
    def __init__(self, client: httpx.AsyncClient, rabbitmq_url: str) -> None:
        self.client = client
        self.rabbitmq_url = rabbitmq_url
        self._worker: tuple[asyncio.Task[None], asyncio.Event] | None = None

    async def start_worker(self, **config: Any) -> None:
        await self.stop_worker()
        stop = asyncio.Event()
        worker = Worker(self.rabbitmq_url, WorkerConfig(**{**FAST, **config}))
        self._worker = (asyncio.create_task(worker.run(stop)), stop)

    async def stop_worker(self) -> None:
        if self._worker is not None:
            task, stop = self._worker
            stop.set()
            await task
            self._worker = None

    async def tenant(self, tenant_id: str) -> dict[str, Any]:
        return (await self.client.get(f"/v1/tenants/{tenant_id}")).json()  # type: ignore[no-any-return]

    async def wait_status(self, tenant_id: str, status: str, within: float = 15) -> dict[str, Any]:
        async def check() -> bool:
            return (await self.tenant(tenant_id))["status"] == status

        await eventually(check, within=within)
        return await self.tenant(tenant_id)

    async def create(self, slug: str) -> dict[str, Any]:
        response = await self.client.post("/v1/tenants", json={"slug": slug, "name": slug.title()})
        assert response.status_code == 201, response.text
        return response.json()  # type: ignore[no-any-return]


@pytest.fixture
async def stack(
    db: AsyncEngine, migrated_database: str, rabbitmq_url: str, amqp: AmqpHelper
) -> AsyncIterator[Stack]:
    app = create_app(make_settings(migrated_database, rabbitmq_url), engine=db)
    async with LifespanManager(app, startup_timeout=20, shutdown_timeout=20):
        await asyncio.wait_for(app.state.broker.wait_ready(), timeout=20)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            s = Stack(client, rabbitmq_url)
            try:
                yield s
            finally:
                with contextlib.suppress(Exception):
                    await s.stop_worker()


async def test_create_walks_full_lifecycle(stack: Stack) -> None:
    await stack.start_worker()
    tenant = await stack.create("acme-corp")
    assert tenant["status"] == "provisioning"

    final = await stack.wait_status(tenant["id"], "active")
    assert final["version"] == 2
    tasks = (await stack.client.get(f"/v1/tenants/{tenant['id']}/tasks")).json()["items"]
    assert [(t["type"], t["status"]) for t in tasks] == [("deploy", "done")]

    ready = (await stack.client.get("/readyz")).json()
    assert ready == {"ready": True, "database": "ok", "broker": "ok"}


async def test_update_and_destroy_lifecycle(stack: Stack) -> None:
    await stack.start_worker()
    tenant = await stack.create("lifecycle")
    await stack.wait_status(tenant["id"], "active")

    patched = await stack.client.patch(
        f"/v1/tenants/{tenant['id']}", json={"version": 2, "name": "Renamed"}
    )
    assert patched.status_code == 202
    after_update = await stack.wait_status(tenant["id"], "active")
    assert (after_update["name"], after_update["version"]) == ("Renamed", 4)

    assert (await stack.client.delete(f"/v1/tenants/{tenant['id']}")).status_code == 202
    destroyed = await stack.wait_status(tenant["id"], "destroyed")
    assert destroyed["version"] == 6

    tasks = (await stack.client.get(f"/v1/tenants/{tenant['id']}/tasks")).json()["items"]
    assert [(t["type"], t["status"]) for t in tasks] == [
        ("deploy", "done"),
        ("update", "done"),
        ("destroy", "done"),
    ]
    again = await stack.client.post("/v1/tenants", json={"slug": "lifecycle", "name": "x"})
    assert again.json()["error"]["code"] == "tenant_already_exists"


async def test_failing_worker_then_recovery_via_delete(stack: Stack) -> None:
    await stack.start_worker(fail_rate=1.0)
    tenant = await stack.create("doomed")
    failed = await stack.wait_status(tenant["id"], "failed")
    task = (await stack.client.get(f"/v1/tasks/{failed['current_task']['id']}")).json()
    assert task["status"] == "failed" and "fail-rate" in task["error"]

    # PATCH is not allowed from failed; DELETE is
    patch = await stack.client.patch(
        f"/v1/tenants/{tenant['id']}", json={"version": 2, "name": "x"}
    )
    assert patch.json()["error"]["code"] == "tenant_update_not_allowed"

    await stack.start_worker()  # restart the worker healthy
    assert (await stack.client.delete(f"/v1/tenants/{tenant['id']}")).status_code == 202
    await stack.wait_status(tenant["id"], "destroyed")


async def test_duplicates_and_reordering_from_worker_are_harmless(
    stack: Stack, db: AsyncEngine
) -> None:
    await stack.start_worker(duplicate_rate=1.0, reorder_rate=1.0)
    tenant = await stack.create("chaos")
    final = await stack.wait_status(tenant["id"], "active")

    async def all_recorded() -> bool:
        return len(await fetch_all(db, "SELECT 1 FROM processed_updates")) == 2

    await eventually(all_recorded)
    await asyncio.sleep(0.3)  # let any straggling duplicates arrive
    assert (await stack.tenant(tenant["id"]))["version"] == final["version"] == 2
    outcomes = await fetch_all(db, "SELECT status, outcome FROM processed_updates ORDER BY status")
    # done arrived first and was applied; the late in_progress was recorded as stale
    assert outcomes == [
        {"status": "done", "outcome": "applied"},
        {"status": "in_progress", "outcome": "stale"},
    ]


async def test_poison_message_is_dead_lettered_and_consumer_survives(
    stack: Stack, amqp: AmqpHelper
) -> None:
    await amqp.publish_update(b"not json at all")
    await amqp.publish_update(b'{"update_id": "nope"}')

    async def dead_lettered() -> bool:
        return await amqp.depth(UPDATES_DLQ) == 2

    await eventually(dead_lettered)

    await stack.start_worker()  # the consumer still processes real traffic
    tenant = await stack.create("after-poison")
    await stack.wait_status(tenant["id"], "active")


async def test_manual_out_of_order_and_duplicate_publishes(
    stack: Stack, amqp: AmqpHelper, db: AsyncEngine
) -> None:
    """The README §14-B scenarios, published straight onto the broker (no worker)."""
    tenant = await stack.create("manual")
    task_id = tenant["current_task"]["id"]
    done_id = uuid.uuid4()

    def body(status: str, update_id: uuid.UUID) -> str:
        return f'{{"update_id": "{update_id}", "task_id": "{task_id}", "status": "{status}"}}'

    await amqp.publish_update(body("done", done_id))
    await amqp.publish_update(body("done", done_id))  # duplicate
    await amqp.publish_update(body("in_progress", uuid.uuid4()))  # stale
    await amqp.publish_update(body("failed", uuid.uuid4()))  # update for a terminal task
    await amqp.publish_update(body("done", uuid.uuid4()).replace(task_id, str(uuid.uuid4())))

    async def settled() -> bool:
        return len(await fetch_all(db, "SELECT 1 FROM processed_updates")) == 4

    await eventually(settled)
    final = await stack.tenant(tenant["id"])
    assert (final["status"], final["version"]) == ("active", 2)
    rows = await fetch_all(db, "SELECT outcome FROM processed_updates")
    assert sorted(r["outcome"] for r in rows) == ["applied", "stale", "stale", "unknown_task"]


async def test_events_published_while_broker_unreachable_are_delivered_later(
    db: AsyncEngine, migrated_database: str, rabbitmq_url: str, amqp: AmqpHelper
) -> None:
    """The API keeps accepting writes with no broker; the outbox delivers them afterwards."""
    dead_broker = "amqp://guest:guest@127.0.0.1:1/"  # nothing listens on port 1
    app = create_app(make_settings(migrated_database, dead_broker), engine=db)
    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/v1/tenants", json={"slug": "offline", "name": "x"})
            assert response.status_code == 201
            ready = await client.get("/readyz")
            assert ready.status_code == 503 and ready.json()["broker"] == "unavailable"
    pending = await fetch_all(db, "SELECT published_at FROM outbox")
    assert pending == [{"published_at": None}]

    # broker "comes back": a control plane that can reach it drains the outbox
    app = create_app(make_settings(migrated_database, rabbitmq_url), engine=db)
    async with LifespanManager(app, startup_timeout=20, shutdown_timeout=20):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            stack = Stack(client, rabbitmq_url)
            await stack.start_worker()
            tenant_id = response.json()["id"]
            await stack.wait_status(tenant_id, "active")
            await stack.stop_worker()


async def test_worker_replay_uses_same_update_ids(stack: Stack, db: AsyncEngine) -> None:
    await stack.start_worker()
    tenant = await stack.create("replay")
    await stack.wait_status(tenant["id"], "active")
    task_id = uuid.UUID(tenant["current_task"]["id"])
    ids = {r["update_id"] for r in await fetch_all(db, "SELECT update_id FROM processed_updates")}
    assert ids == {
        deterministic_update_id(task_id, "in_progress"),
        deterministic_update_id(task_id, "done"),
    }


async def test_consumers_resubscribe_after_broker_drops_all_connections(
    stack: Stack, rabbitmq_mgmt_url: str
) -> None:
    """Regression: queues consumed via an undeclared handle were not re-subscribed after a
    reconnect, so the system silently stalled after a broker restart."""
    await stack.start_worker()
    first = await stack.create("before-drop")
    await stack.wait_status(first["id"], "active")

    async with httpx.AsyncClient(base_url=rabbitmq_mgmt_url, timeout=10) as mgmt:
        connections: list[dict[str, Any]] = []

        async def listed() -> bool:  # management stats refresh every ~5 s
            nonlocal connections
            connections = (await mgmt.get("/api/connections")).json()
            return len(connections) >= 2  # control plane + worker

        await eventually(listed, within=20, interval=0.5)
        for conn in connections:
            response = await mgmt.delete(
                f"/api/connections/{conn['name']}", headers={"X-Reason": "test: simulated restart"}
            )
            assert response.status_code in (204, 404)

    # the API keeps working, and once clients reconnect the lifecycle completes again
    second = await stack.create("after-drop")
    await stack.wait_status(second["id"], "active", within=45)
