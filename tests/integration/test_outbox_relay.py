"""Transactional outbox: publish only after commit, never on rollback, survive broker failures."""

from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from control_plane.db import tasks_repo
from control_plane.messaging.outbox_relay import OutboxRelay
from control_plane.service import TenantService
from control_plane.timeutil import now_ms
from tests.conftest import fetch_all

pytestmark = pytest.mark.db


class FakePublisher:
    """Records publishes; can be told to fail N times or to fail *after* sending."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, dict, str]] = []  # type: ignore[type-arg]
        self.fail_next = 0
        self.fail_after_send = False

    async def publish(self, routing_key: str, body: bytes, *, message_id: str) -> None:
        if self.fail_next:
            self.fail_next -= 1
            raise ConnectionError("broker down")
        self.sent.append((routing_key, json.loads(body), message_id))
        if self.fail_after_send:
            self.fail_after_send = False
            raise TimeoutError("confirm lost")


@pytest.fixture
def publisher() -> FakePublisher:
    return FakePublisher()


@pytest.fixture
def relay(db: AsyncEngine, publisher: FakePublisher) -> OutboxRelay:
    return OutboxRelay(db, publisher, batch_size=10)


async def test_committed_write_is_published_once(
    service: TenantService, relay: OutboxRelay, publisher: FakePublisher, db: AsyncEngine
) -> None:
    tenant = await service.create_tenant(slug="acme", name="Acme")

    result = await relay.publish_batch()

    assert result.published == 1 and result.error is None
    routing_key, payload, message_id = publisher.sent[0]
    assert routing_key == "task.created"
    assert payload["task"]["id"] == message_id == payload["event_id"]
    assert payload["task"]["tenant_id"] == str(tenant["id"])
    assert payload["task"]["status"] == "accepted"
    assert payload["tenant"] == {"slug": "acme", "name": "Acme"}
    row = (await fetch_all(db, "SELECT published_at, attempts FROM outbox"))[0]
    assert row["published_at"] is not None and row["attempts"] == 1

    assert (await relay.publish_batch()).published == 0  # nothing left
    assert len(publisher.sent) == 1


async def test_rolled_back_write_is_never_published(
    db: AsyncEngine, relay: OutboxRelay, publisher: FakePublisher
) -> None:
    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        async with db.begin() as conn:
            await tasks_repo.insert_outbox(
                conn, event_id=uuid.uuid4(), routing_key="task.created", payload={}, now=now_ms()
            )
            raise Boom  # e.g. the task insert failed after the outbox row was written

    assert (await relay.publish_batch()).published == 0
    assert publisher.sent == []
    assert await fetch_all(db, "SELECT 1 FROM outbox") == []


async def test_uncommitted_write_is_invisible_to_the_relay(
    db: AsyncEngine, relay: OutboxRelay, publisher: FakePublisher
) -> None:
    async with db.begin() as conn:
        await tasks_repo.insert_outbox(
            conn, event_id=uuid.uuid4(), routing_key="task.created", payload={}, now=now_ms()
        )
        # transaction still open: the relay must not see the row
        assert (await relay.publish_batch()).published == 0
    assert (await relay.publish_batch()).published == 1


async def test_broker_down_keeps_row_pending_then_recovers(
    service: TenantService, relay: OutboxRelay, publisher: FakePublisher, db: AsyncEngine
) -> None:
    await service.create_tenant(slug="acme", name="Acme")
    publisher.fail_next = 2

    for _ in range(2):
        result = await relay.publish_batch()
        assert result.published == 0 and isinstance(result.error, ConnectionError)

    row = (await fetch_all(db, "SELECT published_at, attempts, last_error FROM outbox"))[0]
    assert row["published_at"] is None
    assert row["attempts"] == 2
    assert "broker down" in row["last_error"]

    assert (await relay.publish_batch()).published == 1
    row = (await fetch_all(db, "SELECT published_at, last_error FROM outbox"))[0]
    assert row["published_at"] is not None and row["last_error"] is None


async def test_lost_confirm_republishes_at_least_once(
    service: TenantService, relay: OutboxRelay, publisher: FakePublisher
) -> None:
    await service.create_tenant(slug="acme", name="Acme")
    publisher.fail_after_send = True  # broker got it, but we never saw the confirm

    assert (await relay.publish_batch()).published == 0
    assert (await relay.publish_batch()).published == 1
    assert len(publisher.sent) == 2
    assert publisher.sent[0][2] == publisher.sent[1][2]  # same message_id: consumers can dedupe


async def test_failure_stops_the_batch_to_preserve_order(
    service: TenantService, relay: OutboxRelay, publisher: FakePublisher
) -> None:
    for i in range(3):
        await service.create_tenant(slug=f"tenant-{i}", name="x")
    publisher.fail_next = 1

    result = await relay.publish_batch()
    assert result.published == 0  # first row failed; later rows were not attempted
    await relay.publish_batch()
    slugs = [payload["tenant"]["slug"] for _, payload, _ in publisher.sent]
    assert slugs == ["tenant-0", "tenant-1", "tenant-2"]


async def test_two_relays_never_publish_the_same_row(
    service: TenantService, db: AsyncEngine
) -> None:
    import asyncio

    for i in range(20):
        await service.create_tenant(slug=f"tenant-{i:02d}", name="x")

    class SlowPublisher(FakePublisher):
        async def publish(self, routing_key: str, body: bytes, *, message_id: str) -> None:
            await asyncio.sleep(0.005)
            await super().publish(routing_key, body, message_id=message_id)

    p1, p2 = SlowPublisher(), SlowPublisher()
    r1, r2 = OutboxRelay(db, p1, batch_size=5), OutboxRelay(db, p2, batch_size=5)
    while True:
        a, b = await asyncio.gather(r1.publish_batch(), r2.publish_batch())
        if a.published == b.published == 0:
            break
    ids = [m for _, _, m in p1.sent + p2.sent]
    assert len(ids) == len(set(ids)) == 20
    assert p1.sent and p2.sent  # SKIP LOCKED let both make progress
