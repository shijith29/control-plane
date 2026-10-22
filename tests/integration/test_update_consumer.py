"""Inbound updates: idempotent, order-tolerant, poison-safe (README §5)."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from control_plane.messaging.contracts import TaskUpdate
from control_plane.messaging.updates_consumer import (
    Disposition,
    Outcome,
    UpdateApplier,
    UpdatesConsumer,
)
from control_plane.service import TenantService
from tests.conftest import fetch_all

pytestmark = pytest.mark.db


def upd(
    task_id: uuid.UUID, status: str, update_id: uuid.UUID | None = None, **kw: Any
) -> TaskUpdate:
    return TaskUpdate(update_id=update_id or uuid.uuid4(), task_id=task_id, status=status, **kw)  # type: ignore[arg-type]


@pytest.fixture
def applier(db: AsyncEngine) -> UpdateApplier:
    return UpdateApplier(db)


async def new_tenant(service: TenantService, slug: str = "acme") -> tuple[uuid.UUID, uuid.UUID]:
    tenant = await service.create_tenant(slug=slug, name="Acme")
    return tenant["id"], tenant["current_task"]["id"]


async def state(service: TenantService, tenant_id: uuid.UUID) -> tuple[str, int, str]:
    tenant = await service.get_tenant(tenant_id)
    return tenant["status"], tenant["version"], tenant["current_task"]["status"]


async def test_happy_path_deploy(service: TenantService, applier: UpdateApplier) -> None:
    tenant_id, task_id = await new_tenant(service)

    assert await applier.apply(upd(task_id, "in_progress")) is Outcome.APPLIED
    # in_progress changes the task only — the tenant row (and its version) is untouched
    assert await state(service, tenant_id) == ("provisioning", 1, "in_progress")

    assert await applier.apply(upd(task_id, "done")) is Outcome.APPLIED
    assert await state(service, tenant_id) == ("active", 2, "done")


@pytest.mark.parametrize(
    ("setup", "outcome", "tenant_status"),
    [
        ("deploy", "failed", "failed"),
        ("update", "done", "active"),
        ("update", "failed", "failed"),
        ("destroy", "done", "destroyed"),
        ("destroy", "failed", "failed"),
    ],
)
async def test_terminal_outcomes_move_tenant(
    service: TenantService, applier: UpdateApplier, setup: str, outcome: str, tenant_status: str
) -> None:
    tenant_id, task_id = await new_tenant(service)
    if setup != "deploy":
        await applier.apply(upd(task_id, "done"))  # -> active, v2
        tenant = (
            await service.update_tenant(tenant_id, expected_version=2, name="B")
            if setup == "update"
            else await service.delete_tenant(tenant_id)
        )
        task_id = tenant["current_task"]["id"]

    await applier.apply(upd(task_id, "in_progress"))
    assert await applier.apply(upd(task_id, outcome)) is Outcome.APPLIED
    assert (await service.get_tenant(tenant_id))["status"] == tenant_status


async def test_failure_reason_is_stored(service: TenantService, applier: UpdateApplier) -> None:
    _, task_id = await new_tenant(service)
    await applier.apply(upd(task_id, "failed", error={"reason": "quota exceeded"}))
    assert (await service.get_task(task_id))["error"] == "quota exceeded"


async def test_duplicate_update_changes_nothing(
    service: TenantService, applier: UpdateApplier, db: AsyncEngine
) -> None:
    _, task_id = await new_tenant(service)
    done = upd(task_id, "done")

    assert await applier.apply(done) is Outcome.APPLIED
    before = await fetch_all(db, "SELECT * FROM tenants")
    assert await applier.apply(done) is Outcome.DUPLICATE
    assert await applier.apply(done) is Outcome.DUPLICATE
    assert await fetch_all(db, "SELECT * FROM tenants") == before  # version not bumped again
    assert len(await fetch_all(db, "SELECT 1 FROM processed_updates")) == 1


async def test_in_progress_after_done_is_stale(
    service: TenantService, applier: UpdateApplier, db: AsyncEngine
) -> None:
    tenant_id, task_id = await new_tenant(service)
    await applier.apply(upd(task_id, "done"))  # accepted -> done directly is allowed

    late = upd(task_id, "in_progress")
    assert await applier.apply(late) is Outcome.STALE
    assert await state(service, tenant_id) == ("active", 2, "done")
    recorded = await fetch_all(
        db, "SELECT outcome FROM processed_updates WHERE update_id = :u", u=late.update_id
    )
    assert recorded == [{"outcome": "stale"}]


@pytest.mark.parametrize("late", ["done", "failed", "in_progress"])
async def test_any_update_for_a_terminal_task_is_stale(
    service: TenantService, applier: UpdateApplier, late: str
) -> None:
    tenant_id, task_id = await new_tenant(service)
    await applier.apply(upd(task_id, "in_progress"))
    await applier.apply(upd(task_id, "done"))

    assert await applier.apply(upd(task_id, late)) is Outcome.STALE
    assert await state(service, tenant_id) == ("active", 2, "done")


async def test_unknown_task_is_recorded_and_ignored(
    applier: UpdateApplier, db: AsyncEngine
) -> None:
    update = upd(uuid.uuid4(), "done")
    assert await applier.apply(update) is Outcome.UNKNOWN_TASK
    assert await applier.apply(update) is Outcome.DUPLICATE
    rows = await fetch_all(db, "SELECT outcome FROM processed_updates")
    assert rows == [{"outcome": "unknown_task"}]


# ------------------------------------------------------------------------- message handling


class FlakyApplier:
    def __init__(self, failures: int, outcome: Outcome = Outcome.APPLIED) -> None:
        self.failures = failures
        self.calls = 0
        self.outcome = outcome

    async def apply(self, update: TaskUpdate) -> Outcome:
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionResetError("db connection lost")
        return self.outcome


def consumer(applier: Any, retries: int = 5) -> UpdatesConsumer:
    return UpdatesConsumer(applier, max_retries=retries, retry_base_s=0.001, retry_cap_s=0.002)


VALID = b'{"update_id": "%s", "task_id": "%s", "status": "done"}' % (
    str(uuid.uuid4()).encode(),
    str(uuid.uuid4()).encode(),
)


@pytest.mark.parametrize(
    "body",
    [b"not json at all", b"", b"{}", b'{"update_id": 1}', b"\xff\xfe", b"[]", b"null"],
)
async def test_poison_messages_are_rejected_not_raised(body: bytes) -> None:
    flaky = FlakyApplier(failures=0)
    assert await consumer(flaky).handle(body) is Disposition.REJECT
    assert flaky.calls == 0


async def test_transient_errors_are_retried_then_acked() -> None:
    flaky = FlakyApplier(failures=3)
    assert await consumer(flaky, retries=5).handle(VALID) is Disposition.ACK
    assert flaky.calls == 4


async def test_persistent_errors_are_requeued_not_raised() -> None:
    flaky = FlakyApplier(failures=100)
    assert await consumer(flaky, retries=3).handle(VALID) is Disposition.REQUEUE
    assert flaky.calls == 3


@pytest.mark.parametrize("outcome", list(Outcome))
async def test_every_outcome_is_acked(outcome: Outcome) -> None:
    assert await consumer(FlakyApplier(0, outcome)).handle(VALID) is Disposition.ACK


async def test_real_applier_through_handle(service: TenantService, applier: UpdateApplier) -> None:
    tenant_id, task_id = await new_tenant(service)
    body = upd(task_id, "done").model_dump_json().encode()
    assert await consumer(applier).handle(body) is Disposition.ACK
    assert await consumer(applier).handle(body) is Disposition.ACK  # duplicate is still acked
    assert (await service.get_tenant(tenant_id))["version"] == 2
