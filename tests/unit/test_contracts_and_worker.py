"""Wire contracts and the worker's (pure) planning logic."""

from __future__ import annotations

import random
import uuid

import pytest
from pydantic import ValidationError

from control_plane.messaging.contracts import (
    EventTask,
    EventTenant,
    TaskEvent,
    TaskUpdate,
    deterministic_update_id,
)
from control_plane.timeutil import now_ms
from worker.__main__ import parse_args
from worker.simulator import WorkerConfig, plan_for


def make_event() -> TaskEvent:
    task_id = uuid.uuid4()
    return TaskEvent(
        event_id=task_id,
        occurred_at=now_ms(),
        task=EventTask(id=task_id, type="deploy", tenant_id=uuid.uuid4(), status="accepted"),
        tenant=EventTenant(slug="acme", name="Acme"),
    )


def test_task_event_exposes_required_task_fields() -> None:
    event = make_event()
    wire = event.model_dump(mode="json")
    assert set(wire["task"]) == {"id", "type", "tenant_id", "status"}
    assert wire["event_id"] == wire["task"]["id"]
    assert wire["occurred_at"].endswith("Z")
    assert TaskEvent.model_validate(wire) == event


def test_update_id_is_deterministic_per_task_and_status() -> None:
    task_id = uuid.uuid4()
    assert deterministic_update_id(task_id, "done") == deterministic_update_id(task_id, "done")
    assert deterministic_update_id(task_id, "done") != deterministic_update_id(
        task_id, "in_progress"
    )
    assert deterministic_update_id(task_id, "done") != deterministic_update_id(uuid.uuid4(), "done")


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"{}",
        b'{"update_id": "x", "task_id": "y", "status": "done"}',
        b'{"update_id": "%s", "task_id": "%s", "status": "accepted"}'
        % (str(uuid.uuid4()).encode(), str(uuid.uuid4()).encode()),
        b'{"update_id": "%s", "task_id": "%s", "status": "done", "schema_version": 2}'
        % (str(uuid.uuid4()).encode(), str(uuid.uuid4()).encode()),
    ],
)
def test_invalid_updates_are_rejected(body: bytes) -> None:
    with pytest.raises(ValidationError):
        TaskUpdate.model_validate_json(body)


def test_update_ignores_unknown_fields_for_forward_compatibility() -> None:
    body = (
        f'{{"update_id": "{uuid.uuid4()}", "task_id": "{uuid.uuid4()}", "status": "done", "x": 1}}'
    )
    assert TaskUpdate.model_validate_json(body).status == "done"


def test_plan_happy_path() -> None:
    event = make_event()
    plan = plan_for(event, WorkerConfig(min_delay_ms=10, max_delay_ms=20), random.Random(1))
    assert [u.status for u in plan.updates] == ["in_progress", "done"]
    assert all(u.task_id == event.task.id for u in plan.updates)
    assert 0.01 <= plan.delay_s <= 0.02
    assert not plan.duplicate


def test_plan_always_fails_at_rate_one() -> None:
    plan = plan_for(make_event(), WorkerConfig(fail_rate=1.0), random.Random(1))
    final = plan.updates[-1]
    assert final.status == "failed"
    assert final.error is not None and "fail-rate" in final.error.reason


def test_plan_reorders_and_duplicates() -> None:
    plan = plan_for(
        make_event(), WorkerConfig(reorder_rate=1.0, duplicate_rate=1.0), random.Random(1)
    )
    assert [u.status for u in plan.updates] == ["done", "in_progress"]
    assert plan.duplicate


def test_replayed_task_produces_identical_update_ids() -> None:
    event = make_event()
    first = plan_for(event, WorkerConfig(), random.Random(1))
    replay = plan_for(event, WorkerConfig(), random.Random(2))
    assert [u.update_id for u in first.updates] == [u.update_id for u in replay.updates]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"fail_rate": 1.5},
        {"duplicate_rate": -0.1},
        {"min_delay_ms": 10, "max_delay_ms": 5},
        {"min_delay_ms": -1},
        {"prefetch": 0},
    ],
)
def test_worker_config_validation(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        WorkerConfig(**kwargs)  # type: ignore[arg-type]


def test_cli_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RABBITMQ_URL", "amqp://x")
    monkeypatch.setenv("WORKER_MAX_DELAY_MS", "9000")
    url, config, level = parse_args(["--fail-rate=1", "--min-delay-ms=100", "--prefetch", "2"])
    assert url == "amqp://x"
    assert config == WorkerConfig(fail_rate=1.0, min_delay_ms=100, max_delay_ms=9000, prefetch=2)
    assert level == "INFO"


def test_cli_rejects_bad_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RABBITMQ_URL", "amqp://x")
    with pytest.raises(SystemExit):
        parse_args(["--fail-rate=2"])


def test_cli_requires_broker_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RABBITMQ_URL", raising=False)
    with pytest.raises(SystemExit):
        parse_args([])
