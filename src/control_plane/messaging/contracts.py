"""Wire contracts for both directions over the broker (README §5).

* ``TaskEvent``  — control plane -> ``tasks``         (the task *is* the event)
* ``TaskUpdate`` — worker        -> ``task_updates``  (carries the idempotency key)
"""

from __future__ import annotations

import uuid
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from control_plane.domain.states import TaskStatus, TaskType
from control_plane.timeutil import UtcMillis

#: Namespace for deterministic update ids: uuid5(NS, f"{task_id}:{status}").
UPDATE_ID_NAMESPACE = UUID("8f3b6a52-6c0e-4c55-9b1e-2f7f0f6d1a41")

UpdateStatus = Literal["in_progress", "done", "failed"]


class EventTask(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    type: TaskType
    tenant_id: UUID
    status: TaskStatus


class EventTenant(BaseModel):
    model_config = ConfigDict(frozen=True)

    slug: str
    name: str


class TaskEvent(BaseModel):
    """Published once per state-changing API call, after the DB commit."""

    model_config = ConfigDict(frozen=True)

    schema_version: Literal[1] = 1
    event_id: UUID
    event_type: Literal["task.created"] = "task.created"
    occurred_at: UtcMillis
    task: EventTask
    tenant: EventTenant


class UpdateError(BaseModel):
    model_config = ConfigDict(frozen=True)

    reason: str = Field(max_length=2000)


class TaskUpdate(BaseModel):
    """Progress report from a worker. ``update_id`` is the idempotency key."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    schema_version: Literal[1] = 1
    update_id: UUID
    task_id: UUID
    status: UpdateStatus
    occurred_at: UtcMillis | None = None
    error: UpdateError | None = None

    @property
    def task_status(self) -> TaskStatus:
        return TaskStatus(self.status)


def deterministic_update_id(task_id: UUID, status: str) -> UUID:
    """Same task + same status => same id, so a worker that crashes and replays a task
    produces updates the control plane already knows about."""
    return uuid.uuid5(UPDATE_ID_NAMESPACE, f"{task_id}:{status}")
