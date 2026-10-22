"""Request/response models. Validation failures become ``422 validation_error``."""

from __future__ import annotations

from typing import Annotated, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StringConstraints, model_validator

from control_plane.db.tables import SLUG_PATTERN
from control_plane.domain.states import TaskStatus, TaskType, TenantStatus
from control_plane.timeutil import UtcMillis

Slug = Annotated[str, StringConstraints(min_length=3, max_length=28, pattern=SLUG_PATTERN)]
Name = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]


class TenantCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slug: Slug = Field(examples=["acme-corp"])
    name: Name = Field(examples=["Acme Corp"])


class TenantPatch(BaseModel):
    """Partial update. ``version`` is the optimistic-locking token from the last read."""

    model_config = ConfigDict(extra="forbid")

    version: StrictInt = Field(ge=1, examples=[2])
    name: Name | None = Field(default=None, examples=["Acme Corporation"])

    @model_validator(mode="after")
    def _has_a_change(self) -> Self:
        if self.name is None:
            raise ValueError("at least one mutable field (name) must be provided")
        return self


class TaskSummary(BaseModel):
    id: UUID
    type: TaskType
    status: TaskStatus


class TenantOut(BaseModel):
    id: UUID
    slug: str
    name: str
    status: TenantStatus
    version: int
    current_task: TaskSummary | None
    created_at: UtcMillis
    updated_at: UtcMillis


class TaskOut(BaseModel):
    id: UUID
    type: TaskType
    tenant_id: UUID
    status: TaskStatus
    error: str | None
    created_at: UtcMillis
    updated_at: UtcMillis


class Page[T](BaseModel):
    items: list[T]
    next_cursor: str | None


class ErrorBody(BaseModel):
    code: str
    message: str
    details: dict[str, object] = Field(default_factory=dict)


class ErrorEnvelope(BaseModel):
    error: ErrorBody
    request_id: str | None
