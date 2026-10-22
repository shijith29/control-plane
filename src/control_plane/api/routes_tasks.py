"""Task resource: read-only. Tasks are created by tenant calls and driven by the worker."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from control_plane.api.deps import ServiceDep
from control_plane.api.pagination import DEFAULT_LIMIT, MAX_LIMIT, decode_cursor, encode_cursor
from control_plane.api.schemas import ErrorEnvelope, Page, TaskOut
from control_plane.domain.states import TaskStatus, TaskType

router = APIRouter(prefix="/v1/tasks", tags=["tasks"])


@router.get(
    "",
    response_model=Page[TaskOut],
    responses={422: {"model": ErrorEnvelope}},
    summary="List tasks",
)
async def list_tasks(
    service: ServiceDep,
    tenant_id: UUID | None = None,
    status_: Annotated[TaskStatus | None, Query(alias="status")] = None,
    type_: Annotated[TaskType | None, Query(alias="type")] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> Page[TaskOut]:
    items, next_cursor = await service.list_tasks(
        tenant_id=tenant_id,
        status=status_,
        task_type=type_,
        after=decode_cursor(cursor),
        limit=limit,
    )
    return Page[TaskOut](
        items=[TaskOut.model_validate(t) for t in items], next_cursor=encode_cursor(next_cursor)
    )


@router.get(
    "/{task_id}",
    response_model=TaskOut,
    responses={404: {"model": ErrorEnvelope, "description": "task_not_found"}},
)
async def get_task(task_id: UUID, service: ServiceDep) -> TaskOut:
    return TaskOut.model_validate(await service.get_task(task_id))
