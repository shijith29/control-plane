"""Tenant resource: full CRUD. Mutations start tasks and return immediately."""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Query, Request, Response, status

from control_plane.api.deps import ServiceDep
from control_plane.api.pagination import DEFAULT_LIMIT, MAX_LIMIT, decode_cursor, encode_cursor
from control_plane.api.schemas import (
    ErrorEnvelope,
    Page,
    TaskOut,
    TenantCreate,
    TenantOut,
    TenantPatch,
)
from control_plane.domain.states import TaskStatus, TaskType, TenantStatus

router = APIRouter(prefix="/v1/tenants", tags=["tenants"])

Limit = Annotated[int, Query(ge=1, le=MAX_LIMIT)]

Responses = dict[int | str, dict[str, Any]]

_404: Responses = {404: {"model": ErrorEnvelope, "description": "tenant_not_found"}}
_409: Responses = {409: {"model": ErrorEnvelope, "description": "conflict (see error.code)"}}
_422: Responses = {422: {"model": ErrorEnvelope, "description": "validation_error"}}


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=TenantOut,
    responses={**_409, **_422},
    summary="Create a tenant (starts a deploy task)",
)
async def create_tenant(
    body: TenantCreate, request: Request, response: Response, service: ServiceDep
) -> TenantOut:
    tenant = await service.create_tenant(slug=body.slug, name=body.name)
    response.headers["Location"] = str(
        request.url_for("get_tenant", tenant_id=str(tenant["id"])).path
    )
    return TenantOut.model_validate(tenant)


@router.get("", response_model=Page[TenantOut], responses=_422, summary="List tenants")
async def list_tenants(
    service: ServiceDep,
    status_: Annotated[TenantStatus | None, Query(alias="status")] = None,
    slug: str | None = None,
    limit: Limit = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> Page[TenantOut]:
    items, next_cursor = await service.list_tenants(
        status=status_, slug=slug, after=decode_cursor(cursor), limit=limit
    )
    return Page[TenantOut](
        items=[TenantOut.model_validate(t) for t in items], next_cursor=encode_cursor(next_cursor)
    )


@router.get("/{tenant_id}", response_model=TenantOut, responses={**_404, **_422})
async def get_tenant(tenant_id: UUID, service: ServiceDep) -> TenantOut:
    return TenantOut.model_validate(await service.get_tenant(tenant_id))


@router.patch(
    "/{tenant_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=TenantOut,
    responses={**_404, **_409, **_422},
    summary="Partially update a tenant (starts an update task); requires the current version",
)
async def patch_tenant(tenant_id: UUID, body: TenantPatch, service: ServiceDep) -> TenantOut:
    tenant = await service.update_tenant(tenant_id, expected_version=body.version, name=body.name)
    return TenantOut.model_validate(tenant)


@router.delete(
    "/{tenant_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=TenantOut,
    responses={**_404, **_409, **_422},
    summary="Delete a tenant (starts a destroy task); the tenant stays readable",
)
async def delete_tenant(tenant_id: UUID, service: ServiceDep) -> TenantOut:
    return TenantOut.model_validate(await service.delete_tenant(tenant_id))


@router.get(
    "/{tenant_id}/tasks",
    response_model=Page[TaskOut],
    responses={**_404, **_422},
    summary="Task history of one tenant",
)
async def list_tenant_tasks(
    tenant_id: UUID,
    service: ServiceDep,
    status_: Annotated[TaskStatus | None, Query(alias="status")] = None,
    type_: Annotated[TaskType | None, Query(alias="type")] = None,
    limit: Limit = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> Page[TaskOut]:
    items, next_cursor = await service.list_tasks(
        tenant_id=tenant_id,
        status=status_,
        task_type=type_,
        after=decode_cursor(cursor),
        limit=limit,
        require_tenant=True,
    )
    return Page[TaskOut](
        items=[TaskOut.model_validate(t) for t in items], next_cursor=encode_cursor(next_cursor)
    )
