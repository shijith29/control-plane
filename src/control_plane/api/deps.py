from __future__ import annotations

from typing import Annotated, cast

from fastapi import Depends, Request

from control_plane.service import TenantService


def get_service(request: Request) -> TenantService:
    return cast(TenantService, request.app.state.service)


ServiceDep = Annotated[TenantService, Depends(get_service)]
