"""One error envelope for every failure (README §7). Clients branch on ``error.code``.

{"error": {"code": "...", "message": "...", "details": {...}}, "request_id": "..."}
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from control_plane.domain.errors import DomainError
from control_plane.logs import request_id_var

log = logging.getLogger(__name__)

#: The documented code -> HTTP status mapping.
STATUS_BY_CODE: dict[str, int] = {
    "validation_error": 422,
    "tenant_not_found": 404,
    "task_not_found": 404,
    "tenant_already_exists": 409,
    "tenant_update_not_allowed": 409,
    "tenant_version_conflict": 409,
    "not_found": 404,
    "method_not_allowed": 405,
    "internal_error": 500,
}


def error_response(
    code: str, message: str, details: dict[str, Any] | None = None, status: int | None = None
) -> JSONResponse:
    return JSONResponse(
        status_code=status or STATUS_BY_CODE.get(code, 500),
        content={
            "error": {"code": code, "message": message, "details": details or {}},
            "request_id": request_id_var.get(),
        },
    )


async def _unhandled(_: Request, exc: Exception) -> JSONResponse:
    log.exception("unhandled error", exc_info=exc)
    return error_response("internal_error", "An unexpected error occurred.")


async def _domain_error(request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, DomainError):  # registered for DomainError only
        return await _unhandled(request, exc)
    return error_response(exc.code, exc.message, exc.details)


async def _validation_error(request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, RequestValidationError):
        return await _unhandled(request, exc)
    fields = [
        {"loc": list(e.get("loc", ())), "msg": e.get("msg", ""), "type": e.get("type", "")}
        for e in exc.errors()
    ]
    return error_response("validation_error", "Request validation failed.", {"fields": fields})


async def _http_error(request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, StarletteHTTPException):
        return await _unhandled(request, exc)
    if exc.status_code == 404:
        return error_response("not_found", "No such route.")
    if exc.status_code == 405:
        return error_response("method_not_allowed", "Method not allowed on this route.")
    return error_response("http_error", str(exc.detail), status=exc.status_code)


def install_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(DomainError, _domain_error)
    app.add_exception_handler(RequestValidationError, _validation_error)
    app.add_exception_handler(StarletteHTTPException, _http_error)
    app.add_exception_handler(Exception, _unhandled)
