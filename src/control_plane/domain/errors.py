"""Domain errors. Each carries the stable, machine-readable ``code`` that is part of the API
contract; the HTTP layer only maps codes to status codes (see ``api/errors.py``)."""

from __future__ import annotations

from typing import Any
from uuid import UUID


class DomainError(Exception):
    code: str = "domain_error"

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


class TenantNotFound(DomainError):
    code = "tenant_not_found"

    def __init__(self, tenant_id: UUID) -> None:
        super().__init__(f"Tenant {tenant_id} does not exist.", tenant_id=str(tenant_id))


class TaskNotFound(DomainError):
    code = "task_not_found"

    def __init__(self, task_id: UUID) -> None:
        super().__init__(f"Task {task_id} does not exist.", task_id=str(task_id))


class TenantAlreadyExists(DomainError):
    code = "tenant_already_exists"

    def __init__(self, slug: str) -> None:
        super().__init__(
            f"A tenant with slug '{slug}' already exists (slugs are never reused).", slug=slug
        )


class TenantUpdateNotAllowed(DomainError):
    code = "tenant_update_not_allowed"

    def __init__(self, operation: str, current_status: str, allowed_from: list[str]) -> None:
        super().__init__(
            f"Cannot {operation} a tenant in status '{current_status}'.",
            operation=operation,
            current_status=current_status,
            allowed_from=allowed_from,
        )


class TenantVersionConflict(DomainError):
    code = "tenant_version_conflict"

    def __init__(self, expected_version: int, current_version: int) -> None:
        super().__init__(
            "Tenant was modified by someone else; re-read it and retry with the current version.",
            expected_version=expected_version,
            current_version=current_version,
        )


class InvalidCursor(DomainError):
    code = "validation_error"

    def __init__(self) -> None:
        super().__init__(
            "The pagination cursor is malformed.",
            fields=[{"loc": ["query", "cursor"], "msg": "malformed cursor"}],
        )
