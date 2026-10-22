"""Tenant and task state machines.

Pure functions and lookup tables only — no I/O — so every rule here is unit-tested
in isolation and the SQL layer just asks "is this allowed?".
"""

from __future__ import annotations

from enum import StrEnum


class TenantStatus(StrEnum):
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    UPDATING = "updating"
    DESTROYING = "destroying"
    DESTROYED = "destroyed"
    FAILED = "failed"


class TaskType(StrEnum):
    DEPLOY = "deploy"
    UPDATE = "update"
    DESTROY = "destroy"


class TaskStatus(StrEnum):
    ACCEPTED = "accepted"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"


# --------------------------------------------------------------------------- tenant (API side)

#: Tenant statuses from which an API call may start each kind of task.
API_TRANSITIONS: dict[TaskType, tuple[frozenset[TenantStatus], TenantStatus]] = {
    TaskType.UPDATE: (frozenset({TenantStatus.ACTIVE}), TenantStatus.UPDATING),
    TaskType.DESTROY: (
        frozenset({TenantStatus.ACTIVE, TenantStatus.FAILED}),
        TenantStatus.DESTROYING,
    ),
}

UPDATABLE_FROM = API_TRANSITIONS[TaskType.UPDATE][0]
DELETABLE_FROM = API_TRANSITIONS[TaskType.DESTROY][0]

#: While a task of this type is open, the tenant must be in this status.
IN_FLIGHT_STATUS: dict[TaskType, TenantStatus] = {
    TaskType.DEPLOY: TenantStatus.PROVISIONING,
    TaskType.UPDATE: TenantStatus.UPDATING,
    TaskType.DESTROY: TenantStatus.DESTROYING,
}


def can_update(status: TenantStatus) -> bool:
    return status in UPDATABLE_FROM


def can_delete(status: TenantStatus) -> bool:
    return status in DELETABLE_FROM


# --------------------------------------------------------------------------- task (worker side)

#: Forward-only ranking. Equal rank (done vs failed) is NOT forward: terminal is terminal.
TASK_RANK: dict[TaskStatus, int] = {
    TaskStatus.ACCEPTED: 0,
    TaskStatus.IN_PROGRESS: 1,
    TaskStatus.DONE: 2,
    TaskStatus.FAILED: 2,
}

TERMINAL_TASK_STATUSES = frozenset({TaskStatus.DONE, TaskStatus.FAILED})
OPEN_TASK_STATUSES = frozenset({TaskStatus.ACCEPTED, TaskStatus.IN_PROGRESS})


def is_terminal(status: TaskStatus) -> bool:
    return status in TERMINAL_TASK_STATUSES


def is_forward(current: TaskStatus, new: TaskStatus) -> bool:
    """True when applying ``new`` on top of ``current`` moves the task forward.

    ``accepted -> done|failed`` is allowed (a lost or late ``in_progress`` must not leave
    the task stuck); anything that does not strictly increase the rank is stale.
    """
    return TASK_RANK[new] > TASK_RANK[current]


def tenant_status_after(task_type: TaskType, outcome: TaskStatus) -> TenantStatus:
    """Tenant status implied by a *terminal* task outcome."""
    if outcome is TaskStatus.FAILED:
        return TenantStatus.FAILED
    if outcome is TaskStatus.DONE:
        return TenantStatus.DESTROYED if task_type is TaskType.DESTROY else TenantStatus.ACTIVE
    raise ValueError(f"{outcome!r} is not a terminal task status")
