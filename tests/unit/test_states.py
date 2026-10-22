"""The state machine rules, exhaustively (README §3)."""

from __future__ import annotations

import itertools

import pytest

from control_plane.domain.states import (
    IN_FLIGHT_STATUS,
    TaskStatus,
    TaskType,
    TenantStatus,
    can_delete,
    can_update,
    is_forward,
    is_terminal,
    tenant_status_after,
)

T = TaskStatus


@pytest.mark.parametrize("status", list(TenantStatus))
def test_update_only_from_active(status: TenantStatus) -> None:
    assert can_update(status) is (status is TenantStatus.ACTIVE)


@pytest.mark.parametrize("status", list(TenantStatus))
def test_delete_only_from_active_or_failed(status: TenantStatus) -> None:
    assert can_delete(status) is (status in {TenantStatus.ACTIVE, TenantStatus.FAILED})


FORWARD = {
    (T.ACCEPTED, T.IN_PROGRESS),
    (T.ACCEPTED, T.DONE),  # in_progress lost/late: still finish
    (T.ACCEPTED, T.FAILED),
    (T.IN_PROGRESS, T.DONE),
    (T.IN_PROGRESS, T.FAILED),
}


@pytest.mark.parametrize(("current", "new"), list(itertools.product(TaskStatus, TaskStatus)))
def test_task_transitions_are_forward_only(current: TaskStatus, new: TaskStatus) -> None:
    assert is_forward(current, new) is ((current, new) in FORWARD)


@pytest.mark.parametrize("terminal", [T.DONE, T.FAILED])
def test_nothing_moves_a_terminal_task(terminal: TaskStatus) -> None:
    assert is_terminal(terminal)
    assert not any(is_forward(terminal, new) for new in TaskStatus)


def test_open_statuses_are_not_terminal() -> None:
    assert not is_terminal(T.ACCEPTED)
    assert not is_terminal(T.IN_PROGRESS)


@pytest.mark.parametrize(
    ("task_type", "outcome", "expected"),
    [
        (TaskType.DEPLOY, T.DONE, TenantStatus.ACTIVE),
        (TaskType.UPDATE, T.DONE, TenantStatus.ACTIVE),
        (TaskType.DESTROY, T.DONE, TenantStatus.DESTROYED),
        (TaskType.DEPLOY, T.FAILED, TenantStatus.FAILED),
        (TaskType.UPDATE, T.FAILED, TenantStatus.FAILED),
        (TaskType.DESTROY, T.FAILED, TenantStatus.FAILED),
    ],
)
def test_terminal_outcome_moves_tenant(
    task_type: TaskType, outcome: TaskStatus, expected: TenantStatus
) -> None:
    assert tenant_status_after(task_type, outcome) is expected


@pytest.mark.parametrize("non_terminal", [T.ACCEPTED, T.IN_PROGRESS])
def test_non_terminal_outcome_has_no_tenant_status(non_terminal: TaskStatus) -> None:
    with pytest.raises(ValueError, match="not a terminal"):
        tenant_status_after(TaskType.DEPLOY, non_terminal)


def test_every_task_type_has_an_in_flight_tenant_status() -> None:
    assert IN_FLIGHT_STATUS == {
        TaskType.DEPLOY: TenantStatus.PROVISIONING,
        TaskType.UPDATE: TenantStatus.UPDATING,
        TaskType.DESTROY: TenantStatus.DESTROYING,
    }
