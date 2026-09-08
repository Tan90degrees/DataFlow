"""Durable orchestration state enums and transition validation."""

from __future__ import annotations

from enum import StrEnum


class PipelineRunStatus(StrEnum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    PLANNING = "PLANNING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ExecutionUnitStatus(StrEnum):
    PENDING = "PENDING"
    READY = "READY"
    SUBMITTING = "SUBMITTING"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


class ExecutionAttemptStatus(StrEnum):
    PENDING = "PENDING"
    SUBMITTING = "SUBMITTING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


TERMINAL_RUN_STATUSES = frozenset(
    {
        PipelineRunStatus.SUCCEEDED,
        PipelineRunStatus.FAILED,
        PipelineRunStatus.CANCELLED,
    }
)

TERMINAL_UNIT_STATUSES = frozenset(
    {
        ExecutionUnitStatus.SUCCEEDED,
        ExecutionUnitStatus.FAILED,
        ExecutionUnitStatus.CANCELLED,
    }
)

TERMINAL_ATTEMPT_STATUSES = frozenset(
    {
        ExecutionAttemptStatus.SUCCEEDED,
        ExecutionAttemptStatus.FAILED,
        ExecutionAttemptStatus.CANCELLED,
    }
)


_RUN_TRANSITIONS: dict[PipelineRunStatus, frozenset[PipelineRunStatus]] = {
    PipelineRunStatus.CREATED: frozenset(
        {PipelineRunStatus.QUEUED, PipelineRunStatus.CANCELLED}
    ),
    PipelineRunStatus.QUEUED: frozenset(
        {PipelineRunStatus.PLANNING, PipelineRunStatus.CANCELLED}
    ),
    PipelineRunStatus.PLANNING: frozenset(
        {
            PipelineRunStatus.RUNNING,
            PipelineRunStatus.FAILED,
            PipelineRunStatus.CANCELLED,
        }
    ),
    PipelineRunStatus.RUNNING: TERMINAL_RUN_STATUSES,
    PipelineRunStatus.SUCCEEDED: frozenset(),
    PipelineRunStatus.FAILED: frozenset(),
    PipelineRunStatus.CANCELLED: frozenset(),
}

_UNIT_TRANSITIONS: dict[ExecutionUnitStatus, frozenset[ExecutionUnitStatus]] = {
    ExecutionUnitStatus.PENDING: frozenset(
        {ExecutionUnitStatus.READY, ExecutionUnitStatus.CANCELLED}
    ),
    ExecutionUnitStatus.READY: frozenset(
        {ExecutionUnitStatus.SUBMITTING, ExecutionUnitStatus.CANCELLED}
    ),
    ExecutionUnitStatus.SUBMITTING: frozenset(
        {
            ExecutionUnitStatus.RUNNING,
            ExecutionUnitStatus.RETRY_WAIT,
            ExecutionUnitStatus.FAILED,
            ExecutionUnitStatus.CANCELLED,
            ExecutionUnitStatus.UNKNOWN,
        }
    ),
    ExecutionUnitStatus.RUNNING: frozenset(
        {
            ExecutionUnitStatus.SUCCEEDED,
            ExecutionUnitStatus.RETRY_WAIT,
            ExecutionUnitStatus.FAILED,
            ExecutionUnitStatus.CANCELLED,
            ExecutionUnitStatus.UNKNOWN,
        }
    ),
    ExecutionUnitStatus.RETRY_WAIT: frozenset(
        {ExecutionUnitStatus.READY, ExecutionUnitStatus.CANCELLED}
    ),
    ExecutionUnitStatus.UNKNOWN: frozenset(
        {
            ExecutionUnitStatus.SUBMITTING,
            ExecutionUnitStatus.RUNNING,
            ExecutionUnitStatus.RETRY_WAIT,
            ExecutionUnitStatus.SUCCEEDED,
            ExecutionUnitStatus.FAILED,
            ExecutionUnitStatus.CANCELLED,
        }
    ),
    ExecutionUnitStatus.SUCCEEDED: frozenset(),
    ExecutionUnitStatus.FAILED: frozenset(),
    ExecutionUnitStatus.CANCELLED: frozenset(),
}

_ATTEMPT_TRANSITIONS: dict[ExecutionAttemptStatus, frozenset[ExecutionAttemptStatus]] = {
    ExecutionAttemptStatus.PENDING: frozenset(
        {ExecutionAttemptStatus.SUBMITTING, ExecutionAttemptStatus.CANCELLED}
    ),
    ExecutionAttemptStatus.SUBMITTING: frozenset(
        {
            ExecutionAttemptStatus.RUNNING,
            ExecutionAttemptStatus.FAILED,
            ExecutionAttemptStatus.CANCELLED,
            ExecutionAttemptStatus.UNKNOWN,
        }
    ),
    ExecutionAttemptStatus.RUNNING: frozenset(
        {
            ExecutionAttemptStatus.SUCCEEDED,
            ExecutionAttemptStatus.FAILED,
            ExecutionAttemptStatus.CANCELLED,
            ExecutionAttemptStatus.UNKNOWN,
        }
    ),
    ExecutionAttemptStatus.UNKNOWN: frozenset(
        {
            ExecutionAttemptStatus.SUBMITTING,
            ExecutionAttemptStatus.RUNNING,
            ExecutionAttemptStatus.SUCCEEDED,
            ExecutionAttemptStatus.FAILED,
            ExecutionAttemptStatus.CANCELLED,
        }
    ),
    ExecutionAttemptStatus.SUCCEEDED: frozenset(),
    ExecutionAttemptStatus.FAILED: frozenset(),
    ExecutionAttemptStatus.CANCELLED: frozenset(),
}


class InvalidStateTransition(ValueError):
    pass


def validate_run_transition(
    current: PipelineRunStatus,
    target: PipelineRunStatus,
) -> None:
    if target not in _RUN_TRANSITIONS[current]:
        raise InvalidStateTransition(f"invalid run transition: {current} -> {target}")


def validate_unit_transition(
    current: ExecutionUnitStatus,
    target: ExecutionUnitStatus,
) -> None:
    if target not in _UNIT_TRANSITIONS[current]:
        raise InvalidStateTransition(f"invalid unit transition: {current} -> {target}")


def validate_attempt_transition(
    current: ExecutionAttemptStatus,
    target: ExecutionAttemptStatus,
) -> None:
    if target not in _ATTEMPT_TRANSITIONS[current]:
        raise InvalidStateTransition(f"invalid attempt transition: {current} -> {target}")
