"""External execution contracts shared by schedulers and reconcilers."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

from dataflow.contracts import ExecutionPlan


class ExternalJobState(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class ExternalJob:
    id: str
    state: ExternalJobState
    error_code: str | None = None
    error_message: str | None = None
    retryable: bool | None = None


class ExecutorError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        error_code: str = "EXECUTOR_ERROR",
        retryable: bool = True,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.retryable = retryable


class Executor(Protocol):
    def submit(self, plan: ExecutionPlan, *, attempt_number: int) -> ExternalJob: ...

    def get(self, plan: ExecutionPlan, *, attempt_number: int) -> ExternalJob | None: ...

    def cancel(self, plan: ExecutionPlan, *, attempt_number: int) -> None: ...


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    initial_backoff_seconds: float = 30.0
    multiplier: float = 2.0
    max_backoff_seconds: float = 600.0
    retryable_error_codes: frozenset[str] = field(
        default_factory=lambda: frozenset(
            {
                "API_UNAVAILABLE",
                "CLUSTER_UNAVAILABLE",
                "EXTERNAL_JOB_NOT_FOUND",
                "RAY_CLUSTER_FAILED",
            }
        )
    )
    permanent_error_codes: frozenset[str] = field(
        default_factory=lambda: frozenset(
            {
                "INVALID_PLAN",
                "USER_CODE_ERROR",
                "IMAGE_PULL_ERROR",
            }
        )
    )

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.initial_backoff_seconds < 0:
            raise ValueError("initial_backoff_seconds cannot be negative")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")
        if self.max_backoff_seconds < 0:
            raise ValueError("max_backoff_seconds cannot be negative")

    def backoff_seconds(self, attempt_number: int) -> float:
        if attempt_number < 1:
            raise ValueError("attempt_number must be at least 1")
        delay = self.initial_backoff_seconds * (self.multiplier ** (attempt_number - 1))
        return min(delay, self.max_backoff_seconds)

    def should_retry(self, job: ExternalJob, *, attempt_number: int) -> bool:
        if attempt_number >= self.max_attempts:
            return False
        if job.retryable is not None:
            return job.retryable
        if job.error_code in self.permanent_error_codes:
            return False
        if job.error_code in self.retryable_error_codes:
            return True
        return job.state is ExternalJobState.UNKNOWN

    def should_retry_error(
        self,
        *,
        attempt_number: int,
        error_code: str,
        retryable: bool,
    ) -> bool:
        if attempt_number >= self.max_attempts:
            return False
        if error_code in self.permanent_error_codes:
            return False
        if error_code in self.retryable_error_codes:
            return True
        return retryable


__all__ = [
    "Executor",
    "ExecutorError",
    "ExternalJob",
    "ExternalJobState",
    "RetryPolicy",
]
