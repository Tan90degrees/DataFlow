"""Durable desired-vs-actual reconciliation for external execution jobs."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID

from dataflow.contracts import ExecutionPlan
from dataflow.executor import (
    Executor,
    ExecutorError,
    ExternalJob,
    ExternalJobState,
    RetryPolicy,
)
from dataflow.metadata.repository import (
    ExecutionAttemptRecord,
    ExecutionUnitRecord,
    PipelineRunRecord,
)
from dataflow.state import (
    TERMINAL_ATTEMPT_STATUSES,
    TERMINAL_UNIT_STATUSES,
    ExecutionAttemptStatus,
    ExecutionUnitStatus,
    PipelineRunStatus,
)


class ReconciliationRepository(Protocol):
    def get_run(self, run_id: UUID) -> PipelineRunRecord: ...

    def get_unit(self, unit_id: UUID) -> ExecutionUnitRecord: ...

    def get_current_attempt(self, unit_id: UUID) -> ExecutionAttemptRecord | None: ...

    def create_attempt(self, execution_unit_id: UUID) -> ExecutionAttemptRecord: ...

    def transition_unit_status(
        self,
        unit_id: UUID,
        target: ExecutionUnitStatus,
        *,
        expected: ExecutionUnitStatus | None = None,
        payload: dict | None = None,
    ) -> ExecutionUnitRecord: ...

    def transition_attempt_status(
        self,
        attempt_id: UUID,
        target: ExecutionAttemptStatus,
        *,
        expected: ExecutionAttemptStatus | None = None,
        external_job_id: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> ExecutionAttemptRecord: ...


class Reconciler:
    """Converge one durable execution unit toward its external RayJob state."""

    def __init__(
        self,
        repository: ReconciliationRepository,
        executor: Executor,
        *,
        retry_policy: RetryPolicy | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._repository = repository
        self._executor = executor
        self._retry_policy = retry_policy or RetryPolicy()
        self._now = now or (lambda: datetime.now(UTC))

    def reconcile_unit(self, unit_id: UUID) -> ExecutionUnitRecord:
        unit = self._repository.get_unit(unit_id)
        if unit.status in TERMINAL_UNIT_STATUSES:
            return unit

        run = self._repository.get_run(unit.pipeline_run_id)
        plan = ExecutionPlan.model_validate(unit.plan_json)

        if run.status is PipelineRunStatus.CANCELLED:
            return self._cancel_unit(unit, plan)
        if run.status in {PipelineRunStatus.SUCCEEDED, PipelineRunStatus.FAILED}:
            return unit
        if unit.status is ExecutionUnitStatus.PENDING:
            return unit

        if unit.status is ExecutionUnitStatus.RETRY_WAIT:
            attempt = self._repository.get_current_attempt(unit.id)
            if attempt is None or attempt.finished_at is None:
                raise RuntimeError("RETRY_WAIT unit must have a finished current attempt")
            ready_at = attempt.finished_at + timedelta(
                seconds=self._retry_policy.backoff_seconds(attempt.attempt_number)
            )
            if self._now() < ready_at:
                return unit
            unit = self._transition_unit(unit, ExecutionUnitStatus.READY)

        if unit.status is ExecutionUnitStatus.READY:
            return self._submit_new_attempt(unit, plan)
        return self._reconcile_existing_attempt(unit, plan)

    def _submit_new_attempt(
        self,
        unit: ExecutionUnitRecord,
        plan: ExecutionPlan,
    ) -> ExecutionUnitRecord:
        unit = self._transition_unit(unit, ExecutionUnitStatus.SUBMITTING)
        attempt = self._repository.create_attempt(unit.id)
        attempt = self._transition_attempt(attempt, ExecutionAttemptStatus.SUBMITTING)
        try:
            job = self._executor.submit(plan, attempt_number=attempt.attempt_number)
        except ExecutorError as error:
            return self._handle_executor_error(unit, attempt, error)
        return self._converge_job(unit, attempt, job)

    def _reconcile_existing_attempt(
        self,
        unit: ExecutionUnitRecord,
        plan: ExecutionPlan,
    ) -> ExecutionUnitRecord:
        attempt = self._repository.get_current_attempt(unit.id)
        if attempt is None:
            if unit.status is ExecutionUnitStatus.SUBMITTING:
                attempt = self._repository.create_attempt(unit.id)
                attempt = self._transition_attempt(
                    attempt,
                    ExecutionAttemptStatus.SUBMITTING,
                )
                try:
                    job = self._executor.submit(
                        plan,
                        attempt_number=attempt.attempt_number,
                    )
                except ExecutorError as error:
                    return self._handle_executor_error(unit, attempt, error)
                return self._converge_job(unit, attempt, job)
            return self._transition_unit(unit, ExecutionUnitStatus.UNKNOWN)

        if attempt.status is ExecutionAttemptStatus.PENDING:
            attempt = self._transition_attempt(attempt, ExecutionAttemptStatus.SUBMITTING)

        if attempt.status in TERMINAL_ATTEMPT_STATUSES:
            return self._converge_terminal_attempt(unit, attempt)

        try:
            job = self._executor.get(plan, attempt_number=attempt.attempt_number)
        except ExecutorError as error:
            return self._handle_executor_error(unit, attempt, error)

        if job is None:
            if attempt.started_at is None and unit.status in {
                ExecutionUnitStatus.SUBMITTING,
                ExecutionUnitStatus.UNKNOWN,
            }:
                try:
                    job = self._executor.submit(
                        plan,
                        attempt_number=attempt.attempt_number,
                    )
                except ExecutorError as error:
                    return self._handle_executor_error(unit, attempt, error)
            else:
                job = ExternalJob(
                    id=attempt.external_job_id or "missing",
                    state=ExternalJobState.FAILED,
                    error_code="EXTERNAL_JOB_NOT_FOUND",
                    error_message="external execution object no longer exists",
                    retryable=True,
                )
        return self._converge_job(unit, attempt, job)

    def _converge_job(
        self,
        unit: ExecutionUnitRecord,
        attempt: ExecutionAttemptRecord,
        job: ExternalJob,
    ) -> ExecutionUnitRecord:
        if job.state is ExternalJobState.PENDING:
            if attempt.status is ExecutionAttemptStatus.UNKNOWN:
                attempt = self._transition_attempt(
                    attempt,
                    ExecutionAttemptStatus.SUBMITTING,
                    external_job_id=job.id,
                )
            if unit.status is ExecutionUnitStatus.UNKNOWN:
                unit = self._transition_unit(unit, ExecutionUnitStatus.SUBMITTING)
            return unit

        if job.state is ExternalJobState.RUNNING:
            attempt = self._transition_attempt(
                attempt,
                ExecutionAttemptStatus.RUNNING,
                external_job_id=job.id,
            )
            if unit.status is not ExecutionUnitStatus.RUNNING:
                unit = self._transition_unit(unit, ExecutionUnitStatus.RUNNING)
            return unit

        if job.state is ExternalJobState.SUCCEEDED:
            self._transition_attempt(
                attempt,
                ExecutionAttemptStatus.SUCCEEDED,
                external_job_id=job.id,
            )
            return self._transition_unit(unit, ExecutionUnitStatus.SUCCEEDED)

        if job.state is ExternalJobState.CANCELLED:
            self._transition_attempt(
                attempt,
                ExecutionAttemptStatus.CANCELLED,
                external_job_id=job.id,
                error_code=job.error_code,
                error_message=job.error_message,
            )
            return self._transition_unit(unit, ExecutionUnitStatus.CANCELLED)

        if job.state is ExternalJobState.UNKNOWN:
            attempt = self._transition_attempt(
                attempt,
                ExecutionAttemptStatus.UNKNOWN,
                external_job_id=job.id,
                error_code=job.error_code,
                error_message=job.error_message,
            )
            if unit.status is not ExecutionUnitStatus.UNKNOWN:
                unit = self._transition_unit(unit, ExecutionUnitStatus.UNKNOWN)
            return unit

        attempt = self._transition_attempt(
            attempt,
            ExecutionAttemptStatus.FAILED,
            external_job_id=job.id,
            error_code=job.error_code,
            error_message=job.error_message,
        )
        return self._retry_or_fail(unit, attempt, job)

    def _converge_terminal_attempt(
        self,
        unit: ExecutionUnitRecord,
        attempt: ExecutionAttemptRecord,
    ) -> ExecutionUnitRecord:
        if attempt.status is ExecutionAttemptStatus.SUCCEEDED:
            return self._transition_unit(unit, ExecutionUnitStatus.SUCCEEDED)
        if attempt.status is ExecutionAttemptStatus.CANCELLED:
            return self._transition_unit(unit, ExecutionUnitStatus.CANCELLED)
        job = ExternalJob(
            id=attempt.external_job_id or "failed",
            state=ExternalJobState.FAILED,
            error_code=attempt.error_code,
            error_message=attempt.error_message,
        )
        return self._retry_or_fail(unit, attempt, job)

    def _retry_or_fail(
        self,
        unit: ExecutionUnitRecord,
        attempt: ExecutionAttemptRecord,
        job: ExternalJob,
    ) -> ExecutionUnitRecord:
        if self._retry_policy.should_retry(job, attempt_number=attempt.attempt_number):
            return self._transition_unit(
                unit,
                ExecutionUnitStatus.RETRY_WAIT,
                payload={
                    "attempt_number": attempt.attempt_number,
                    "error_code": job.error_code,
                },
            )
        return self._transition_unit(
            unit,
            ExecutionUnitStatus.FAILED,
            payload={
                "attempt_number": attempt.attempt_number,
                "error_code": job.error_code,
            },
        )

    def _handle_executor_error(
        self,
        unit: ExecutionUnitRecord,
        attempt: ExecutionAttemptRecord,
        error: ExecutorError,
    ) -> ExecutionUnitRecord:
        if error.retryable:
            attempt = self._transition_attempt(
                attempt,
                ExecutionAttemptStatus.UNKNOWN,
                error_code=error.error_code,
                error_message=str(error),
            )
            if unit.status is not ExecutionUnitStatus.UNKNOWN:
                unit = self._transition_unit(
                    unit,
                    ExecutionUnitStatus.UNKNOWN,
                    payload={"error_code": error.error_code},
                )
            return unit

        attempt = self._transition_attempt(
            attempt,
            ExecutionAttemptStatus.FAILED,
            error_code=error.error_code,
            error_message=str(error),
        )
        should_retry = self._retry_policy.should_retry_error(
            attempt_number=attempt.attempt_number,
            error_code=error.error_code,
            retryable=error.retryable,
        )
        if should_retry:
            return self._transition_unit(unit, ExecutionUnitStatus.RETRY_WAIT)
        return self._transition_unit(unit, ExecutionUnitStatus.FAILED)

    def _cancel_unit(
        self,
        unit: ExecutionUnitRecord,
        plan: ExecutionPlan,
    ) -> ExecutionUnitRecord:
        attempt = self._repository.get_current_attempt(unit.id)
        if attempt is not None and attempt.status not in TERMINAL_ATTEMPT_STATUSES:
            try:
                self._executor.cancel(plan, attempt_number=attempt.attempt_number)
            except ExecutorError as error:
                if error.retryable:
                    if attempt.status is not ExecutionAttemptStatus.UNKNOWN:
                        self._transition_attempt(
                            attempt,
                            ExecutionAttemptStatus.UNKNOWN,
                            error_code=error.error_code,
                            error_message=str(error),
                        )
                    if unit.status is not ExecutionUnitStatus.UNKNOWN:
                        return self._transition_unit(unit, ExecutionUnitStatus.UNKNOWN)
                    return unit
                raise
            self._transition_attempt(attempt, ExecutionAttemptStatus.CANCELLED)
        return self._transition_unit(
            unit,
            ExecutionUnitStatus.CANCELLED,
            payload={"reason": "RUN_CANCELLED"},
        )

    def _transition_unit(
        self,
        unit: ExecutionUnitRecord,
        target: ExecutionUnitStatus,
        *,
        payload: dict | None = None,
    ) -> ExecutionUnitRecord:
        if unit.status is target:
            return unit
        return self._repository.transition_unit_status(
            unit.id,
            target,
            expected=unit.status,
            payload=payload,
        )

    def _transition_attempt(
        self,
        attempt: ExecutionAttemptRecord,
        target: ExecutionAttemptStatus,
        *,
        external_job_id: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> ExecutionAttemptRecord:
        if attempt.status is target:
            return attempt
        return self._repository.transition_attempt_status(
            attempt.id,
            target,
            expected=attempt.status,
            external_job_id=external_job_id,
            error_code=error_code,
            error_message=error_message,
        )


__all__ = ["Reconciler", "ReconciliationRepository"]
