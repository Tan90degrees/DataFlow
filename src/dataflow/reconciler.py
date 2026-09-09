"""Durable desired-vs-actual reconciliation for external execution jobs."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Protocol
from uuid import UUID

from dataflow.artifact_manager import ArtifactManager
from dataflow.artifacts import ArtifactCommitError
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
from dataflow.observability import DEFAULT_OBSERVABILITY, Observability
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
        artifact_manager: ArtifactManager | None = None,
        now: Callable[[], datetime] | None = None,
        observability: Observability | None = None,
    ) -> None:
        self._repository = repository
        self._executor = executor
        self._retry_policy = retry_policy or RetryPolicy()
        self._artifact_manager = artifact_manager
        self._now = now or (lambda: datetime.now(UTC))
        self._observability = observability or DEFAULT_OBSERVABILITY

    def reconcile_unit(self, unit_id: UUID) -> ExecutionUnitRecord:
        unit = self._repository.get_unit(unit_id)
        started = perf_counter()
        outcome = "error"
        with self._observability.bind(
            run_id=str(unit.pipeline_run_id),
            unit_id=str(unit.id),
            unit_key=unit.unit_key,
        ), self._observability.span(
            "dataflow.reconcile_unit",
            run_id=str(unit.pipeline_run_id),
            unit_id=str(unit.id),
            unit_key=unit.unit_key,
            unit_status=unit.status.value,
        ):
            self._observability.info("reconcile_unit_started", unit_status=unit.status.value)
            try:
                result = self._reconcile_loaded_unit(unit)
                outcome = result.status.value.lower()
                self._observability.info(
                    "reconcile_unit_finished",
                    unit_status=result.status.value,
                )
                return result
            except Exception as error:
                self._observability.error(
                    "reconcile_unit_error",
                    error_type=type(error).__name__,
                )
                raise
            finally:
                self._observability.metrics.observe_reconciliation(
                    outcome=outcome,
                    duration_seconds=perf_counter() - started,
                )

    def _reconcile_loaded_unit(self, unit: ExecutionUnitRecord) -> ExecutionUnitRecord:
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
        self._observability.info(
            "execution_attempt_submitting",
            attempt_id=str(attempt.id),
            attempt_number=attempt.attempt_number,
        )
        try:
            job = self._executor.submit(plan, attempt_number=attempt.attempt_number)
        except ExecutorError as error:
            return self._handle_executor_error(unit, attempt, error)
        return self._converge_job(unit, attempt, job, plan)

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
                return self._converge_job(unit, attempt, job, plan)
            return self._transition_unit(unit, ExecutionUnitStatus.UNKNOWN)

        if attempt.status is ExecutionAttemptStatus.PENDING:
            attempt = self._transition_attempt(attempt, ExecutionAttemptStatus.SUBMITTING)

        if attempt.status in TERMINAL_ATTEMPT_STATUSES:
            return self._converge_terminal_attempt(unit, attempt, plan)

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
        return self._converge_job(unit, attempt, job, plan)

    def _converge_job(
        self,
        unit: ExecutionUnitRecord,
        attempt: ExecutionAttemptRecord,
        job: ExternalJob,
        plan: ExecutionPlan,
    ) -> ExecutionUnitRecord:
        with self._observability.bind(
            attempt_id=str(attempt.id),
            attempt_number=attempt.attempt_number,
            external_job_id=job.id,
        ):
            self._observability.info(
                "external_job_observed",
                external_job_state=job.state.value,
                error_code=job.error_code,
            )

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
            try:
                self._commit_artifacts(plan, attempt.attempt_number)
            except ArtifactCommitError as error:
                return self._handle_artifact_error(unit, attempt, plan, error)
            self._transition_attempt(
                attempt,
                ExecutionAttemptStatus.SUCCEEDED,
                external_job_id=job.id,
            )
            return self._transition_unit(unit, ExecutionUnitStatus.SUCCEEDED)

        if job.state is ExternalJobState.CANCELLED:
            self._abort_artifacts(plan, attempt.attempt_number)
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

        self._abort_artifacts(plan, attempt.attempt_number)
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
        plan: ExecutionPlan,
    ) -> ExecutionUnitRecord:
        if attempt.status is ExecutionAttemptStatus.SUCCEEDED:
            try:
                self._commit_artifacts(plan, attempt.attempt_number)
            except ArtifactCommitError as error:
                if error.retryable:
                    return self._transition_unit(
                        unit,
                        ExecutionUnitStatus.UNKNOWN,
                        payload={"error_code": error.error_code},
                    )
                return self._transition_unit(
                    unit,
                    ExecutionUnitStatus.FAILED,
                    payload={"error_code": error.error_code},
                )
            return self._transition_unit(unit, ExecutionUnitStatus.SUCCEEDED)
        if attempt.status is ExecutionAttemptStatus.CANCELLED:
            self._abort_artifacts(plan, attempt.attempt_number)
            return self._transition_unit(unit, ExecutionUnitStatus.CANCELLED)
        self._abort_artifacts(plan, attempt.attempt_number)
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
            self._observability.metrics.observe_unit_retry(error_code=job.error_code)
            self._observability.warning(
                "execution_unit_retry_scheduled",
                attempt_number=attempt.attempt_number,
                error_code=job.error_code,
            )
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
        self._observability.metrics.observe_reconciliation_error(error_code=error.error_code)
        self._observability.warning(
            "executor_error",
            attempt_number=attempt.attempt_number,
            error_code=error.error_code,
            retryable=error.retryable,
        )
        if error.retryable:
            self._transition_attempt(
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
            self._observability.metrics.observe_unit_retry(error_code=error.error_code)
            return self._transition_unit(unit, ExecutionUnitStatus.RETRY_WAIT)
        return self._transition_unit(unit, ExecutionUnitStatus.FAILED)

    def _handle_artifact_error(
        self,
        unit: ExecutionUnitRecord,
        attempt: ExecutionAttemptRecord,
        plan: ExecutionPlan,
        error: ArtifactCommitError,
    ) -> ExecutionUnitRecord:
        self._observability.metrics.observe_reconciliation_error(error_code=error.error_code)
        self._observability.warning(
            "artifact_publication_error",
            attempt_number=attempt.attempt_number,
            error_code=error.error_code,
            retryable=error.retryable,
        )
        if error.retryable:
            if attempt.status is not ExecutionAttemptStatus.UNKNOWN:
                self._transition_attempt(
                    attempt,
                    ExecutionAttemptStatus.UNKNOWN,
                    error_code=error.error_code,
                    error_message=str(error),
                )
            if unit.status is not ExecutionUnitStatus.UNKNOWN:
                return self._transition_unit(
                    unit,
                    ExecutionUnitStatus.UNKNOWN,
                    payload={"error_code": error.error_code},
                )
            return unit

        self._abort_artifacts(plan, attempt.attempt_number)
        self._transition_attempt(
            attempt,
            ExecutionAttemptStatus.FAILED,
            error_code=error.error_code,
            error_message=str(error),
        )
        return self._transition_unit(
            unit,
            ExecutionUnitStatus.FAILED,
            payload={"error_code": error.error_code},
        )

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
            self._abort_artifacts(plan, attempt.attempt_number)
            self._transition_attempt(attempt, ExecutionAttemptStatus.CANCELLED)
        return self._transition_unit(
            unit,
            ExecutionUnitStatus.CANCELLED,
            payload={"reason": "RUN_CANCELLED"},
        )

    def _commit_artifacts(self, plan: ExecutionPlan, attempt_number: int) -> None:
        if not plan.output_artifacts:
            return
        if self._artifact_manager is None:
            self._observability.metrics.observe_artifact_publication(outcome="error")
            raise ArtifactCommitError(
                "durable artifact outputs require an ArtifactManager",
                error_code="ARTIFACT_MANAGER_NOT_CONFIGURED",
                retryable=False,
            )
        try:
            self._artifact_manager.commit_outputs(plan, attempt_number=attempt_number)
        except ArtifactCommitError:
            self._observability.metrics.observe_artifact_publication(outcome="error")
            raise
        self._observability.metrics.observe_artifact_publication(outcome="committed")

    def _abort_artifacts(self, plan: ExecutionPlan, attempt_number: int) -> None:
        if not plan.output_artifacts or self._artifact_manager is None:
            return
        self._artifact_manager.abort_outputs(
            plan,
            attempt_number=attempt_number,
            best_effort=True,
        )
        self._observability.metrics.observe_artifact_publication(outcome="aborted")

    def _transition_unit(
        self,
        unit: ExecutionUnitRecord,
        target: ExecutionUnitStatus,
        *,
        payload: dict | None = None,
    ) -> ExecutionUnitRecord:
        if unit.status is target:
            return unit
        updated = self._repository.transition_unit_status(
            unit.id,
            target,
            expected=unit.status,
            payload=payload,
        )
        self._observability.info(
            "execution_unit_state_changed",
            previous_status=unit.status.value,
            unit_status=target.value,
        )
        if target in TERMINAL_UNIT_STATUSES:
            duration_seconds = None
            if updated.started_at is not None and updated.finished_at is not None:
                duration_seconds = (updated.finished_at - updated.started_at).total_seconds()
            self._observability.metrics.observe_unit_terminal(
                status=target.value,
                duration_seconds=duration_seconds,
            )
        return updated

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
        updated = self._repository.transition_attempt_status(
            attempt.id,
            target,
            expected=attempt.status,
            external_job_id=external_job_id,
            error_code=error_code,
            error_message=error_message,
        )
        self._observability.info(
            "execution_attempt_state_changed",
            attempt_id=str(attempt.id),
            attempt_number=attempt.attempt_number,
            previous_status=attempt.status.value,
            attempt_status=target.value,
            external_job_id=external_job_id,
            error_code=error_code,
        )
        return updated


__all__ = ["Reconciler", "ReconciliationRepository"]
