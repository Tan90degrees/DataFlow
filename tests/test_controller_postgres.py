from __future__ import annotations

import os
from collections.abc import Iterator
from uuid import UUID, uuid4

import psycopg
import pytest

from dataflow.compiler import (
    ExecutionBoundary,
    PipelineCompiler,
    PipelineEdgeSpec,
    PipelineNodeSpec,
    PipelineSpec,
)
from dataflow.contracts import ExecutionPlan, OperatorKind, RuntimeSpec
from dataflow.control_repository import PostgresControlRepository
from dataflow.controller import OrchestrationController
from dataflow.executor import ExternalJob, ExternalJobState, RetryPolicy
from dataflow.metadata.migrations import migrate
from dataflow.reconciler import Reconciler
from dataflow.scheduler import Scheduler
from dataflow.state import (
    ExecutionAttemptStatus,
    ExecutionUnitStatus,
    PipelineRunStatus,
)


class FakeExecutor:
    def __init__(self) -> None:
        self.jobs: dict[tuple[str, str, int], ExternalJob] = {}
        self.submit_count = 0
        self.cancel_count = 0

    @staticmethod
    def _key(plan: ExecutionPlan, attempt_number: int) -> tuple[str, str, int]:
        return (plan.run_id, plan.unit_id, attempt_number)

    def submit(self, plan: ExecutionPlan, *, attempt_number: int) -> ExternalJob:
        key = self._key(plan, attempt_number)
        self.submit_count += 1
        if key not in self.jobs:
            self.jobs[key] = ExternalJob(
                id=f"fake-{plan.unit_id}-a{attempt_number:03d}",
                state=ExternalJobState.RUNNING,
            )
        return self.jobs[key]

    def get(self, plan: ExecutionPlan, *, attempt_number: int) -> ExternalJob | None:
        return self.jobs.get(self._key(plan, attempt_number))

    def cancel(self, plan: ExecutionPlan, *, attempt_number: int) -> None:
        self.cancel_count += 1
        key = self._key(plan, attempt_number)
        job = self.jobs.get(key)
        if job is not None:
            self.jobs[key] = ExternalJob(id=job.id, state=ExternalJobState.CANCELLED)

    def put(
        self,
        plan: ExecutionPlan,
        attempt_number: int,
        state: ExternalJobState,
        *,
        error_code: str | None = None,
        retryable: bool | None = None,
    ) -> None:
        self.jobs[self._key(plan, attempt_number)] = ExternalJob(
            id=f"fake-{plan.unit_id}-a{attempt_number:03d}",
            state=state,
            error_code=error_code,
            retryable=retryable,
        )


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    dsn = os.environ.get("DATAFLOW_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("DATAFLOW_TEST_DATABASE_URL is not configured")
    migrate(dsn)
    return dsn


@pytest.fixture
def repository(postgres_dsn: str) -> Iterator[PostgresControlRepository]:
    with psycopg.connect(postgres_dsn) as connection:
        connection.execute(
            """
            TRUNCATE TABLE
                events,
                node_runs,
                execution_attempts,
                execution_units,
                pipeline_runs,
                pipeline_versions,
                pipelines
            RESTART IDENTITY CASCADE
            """
        )
    yield PostgresControlRepository(postgres_dsn)


def _pipeline_spec() -> PipelineSpec:
    return PipelineSpec(
        name="controller-test",
        runtime=RuntimeSpec(image="dataflow-runtime:test"),
        cluster_profile="cpu-test",
        artifact_base_uri="s3://bucket/dataflow",
        nodes=[
            PipelineNodeSpec(
                id="read",
                kind=OperatorKind.READ_PARQUET,
                config={"path": "s3://bucket/input"},
            ),
            PipelineNodeSpec(
                id="filter",
                kind=OperatorKind.FILTER,
                config={"callable": "dataflow.callables.keep_all"},
            ),
            PipelineNodeSpec(
                id="write",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "s3://bucket/output"},
            ),
        ],
        edges=[
            PipelineEdgeSpec.model_validate(
                {
                    "from": "read",
                    "to": "filter",
                    "kind": "data",
                    "boundary": ExecutionBoundary.HARD,
                }
            ),
            PipelineEdgeSpec.model_validate(
                {"from": "filter", "to": "write", "kind": "data"}
            ),
        ],
    )


def _persist_running_run(
    repository: PostgresControlRepository,
) -> tuple[UUID, dict[str, UUID]]:
    spec = _pipeline_spec()
    pipeline = repository.create_pipeline(name=f"controller-{uuid4()}")
    version = repository.create_pipeline_version(
        pipeline.id,
        spec.model_dump(mode="json", by_alias=True),
    )
    run = repository.create_pipeline_run(version.id, cluster_profile=spec.cluster_profile)
    graph = PipelineCompiler().compile(spec, run_id=str(run.id))
    unit_ids = repository.create_execution_graph(run.id, graph)
    repository.transition_run_status(run.id, PipelineRunStatus.QUEUED)
    repository.transition_run_status(run.id, PipelineRunStatus.PLANNING)
    repository.transition_run_status(run.id, PipelineRunStatus.RUNNING)
    return run.id, unit_ids


def _plan(repository: PostgresControlRepository, unit_id: UUID) -> ExecutionPlan:
    return ExecutionPlan.model_validate(repository.get_unit(unit_id).plan_json)


def test_all_success_readiness_and_terminal_convergence(
    repository: PostgresControlRepository,
) -> None:
    run_id, unit_ids = _persist_running_run(repository)
    first_id = unit_ids["unit-001"]
    second_id = unit_ids["unit-002"]
    executor = FakeExecutor()
    scheduler = Scheduler(repository)
    reconciler = Reconciler(repository, executor)

    scheduler.reconcile_run(run_id)
    assert repository.get_unit(first_id).status is ExecutionUnitStatus.READY
    assert repository.get_unit(second_id).status is ExecutionUnitStatus.PENDING

    assert reconciler.reconcile_unit(first_id).status is ExecutionUnitStatus.RUNNING
    assert executor.submit_count == 1
    assert reconciler.reconcile_unit(first_id).status is ExecutionUnitStatus.RUNNING
    assert executor.submit_count == 1

    executor.put(_plan(repository, first_id), 1, ExternalJobState.SUCCEEDED)
    assert reconciler.reconcile_unit(first_id).status is ExecutionUnitStatus.SUCCEEDED

    scheduler.reconcile_run(run_id)
    assert repository.get_unit(second_id).status is ExecutionUnitStatus.READY


def test_controller_restart_recovers_existing_rayjob_without_resubmit(
    repository: PostgresControlRepository,
) -> None:
    run_id, unit_ids = _persist_running_run(repository)
    unit_id = unit_ids["unit-001"]
    scheduler = Scheduler(repository)
    scheduler.reconcile_run(run_id)
    repository.transition_unit_status(
        unit_id,
        ExecutionUnitStatus.SUBMITTING,
        expected=ExecutionUnitStatus.READY,
    )
    attempt = repository.create_attempt(unit_id)
    repository.transition_attempt_status(
        attempt.id,
        ExecutionAttemptStatus.SUBMITTING,
        expected=ExecutionAttemptStatus.PENDING,
    )

    executor = FakeExecutor()
    executor.put(_plan(repository, unit_id), 1, ExternalJobState.RUNNING)
    reconciler = Reconciler(repository, executor)
    restarted = OrchestrationController(repository, Scheduler(repository), reconciler)

    restarted.reconcile_once()

    assert repository.get_unit(unit_id).status is ExecutionUnitStatus.RUNNING
    assert executor.submit_count == 0
    assert repository.get_current_attempt(unit_id).status is ExecutionAttemptStatus.RUNNING


def test_cancellation_stops_active_job_and_prevents_downstream_submission(
    repository: PostgresControlRepository,
) -> None:
    run_id, unit_ids = _persist_running_run(repository)
    first_id = unit_ids["unit-001"]
    second_id = unit_ids["unit-002"]
    executor = FakeExecutor()
    scheduler = Scheduler(repository)
    reconciler = Reconciler(repository, executor)
    controller = OrchestrationController(repository, scheduler, reconciler)

    scheduler.reconcile_run(run_id)
    reconciler.reconcile_unit(first_id)
    assert executor.submit_count == 1

    controller.cancel_run(run_id)

    assert repository.get_run(run_id).status is PipelineRunStatus.CANCELLED
    assert repository.get_unit(first_id).status is ExecutionUnitStatus.CANCELLED
    assert repository.get_unit(second_id).status is ExecutionUnitStatus.CANCELLED
    assert executor.cancel_count == 1
    assert executor.submit_count == 1


def test_retry_creates_new_durable_attempt_after_backoff(
    repository: PostgresControlRepository,
) -> None:
    run_id, unit_ids = _persist_running_run(repository)
    unit_id = unit_ids["unit-001"]
    executor = FakeExecutor()
    scheduler = Scheduler(repository)
    reconciler = Reconciler(
        repository,
        executor,
        retry_policy=RetryPolicy(max_attempts=2, initial_backoff_seconds=0),
    )

    scheduler.reconcile_run(run_id)
    reconciler.reconcile_unit(unit_id)
    executor.put(
        _plan(repository, unit_id),
        1,
        ExternalJobState.FAILED,
        error_code="TRANSIENT",
        retryable=True,
    )

    assert reconciler.reconcile_unit(unit_id).status is ExecutionUnitStatus.RETRY_WAIT
    attempts = repository.list_attempts(unit_id)
    assert [attempt.attempt_number for attempt in attempts] == [1]
    assert attempts[0].status is ExecutionAttemptStatus.FAILED

    assert reconciler.reconcile_unit(unit_id).status is ExecutionUnitStatus.RUNNING
    attempts = repository.list_attempts(unit_id)
    assert [attempt.attempt_number for attempt in attempts] == [1, 2]
    assert attempts[1].status is ExecutionAttemptStatus.RUNNING
    assert executor.submit_count == 2
