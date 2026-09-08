from __future__ import annotations

import os
from collections.abc import Iterator
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from dataflow.compiler import (
    PipelineCompiler,
    PipelineEdgeSpec,
    PipelineNodeSpec,
    PipelineSpec,
)
from dataflow.contracts import OperatorKind, RuntimeSpec
from dataflow.metadata.migrations import migrate
from dataflow.metadata.repository import (
    ConcurrentStateChange,
    PostgresMetadataRepository,
)
from dataflow.state import (
    ExecutionUnitStatus,
    InvalidStateTransition,
    PipelineRunStatus,
)


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    dsn = os.environ.get("DATAFLOW_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("DATAFLOW_TEST_DATABASE_URL is not configured")
    migrate(dsn)
    return dsn


@pytest.fixture
def repository(postgres_dsn: str) -> Iterator[PostgresMetadataRepository]:
    with psycopg.connect(postgres_dsn) as connection:
        connection.execute(
            """
            TRUNCATE TABLE
                artifacts,
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
    yield PostgresMetadataRepository(postgres_dsn)


def _pipeline_spec() -> PipelineSpec:
    return PipelineSpec(
        name="metadata-test",
        runtime=RuntimeSpec(image="dataflow-runtime:test"),
        cluster_profile="cpu-test",
        nodes=[
            PipelineNodeSpec(
                id="read",
                kind=OperatorKind.READ_PARQUET,
                config={"path": "s3://bucket/input"},
            ),
            PipelineNodeSpec(
                id="write",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "s3://bucket/output"},
            ),
        ],
        edges=[
            PipelineEdgeSpec.model_validate(
                {"from": "read", "to": "write", "kind": "data"}
            )
        ],
    )


def _persist_run(
    repository: PostgresMetadataRepository,
) -> tuple[UUID, UUID]:
    spec = _pipeline_spec()
    pipeline = repository.create_pipeline(name=f"pipeline-{uuid4()}")
    version = repository.create_pipeline_version(
        pipeline.id,
        spec.model_dump(mode="json", by_alias=True),
    )
    run = repository.create_pipeline_run(
        version.id,
        cluster_profile=spec.cluster_profile,
        parameters={"input": "s3://bucket/input"},
    )
    graph = PipelineCompiler().compile(spec, run_id=str(run.id))
    unit_ids = repository.create_execution_graph(run.id, graph)
    return run.id, unit_ids["unit-001"]


def test_migrations_are_idempotent(postgres_dsn: str) -> None:
    assert migrate(postgres_dsn) == []
    with psycopg.connect(postgres_dsn) as connection:
        versions = connection.execute(
            "SELECT version FROM dataflow_schema_migrations ORDER BY version"
        ).fetchall()
    assert versions == [("0001_initial.sql",), ("0002_artifacts.sql",)]


def test_pipeline_versions_are_immutable_and_hash_canonical(
    repository: PostgresMetadataRepository,
    postgres_dsn: str,
) -> None:
    pipeline = repository.create_pipeline(name=f"versions-{uuid4()}")
    first = repository.create_pipeline_version(pipeline.id, {"b": 2, "a": 1})
    second = repository.create_pipeline_version(pipeline.id, {"a": 1, "b": 2})

    assert first.version == 1
    assert second.version == 2
    assert first.id != second.id
    assert first.spec_hash == second.spec_hash

    with psycopg.connect(postgres_dsn) as connection:
        with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
            with connection.transaction():
                connection.execute(
                    "UPDATE pipeline_versions SET spec_json = %s WHERE id = %s",
                    (Jsonb({"mutated": True}), first.id),
                )


def test_attempts_append_instead_of_overwriting(
    repository: PostgresMetadataRepository,
) -> None:
    run_id, unit_id = _persist_run(repository)

    first = repository.create_attempt(unit_id)
    second = repository.create_attempt(unit_id)

    assert first.attempt_number == 1
    assert second.attempt_number == 2
    assert first.id != second.id
    assert [attempt.attempt_number for attempt in repository.list_attempts(unit_id)] == [1, 2]
    assert repository.get_unit(unit_id).current_attempt == 2
    assert [event.event_type for event in repository.list_events(run_id)].count(
        "ATTEMPT_CREATED"
    ) == 2


def test_state_transition_and_event_are_atomic(
    repository: PostgresMetadataRepository,
) -> None:
    run_id, unit_id = _persist_run(repository)

    repository.transition_run_status(
        run_id,
        PipelineRunStatus.QUEUED,
        expected=PipelineRunStatus.CREATED,
    )
    repository.transition_run_status(run_id, PipelineRunStatus.PLANNING)
    running = repository.transition_run_status(run_id, PipelineRunStatus.RUNNING)
    ready = repository.transition_unit_status(
        unit_id,
        ExecutionUnitStatus.READY,
        expected=ExecutionUnitStatus.PENDING,
    )

    assert running.started_at is not None
    assert ready.status is ExecutionUnitStatus.READY
    event_count = len(repository.list_events(run_id))

    with pytest.raises(InvalidStateTransition):
        repository.transition_unit_status(unit_id, ExecutionUnitStatus.RUNNING)

    assert repository.get_unit(unit_id).status is ExecutionUnitStatus.READY
    assert len(repository.list_events(run_id)) == event_count

    with pytest.raises(ConcurrentStateChange):
        repository.transition_unit_status(
            unit_id,
            ExecutionUnitStatus.SUBMITTING,
            expected=ExecutionUnitStatus.PENDING,
        )


def test_terminal_units_are_not_recovered(
    repository: PostgresMetadataRepository,
) -> None:
    _, unit_id = _persist_run(repository)

    repository.transition_unit_status(unit_id, ExecutionUnitStatus.READY)
    assert {unit.id for unit in repository.list_recoverable_units()} == {unit_id}

    repository.transition_unit_status(unit_id, ExecutionUnitStatus.SUBMITTING)
    repository.transition_unit_status(unit_id, ExecutionUnitStatus.RUNNING)
    terminal = repository.transition_unit_status(unit_id, ExecutionUnitStatus.SUCCEEDED)

    assert terminal.finished_at is not None
    assert repository.list_recoverable_units() == []
