from __future__ import annotations

import os
from collections.abc import Iterator
from uuid import UUID, uuid4

import psycopg
import pytest

from dataflow.artifact_repository import PostgresArtifactRepository
from dataflow.artifacts import ArtifactFormat, ArtifactState, ArtifactStorageMetadata
from dataflow.compiler import PipelineCompiler, PipelineEdgeSpec, PipelineNodeSpec, PipelineSpec
from dataflow.contracts import OperatorKind, RuntimeSpec
from dataflow.metadata.migrations import migrate


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    dsn = os.environ.get("DATAFLOW_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("DATAFLOW_TEST_DATABASE_URL is not configured")
    migrate(dsn)
    return dsn


@pytest.fixture
def repository(postgres_dsn: str) -> Iterator[PostgresArtifactRepository]:
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
    yield PostgresArtifactRepository(postgres_dsn)


def _persist_unit(repository: PostgresArtifactRepository) -> tuple[UUID, UUID]:
    spec = PipelineSpec(
        name="artifact-registry-test",
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
    pipeline = repository.create_pipeline(name=f"artifact-{uuid4()}")
    version = repository.create_pipeline_version(
        pipeline.id,
        spec.model_dump(mode="json", by_alias=True),
    )
    run = repository.create_pipeline_run(version.id, cluster_profile=spec.cluster_profile)
    graph = PipelineCompiler().compile(spec, run_id=str(run.id))
    unit_ids = repository.create_execution_graph(run.id, graph)
    return run.id, unit_ids["unit-001"]


def _begin(
    repository: PostgresArtifactRepository,
    run_id: UUID,
    *,
    attempt_number: int,
):
    return repository.begin_artifact(
        run_id=run_id,
        unit_key="unit-001",
        node_id="read",
        attempt_number=attempt_number,
        format=ArtifactFormat.PARQUET,
        staging_uri=(
            "s3://bucket/dataflow/runs/"
            f"{run_id}/artifacts/read/attempts/{attempt_number:03d}/data"
        ),
        committed_uri=f"s3://bucket/dataflow/runs/{run_id}/artifacts/read/committed",
        checkpoint=True,
    )


def test_artifact_is_bound_to_a_real_execution_attempt(
    repository: PostgresArtifactRepository,
) -> None:
    run_id, _ = _persist_unit(repository)

    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        _begin(repository, run_id, attempt_number=1)


def test_committed_artifact_metadata_is_queryable_without_dataset_scan(
    repository: PostgresArtifactRepository,
) -> None:
    run_id, unit_id = _persist_unit(repository)
    repository.create_attempt(unit_id)
    staging = _begin(repository, run_id, attempt_number=1)

    committed = repository.commit_artifact(
        staging.id,
        ArtifactStorageMetadata(
            size_bytes=4096,
            schema_json={"fields": [{"name": "id", "type": "int64"}]},
            row_count=100,
            content_hash="abc123",
        ),
    )

    assert committed.state is ArtifactState.COMMITTED
    assert committed.checkpoint is True
    assert committed.size_bytes == 4096
    assert committed.row_count == 100
    assert committed.ref().uri == committed.committed_uri
    assert committed.ref().schema_json == {
        "fields": [{"name": "id", "type": "int64"}]
    }
    assert repository.get_committed_artifact(run_id, "read") == committed
    assert repository.list_run_artifacts(run_id) == [committed]


def test_logical_output_commit_is_idempotent_across_attempts(
    repository: PostgresArtifactRepository,
) -> None:
    run_id, unit_id = _persist_unit(repository)
    repository.create_attempt(unit_id)
    first = _begin(repository, run_id, attempt_number=1)
    metadata = ArtifactStorageMetadata(size_bytes=10, content_hash="first")
    committed = repository.commit_artifact(first.id, metadata)

    repository.create_attempt(unit_id)
    second = _begin(repository, run_id, attempt_number=2)
    winner = repository.commit_artifact(
        second.id,
        ArtifactStorageMetadata(size_bytes=20, content_hash="second"),
    )

    assert winner.id == committed.id
    assert winner.content_hash == "first"
    artifacts = repository.list_run_artifacts(run_id)
    assert [artifact.state for artifact in artifacts] == [
        ArtifactState.COMMITTED,
        ArtifactState.ABORTED,
    ]
    assert len([item for item in artifacts if item.state is ArtifactState.COMMITTED]) == 1


def test_terminal_artifact_rows_are_immutable(
    repository: PostgresArtifactRepository,
    postgres_dsn: str,
) -> None:
    run_id, unit_id = _persist_unit(repository)
    repository.create_attempt(unit_id)
    artifact = _begin(repository, run_id, attempt_number=1)
    committed = repository.commit_artifact(
        artifact.id,
        ArtifactStorageMetadata(size_bytes=1),
    )

    with psycopg.connect(postgres_dsn) as connection:
        with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
            with connection.transaction():
                connection.execute(
                    "UPDATE artifacts SET size_bytes = 999 WHERE id = %s",
                    (committed.id,),
                )
