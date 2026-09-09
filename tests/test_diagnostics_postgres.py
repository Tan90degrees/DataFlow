from __future__ import annotations

import os
from collections.abc import Iterator
from uuid import UUID

import psycopg
import pytest
from fastapi.testclient import TestClient

from dataflow.api import create_app
from dataflow.api_repository import PostgresApiRepository
from dataflow.api_service import ControlPlaneService
from dataflow.artifacts import ArtifactFormat
from dataflow.metadata.migrations import migrate
from dataflow.state import ExecutionAttemptStatus, ExecutionUnitStatus


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    dsn = os.environ.get("DATAFLOW_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("DATAFLOW_TEST_DATABASE_URL is not configured")
    migrate(dsn)
    return dsn


@pytest.fixture
def repository(postgres_dsn: str) -> Iterator[PostgresApiRepository]:
    with psycopg.connect(postgres_dsn) as connection:
        connection.execute(
            """
            TRUNCATE TABLE
                cluster_profile_versions,
                cluster_profiles,
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
    yield PostgresApiRepository(postgres_dsn)


@pytest.fixture
def client(repository: PostgresApiRepository) -> Iterator[TestClient]:
    with TestClient(create_app(ControlPlaneService(repository))) as test_client:
        yield test_client


def _spec() -> dict:
    return {
        "api_version": "dataflow.io/v1alpha1",
        "kind": "Pipeline",
        "name": "diagnostic-pipeline",
        "runtime": {"image": "dataflow-runtime:test"},
        "cluster_profile": "default",
        "artifact_base_uri": "s3://bucket/dataflow",
        "nodes": [
            {
                "id": "read",
                "kind": "read_parquet",
                "config": {"path": "s3://bucket/input"},
            },
            {
                "id": "filter",
                "kind": "filter",
                "config": {"callable": "dataflow.callables.keep_all"},
            },
            {
                "id": "write",
                "kind": "write_parquet",
                "config": {"path": "s3://bucket/output"},
            },
        ],
        "edges": [
            {
                "from": "read",
                "to": "filter",
                "kind": "data",
                "boundary": "hard",
            },
            {"from": "filter", "to": "write", "kind": "data"},
        ],
    }


def test_run_diagnostics_correlate_attempt_job_artifact_and_events(
    client: TestClient,
    repository: PostgresApiRepository,
) -> None:
    pipeline = client.post(
        "/v1/pipelines",
        json={"name": "diagnostic-pipeline"},
    ).json()
    version = client.post(
        f"/v1/pipelines/{pipeline['id']}/versions",
        json=_spec(),
    ).json()
    run = client.post(
        "/v1/pipeline-runs",
        json={"pipeline_version_id": version["id"]},
    ).json()
    run_id = UUID(run["id"])

    first = repository.list_units(run_id)[0]
    assert first.status is ExecutionUnitStatus.READY
    submitting = repository.transition_unit_status(
        first.id,
        ExecutionUnitStatus.SUBMITTING,
        expected=ExecutionUnitStatus.READY,
    )
    attempt = repository.create_attempt(submitting.id)
    repository.transition_attempt_status(
        attempt.id,
        ExecutionAttemptStatus.SUBMITTING,
        expected=ExecutionAttemptStatus.PENDING,
        external_job_id="dataflow-run-unit-a001",
    )
    repository.begin_artifact(
        run_id=run_id,
        unit_key=first.unit_key,
        node_id="read",
        attempt_number=1,
        format=ArtifactFormat.PARQUET,
        staging_uri="s3://bucket/staging/read/1",
        committed_uri="s3://bucket/committed/read",
        checkpoint=True,
    )

    response = client.get(f"/v1/pipeline-runs/{run_id}/diagnostics?event_limit=20")

    assert response.status_code == 200
    body = response.json()
    assert body["pipeline"]["name"] == "diagnostic-pipeline"
    assert body["version"]["id"] == version["id"]
    assert body["run_id"] == str(run_id)
    first_diagnostic = body["units"][0]
    assert first_diagnostic["unit"]["unit_key"] == first.unit_key
    assert first_diagnostic["latest_external_job_id"] == "dataflow-run-unit-a001"
    assert first_diagnostic["unit"]["attempts"][0]["attempt_number"] == 1
    assert first_diagnostic["artifacts"][0]["node_id"] == "read"
    assert first_diagnostic["artifacts"][0]["execution_unit_id"] == str(first.id)
    assert first_diagnostic["artifacts"][0]["state"] == "STAGING"
    assert any(event["event_type"] == "ATTEMPT_CREATED" for event in body["events"])


def test_diagnostic_event_limit_is_validated(client: TestClient) -> None:
    missing = UUID("00000000-0000-0000-0000-000000000001")

    response = client.get(f"/v1/pipeline-runs/{missing}/diagnostics?event_limit=0")

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"
