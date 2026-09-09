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
from dataflow.metadata.migrations import migrate


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
def client(repository: PostgresApiRepository) -> TestClient:
    return TestClient(create_app(ControlPlaneService(repository)))


def _pipeline_spec(name: str = "api-pipeline") -> dict:
    return {
        "api_version": "dataflow.io/v1alpha1",
        "kind": "Pipeline",
        "name": name,
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


def _create_pipeline_and_version(client: TestClient) -> tuple[dict, dict]:
    pipeline_response = client.post(
        "/v1/pipelines",
        json={"name": "api-pipeline", "description": "integration test"},
    )
    assert pipeline_response.status_code == 201
    pipeline = pipeline_response.json()

    version_response = client.post(
        f"/v1/pipelines/{pipeline['id']}/versions",
        json=_pipeline_spec(),
    )
    assert version_response.status_code == 201
    return pipeline, version_response.json()


def test_health_and_readiness_use_database(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}
    ready = client.get("/readyz")
    assert ready.status_code == 200
    assert ready.json() == {"status": "ready"}


def test_pipeline_version_and_run_lifecycle_is_persisted(client: TestClient) -> None:
    pipeline, version = _create_pipeline_and_version(client)

    detail = client.get(f"/v1/pipelines/{pipeline['id']}")
    assert detail.status_code == 200
    assert detail.json()["pipeline"]["name"] == "api-pipeline"
    assert [item["version"] for item in detail.json()["versions"]] == [1]

    run_response = client.post(
        "/v1/pipeline-runs",
        json={
            "pipeline_version_id": version["id"],
            "parameters": {"country": "jp"},
            "created_by": "api-test",
        },
    )
    assert run_response.status_code == 201
    run = run_response.json()

    assert run["status"] == "RUNNING"
    assert run["parameters"] == {"country": "jp"}
    assert [unit["unit_key"] for unit in run["units"]] == ["unit-001", "unit-002"]
    assert [unit["status"] for unit in run["units"]] == ["READY", "PENDING"]
    assert run["units"][0]["attempts"] == []
    assert run["artifacts"] == []

    persisted = client.get(f"/v1/pipeline-runs/{run['id']}")
    assert persisted.status_code == 200
    assert persisted.json()["id"] == run["id"]

    events = client.get(f"/v1/pipeline-runs/{run['id']}/events")
    assert events.status_code == 200
    event_types = [item["event_type"] for item in events.json()]
    assert "RUN_CREATED" in event_types
    assert "EXECUTION_GRAPH_CREATED" in event_types
    assert "RUN_RUNNING" in event_types
    assert "UNIT_READY" in event_types

    artifacts = client.get(f"/v1/pipeline-runs/{run['id']}/artifacts")
    assert artifacts.status_code == 200
    assert artifacts.json() == []


def test_cancel_run_uses_durable_scheduler_state_machine(client: TestClient) -> None:
    _, version = _create_pipeline_and_version(client)
    run = client.post(
        "/v1/pipeline-runs",
        json={"pipeline_version_id": version["id"]},
    ).json()

    cancelled = client.post(f"/v1/pipeline-runs/{run['id']}/cancel")

    assert cancelled.status_code == 200
    body = cancelled.json()
    assert body["status"] == "CANCELLED"
    assert [unit["status"] for unit in body["units"]] == ["CANCELLED", "CANCELLED"]

    event_types = [
        item["event_type"]
        for item in client.get(f"/v1/pipeline-runs/{run['id']}/events").json()
    ]
    assert "RUN_CANCELLED" in event_types
    assert event_types.count("UNIT_CANCELLED") == 2


def test_invalid_version_does_not_persist_pipeline_version(client: TestClient) -> None:
    pipeline = client.post("/v1/pipelines", json={"name": "api-pipeline"}).json()
    invalid_spec = _pipeline_spec(name="different-name")

    response = client.post(
        f"/v1/pipelines/{pipeline['id']}/versions",
        json=invalid_spec,
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_REQUEST"
    detail = client.get(f"/v1/pipelines/{pipeline['id']}").json()
    assert detail["versions"] == []


def test_duplicate_pipeline_is_reported_as_conflict(client: TestClient) -> None:
    first = client.post("/v1/pipelines", json={"name": "duplicate"})
    second = client.post("/v1/pipelines", json={"name": "duplicate"})

    assert first.status_code == 201
    assert second.status_code == 409
    assert second.json()["error"]["code"] == "ALREADY_EXISTS"


def test_unknown_resources_return_error_envelope(client: TestClient) -> None:
    missing = UUID("00000000-0000-0000-0000-000000000001")

    response = client.get(f"/v1/pipelines/{missing}")

    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "NOT_FOUND",
            "message": f"pipeline not found: {missing}",
        }
    }

    run_response = client.post(
        "/v1/pipeline-runs",
        json={"pipeline_version_id": str(missing)},
    )
    assert run_response.status_code == 404
    assert run_response.json()["error"]["code"] == "NOT_FOUND"


def test_request_validation_has_stable_error_shape(client: TestClient) -> None:
    response = client.post("/v1/pipelines", json={"name": ""})

    assert response.status_code == 422
    body = response.json()["error"]
    assert body["code"] == "VALIDATION_ERROR"
    assert body["message"] == "request validation failed"
    assert body["details"]
