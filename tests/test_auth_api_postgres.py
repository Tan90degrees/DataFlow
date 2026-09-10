from __future__ import annotations

import os
from collections.abc import Iterator

import psycopg
import pytest
from fastapi.testclient import TestClient

from dataflow.api_repository import PostgresApiRepository
from dataflow.api_service import ControlPlaneService
from dataflow.auth import (
    ROLE_CLUSTER_PROFILE_ADMIN,
    ROLE_PIPELINE_READ,
    ROLE_PIPELINE_SUBMIT,
    ROLE_RUN_CANCEL,
    ApiAuthSettings,
)
from dataflow.authenticated_api import create_app
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
    settings = ApiAuthSettings(
        mode="static",
        static_tokens={
            "reader": {"sub": "reader", "roles": [ROLE_PIPELINE_READ]},
            "submitter": {
                "sub": "submitter",
                "roles": [ROLE_PIPELINE_READ, ROLE_PIPELINE_SUBMIT],
            },
            "canceller": {
                "sub": "canceller",
                "roles": [ROLE_PIPELINE_READ, ROLE_RUN_CANCEL],
            },
            "profile-admin": {
                "sub": "profile-admin",
                "roles": [ROLE_CLUSTER_PROFILE_ADMIN],
            },
        },
    )
    return TestClient(create_app(ControlPlaneService(repository), auth_settings=settings))


def _headers(token: str) -> dict[str, str]:
    return {"authorization": f"Bearer {token}"}


def _pipeline_spec() -> dict:
    return {
        "api_version": "dataflow.io/v1alpha1",
        "kind": "Pipeline",
        "name": "secured-pipeline",
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
                "id": "write",
                "kind": "write_parquet",
                "config": {"path": "s3://bucket/output"},
            },
        ],
        "edges": [{"from": "read", "to": "write", "kind": "data"}],
    }


def _cluster_profile() -> dict:
    return {
        "api_version": "dataflow.io/v1alpha1",
        "kind": "ClusterProfile",
        "name": "secured-cpu",
        "ray_version": "2.58.0",
        "kubernetes_namespace": "dataflow",
        "service_account": "dataflow-ray",
        "head": {
            "resources": {"cpu": 1, "memory_bytes": 2147483648, "gpu": 0},
            "placement": {"node_selector": {}, "tolerations": []},
        },
        "worker_groups": [
            {
                "name": "workers",
                "replicas": 1,
                "min_replicas": 1,
                "max_replicas": 2,
                "resources": {"cpu": 1, "memory_bytes": 2147483648, "gpu": 0},
                "gpu_resource_name": "nvidia.com/gpu",
                "placement": {"node_selector": {}, "tolerations": []},
                "autoscaler_priority": 0,
            }
        ],
        "autoscaling": {"enabled": False, "version": "v2", "idle_timeout_seconds": 60},
    }


def test_public_health_but_v1_mutations_require_authentication(client: TestClient) -> None:
    assert client.get("/healthz").status_code == 200

    response = client.post("/v1/pipelines", json={"name": "anonymous"})
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "UNAUTHENTICATED"

    listed = client.get("/v1/pipelines", headers=_headers("reader"))
    assert listed.status_code == 200
    assert listed.json() == []


def test_pipeline_submit_role_does_not_grant_admin_or_cancel(client: TestClient) -> None:
    pipeline_response = client.post(
        "/v1/pipelines",
        json={"name": "secured-pipeline"},
        headers=_headers("submitter"),
    )
    assert pipeline_response.status_code == 201
    pipeline = pipeline_response.json()

    reader_write = client.post(
        "/v1/pipelines",
        json={"name": "reader-must-not-write"},
        headers=_headers("reader"),
    )
    assert reader_write.status_code == 403

    version_response = client.post(
        f"/v1/pipelines/{pipeline['id']}/versions",
        json=_pipeline_spec(),
        headers=_headers("submitter"),
    )
    assert version_response.status_code == 201

    run_response = client.post(
        "/v1/pipeline-runs",
        json={"pipeline_version_id": version_response.json()["id"]},
        headers=_headers("submitter"),
    )
    assert run_response.status_code == 201
    run_id = run_response.json()["id"]

    forbidden_cancel = client.post(
        f"/v1/pipeline-runs/{run_id}/cancel",
        headers=_headers("submitter"),
    )
    assert forbidden_cancel.status_code == 403

    cancelled = client.post(
        f"/v1/pipeline-runs/{run_id}/cancel",
        headers=_headers("canceller"),
    )
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "CANCELLED"

    forbidden_profile = client.post(
        "/v1/cluster-profiles",
        json=_cluster_profile(),
        headers=_headers("submitter"),
    )
    assert forbidden_profile.status_code == 403


def test_cluster_profile_admin_is_separate_from_pipeline_submission(client: TestClient) -> None:
    created = client.post(
        "/v1/cluster-profiles",
        json=_cluster_profile(),
        headers=_headers("profile-admin"),
    )
    assert created.status_code == 201
    assert created.json()["name"] == "secured-cpu"

    cannot_submit = client.post(
        "/v1/pipelines",
        json={"name": "admin-is-not-submitter"},
        headers=_headers("profile-admin"),
    )
    assert cannot_submit.status_code == 403
