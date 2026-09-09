from __future__ import annotations

import os
from collections.abc import Iterator
from uuid import UUID

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb

from dataflow.api import create_app
from dataflow.api_repository import PostgresApiRepository
from dataflow.api_service import ControlPlaneService
from dataflow.contracts import ExecutionPlan
from dataflow.kuberay import render_rayjob
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
def client(repository: PostgresApiRepository) -> Iterator[TestClient]:
    with TestClient(create_app(ControlPlaneService(repository))) as test_client:
        yield test_client


def _profile(*, namespace: str, max_replicas: int = 4) -> dict:
    return {
        "api_version": "dataflow.io/v1alpha1",
        "kind": "ClusterProfile",
        "name": "gpu-prod",
        "ray_version": "2.58.0",
        "kubernetes_namespace": namespace,
        "service_account": "dataflow-runner",
        "priority_class_name": "dataflow-high",
        "queue": "gpu-queue",
        "head": {
            "resources": {"cpu": 1, "memory_bytes": 2 * 1024**3, "gpu": 0},
            "placement": {"node_selector": {"node-pool": "control"}},
        },
        "worker_groups": [
            {
                "name": "a100-workers",
                "replicas": 0,
                "min_replicas": 0,
                "max_replicas": max_replicas,
                "resources": {
                    "cpu": 8,
                    "memory_bytes": 32 * 1024**3,
                    "gpu": 1,
                },
                "accelerator_type": "A100",
                "gpu_resource_name": "nvidia.com/gpu",
                "placement": {
                    "node_selector": {"accelerator": "a100"},
                    "tolerations": [
                        {
                            "key": "nvidia.com/gpu",
                            "operator": "Exists",
                            "effect": "NoSchedule",
                        }
                    ],
                },
                "autoscaler_priority": 10,
            }
        ],
        "autoscaling": {
            "enabled": True,
            "version": "v2",
            "idle_timeout_seconds": 120,
        },
    }


def _pipeline_spec(*, accelerator_type: str = "A100", gpu: float = 1) -> dict:
    return {
        "api_version": "dataflow.io/v1alpha1",
        "kind": "Pipeline",
        "name": "gpu-pipeline",
        "runtime": {"image": "dataflow-runtime:test"},
        "cluster_profile": "gpu-prod",
        "nodes": [
            {
                "id": "read",
                "kind": "read_parquet",
                "config": {"path": "s3://bucket/input"},
            },
            {
                "id": "predict",
                "kind": "map_batches",
                "config": {"callable": "dataflow.callables.identity_batch"},
                "resources": {
                    "cpu": 2,
                    "gpu": gpu,
                    "memory_bytes": 8 * 1024**3,
                    "accelerator_type": accelerator_type,
                },
            },
            {
                "id": "write",
                "kind": "write_parquet",
                "config": {"path": "s3://bucket/output"},
            },
        ],
        "edges": [
            {"from": "read", "to": "predict", "kind": "data"},
            {"from": "predict", "to": "write", "kind": "data"},
        ],
    }


def _create_pipeline(client: TestClient) -> dict:
    response = client.post("/v1/pipelines", json={"name": "gpu-pipeline"})
    assert response.status_code == 201
    return response.json()


def test_cluster_profile_api_exposes_builtin_and_append_only_revisions(
    client: TestClient,
    repository: PostgresApiRepository,
    postgres_dsn: str,
) -> None:
    builtin = client.get("/v1/cluster-profiles/default")
    assert builtin.status_code == 200
    assert builtin.json()["current"]["builtin"] is True
    assert builtin.json()["current"]["revision"] == 0

    created = client.post("/v1/cluster-profiles", json=_profile(namespace="ml-v1"))
    assert created.status_code == 201
    assert created.json()["revision"] == 1
    first_id = UUID(created.json()["id"])

    updated = client.put(
        "/v1/cluster-profiles/gpu-prod",
        json=_profile(namespace="ml-v2", max_replicas=8),
    )
    assert updated.status_code == 200
    assert updated.json()["revision"] == 2
    assert updated.json()["spec"]["kubernetes_namespace"] == "ml-v2"

    detail = client.get("/v1/cluster-profiles/gpu-prod")
    assert detail.status_code == 200
    assert detail.json()["current"]["revision"] == 2
    assert [version["revision"] for version in detail.json()["versions"]] == [1, 2]
    assert [record.revision for record in repository.list_cluster_profile_versions("gpu-prod")] == [
        1,
        2,
    ]

    with psycopg.connect(postgres_dsn) as connection:
        with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
            with connection.transaction():
                connection.execute(
                    "UPDATE cluster_profile_versions SET spec_json = %s WHERE id = %s",
                    (Jsonb({"mutated": True}), first_id),
                )


def test_pipeline_version_rejects_resources_profile_cannot_satisfy(
    client: TestClient,
) -> None:
    created = client.post("/v1/cluster-profiles", json=_profile(namespace="ml"))
    assert created.status_code == 201
    pipeline = _create_pipeline(client)

    invalid_spec = _pipeline_spec(accelerator_type="T4")
    response = client.post(
        f"/v1/pipelines/{pipeline['id']}/versions",
        json=invalid_spec,
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_REQUEST"
    assert "no compatible worker group" in response.json()["error"]["message"]
    assert client.get(f"/v1/pipelines/{pipeline['id']}").json()["versions"] == []


def test_pipeline_runs_pin_cluster_profile_revision(
    client: TestClient,
    repository: PostgresApiRepository,
) -> None:
    created = client.post("/v1/cluster-profiles", json=_profile(namespace="ml-v1"))
    assert created.status_code == 201
    pipeline = _create_pipeline(client)
    version_response = client.post(
        f"/v1/pipelines/{pipeline['id']}/versions",
        json=_pipeline_spec(),
    )
    assert version_response.status_code == 201
    version = version_response.json()

    run1_response = client.post(
        "/v1/pipeline-runs",
        json={"pipeline_version_id": version["id"]},
    )
    assert run1_response.status_code == 201
    run1_id = UUID(run1_response.json()["id"])
    run1_plan_json = repository.list_units(run1_id)[0].plan_json
    run1_plan = ExecutionPlan.model_validate(run1_plan_json)
    assert run1_plan.cluster_profile is not None
    assert run1_plan.cluster_profile.revision == 1
    assert run1_plan.cluster_profile.spec.kubernetes_namespace == "ml-v1"

    updated = client.put(
        "/v1/cluster-profiles/gpu-prod",
        json=_profile(namespace="ml-v2", max_replicas=8),
    )
    assert updated.status_code == 200
    assert updated.json()["revision"] == 2

    run2_response = client.post(
        "/v1/pipeline-runs",
        json={"pipeline_version_id": version["id"]},
    )
    assert run2_response.status_code == 201
    run2_id = UUID(run2_response.json()["id"])
    run2_plan = ExecutionPlan.model_validate(repository.list_units(run2_id)[0].plan_json)
    assert run2_plan.cluster_profile is not None
    assert run2_plan.cluster_profile.revision == 2
    assert run2_plan.cluster_profile.spec.kubernetes_namespace == "ml-v2"
    assert run2_plan.cluster_profile.spec.worker_groups[0].max_replicas == 8

    persisted_run1 = ExecutionPlan.model_validate(repository.list_units(run1_id)[0].plan_json)
    assert persisted_run1.cluster_profile is not None
    assert persisted_run1.cluster_profile.revision == 1
    assert persisted_run1.cluster_profile.spec.kubernetes_namespace == "ml-v1"
    assert persisted_run1.model_dump(mode="json") == run1_plan.model_dump(mode="json")

    manifest1 = render_rayjob(persisted_run1, attempt_number=1)
    manifest2 = render_rayjob(run2_plan, attempt_number=1)
    assert manifest1["metadata"]["namespace"] == "ml-v1"
    assert manifest1["metadata"]["labels"]["dataflow.io/cluster-profile-revision"] == "1"
    assert manifest2["metadata"]["namespace"] == "ml-v2"
    assert manifest2["metadata"]["labels"]["dataflow.io/cluster-profile-revision"] == "2"
    assert manifest1["spec"]["rayClusterSpec"]["workerGroupSpecs"][0]["maxReplicas"] == 4
    assert manifest2["spec"]["rayClusterSpec"]["workerGroupSpecs"][0]["maxReplicas"] == 8
