from __future__ import annotations

import os
from collections.abc import Iterator

import psycopg
import pytest
from fastapi.testclient import TestClient

from dataflow.api import create_app
from dataflow.api_repository import PostgresApiRepository
from dataflow.api_service import ControlPlaneService
from dataflow.metadata.migrations import migrate
from dataflow.sdk import DataFlowApiError, DataFlowClient, pipeline


def keep_all(row):
    return True


@pipeline(
    name="sdk-api-pipeline",
    runtime_image="dataflow-runtime:test",
    artifact_base_uri="s3://bucket/dataflow",
)
def sdk_api_pipeline(flow, input_path: str, output_path: str) -> None:
    dataset = flow.read_parquet(input_path, node_id="read")
    dataset = dataset.checkpoint().filter(keep_all, node_id="filter")
    dataset.write_parquet(output_path, node_id="write")


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
def sdk_client(repository: PostgresApiRepository) -> Iterator[DataFlowClient]:
    with TestClient(create_app(ControlPlaneService(repository))) as http_client:
        yield DataFlowClient("http://testserver", http_client=http_client)


def test_sdk_submits_version_and_starts_run_against_control_plane(
    sdk_client: DataFlowClient,
) -> None:
    spec = sdk_api_pipeline.spec("s3://bucket/input", "s3://bucket/output")

    submission = sdk_client.submit(
        spec,
        parameters={"batch": "2026-09-09"},
        created_by="sdk-test",
    )

    assert submission.pipeline["name"] == "sdk-api-pipeline"
    assert submission.version["version"] == 1
    assert submission.version["spec_hash"]
    assert submission.run["status"] == "RUNNING"
    assert submission.run["parameters"] == {"batch": "2026-09-09"}
    assert [unit["status"] for unit in submission.run["units"]] == ["READY", "PENDING"]

    run_id = submission.run["id"]
    assert sdk_client.get_run(run_id)["id"] == run_id
    assert sdk_client.list_artifacts(run_id) == []
    assert "RUN_RUNNING" in {
        event["event_type"] for event in sdk_client.list_events(run_id)
    }

    cancelled = sdk_client.cancel_run(run_id)
    assert cancelled["status"] == "CANCELLED"
    assert [unit["status"] for unit in cancelled["units"]] == [
        "CANCELLED",
        "CANCELLED",
    ]


def test_sdk_can_add_a_new_version_to_an_existing_pipeline(
    sdk_client: DataFlowClient,
) -> None:
    spec = sdk_api_pipeline.spec("s3://bucket/input", "s3://bucket/output")
    pipeline_record = sdk_client.create_pipeline(spec.name)

    first = sdk_client.submit(spec, pipeline_id=pipeline_record["id"])
    second = sdk_client.submit(spec, pipeline_id=pipeline_record["id"])

    assert first.pipeline["id"] == second.pipeline["id"] == pipeline_record["id"]
    assert first.version["version"] == 1
    assert second.version["version"] == 2
    assert first.version["spec_hash"] == second.version["spec_hash"]


def test_sdk_exposes_control_plane_error_codes(sdk_client: DataFlowClient) -> None:
    sdk_client.create_pipeline("duplicate")

    with pytest.raises(DataFlowApiError) as raised:
        sdk_client.create_pipeline("duplicate")

    assert raised.value.status_code == 409
    assert raised.value.code == "ALREADY_EXISTS"
