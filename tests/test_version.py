from __future__ import annotations

from fastapi.testclient import TestClient

from dataflow import __version__
from dataflow.api import create_app
from dataflow.version import BuildInfo, package_version


class ReadyService:
    def ready(self) -> bool:
        return True


def test_build_info_reads_optional_immutable_identity_from_environment() -> None:
    build = BuildInfo.from_env(
        {
            "DATAFLOW_BUILD_COMMIT": " abc123 ",
            "DATAFLOW_BUILD_IMAGE": " ghcr.io/example/dataflow:sha-abc123 ",
        }
    )

    assert build == BuildInfo(
        version=package_version(),
        commit="abc123",
        image="ghcr.io/example/dataflow:sha-abc123",
    )
    assert __version__ == package_version()


def test_build_info_ignores_empty_optional_values() -> None:
    assert BuildInfo.from_env(
        {"DATAFLOW_BUILD_COMMIT": " ", "DATAFLOW_BUILD_IMAGE": ""}
    ) == BuildInfo(version=package_version())


def test_version_endpoint_and_openapi_report_the_same_build() -> None:
    build = BuildInfo(
        version="1.2.3",
        commit="abc123",
        image="ghcr.io/example/dataflow:1.2.3",
    )
    client = TestClient(create_app(ReadyService(), build_info=build))

    response = client.get("/version")

    assert response.status_code == 200
    assert response.json() == {
        "version": "1.2.3",
        "commit": "abc123",
        "image": "ghcr.io/example/dataflow:1.2.3",
    }
    assert client.get("/openapi.json").json()["info"]["version"] == "1.2.3"
