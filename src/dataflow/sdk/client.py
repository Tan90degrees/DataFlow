"""Synchronous Python client for the DataFlow control-plane HTTP API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

try:
    import httpx
except ImportError as error:  # pragma: no cover - exercised only without the SDK extra.
    raise RuntimeError("install DataFlow with the 'sdk' extra to use DataFlowClient") from error

from dataflow.compiler import PipelineSpec


class DataFlowApiError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        code: str = "HTTP_ERROR",
        details: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.details = details


@dataclass(frozen=True, slots=True)
class Submission:
    pipeline: dict[str, Any]
    version: dict[str, Any]
    run: dict[str, Any]


class DataFlowClient:
    """Small typed request surface over the public v1 control-plane API."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        if not base_url.strip():
            raise ValueError("base_url must not be empty")
        self._base_url = base_url.rstrip("/")
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(timeout=timeout, headers=headers)

    def __enter__(self) -> DataFlowClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/healthz")

    def ready(self) -> dict[str, Any]:
        return self._request("GET", "/readyz")

    def create_pipeline(
        self,
        name: str,
        *,
        tenant_id: str = "default",
        description: str | None = None,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/pipelines",
            json={
                "name": name,
                "tenant_id": tenant_id,
                "description": description,
            },
        )

    def list_pipelines(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if tenant_id is not None:
            params["tenant_id"] = tenant_id
        return self._request("GET", "/v1/pipelines", params=params)

    def get_pipeline(self, pipeline_id: UUID | str) -> dict[str, Any]:
        return self._request("GET", f"/v1/pipelines/{pipeline_id}")

    def create_pipeline_version(
        self,
        pipeline_id: UUID | str,
        spec: PipelineSpec | dict[str, Any],
    ) -> dict[str, Any]:
        payload = (
            spec.model_dump(mode="json", by_alias=True)
            if isinstance(spec, PipelineSpec)
            else dict(spec)
        )
        return self._request(
            "POST",
            f"/v1/pipelines/{pipeline_id}/versions",
            json=payload,
        )

    def create_run(
        self,
        pipeline_version_id: UUID | str,
        *,
        parameters: dict[str, Any] | None = None,
        created_by: str | None = None,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/pipeline-runs",
            json={
                "pipeline_version_id": str(pipeline_version_id),
                "parameters": parameters or {},
                "created_by": created_by,
            },
        )

    def get_run(self, run_id: UUID | str) -> dict[str, Any]:
        return self._request("GET", f"/v1/pipeline-runs/{run_id}")

    def cancel_run(self, run_id: UUID | str) -> dict[str, Any]:
        return self._request("POST", f"/v1/pipeline-runs/{run_id}/cancel")

    def list_events(self, run_id: UUID | str) -> list[dict[str, Any]]:
        return self._request("GET", f"/v1/pipeline-runs/{run_id}/events")

    def list_artifacts(self, run_id: UUID | str) -> list[dict[str, Any]]:
        return self._request("GET", f"/v1/pipeline-runs/{run_id}/artifacts")

    def submit(
        self,
        spec: PipelineSpec,
        *,
        pipeline_id: UUID | str | None = None,
        tenant_id: str = "default",
        description: str | None = None,
        parameters: dict[str, Any] | None = None,
        created_by: str | None = None,
    ) -> Submission:
        if pipeline_id is None:
            pipeline = self.create_pipeline(
                spec.name,
                tenant_id=tenant_id,
                description=description,
            )
            pipeline_id = pipeline["id"]
        else:
            detail = self.get_pipeline(pipeline_id)
            pipeline = detail["pipeline"]
            if pipeline["name"] != spec.name:
                raise ValueError(
                    f"spec name {spec.name!r} does not match pipeline {pipeline['name']!r}"
                )

        version = self.create_pipeline_version(pipeline_id, spec)
        run = self.create_run(
            version["id"],
            parameters=parameters,
            created_by=created_by,
        )
        return Submission(pipeline=pipeline, version=version, run=run)

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        response = self._client.request(
            method,
            f"{self._base_url}{path}",
            json=json,
            params=params,
        )
        if response.is_error:
            raise _api_error(response)
        if response.status_code == 204:
            return None
        return response.json()


def _api_error(response: httpx.Response) -> DataFlowApiError:
    try:
        payload = response.json()
    except ValueError:
        return DataFlowApiError(
            response.text or f"HTTP {response.status_code}",
            status_code=response.status_code,
        )

    error = payload.get("error") if isinstance(payload, dict) else None
    if isinstance(error, dict):
        return DataFlowApiError(
            str(error.get("message") or f"HTTP {response.status_code}"),
            status_code=response.status_code,
            code=str(error.get("code") or "HTTP_ERROR"),
            details=error.get("details"),
        )
    return DataFlowApiError(
        f"HTTP {response.status_code}",
        status_code=response.status_code,
        details=payload,
    )


__all__ = ["DataFlowApiError", "DataFlowClient", "Submission"]
