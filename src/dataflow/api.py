"""FastAPI control-plane surface for durable DataFlow orchestration."""

from __future__ import annotations

import os
from datetime import datetime
from time import perf_counter
from typing import Any
from uuid import UUID

import psycopg
from fastapi import FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field

from dataflow.api_repository import PostgresApiRepository
from dataflow.api_service import (
    ClusterProfileHistory,
    ControlPlaneService,
    RunDiagnosticSnapshot,
    RunSnapshot,
)
from dataflow.artifact_repository import ArtifactRecord
from dataflow.artifacts import ArtifactFormat, ArtifactState
from dataflow.cluster_profile_repository import ClusterProfileVersionRecord
from dataflow.cluster_profiles import ClusterProfileSnapshot, ClusterProfileSpec
from dataflow.compiler import PipelineSpec
from dataflow.metadata.repository import (
    ConcurrentStateChange,
    EventRecord,
    ExecutionAttemptRecord,
    ExecutionUnitRecord,
    MetadataNotFoundError,
    PipelineRecord,
    PipelineVersionRecord,
)
from dataflow.observability import (
    DEFAULT_OBSERVABILITY,
    Observability,
    configure_json_logging,
)
from dataflow.state import (
    ExecutionAttemptStatus,
    ExecutionUnitStatus,
    InvalidStateTransition,
    PipelineRunStatus,
)


class CreatePipelineRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=255)
    tenant_id: str = Field(default="default", min_length=1, max_length=255)
    description: str | None = None


class CreateRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_version_id: UUID
    parameters: dict[str, Any] = Field(default_factory=dict)
    created_by: str | None = Field(default=None, max_length=255)


class PipelineResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    tenant_id: str
    name: str
    description: str | None
    created_at: datetime

    @classmethod
    def from_record(cls, record: PipelineRecord) -> PipelineResponse:
        return cls.model_validate(record)


class PipelineVersionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    pipeline_id: UUID
    version: int
    spec: dict[str, Any]
    spec_hash: str
    created_at: datetime

    @classmethod
    def from_record(cls, record: PipelineVersionRecord) -> PipelineVersionResponse:
        return cls(
            id=record.id,
            pipeline_id=record.pipeline_id,
            version=record.version,
            spec=record.spec_json,
            spec_hash=record.spec_hash,
            created_at=record.created_at,
        )


class PipelineDetailResponse(BaseModel):
    pipeline: PipelineResponse
    versions: list[PipelineVersionResponse]


class ClusterProfileVersionResponse(BaseModel):
    id: UUID | None
    name: str
    revision: int
    spec: ClusterProfileSpec
    spec_hash: str
    created_at: datetime | None
    builtin: bool = False

    @classmethod
    def from_value(
        cls,
        value: ClusterProfileVersionRecord | ClusterProfileSnapshot,
    ) -> ClusterProfileVersionResponse:
        if isinstance(value, ClusterProfileSnapshot):
            return cls(
                id=None,
                name=value.name,
                revision=value.revision,
                spec=value.spec,
                spec_hash=value.spec_hash,
                created_at=None,
                builtin=True,
            )
        return cls(
            id=value.id,
            name=value.spec.name,
            revision=value.revision,
            spec=value.spec,
            spec_hash=value.spec_hash,
            created_at=value.created_at,
            builtin=False,
        )


class ClusterProfileDetailResponse(BaseModel):
    current: ClusterProfileVersionResponse
    versions: list[ClusterProfileVersionResponse]

    @classmethod
    def from_history(cls, history: ClusterProfileHistory) -> ClusterProfileDetailResponse:
        return cls(
            current=ClusterProfileVersionResponse.from_value(history.current),
            versions=[
                ClusterProfileVersionResponse.from_value(version)
                for version in history.versions
            ],
        )


class AttemptResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    attempt_number: int
    status: ExecutionAttemptStatus
    external_job_id: str | None
    error_code: str | None
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None

    @classmethod
    def from_record(cls, record: ExecutionAttemptRecord) -> AttemptResponse:
        return cls.model_validate(record)


class UnitResponse(BaseModel):
    id: UUID
    unit_key: str
    status: ExecutionUnitStatus
    current_attempt: int
    cluster_profile: str
    dependencies: list[str]
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    attempts: list[AttemptResponse]

    @classmethod
    def from_record(
        cls,
        record: ExecutionUnitRecord,
        attempts: list[ExecutionAttemptRecord],
    ) -> UnitResponse:
        return cls(
            id=record.id,
            unit_key=record.unit_key,
            status=record.status,
            current_attempt=record.current_attempt,
            cluster_profile=record.cluster_profile,
            dependencies=record.dependencies,
            created_at=record.created_at,
            started_at=record.started_at,
            finished_at=record.finished_at,
            attempts=[AttemptResponse.from_record(item) for item in attempts],
        )


class ArtifactResponse(BaseModel):
    id: UUID
    node_id: str
    execution_unit_id: UUID | None
    attempt_number: int
    format: ArtifactFormat
    state: ArtifactState
    committed_uri: str
    schema_json: dict[str, Any] | None
    size_bytes: int | None
    row_count: int | None
    content_hash: str | None
    checkpoint: bool
    created_at: datetime
    committed_at: datetime | None
    aborted_at: datetime | None

    @classmethod
    def from_record(cls, record: ArtifactRecord) -> ArtifactResponse:
        return cls(
            id=record.id,
            node_id=record.node_id,
            execution_unit_id=record.execution_unit_id,
            attempt_number=record.attempt_number,
            format=record.format,
            state=record.state,
            committed_uri=record.committed_uri,
            schema_json=record.schema_json,
            size_bytes=record.size_bytes,
            row_count=record.row_count,
            content_hash=record.content_hash,
            checkpoint=record.checkpoint,
            created_at=record.created_at,
            committed_at=record.committed_at,
            aborted_at=record.aborted_at,
        )


class RunResponse(BaseModel):
    id: UUID
    pipeline_version_id: UUID
    status: PipelineRunStatus
    parameters: dict[str, Any]
    cluster_profile: str | None
    created_at: datetime
    queued_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    units: list[UnitResponse]
    artifacts: list[ArtifactResponse]

    @classmethod
    def from_snapshot(cls, snapshot: RunSnapshot) -> RunResponse:
        run = snapshot.run
        return cls(
            id=run.id,
            pipeline_version_id=run.pipeline_version_id,
            status=run.status,
            parameters=run.parameters_json,
            cluster_profile=run.cluster_profile,
            created_at=run.created_at,
            queued_at=run.queued_at,
            started_at=run.started_at,
            finished_at=run.finished_at,
            units=[
                UnitResponse.from_record(item.unit, item.attempts)
                for item in snapshot.units
            ],
            artifacts=[ArtifactResponse.from_record(item) for item in snapshot.artifacts],
        )


class EventResponse(BaseModel):
    id: int
    aggregate_type: str
    aggregate_id: UUID
    event_type: str
    payload: dict[str, Any]
    created_at: datetime

    @classmethod
    def from_record(cls, record: EventRecord) -> EventResponse:
        return cls(
            id=record.id,
            aggregate_type=record.aggregate_type,
            aggregate_id=record.aggregate_id,
            event_type=record.event_type,
            payload=record.payload_json,
            created_at=record.created_at,
        )


class DiagnosticUnitResponse(BaseModel):
    unit: UnitResponse
    latest_external_job_id: str | None
    artifacts: list[ArtifactResponse]


class RunDiagnosticResponse(BaseModel):
    pipeline: PipelineResponse
    version: PipelineVersionResponse
    run_id: UUID
    status: PipelineRunStatus
    cluster_profile: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    units: list[DiagnosticUnitResponse]
    events: list[EventResponse]

    @classmethod
    def from_snapshot(cls, snapshot: RunDiagnosticSnapshot) -> RunDiagnosticResponse:
        diagnostic_units: list[DiagnosticUnitResponse] = []
        for item in snapshot.units:
            latest_job_id = next(
                (
                    attempt.external_job_id
                    for attempt in reversed(item.attempts)
                    if attempt.external_job_id
                ),
                None,
            )
            diagnostic_units.append(
                DiagnosticUnitResponse(
                    unit=UnitResponse.from_record(item.unit, item.attempts),
                    latest_external_job_id=latest_job_id,
                    artifacts=[ArtifactResponse.from_record(value) for value in item.artifacts],
                )
            )
        run = snapshot.run
        return cls(
            pipeline=PipelineResponse.from_record(snapshot.pipeline),
            version=PipelineVersionResponse.from_record(snapshot.version),
            run_id=run.id,
            status=run.status,
            cluster_profile=run.cluster_profile,
            created_at=run.created_at,
            started_at=run.started_at,
            finished_at=run.finished_at,
            units=diagnostic_units,
            events=[EventResponse.from_record(event) for event in snapshot.events],
        )


def create_app(
    service: ControlPlaneService,
    *,
    observability: Observability | None = None,
) -> FastAPI:
    obs = observability or DEFAULT_OBSERVABILITY
    app = FastAPI(title="DataFlow Control Plane", version="0.1.0")

    @app.middleware("http")
    async def observe_request(request: Request, call_next):
        started = perf_counter()
        status_code = 500
        with obs.span(
            "dataflow.http_request",
            **{"http.request.method": request.method, "url.path": request.url.path},
        ):
            try:
                response = await call_next(request)
                status_code = response.status_code
                return response
            finally:
                route_object = request.scope.get("route")
                route = getattr(route_object, "path", request.url.path)
                duration = perf_counter() - started
                if route != "/metrics":
                    obs.metrics.observe_api_request(
                        method=request.method,
                        route=route,
                        status_code=status_code,
                        duration_seconds=duration,
                    )
                    obs.info(
                        "api_request",
                        method=request.method,
                        route=route,
                        status_code=status_code,
                        duration_seconds=round(duration, 6),
                    )

    @app.exception_handler(MetadataNotFoundError)
    def handle_not_found(_request: Request, error: MetadataNotFoundError) -> JSONResponse:
        return _error_response(404, "NOT_FOUND", _exception_message(error))

    @app.exception_handler(ConcurrentStateChange)
    def handle_concurrent_state(
        _request: Request,
        error: ConcurrentStateChange,
    ) -> JSONResponse:
        return _error_response(409, "CONCURRENT_STATE_CHANGE", str(error))

    @app.exception_handler(InvalidStateTransition)
    def handle_invalid_transition(
        _request: Request,
        error: InvalidStateTransition,
    ) -> JSONResponse:
        return _error_response(409, "INVALID_STATE_TRANSITION", str(error))

    @app.exception_handler(psycopg.errors.UniqueViolation)
    def handle_unique_violation(
        _request: Request,
        _error: psycopg.errors.UniqueViolation,
    ) -> JSONResponse:
        return _error_response(409, "ALREADY_EXISTS", "resource already exists")

    @app.exception_handler(ValueError)
    def handle_value_error(_request: Request, error: ValueError) -> JSONResponse:
        return _error_response(400, "INVALID_REQUEST", str(error))

    @app.exception_handler(RequestValidationError)
    def handle_request_validation(
        _request: Request,
        error: RequestValidationError,
    ) -> JSONResponse:
        return _error_response(
            422,
            "VALIDATION_ERROR",
            "request validation failed",
            details=jsonable_encoder(error.errors()),
        )

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", response_model=None)
    def readyz() -> Any:
        if not service.ready():
            return _error_response(503, "NOT_READY", "PostgreSQL is unavailable")
        return {"status": "ready"}

    @app.get("/metrics", response_model=None)
    def metrics() -> Response:
        rendered = obs.metrics.render()
        if rendered is None:
            return Response(status_code=404)
        body, content_type = rendered
        return Response(content=body, media_type=content_type)

    @app.post(
        "/v1/cluster-profiles",
        response_model=ClusterProfileVersionResponse,
        status_code=201,
    )
    def create_cluster_profile(spec: ClusterProfileSpec) -> ClusterProfileVersionResponse:
        return ClusterProfileVersionResponse.from_value(service.create_cluster_profile(spec))

    @app.get(
        "/v1/cluster-profiles",
        response_model=list[ClusterProfileVersionResponse],
    )
    def list_cluster_profiles() -> list[ClusterProfileVersionResponse]:
        return [
            ClusterProfileVersionResponse.from_value(value)
            for value in service.list_cluster_profiles()
        ]

    @app.get(
        "/v1/cluster-profiles/{name}",
        response_model=ClusterProfileDetailResponse,
    )
    def get_cluster_profile(name: str) -> ClusterProfileDetailResponse:
        return ClusterProfileDetailResponse.from_history(service.get_cluster_profile(name))

    @app.put(
        "/v1/cluster-profiles/{name}",
        response_model=ClusterProfileVersionResponse,
    )
    def update_cluster_profile(
        name: str,
        spec: ClusterProfileSpec,
    ) -> ClusterProfileVersionResponse:
        return ClusterProfileVersionResponse.from_value(
            service.update_cluster_profile(name, spec)
        )

    @app.post("/v1/pipelines", response_model=PipelineResponse, status_code=201)
    def create_pipeline(request: CreatePipelineRequest) -> PipelineResponse:
        record = service.create_pipeline(
            name=request.name,
            tenant_id=request.tenant_id,
            description=request.description,
        )
        return PipelineResponse.from_record(record)

    @app.get("/v1/pipelines", response_model=list[PipelineResponse])
    def list_pipelines(
        tenant_id: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> list[PipelineResponse]:
        return [
            PipelineResponse.from_record(record)
            for record in service.list_pipelines(
                tenant_id=tenant_id,
                limit=limit,
                offset=offset,
            )
        ]

    @app.get("/v1/pipelines/{pipeline_id}", response_model=PipelineDetailResponse)
    def get_pipeline(pipeline_id: UUID) -> PipelineDetailResponse:
        snapshot = service.get_pipeline(pipeline_id)
        return PipelineDetailResponse(
            pipeline=PipelineResponse.from_record(snapshot.pipeline),
            versions=[
                PipelineVersionResponse.from_record(record) for record in snapshot.versions
            ],
        )

    @app.post(
        "/v1/pipelines/{pipeline_id}/versions",
        response_model=PipelineVersionResponse,
        status_code=201,
    )
    def create_pipeline_version(
        pipeline_id: UUID,
        spec: PipelineSpec,
    ) -> PipelineVersionResponse:
        return PipelineVersionResponse.from_record(
            service.create_pipeline_version(pipeline_id, spec)
        )

    @app.post("/v1/pipeline-runs", response_model=RunResponse, status_code=201)
    def create_run(request: CreateRunRequest) -> RunResponse:
        return RunResponse.from_snapshot(
            service.create_run(
                request.pipeline_version_id,
                parameters=request.parameters,
                created_by=request.created_by,
            )
        )

    @app.get("/v1/pipeline-runs/{run_id}", response_model=RunResponse)
    def get_run(run_id: UUID) -> RunResponse:
        return RunResponse.from_snapshot(service.get_run(run_id))

    @app.get(
        "/v1/pipeline-runs/{run_id}/diagnostics",
        response_model=RunDiagnosticResponse,
    )
    def get_run_diagnostics(
        run_id: UUID,
        event_limit: int = Query(default=200, ge=1, le=1000),
    ) -> RunDiagnosticResponse:
        return RunDiagnosticResponse.from_snapshot(
            service.get_run_diagnostics(run_id, event_limit=event_limit)
        )

    @app.post("/v1/pipeline-runs/{run_id}/cancel", response_model=RunResponse)
    def cancel_run(run_id: UUID) -> RunResponse:
        return RunResponse.from_snapshot(service.cancel_run(run_id))

    @app.get("/v1/pipeline-runs/{run_id}/events", response_model=list[EventResponse])
    def list_events(run_id: UUID) -> list[EventResponse]:
        return [EventResponse.from_record(record) for record in service.list_events(run_id)]

    @app.get(
        "/v1/pipeline-runs/{run_id}/artifacts",
        response_model=list[ArtifactResponse],
    )
    def list_artifacts(run_id: UUID) -> list[ArtifactResponse]:
        return [
            ArtifactResponse.from_record(record) for record in service.list_artifacts(run_id)
        ]

    return app


def create_app_from_env() -> FastAPI:
    dsn = os.environ.get("DATAFLOW_DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATAFLOW_DATABASE_URL is required")
    observability = Observability.from_env()
    service = ControlPlaneService(PostgresApiRepository(dsn), observability=observability)
    return create_app(service, observability=observability)


def main() -> None:
    try:
        import uvicorn
    except ImportError as error:
        raise RuntimeError("install DataFlow with the 'api' extra to run the API server") from error

    if os.environ.get("DATAFLOW_JSON_LOGS", "false").lower() in {"1", "true", "yes"}:
        configure_json_logging()
    host = os.environ.get("DATAFLOW_API_HOST", "0.0.0.0")
    port = int(os.environ.get("DATAFLOW_API_PORT", "8080"))
    uvicorn.run(
        "dataflow.api:create_app_from_env",
        factory=True,
        host=host,
        port=port,
    )


def _error_response(
    status_code: int,
    code: str,
    message: str,
    *,
    details: Any | None = None,
) -> JSONResponse:
    body: dict[str, Any] = {"error": {"code": code, "message": message}}
    if details is not None:
        body["error"]["details"] = details
    return JSONResponse(status_code=status_code, content=body)


def _exception_message(error: Exception) -> str:
    if error.args:
        return str(error.args[0])
    return str(error)


__all__ = ["create_app", "create_app_from_env", "main"]
