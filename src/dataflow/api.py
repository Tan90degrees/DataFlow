"""FastAPI control-plane surface for durable DataFlow orchestration."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg
from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from dataflow.api_repository import PostgresApiRepository
from dataflow.api_service import ControlPlaneService, RunSnapshot
from dataflow.artifact_repository import ArtifactRecord
from dataflow.artifacts import ArtifactFormat, ArtifactState
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


def create_app(service: ControlPlaneService) -> FastAPI:
    app = FastAPI(title="DataFlow Control Plane", version="0.1.0")

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
            details=error.errors(),
        )

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz() -> dict[str, str] | JSONResponse:
        if not service.ready():
            return _error_response(503, "NOT_READY", "PostgreSQL is unavailable")
        return {"status": "ready"}

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
    return create_app(ControlPlaneService(PostgresApiRepository(dsn)))


def main() -> None:
    try:
        import uvicorn
    except ImportError as error:
        raise RuntimeError("install DataFlow with the 'api' extra to run the API server") from error

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
