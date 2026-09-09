"""PostgreSQL read/write surface used by the HTTP control plane."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

import psycopg

from dataflow.artifact_repository import ArtifactRecord, PostgresArtifactRepository
from dataflow.metadata.repository import (
    EventRecord,
    ExecutionAttemptRecord,
    ExecutionUnitRecord,
    MetadataNotFoundError,
    PipelineRecord,
    PipelineRunRecord,
    PipelineVersionRecord,
)


class ApiRepository(Protocol):
    def ping(self) -> bool: ...

    def create_pipeline(
        self,
        *,
        name: str,
        tenant_id: str = "default",
        description: str | None = None,
    ) -> PipelineRecord: ...

    def get_pipeline(self, pipeline_id: UUID) -> PipelineRecord: ...

    def list_pipelines(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[PipelineRecord]: ...

    def create_pipeline_version(
        self,
        pipeline_id: UUID,
        spec: dict,
    ) -> PipelineVersionRecord: ...

    def get_pipeline_version(self, version_id: UUID) -> PipelineVersionRecord: ...

    def list_pipeline_versions(self, pipeline_id: UUID) -> list[PipelineVersionRecord]: ...

    def create_pipeline_run(
        self,
        pipeline_version_id: UUID,
        *,
        parameters: dict | None = None,
        cluster_profile: str | None = None,
        created_by: str | None = None,
    ) -> PipelineRunRecord: ...

    def create_execution_graph(self, pipeline_run_id: UUID, graph) -> dict[str, UUID]: ...

    def transition_run_status(self, run_id: UUID, target, **kwargs) -> PipelineRunRecord: ...

    def get_run(self, run_id: UUID) -> PipelineRunRecord: ...

    def list_units(self, run_id: UUID) -> list[ExecutionUnitRecord]: ...

    def list_attempts(self, unit_id: UUID) -> list[ExecutionAttemptRecord]: ...

    def list_events(self, run_id: UUID) -> list[EventRecord]: ...

    def list_run_artifacts(self, run_id: UUID) -> list[ArtifactRecord]: ...


class PostgresApiRepository(PostgresArtifactRepository):
    """Control-plane repository including API-oriented lookup methods."""

    def ping(self) -> bool:
        try:
            with self._connect() as connection:
                connection.execute("SELECT 1").fetchone()
        except psycopg.Error:
            return False
        return True

    def get_pipeline(self, pipeline_id: UUID) -> PipelineRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT id, tenant_id, name, description, created_at "
                "FROM pipelines WHERE id = %s",
                (pipeline_id,),
            ).fetchone()
        if row is None:
            raise MetadataNotFoundError(f"pipeline not found: {pipeline_id}")
        return self._pipeline_from_row(row)

    def list_pipelines(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[PipelineRecord]:
        if limit < 1 or limit > 500:
            raise ValueError("limit must be between 1 and 500")
        if offset < 0:
            raise ValueError("offset must be non-negative")

        query = "SELECT id, tenant_id, name, description, created_at FROM pipelines"
        params: list[object] = []
        if tenant_id is not None:
            query += " WHERE tenant_id = %s"
            params.append(tenant_id)
        query += " ORDER BY created_at, id LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        with self._connect() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
        return [self._pipeline_from_row(row) for row in rows]

    def get_pipeline_version(self, version_id: UUID) -> PipelineVersionRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT id, pipeline_id, version, spec_json, spec_hash, created_at "
                "FROM pipeline_versions WHERE id = %s",
                (version_id,),
            ).fetchone()
        if row is None:
            raise MetadataNotFoundError(f"pipeline version not found: {version_id}")
        return self._version_from_row(row)

    def list_pipeline_versions(self, pipeline_id: UUID) -> list[PipelineVersionRecord]:
        self.get_pipeline(pipeline_id)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, pipeline_id, version, spec_json, spec_hash, created_at
                FROM pipeline_versions
                WHERE pipeline_id = %s
                ORDER BY version
                """,
                (pipeline_id,),
            ).fetchall()
        return [self._version_from_row(row) for row in rows]


__all__ = ["ApiRepository", "PostgresApiRepository"]
