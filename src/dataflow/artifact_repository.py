"""PostgreSQL registry for durable logical artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from dataflow.artifacts import (
    ArtifactFormat,
    ArtifactRef,
    ArtifactState,
    ArtifactStorageMetadata,
)
from dataflow.control_repository import PostgresControlRepository
from dataflow.metadata.repository import MetadataNotFoundError


@dataclass(frozen=True, slots=True)
class ArtifactRecord:
    id: UUID
    pipeline_run_id: UUID
    node_id: str
    execution_unit_id: UUID | None
    attempt_number: int
    format: ArtifactFormat
    state: ArtifactState
    staging_uri: str
    committed_uri: str
    schema_json: dict[str, Any] | None
    size_bytes: int | None
    row_count: int | None
    content_hash: str | None
    checkpoint: bool
    created_at: datetime
    committed_at: datetime | None
    aborted_at: datetime | None

    def ref(self) -> ArtifactRef:
        return ArtifactRef(
            run_id=str(self.pipeline_run_id),
            node_id=self.node_id,
            format=self.format,
            uri=self.committed_uri,
            schema_json=self.schema_json,
            size_bytes=self.size_bytes,
            row_count=self.row_count,
            content_hash=self.content_hash,
        )


class ArtifactRepository(Protocol):
    def begin_artifact(
        self,
        *,
        run_id: UUID,
        unit_key: str,
        node_id: str,
        attempt_number: int,
        format: ArtifactFormat,
        staging_uri: str,
        committed_uri: str,
        checkpoint: bool,
    ) -> ArtifactRecord: ...

    def commit_artifact(
        self,
        artifact_id: UUID,
        metadata: ArtifactStorageMetadata,
    ) -> ArtifactRecord: ...

    def abort_artifact(self, artifact_id: UUID) -> ArtifactRecord: ...

    def get_committed_artifact(self, run_id: UUID, node_id: str) -> ArtifactRecord | None: ...

    def list_run_artifacts(self, run_id: UUID) -> list[ArtifactRecord]: ...


class PostgresArtifactRepository(PostgresControlRepository):
    """Control-plane repository with transactional artifact publication."""

    def begin_artifact(
        self,
        *,
        run_id: UUID,
        unit_key: str,
        node_id: str,
        attempt_number: int,
        format: ArtifactFormat,
        staging_uri: str,
        committed_uri: str,
        checkpoint: bool,
    ) -> ArtifactRecord:
        if attempt_number < 1:
            raise ValueError("attempt_number must be at least 1")

        with self._connect() as connection, connection.transaction():
            self._lock_logical_output(connection, run_id, node_id)
            existing = connection.execute(
                """
                SELECT * FROM artifacts
                WHERE pipeline_run_id = %s AND node_id = %s AND attempt_number = %s
                """,
                (run_id, node_id, attempt_number),
            ).fetchone()
            if existing is not None:
                return self._artifact_from_row(existing)

            unit = connection.execute(
                """
                SELECT id FROM execution_units
                WHERE pipeline_run_id = %s AND unit_key = %s
                """,
                (run_id, unit_key),
            ).fetchone()
            if unit is None:
                raise MetadataNotFoundError(
                    f"execution unit not found for run {run_id}: {unit_key}"
                )

            artifact_id = uuid4()
            row = connection.execute(
                """
                INSERT INTO artifacts (
                    id, pipeline_run_id, node_id, execution_unit_id, attempt_number,
                    format, state, staging_uri, committed_uri, checkpoint
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    artifact_id,
                    run_id,
                    node_id,
                    unit["id"],
                    attempt_number,
                    format.value,
                    ArtifactState.STAGING.value,
                    staging_uri,
                    committed_uri,
                    checkpoint,
                ),
            ).fetchone()
            self._insert_event(
                connection,
                pipeline_run_id=run_id,
                aggregate_type="artifact",
                aggregate_id=artifact_id,
                event_type="ARTIFACT_STAGING",
                payload={
                    "node_id": node_id,
                    "attempt_number": attempt_number,
                    "staging_uri": staging_uri,
                    "committed_uri": committed_uri,
                    "checkpoint": checkpoint,
                },
            )
        assert row is not None
        return self._artifact_from_row(row)

    def commit_artifact(
        self,
        artifact_id: UUID,
        metadata: ArtifactStorageMetadata,
    ) -> ArtifactRecord:
        with self._connect() as connection, connection.transaction():
            row = connection.execute(
                "SELECT * FROM artifacts WHERE id = %s FOR UPDATE",
                (artifact_id,),
            ).fetchone()
            if row is None:
                raise MetadataNotFoundError(f"artifact not found: {artifact_id}")
            artifact = self._artifact_from_row(row)
            if artifact.state is ArtifactState.COMMITTED:
                return artifact
            if artifact.state is ArtifactState.ABORTED:
                raise ValueError("aborted artifacts cannot be committed")

            self._lock_logical_output(
                connection,
                artifact.pipeline_run_id,
                artifact.node_id,
            )
            committed = connection.execute(
                """
                SELECT * FROM artifacts
                WHERE pipeline_run_id = %s AND node_id = %s AND state = 'COMMITTED'
                FOR SHARE
                """,
                (artifact.pipeline_run_id, artifact.node_id),
            ).fetchone()
            if committed is not None:
                connection.execute(
                    """
                    UPDATE artifacts
                    SET state = 'ABORTED', aborted_at = COALESCE(aborted_at, NOW())
                    WHERE id = %s
                    """,
                    (artifact.id,),
                )
                return self._artifact_from_row(committed)

            updated = connection.execute(
                """
                UPDATE artifacts
                SET state = 'COMMITTED',
                    schema_json = %s,
                    size_bytes = %s,
                    row_count = %s,
                    content_hash = %s,
                    committed_at = COALESCE(committed_at, NOW())
                WHERE id = %s
                RETURNING *
                """,
                (
                    Jsonb(metadata.schema_json) if metadata.schema_json is not None else None,
                    metadata.size_bytes,
                    metadata.row_count,
                    metadata.content_hash,
                    artifact.id,
                ),
            ).fetchone()
            self._insert_event(
                connection,
                pipeline_run_id=artifact.pipeline_run_id,
                aggregate_type="artifact",
                aggregate_id=artifact.id,
                event_type="ARTIFACT_COMMITTED",
                payload={
                    "node_id": artifact.node_id,
                    "attempt_number": artifact.attempt_number,
                    "uri": artifact.committed_uri,
                    "size_bytes": metadata.size_bytes,
                    "row_count": metadata.row_count,
                    "content_hash": metadata.content_hash,
                },
            )
        assert updated is not None
        return self._artifact_from_row(updated)

    def abort_artifact(self, artifact_id: UUID) -> ArtifactRecord:
        with self._connect() as connection, connection.transaction():
            row = connection.execute(
                "SELECT * FROM artifacts WHERE id = %s FOR UPDATE",
                (artifact_id,),
            ).fetchone()
            if row is None:
                raise MetadataNotFoundError(f"artifact not found: {artifact_id}")
            artifact = self._artifact_from_row(row)
            if artifact.state in {ArtifactState.ABORTED, ArtifactState.COMMITTED}:
                return artifact
            updated = connection.execute(
                """
                UPDATE artifacts
                SET state = 'ABORTED', aborted_at = COALESCE(aborted_at, NOW())
                WHERE id = %s
                RETURNING *
                """,
                (artifact.id,),
            ).fetchone()
            self._insert_event(
                connection,
                pipeline_run_id=artifact.pipeline_run_id,
                aggregate_type="artifact",
                aggregate_id=artifact.id,
                event_type="ARTIFACT_ABORTED",
                payload={
                    "node_id": artifact.node_id,
                    "attempt_number": artifact.attempt_number,
                },
            )
        assert updated is not None
        return self._artifact_from_row(updated)

    def get_committed_artifact(self, run_id: UUID, node_id: str) -> ArtifactRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM artifacts
                WHERE pipeline_run_id = %s AND node_id = %s AND state = 'COMMITTED'
                """,
                (run_id, node_id),
            ).fetchone()
        return None if row is None else self._artifact_from_row(row)

    def get_attempt_artifact(
        self,
        run_id: UUID,
        node_id: str,
        attempt_number: int,
    ) -> ArtifactRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM artifacts
                WHERE pipeline_run_id = %s AND node_id = %s AND attempt_number = %s
                """,
                (run_id, node_id, attempt_number),
            ).fetchone()
        return None if row is None else self._artifact_from_row(row)

    def list_run_artifacts(self, run_id: UUID) -> list[ArtifactRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM artifacts
                WHERE pipeline_run_id = %s
                ORDER BY node_id, attempt_number
                """,
                (run_id,),
            ).fetchall()
        return [self._artifact_from_row(row) for row in rows]

    @staticmethod
    def _lock_logical_output(connection: Any, run_id: UUID, node_id: str) -> None:
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"dataflow-artifact:{run_id}:{node_id}",),
        )

    @staticmethod
    def _artifact_from_row(row: Any) -> ArtifactRecord:
        return ArtifactRecord(
            id=row["id"],
            pipeline_run_id=row["pipeline_run_id"],
            node_id=row["node_id"],
            execution_unit_id=row["execution_unit_id"],
            attempt_number=row["attempt_number"],
            format=ArtifactFormat(row["format"]),
            state=ArtifactState(row["state"]),
            staging_uri=row["staging_uri"],
            committed_uri=row["committed_uri"],
            schema_json=row["schema_json"],
            size_bytes=row["size_bytes"],
            row_count=row["row_count"],
            content_hash=row["content_hash"],
            checkpoint=row["checkpoint"],
            created_at=row["created_at"],
            committed_at=row["committed_at"],
            aborted_at=row["aborted_at"],
        )


__all__ = [
    "ArtifactRecord",
    "ArtifactRepository",
    "PostgresArtifactRepository",
]
