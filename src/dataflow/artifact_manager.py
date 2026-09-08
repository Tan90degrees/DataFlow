"""Coordinate object-store publication with durable artifact metadata."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from dataflow.artifact_repository import ArtifactRecord
from dataflow.artifacts import (
    ArtifactCommitError,
    ArtifactFormat,
    ArtifactState,
    ArtifactStorage,
    ArtifactStorageMetadata,
)
from dataflow.contracts import ExecutionPlan


class ArtifactRegistry(Protocol):
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


class ArtifactManager:
    """Publish attempt staging output into one idempotent logical artifact."""

    def __init__(self, repository: ArtifactRegistry, storage: ArtifactStorage) -> None:
        self._repository = repository
        self._storage = storage

    def commit_outputs(
        self,
        plan: ExecutionPlan,
        *,
        attempt_number: int,
    ) -> list[ArtifactRecord]:
        if not plan.output_artifacts:
            return []
        run_id = _uuid_run_id(plan.run_id)
        committed: list[ArtifactRecord] = []

        for output in plan.output_artifacts:
            existing = self._repository.get_committed_artifact(run_id, output.node_id)
            if existing is not None:
                committed.append(existing)
                continue

            staging_uri = output.staging_uri(attempt_number)
            record = self._repository.begin_artifact(
                run_id=run_id,
                unit_key=plan.unit_id,
                node_id=output.node_id,
                attempt_number=attempt_number,
                format=output.format,
                staging_uri=staging_uri,
                committed_uri=output.committed_uri,
                checkpoint=output.checkpoint,
            )
            if record.state is ArtifactState.COMMITTED:
                committed.append(record)
                continue
            if record.state is ArtifactState.ABORTED:
                raise ArtifactCommitError(
                    "cannot publish an artifact from an aborted attempt",
                    error_code="ARTIFACT_ATTEMPT_ABORTED",
                    retryable=False,
                )

            metadata = self._storage.commit(
                staging_uri,
                output.committed_uri,
                format=output.format,
            )
            committed.append(self._repository.commit_artifact(record.id, metadata))
        return committed

    def abort_outputs(
        self,
        plan: ExecutionPlan,
        *,
        attempt_number: int,
        best_effort: bool = False,
    ) -> None:
        if not plan.output_artifacts:
            return
        run_id = _uuid_run_id(plan.run_id)
        first_error: ArtifactCommitError | None = None

        for output in plan.output_artifacts:
            staging_uri = output.staging_uri(attempt_number)
            record = self._repository.begin_artifact(
                run_id=run_id,
                unit_key=plan.unit_id,
                node_id=output.node_id,
                attempt_number=attempt_number,
                format=output.format,
                staging_uri=staging_uri,
                committed_uri=output.committed_uri,
                checkpoint=output.checkpoint,
            )
            if record.state is ArtifactState.STAGING:
                self._repository.abort_artifact(record.id)
            try:
                self._storage.abort(staging_uri)
            except ArtifactCommitError as error:
                first_error = first_error or error

        if first_error is not None and not best_effort:
            raise first_error


def _uuid_run_id(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as error:
        raise ArtifactCommitError(
            f"durable artifact run_id must be a UUID, got {value!r}",
            error_code="INVALID_ARTIFACT_RUN_ID",
            retryable=False,
        ) from error


__all__ = ["ArtifactManager", "ArtifactRegistry"]
