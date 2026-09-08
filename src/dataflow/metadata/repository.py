"""PostgreSQL repository for durable DataFlow orchestration state."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID, uuid4

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from dataflow.compiler import ExecutionGraph
from dataflow.state import (
    TERMINAL_ATTEMPT_STATUSES,
    TERMINAL_RUN_STATUSES,
    TERMINAL_UNIT_STATUSES,
    ExecutionAttemptStatus,
    ExecutionUnitStatus,
    PipelineRunStatus,
    validate_attempt_transition,
    validate_run_transition,
    validate_unit_transition,
)


class MetadataNotFoundError(KeyError):
    pass


class ConcurrentStateChange(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class PipelineRecord:
    id: UUID
    tenant_id: str
    name: str
    description: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class PipelineVersionRecord:
    id: UUID
    pipeline_id: UUID
    version: int
    spec_json: dict[str, Any]
    spec_hash: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class PipelineRunRecord:
    id: UUID
    pipeline_version_id: UUID
    status: PipelineRunStatus
    parameters_json: dict[str, Any]
    cluster_profile: str | None
    created_at: datetime
    queued_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True, slots=True)
class ExecutionUnitRecord:
    id: UUID
    pipeline_run_id: UUID
    unit_key: str
    status: ExecutionUnitStatus
    current_attempt: int
    cluster_profile: str
    dependencies: list[str]
    plan_json: dict[str, Any]
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True, slots=True)
class ExecutionAttemptRecord:
    id: UUID
    execution_unit_id: UUID
    attempt_number: int
    status: ExecutionAttemptStatus
    external_job_id: str | None
    error_code: str | None
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True, slots=True)
class EventRecord:
    id: int
    pipeline_run_id: UUID | None
    aggregate_type: str
    aggregate_id: UUID
    event_type: str
    payload_json: dict[str, Any]
    created_at: datetime


class MetadataRepository(Protocol):
    def create_pipeline(
        self,
        *,
        name: str,
        tenant_id: str = "default",
        description: str | None = None,
    ) -> PipelineRecord: ...

    def create_pipeline_version(
        self,
        pipeline_id: UUID,
        spec: Mapping[str, Any],
    ) -> PipelineVersionRecord: ...

    def create_pipeline_run(
        self,
        pipeline_version_id: UUID,
        *,
        parameters: Mapping[str, Any] | None = None,
        cluster_profile: str | None = None,
        created_by: str | None = None,
    ) -> PipelineRunRecord: ...

    def create_execution_graph(
        self,
        pipeline_run_id: UUID,
        graph: ExecutionGraph,
    ) -> dict[str, UUID]: ...

    def create_attempt(self, execution_unit_id: UUID) -> ExecutionAttemptRecord: ...

    def transition_run_status(
        self,
        run_id: UUID,
        target: PipelineRunStatus,
        *,
        expected: PipelineRunStatus | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> PipelineRunRecord: ...

    def transition_unit_status(
        self,
        unit_id: UUID,
        target: ExecutionUnitStatus,
        *,
        expected: ExecutionUnitStatus | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> ExecutionUnitRecord: ...


class PostgresMetadataRepository:
    def __init__(self, dsn: str):
        self._dsn = dsn

    def _connect(self) -> Connection:
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def create_pipeline(
        self,
        *,
        name: str,
        tenant_id: str = "default",
        description: str | None = None,
    ) -> PipelineRecord:
        pipeline_id = uuid4()
        with self._connect() as connection, connection.transaction():
            row = connection.execute(
                """
                INSERT INTO pipelines (id, tenant_id, name, description)
                VALUES (%s, %s, %s, %s)
                RETURNING id, tenant_id, name, description, created_at
                """,
                (pipeline_id, tenant_id, name, description),
            ).fetchone()
        assert row is not None
        return self._pipeline_from_row(row)

    def create_pipeline_version(
        self,
        pipeline_id: UUID,
        spec: Mapping[str, Any],
    ) -> PipelineVersionRecord:
        spec_json = dict(spec)
        spec_hash = canonical_spec_hash(spec_json)
        version_id = uuid4()

        with self._connect() as connection, connection.transaction():
            locked = connection.execute(
                "SELECT id FROM pipelines WHERE id = %s FOR UPDATE",
                (pipeline_id,),
            ).fetchone()
            if locked is None:
                raise MetadataNotFoundError(f"pipeline not found: {pipeline_id}")

            row = connection.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 AS next_version "
                "FROM pipeline_versions WHERE pipeline_id = %s",
                (pipeline_id,),
            ).fetchone()
            assert row is not None
            version = int(row["next_version"])

            inserted = connection.execute(
                """
                INSERT INTO pipeline_versions (
                    id, pipeline_id, version, spec_json, spec_hash
                )
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, pipeline_id, version, spec_json, spec_hash, created_at
                """,
                (version_id, pipeline_id, version, Jsonb(spec_json), spec_hash),
            ).fetchone()
        assert inserted is not None
        return self._version_from_row(inserted)

    def create_pipeline_run(
        self,
        pipeline_version_id: UUID,
        *,
        parameters: Mapping[str, Any] | None = None,
        cluster_profile: str | None = None,
        created_by: str | None = None,
    ) -> PipelineRunRecord:
        run_id = uuid4()
        parameters_json = dict(parameters or {})
        with self._connect() as connection, connection.transaction():
            row = connection.execute(
                """
                INSERT INTO pipeline_runs (
                    id, pipeline_version_id, status, parameters_json,
                    cluster_profile, created_by
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    run_id,
                    pipeline_version_id,
                    PipelineRunStatus.CREATED.value,
                    Jsonb(parameters_json),
                    cluster_profile,
                    created_by,
                ),
            ).fetchone()
            self._insert_event(
                connection,
                pipeline_run_id=run_id,
                aggregate_type="pipeline_run",
                aggregate_id=run_id,
                event_type="RUN_CREATED",
                payload={"pipeline_version_id": str(pipeline_version_id)},
            )
        assert row is not None
        return self._run_from_row(row)

    def create_execution_graph(
        self,
        pipeline_run_id: UUID,
        graph: ExecutionGraph,
    ) -> dict[str, UUID]:
        if graph.run_id != str(pipeline_run_id):
            raise ValueError(
                "execution graph run_id must match the persisted pipeline_run_id"
            )

        unit_ids: dict[str, UUID] = {}
        with self._connect() as connection, connection.transaction():
            run = connection.execute(
                "SELECT id FROM pipeline_runs WHERE id = %s FOR UPDATE",
                (pipeline_run_id,),
            ).fetchone()
            if run is None:
                raise MetadataNotFoundError(f"pipeline run not found: {pipeline_run_id}")

            for unit in graph.units:
                unit_id = uuid4()
                unit_ids[unit.id] = unit_id
                connection.execute(
                    """
                    INSERT INTO execution_units (
                        id, pipeline_run_id, unit_key, plan_json,
                        dependencies_json, cluster_profile, status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        unit_id,
                        pipeline_run_id,
                        unit.id,
                        Jsonb(unit.plan.model_dump(mode="json")),
                        Jsonb(unit.dependencies),
                        unit.cluster_profile,
                        ExecutionUnitStatus.PENDING.value,
                    ),
                )
                for node_id in unit.node_ids:
                    connection.execute(
                        """
                        INSERT INTO node_runs (
                            id, pipeline_run_id, node_id, execution_unit_id, status
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            uuid4(),
                            pipeline_run_id,
                            node_id,
                            unit_id,
                            ExecutionUnitStatus.PENDING.value,
                        ),
                    )

            self._insert_event(
                connection,
                pipeline_run_id=pipeline_run_id,
                aggregate_type="pipeline_run",
                aggregate_id=pipeline_run_id,
                event_type="EXECUTION_GRAPH_CREATED",
                payload={
                    "unit_keys": [unit.id for unit in graph.units],
                    "pipeline_name": graph.pipeline_name,
                },
            )
        return unit_ids

    def create_attempt(self, execution_unit_id: UUID) -> ExecutionAttemptRecord:
        attempt_id = uuid4()
        with self._connect() as connection, connection.transaction():
            unit = connection.execute(
                """
                SELECT id, pipeline_run_id, current_attempt
                FROM execution_units
                WHERE id = %s
                FOR UPDATE
                """,
                (execution_unit_id,),
            ).fetchone()
            if unit is None:
                raise MetadataNotFoundError(
                    f"execution unit not found: {execution_unit_id}"
                )
            attempt_number = int(unit["current_attempt"]) + 1
            row = connection.execute(
                """
                INSERT INTO execution_attempts (
                    id, execution_unit_id, attempt_number, status
                )
                VALUES (%s, %s, %s, %s)
                RETURNING *
                """,
                (
                    attempt_id,
                    execution_unit_id,
                    attempt_number,
                    ExecutionAttemptStatus.PENDING.value,
                ),
            ).fetchone()
            connection.execute(
                "UPDATE execution_units SET current_attempt = %s WHERE id = %s",
                (attempt_number, execution_unit_id),
            )
            self._insert_event(
                connection,
                pipeline_run_id=unit["pipeline_run_id"],
                aggregate_type="execution_unit",
                aggregate_id=execution_unit_id,
                event_type="ATTEMPT_CREATED",
                payload={
                    "attempt_id": str(attempt_id),
                    "attempt_number": attempt_number,
                },
            )
        assert row is not None
        return self._attempt_from_row(row)

    def transition_run_status(
        self,
        run_id: UUID,
        target: PipelineRunStatus,
        *,
        expected: PipelineRunStatus | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> PipelineRunRecord:
        with self._connect() as connection, connection.transaction():
            row = connection.execute(
                "SELECT * FROM pipeline_runs WHERE id = %s FOR UPDATE",
                (run_id,),
            ).fetchone()
            if row is None:
                raise MetadataNotFoundError(f"pipeline run not found: {run_id}")
            current = PipelineRunStatus(row["status"])
            self._check_expected(current, expected)
            validate_run_transition(current, target)

            timestamp_sql = self._run_timestamp_sql(target)
            updated = connection.execute(
                f"UPDATE pipeline_runs SET status = %s{timestamp_sql} "
                "WHERE id = %s RETURNING *",
                (target.value, run_id),
            ).fetchone()
            self._insert_event(
                connection,
                pipeline_run_id=run_id,
                aggregate_type="pipeline_run",
                aggregate_id=run_id,
                event_type=f"RUN_{target.value}",
                payload=dict(payload or {}),
            )
        assert updated is not None
        return self._run_from_row(updated)

    def transition_unit_status(
        self,
        unit_id: UUID,
        target: ExecutionUnitStatus,
        *,
        expected: ExecutionUnitStatus | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> ExecutionUnitRecord:
        with self._connect() as connection, connection.transaction():
            row = connection.execute(
                "SELECT * FROM execution_units WHERE id = %s FOR UPDATE",
                (unit_id,),
            ).fetchone()
            if row is None:
                raise MetadataNotFoundError(f"execution unit not found: {unit_id}")
            current = ExecutionUnitStatus(row["status"])
            self._check_expected(current, expected)
            validate_unit_transition(current, target)

            timestamp_sql = self._unit_timestamp_sql(target)
            updated = connection.execute(
                f"UPDATE execution_units SET status = %s{timestamp_sql} "
                "WHERE id = %s RETURNING *",
                (target.value, unit_id),
            ).fetchone()
            self._insert_event(
                connection,
                pipeline_run_id=row["pipeline_run_id"],
                aggregate_type="execution_unit",
                aggregate_id=unit_id,
                event_type=f"UNIT_{target.value}",
                payload=dict(payload or {}),
            )
        assert updated is not None
        return self._unit_from_row(updated)

    def transition_attempt_status(
        self,
        attempt_id: UUID,
        target: ExecutionAttemptStatus,
        *,
        expected: ExecutionAttemptStatus | None = None,
        external_job_id: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> ExecutionAttemptRecord:
        with self._connect() as connection, connection.transaction():
            row = connection.execute(
                """
                SELECT a.*, u.pipeline_run_id
                FROM execution_attempts AS a
                JOIN execution_units AS u ON u.id = a.execution_unit_id
                WHERE a.id = %s
                FOR UPDATE OF a
                """,
                (attempt_id,),
            ).fetchone()
            if row is None:
                raise MetadataNotFoundError(f"execution attempt not found: {attempt_id}")
            current = ExecutionAttemptStatus(row["status"])
            self._check_expected(current, expected)
            validate_attempt_transition(current, target)

            timestamp_sql = self._attempt_timestamp_sql(target)
            updated = connection.execute(
                f"""
                UPDATE execution_attempts
                SET status = %s,
                    external_job_id = COALESCE(%s, external_job_id),
                    error_code = COALESCE(%s, error_code),
                    error_message = COALESCE(%s, error_message)
                    {timestamp_sql}
                WHERE id = %s
                RETURNING *
                """,
                (
                    target.value,
                    external_job_id,
                    error_code,
                    error_message,
                    attempt_id,
                ),
            ).fetchone()
            self._insert_event(
                connection,
                pipeline_run_id=row["pipeline_run_id"],
                aggregate_type="execution_attempt",
                aggregate_id=attempt_id,
                event_type=f"ATTEMPT_{target.value}",
                payload={
                    "external_job_id": external_job_id,
                    "error_code": error_code,
                },
            )
        assert updated is not None
        return self._attempt_from_row(updated)

    def get_run(self, run_id: UUID) -> PipelineRunRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM pipeline_runs WHERE id = %s",
                (run_id,),
            ).fetchone()
        if row is None:
            raise MetadataNotFoundError(f"pipeline run not found: {run_id}")
        return self._run_from_row(row)

    def get_unit(self, unit_id: UUID) -> ExecutionUnitRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM execution_units WHERE id = %s",
                (unit_id,),
            ).fetchone()
        if row is None:
            raise MetadataNotFoundError(f"execution unit not found: {unit_id}")
        return self._unit_from_row(row)

    def list_attempts(self, unit_id: UUID) -> list[ExecutionAttemptRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM execution_attempts
                WHERE execution_unit_id = %s
                ORDER BY attempt_number
                """,
                (unit_id,),
            ).fetchall()
        return [self._attempt_from_row(row) for row in rows]

    def list_recoverable_units(self) -> list[ExecutionUnitRecord]:
        terminal = tuple(status.value for status in TERMINAL_UNIT_STATUSES)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM execution_units
                WHERE status <> ALL(%s)
                ORDER BY created_at, unit_key
                """,
                (list(terminal),),
            ).fetchall()
        return [self._unit_from_row(row) for row in rows]

    def list_events(self, run_id: UUID) -> list[EventRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM events WHERE pipeline_run_id = %s ORDER BY id",
                (run_id,),
            ).fetchall()
        return [self._event_from_row(row) for row in rows]

    @staticmethod
    def _check_expected(current: Any, expected: Any | None) -> None:
        if expected is not None and current != expected:
            raise ConcurrentStateChange(
                f"expected state {expected}, but durable state is {current}"
            )

    @staticmethod
    def _run_timestamp_sql(target: PipelineRunStatus) -> str:
        if target is PipelineRunStatus.QUEUED:
            return ", queued_at = COALESCE(queued_at, NOW())"
        if target is PipelineRunStatus.RUNNING:
            return ", started_at = COALESCE(started_at, NOW())"
        if target in TERMINAL_RUN_STATUSES:
            return ", finished_at = COALESCE(finished_at, NOW())"
        return ""

    @staticmethod
    def _unit_timestamp_sql(target: ExecutionUnitStatus) -> str:
        if target is ExecutionUnitStatus.RUNNING:
            return ", started_at = COALESCE(started_at, NOW())"
        if target in TERMINAL_UNIT_STATUSES:
            return ", finished_at = COALESCE(finished_at, NOW())"
        return ""

    @staticmethod
    def _attempt_timestamp_sql(target: ExecutionAttemptStatus) -> str:
        if target is ExecutionAttemptStatus.RUNNING:
            return ", started_at = COALESCE(started_at, NOW())"
        if target in TERMINAL_ATTEMPT_STATUSES:
            return ", finished_at = COALESCE(finished_at, NOW())"
        return ""

    @staticmethod
    def _insert_event(
        connection: Connection,
        *,
        pipeline_run_id: UUID | None,
        aggregate_type: str,
        aggregate_id: UUID,
        event_type: str,
        payload: Mapping[str, Any],
    ) -> None:
        connection.execute(
            """
            INSERT INTO events (
                pipeline_run_id, aggregate_type, aggregate_id, event_type, payload_json
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                pipeline_run_id,
                aggregate_type,
                aggregate_id,
                event_type,
                Jsonb(dict(payload)),
            ),
        )

    @staticmethod
    def _pipeline_from_row(row: Mapping[str, Any]) -> PipelineRecord:
        return PipelineRecord(
            id=row["id"],
            tenant_id=row["tenant_id"],
            name=row["name"],
            description=row["description"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _version_from_row(row: Mapping[str, Any]) -> PipelineVersionRecord:
        return PipelineVersionRecord(
            id=row["id"],
            pipeline_id=row["pipeline_id"],
            version=row["version"],
            spec_json=row["spec_json"],
            spec_hash=row["spec_hash"].strip(),
            created_at=row["created_at"],
        )

    @staticmethod
    def _run_from_row(row: Mapping[str, Any]) -> PipelineRunRecord:
        return PipelineRunRecord(
            id=row["id"],
            pipeline_version_id=row["pipeline_version_id"],
            status=PipelineRunStatus(row["status"]),
            parameters_json=row["parameters_json"],
            cluster_profile=row["cluster_profile"],
            created_at=row["created_at"],
            queued_at=row["queued_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
        )

    @staticmethod
    def _unit_from_row(row: Mapping[str, Any]) -> ExecutionUnitRecord:
        return ExecutionUnitRecord(
            id=row["id"],
            pipeline_run_id=row["pipeline_run_id"],
            unit_key=row["unit_key"],
            status=ExecutionUnitStatus(row["status"]),
            current_attempt=row["current_attempt"],
            cluster_profile=row["cluster_profile"],
            dependencies=row["dependencies_json"],
            plan_json=row["plan_json"],
            created_at=row["created_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
        )

    @staticmethod
    def _attempt_from_row(row: Mapping[str, Any]) -> ExecutionAttemptRecord:
        return ExecutionAttemptRecord(
            id=row["id"],
            execution_unit_id=row["execution_unit_id"],
            attempt_number=row["attempt_number"],
            status=ExecutionAttemptStatus(row["status"]),
            external_job_id=row["external_job_id"],
            error_code=row["error_code"],
            error_message=row["error_message"],
            created_at=row["created_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
        )

    @staticmethod
    def _event_from_row(row: Mapping[str, Any]) -> EventRecord:
        return EventRecord(
            id=row["id"],
            pipeline_run_id=row["pipeline_run_id"],
            aggregate_type=row["aggregate_type"],
            aggregate_id=row["aggregate_id"],
            event_type=row["event_type"],
            payload_json=row["payload_json"],
            created_at=row["created_at"],
        )


def canonical_spec_hash(spec: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        spec,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


__all__ = [
    "ConcurrentStateChange",
    "EventRecord",
    "ExecutionAttemptRecord",
    "ExecutionUnitRecord",
    "MetadataNotFoundError",
    "MetadataRepository",
    "PipelineRecord",
    "PipelineRunRecord",
    "PipelineVersionRecord",
    "PostgresMetadataRepository",
    "canonical_spec_hash",
]
