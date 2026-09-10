"""Durable execution-unit admission control with bounded fair selection."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from dataflow.observability import DEFAULT_OBSERVABILITY, Observability
from dataflow.state import ExecutionUnitStatus

_ADMISSION_LOCK_KEY = 0x41444D495353494F
_RESERVED_STATUSES = frozenset(
    {
        ExecutionUnitStatus.READY,
        ExecutionUnitStatus.SUBMITTING,
        ExecutionUnitStatus.RUNNING,
        ExecutionUnitStatus.RETRY_WAIT,
        ExecutionUnitStatus.UNKNOWN,
    }
)
_ACTIVE_RECOVERY_STATUSES = frozenset(
    {
        ExecutionUnitStatus.SUBMITTING,
        ExecutionUnitStatus.RUNNING,
        ExecutionUnitStatus.RETRY_WAIT,
        ExecutionUnitStatus.UNKNOWN,
    }
)


class AdmissionState(StrEnum):
    WAITING = "WAITING"
    ADMITTED = "ADMITTED"
    RELEASED = "RELEASED"


@dataclass(frozen=True, slots=True)
class AdmissionPolicy:
    """Concurrency policy enforced when selecting new READY units."""

    global_limit: int | None = None
    profile_limits: Mapping[str, int] | None = None
    max_new_per_pass: int = 32

    def __post_init__(self) -> None:
        if self.global_limit is not None and self.global_limit < 0:
            raise ValueError("global_limit must be non-negative")
        if self.max_new_per_pass <= 0:
            raise ValueError("max_new_per_pass must be positive")
        for profile, limit in (self.profile_limits or {}).items():
            if not profile:
                raise ValueError("profile limit names must not be empty")
            if limit < 0:
                raise ValueError("profile limits must be non-negative")

    @classmethod
    def from_env(cls, environ: Mapping[str, str]) -> AdmissionPolicy:
        raw_global = environ.get("DATAFLOW_ADMISSION_GLOBAL_LIMIT")
        global_limit = _parse_optional_limit("DATAFLOW_ADMISSION_GLOBAL_LIMIT", raw_global)

        raw_profiles = environ.get("DATAFLOW_ADMISSION_PROFILE_LIMITS", "{}")
        try:
            decoded = json.loads(raw_profiles)
        except json.JSONDecodeError as error:
            raise RuntimeError("DATAFLOW_ADMISSION_PROFILE_LIMITS must be valid JSON") from error
        if not isinstance(decoded, dict):
            raise RuntimeError("DATAFLOW_ADMISSION_PROFILE_LIMITS must be a JSON object")
        profile_limits: dict[str, int] = {}
        for profile, raw_limit in decoded.items():
            if not isinstance(profile, str) or not profile:
                raise RuntimeError("admission profile names must be non-empty strings")
            if isinstance(raw_limit, bool) or not isinstance(raw_limit, int) or raw_limit < 0:
                raise RuntimeError("admission profile limits must be non-negative integers")
            profile_limits[profile] = raw_limit

        raw_max = environ.get("DATAFLOW_ADMISSION_MAX_NEW_PER_PASS", "32")
        try:
            max_new = int(raw_max)
        except ValueError as error:
            raise RuntimeError("DATAFLOW_ADMISSION_MAX_NEW_PER_PASS must be an integer") from error
        if max_new <= 0:
            raise RuntimeError("DATAFLOW_ADMISSION_MAX_NEW_PER_PASS must be positive")
        return cls(
            global_limit=global_limit,
            profile_limits=profile_limits,
            max_new_per_pass=max_new,
        )

    def profile_limit(self, cluster_profile: str) -> int | None:
        return (self.profile_limits or {}).get(cluster_profile)


@dataclass(frozen=True, slots=True)
class AdmissionRecord:
    execution_unit_id: UUID
    pipeline_run_id: UUID
    cluster_profile: str
    state: AdmissionState
    admission_sequence: int | None
    queued_at: datetime | None
    admitted_at: datetime | None
    released_at: datetime | None


@dataclass(frozen=True, slots=True)
class AdmissionBatch:
    """One durable admission pass consumed by the orchestration controller."""

    admitted_unit_ids: frozenset[UUID]
    newly_admitted_unit_ids: tuple[UUID, ...]
    active_slots: int
    profile_slots: Mapping[str, int]
    queued_units: int
    throttled_units: int
    recovered_units: int
    released_units: int


@dataclass(frozen=True, slots=True)
class _Candidate:
    execution_unit_id: UUID
    pipeline_run_id: UUID
    cluster_profile: str
    run_created_at: datetime
    unit_created_at: datetime
    queued_at: datetime
    unit_key: str


class PostgresAdmissionController:
    """Synchronize durable slot ownership and fairly admit READY execution units.

    An admission is a logical-unit reservation, not a Ray resource reservation. The unit
    keeps its slot through SUBMITTING/RUNNING/UNKNOWN and RETRY_WAIT so retries never
    acquire a second slot. Terminal/cancelled units release their reservation on the next
    admission pass. A PostgreSQL transaction advisory lock serializes admission decisions
    even during the narrow HA failover window where two controller processes may overlap.
    """

    def __init__(
        self,
        dsn: str,
        policy: AdmissionPolicy | None = None,
        *,
        observability: Observability | None = None,
    ) -> None:
        self._dsn = dsn
        self._policy = policy or AdmissionPolicy()
        self._observability = observability or DEFAULT_OBSERVABILITY

    def reconcile(self) -> AdmissionBatch:
        with self._connect() as connection, connection.transaction():
            connection.execute("SELECT pg_advisory_xact_lock(%s)", (_ADMISSION_LOCK_KEY,))
            released = self._release_inactive(connection)
            recovered = self._recover_active(connection)
            self._queue_ready(connection)

            admitted_rows = self._load_admitted(connection)
            admitted_ids = {UUID(str(row["execution_unit_id"])) for row in admitted_rows}
            profile_slots = _profile_counts(admitted_rows)
            active_slots = len(admitted_rows)
            run_sequences = self._load_run_sequences(connection)
            candidates = self._load_candidates(connection)
            newly_admitted: list[UUID] = []

            while candidates and len(newly_admitted) < self._policy.max_new_per_pass:
                eligible = [
                    candidate
                    for candidate in candidates
                    if self._has_capacity(
                        candidate.cluster_profile,
                        active_slots=active_slots,
                        profile_slots=profile_slots,
                    )
                ]
                if not eligible:
                    break
                candidate = min(
                    eligible,
                    key=lambda item: (
                        run_sequences.get(item.pipeline_run_id, 0),
                        item.run_created_at,
                        str(item.pipeline_run_id),
                        item.queued_at,
                        item.unit_created_at,
                        item.unit_key,
                    ),
                )
                sequence = self._next_sequence(connection)
                self._admit(connection, candidate, sequence)
                newly_admitted.append(candidate.execution_unit_id)
                admitted_ids.add(candidate.execution_unit_id)
                active_slots += 1
                profile_slots[candidate.cluster_profile] = (
                    profile_slots.get(candidate.cluster_profile, 0) + 1
                )
                run_sequences[candidate.pipeline_run_id] = sequence
                candidates.remove(candidate)

            queued_units = self._count_waiting(connection)
            batch = AdmissionBatch(
                admitted_unit_ids=frozenset(admitted_ids),
                newly_admitted_unit_ids=tuple(newly_admitted),
                active_slots=active_slots,
                profile_slots=dict(profile_slots),
                queued_units=queued_units,
                throttled_units=queued_units,
                recovered_units=recovered,
                released_units=released,
            )

        self._observability.info(
            "admission_reconciled",
            active_slots=batch.active_slots,
            profile_slots=dict(batch.profile_slots),
            newly_admitted=len(batch.newly_admitted_unit_ids),
            queued_units=batch.queued_units,
            throttled_units=batch.throttled_units,
            recovered_units=batch.recovered_units,
            released_units=batch.released_units,
            global_limit=self._policy.global_limit,
            max_new_per_pass=self._policy.max_new_per_pass,
        )
        return batch

    def list_records(self) -> list[AdmissionRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT execution_unit_id, pipeline_run_id, cluster_profile, state,
                       admission_sequence, queued_at, admitted_at, released_at
                FROM execution_admissions
                ORDER BY COALESCE(admission_sequence, 9223372036854775807),
                         queued_at,
                         execution_unit_id
                """
            ).fetchall()
        return [self._record_from_row(row) for row in rows]

    def _connect(self) -> Connection:
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def _release_inactive(self, connection: Connection) -> int:
        reserved = [status.value for status in _RESERVED_STATUSES]
        rows = connection.execute(
            """
            UPDATE execution_admissions AS a
            SET state = %s,
                released_at = NOW(),
                updated_at = NOW()
            FROM execution_units AS u
            WHERE u.id = a.execution_unit_id
              AND a.state IN (%s, %s)
              AND u.status <> ALL(%s)
            RETURNING a.execution_unit_id, a.pipeline_run_id, a.cluster_profile
            """,
            (
                AdmissionState.RELEASED.value,
                AdmissionState.WAITING.value,
                AdmissionState.ADMITTED.value,
                reserved,
            ),
        ).fetchall()
        for row in rows:
            self._insert_event(
                connection,
                pipeline_run_id=row["pipeline_run_id"],
                unit_id=row["execution_unit_id"],
                event_type="ADMISSION_RELEASED",
                payload={"cluster_profile": row["cluster_profile"]},
            )
        return len(rows)

    def _recover_active(self, connection: Connection) -> int:
        active_statuses = [status.value for status in _ACTIVE_RECOVERY_STATUSES]
        rows = connection.execute(
            """
            SELECT u.id AS execution_unit_id,
                   u.pipeline_run_id,
                   u.cluster_profile,
                   a.state AS admission_state
            FROM execution_units AS u
            LEFT JOIN execution_admissions AS a ON a.execution_unit_id = u.id
            WHERE u.status = ANY(%s)
            ORDER BY u.created_at, u.unit_key
            """,
            (active_statuses,),
        ).fetchall()
        recovered = 0
        for row in rows:
            if row["admission_state"] == AdmissionState.ADMITTED.value:
                continue
            sequence = self._next_sequence(connection)
            connection.execute(
                """
                INSERT INTO execution_admissions (
                    execution_unit_id, pipeline_run_id, cluster_profile, state,
                    admission_sequence, queued_at, admitted_at, released_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW(), NOW(), NULL, NOW())
                ON CONFLICT (execution_unit_id) DO UPDATE
                SET pipeline_run_id = EXCLUDED.pipeline_run_id,
                    cluster_profile = EXCLUDED.cluster_profile,
                    state = EXCLUDED.state,
                    admission_sequence = EXCLUDED.admission_sequence,
                    admitted_at = NOW(),
                    released_at = NULL,
                    updated_at = NOW()
                """,
                (
                    row["execution_unit_id"],
                    row["pipeline_run_id"],
                    row["cluster_profile"],
                    AdmissionState.ADMITTED.value,
                    sequence,
                ),
            )
            self._insert_event(
                connection,
                pipeline_run_id=row["pipeline_run_id"],
                unit_id=row["execution_unit_id"],
                event_type="ADMISSION_RECOVERED",
                payload={
                    "cluster_profile": row["cluster_profile"],
                    "admission_sequence": sequence,
                },
            )
            recovered += 1
        return recovered

    def _queue_ready(self, connection: Connection) -> None:
        inserted = connection.execute(
            """
            INSERT INTO execution_admissions (
                execution_unit_id, pipeline_run_id, cluster_profile, state,
                queued_at, updated_at
            )
            SELECT u.id, u.pipeline_run_id, u.cluster_profile, %s, NOW(), NOW()
            FROM execution_units AS u
            WHERE u.status = %s
            ON CONFLICT (execution_unit_id) DO NOTHING
            RETURNING execution_unit_id, pipeline_run_id, cluster_profile
            """,
            (AdmissionState.WAITING.value, ExecutionUnitStatus.READY.value),
        ).fetchall()
        requeued = connection.execute(
            """
            UPDATE execution_admissions AS a
            SET state = %s,
                queued_at = NOW(),
                admitted_at = NULL,
                released_at = NULL,
                updated_at = NOW()
            FROM execution_units AS u
            WHERE u.id = a.execution_unit_id
              AND u.status = %s
              AND a.state = %s
            RETURNING a.execution_unit_id, a.pipeline_run_id, a.cluster_profile
            """,
            (
                AdmissionState.WAITING.value,
                ExecutionUnitStatus.READY.value,
                AdmissionState.RELEASED.value,
            ),
        ).fetchall()
        for row in [*inserted, *requeued]:
            self._insert_event(
                connection,
                pipeline_run_id=row["pipeline_run_id"],
                unit_id=row["execution_unit_id"],
                event_type="ADMISSION_QUEUED",
                payload={"cluster_profile": row["cluster_profile"]},
            )

    def _load_admitted(self, connection: Connection) -> list[dict[str, Any]]:
        return connection.execute(
            """
            SELECT execution_unit_id, pipeline_run_id, cluster_profile, admission_sequence
            FROM execution_admissions
            WHERE state = %s
            ORDER BY admission_sequence, execution_unit_id
            """,
            (AdmissionState.ADMITTED.value,),
        ).fetchall()

    def _load_run_sequences(self, connection: Connection) -> dict[UUID, int]:
        rows = connection.execute(
            """
            SELECT pipeline_run_id, COALESCE(MAX(admission_sequence), 0) AS last_sequence
            FROM execution_admissions
            GROUP BY pipeline_run_id
            """
        ).fetchall()
        return {
            UUID(str(row["pipeline_run_id"])): int(row["last_sequence"])
            for row in rows
        }

    def _load_candidates(self, connection: Connection) -> list[_Candidate]:
        rows = connection.execute(
            """
            SELECT a.execution_unit_id,
                   a.pipeline_run_id,
                   a.cluster_profile,
                   a.queued_at,
                   u.created_at AS unit_created_at,
                   u.unit_key,
                   r.created_at AS run_created_at
            FROM execution_admissions AS a
            JOIN execution_units AS u ON u.id = a.execution_unit_id
            JOIN pipeline_runs AS r ON r.id = a.pipeline_run_id
            WHERE a.state = %s
              AND u.status = %s
              AND r.status = 'RUNNING'
            ORDER BY r.created_at, r.id, a.queued_at, u.created_at, u.unit_key
            """,
            (AdmissionState.WAITING.value, ExecutionUnitStatus.READY.value),
        ).fetchall()
        return [
            _Candidate(
                execution_unit_id=UUID(str(row["execution_unit_id"])),
                pipeline_run_id=UUID(str(row["pipeline_run_id"])),
                cluster_profile=str(row["cluster_profile"]),
                run_created_at=row["run_created_at"],
                unit_created_at=row["unit_created_at"],
                queued_at=row["queued_at"],
                unit_key=str(row["unit_key"]),
            )
            for row in rows
        ]

    def _has_capacity(
        self,
        cluster_profile: str,
        *,
        active_slots: int,
        profile_slots: Mapping[str, int],
    ) -> bool:
        if self._policy.global_limit is not None and active_slots >= self._policy.global_limit:
            return False
        profile_limit = self._policy.profile_limit(cluster_profile)
        if profile_limit is not None and profile_slots.get(cluster_profile, 0) >= profile_limit:
            return False
        return True

    def _admit(
        self,
        connection: Connection,
        candidate: _Candidate,
        sequence: int,
    ) -> None:
        updated = connection.execute(
            """
            UPDATE execution_admissions
            SET state = %s,
                admission_sequence = %s,
                admitted_at = NOW(),
                released_at = NULL,
                updated_at = NOW()
            WHERE execution_unit_id = %s
              AND state = %s
            RETURNING execution_unit_id
            """,
            (
                AdmissionState.ADMITTED.value,
                sequence,
                candidate.execution_unit_id,
                AdmissionState.WAITING.value,
            ),
        ).fetchone()
        if updated is None:
            raise RuntimeError(
                f"admission candidate changed concurrently: {candidate.execution_unit_id}"
            )
        self._insert_event(
            connection,
            pipeline_run_id=candidate.pipeline_run_id,
            unit_id=candidate.execution_unit_id,
            event_type="ADMISSION_ADMITTED",
            payload={
                "cluster_profile": candidate.cluster_profile,
                "admission_sequence": sequence,
            },
        )

    @staticmethod
    def _next_sequence(connection: Connection) -> int:
        row = connection.execute(
            """
            UPDATE admission_scheduler_state
            SET next_sequence = next_sequence + 1
            WHERE singleton = TRUE
            RETURNING next_sequence - 1 AS sequence
            """
        ).fetchone()
        if row is None:
            raise RuntimeError("admission scheduler state is missing")
        return int(row["sequence"])

    @staticmethod
    def _count_waiting(connection: Connection) -> int:
        row = connection.execute(
            "SELECT COUNT(*) AS count FROM execution_admissions WHERE state = %s",
            (AdmissionState.WAITING.value,),
        ).fetchone()
        assert row is not None
        return int(row["count"])

    @staticmethod
    def _insert_event(
        connection: Connection,
        *,
        pipeline_run_id: UUID,
        unit_id: UUID,
        event_type: str,
        payload: Mapping[str, Any],
    ) -> None:
        connection.execute(
            """
            INSERT INTO events (
                pipeline_run_id, aggregate_type, aggregate_id, event_type, payload_json
            )
            VALUES (%s, 'execution_unit', %s, %s, %s)
            """,
            (pipeline_run_id, unit_id, event_type, Jsonb(dict(payload))),
        )

    @staticmethod
    def _record_from_row(row: Mapping[str, Any]) -> AdmissionRecord:
        return AdmissionRecord(
            execution_unit_id=UUID(str(row["execution_unit_id"])),
            pipeline_run_id=UUID(str(row["pipeline_run_id"])),
            cluster_profile=str(row["cluster_profile"]),
            state=AdmissionState(row["state"]),
            admission_sequence=(
                int(row["admission_sequence"])
                if row["admission_sequence"] is not None
                else None
            ),
            queued_at=row["queued_at"],
            admitted_at=row["admitted_at"],
            released_at=row["released_at"],
        )


def _profile_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        profile = str(row["cluster_profile"])
        counts[profile] = counts.get(profile, 0) + 1
    return counts


def _parse_optional_limit(name: str, raw: str | None) -> int | None:
    if raw is None or not raw.strip():
        return None
    try:
        value = int(raw)
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer") from error
    if value < 0:
        raise RuntimeError(f"{name} must be non-negative")
    return value


__all__ = [
    "AdmissionBatch",
    "AdmissionPolicy",
    "AdmissionRecord",
    "AdmissionState",
    "PostgresAdmissionController",
]
