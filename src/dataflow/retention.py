"""Retention policy and resumable durable artifact garbage collection."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Mapping, Protocol
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from dataflow.artifacts import ArtifactState
from dataflow.observability import DEFAULT_OBSERVABILITY, Observability
from dataflow.state import TERMINAL_ATTEMPT_STATUSES, TERMINAL_RUN_STATUSES

_GC_LOCK_KEY = 0x4743524554454E54
_TERMINAL_ATTEMPTS = [status.value for status in TERMINAL_ATTEMPT_STATUSES]
_TERMINAL_RUNS = [status.value for status in TERMINAL_RUN_STATUSES]
_DEFAULT_RUN_SECONDS = 7 * 24 * 60 * 60
_DEFAULT_EVENT_SECONDS = 30 * 24 * 60 * 60
_DEFAULT_STAGING_SECONDS = 24 * 60 * 60
_DEFAULT_ARTIFACT_SECONDS = 30 * 24 * 60 * 60
_DEFAULT_SCAN_BATCH_SIZE = 256
_DEFAULT_WORK_BATCH_SIZE = 64


class GcTargetKind(StrEnum):
    STAGING = "STAGING"
    COMMITTED = "COMMITTED"


class GcTargetState(StrEnum):
    PENDING = "PENDING"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"
    DELETED = "DELETED"


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """Explicit retention windows for durable workflow data."""

    terminal_run_seconds: int = _DEFAULT_RUN_SECONDS
    event_seconds: int = _DEFAULT_EVENT_SECONDS
    staging_seconds: int = _DEFAULT_STAGING_SECONDS
    committed_artifact_seconds: int = _DEFAULT_ARTIFACT_SECONDS
    scan_batch_size: int = _DEFAULT_SCAN_BATCH_SIZE
    work_batch_size: int = _DEFAULT_WORK_BATCH_SIZE

    def __post_init__(self) -> None:
        for name in (
            "terminal_run_seconds",
            "event_seconds",
            "staging_seconds",
            "committed_artifact_seconds",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be non-negative")
        for name in ("scan_batch_size", "work_batch_size"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")

    @classmethod
    def from_env(cls, environment: Mapping[str, str] | None = None) -> RetentionPolicy:
        env = os.environ if environment is None else environment
        return cls(
            terminal_run_seconds=_env_int(
                env,
                "DATAFLOW_RETENTION_RUN_SECONDS",
                _DEFAULT_RUN_SECONDS,
                minimum=0,
            ),
            event_seconds=_env_int(
                env,
                "DATAFLOW_RETENTION_EVENT_SECONDS",
                _DEFAULT_EVENT_SECONDS,
                minimum=0,
            ),
            staging_seconds=_env_int(
                env,
                "DATAFLOW_RETENTION_STAGING_SECONDS",
                _DEFAULT_STAGING_SECONDS,
                minimum=0,
            ),
            committed_artifact_seconds=_env_int(
                env,
                "DATAFLOW_RETENTION_ARTIFACT_SECONDS",
                _DEFAULT_ARTIFACT_SECONDS,
                minimum=0,
            ),
            scan_batch_size=_env_int(
                env,
                "DATAFLOW_GC_SCAN_BATCH_SIZE",
                _DEFAULT_SCAN_BATCH_SIZE,
                minimum=1,
            ),
            work_batch_size=_env_int(
                env,
                "DATAFLOW_GC_WORK_BATCH_SIZE",
                _DEFAULT_WORK_BATCH_SIZE,
                minimum=1,
            ),
        )


@dataclass(frozen=True, slots=True)
class GcTarget:
    artifact_id: UUID
    target_kind: GcTargetKind
    uri: str
    eligible_at: datetime
    state: GcTargetState
    attempts: int


@dataclass(slots=True)
class GcReport:
    dry_run: bool
    lock_acquired: bool = True
    scanned_artifacts: int = 0
    discovered_targets: int = 0
    processed_targets: int = 0
    deleted_targets: int = 0
    blocked_targets: int = 0
    failed_targets: int = 0
    objects_deleted: int = 0
    events_pruned: int = 0

    def as_dict(self) -> dict[str, int | bool]:
        return {
            "dry_run": self.dry_run,
            "lock_acquired": self.lock_acquired,
            "scanned_artifacts": self.scanned_artifacts,
            "discovered_targets": self.discovered_targets,
            "processed_targets": self.processed_targets,
            "deleted_targets": self.deleted_targets,
            "blocked_targets": self.blocked_targets,
            "failed_targets": self.failed_targets,
            "objects_deleted": self.objects_deleted,
            "events_pruned": self.events_pruned,
        }


class ArtifactPrefixStorage(Protocol):
    def delete_prefix(self, uri: str) -> int: ...


class PostgresRetentionGc:
    """Discover and execute resumable GC work against PostgreSQL and object storage."""

    def __init__(
        self,
        dsn: str,
        storage: ArtifactPrefixStorage,
        policy: RetentionPolicy,
        *,
        observability: Observability | None = None,
    ) -> None:
        self._dsn = dsn
        self._storage = storage
        self._policy = policy
        self._observability = observability or DEFAULT_OBSERVABILITY

    def run_once(
        self,
        *,
        dry_run: bool = False,
        now: datetime | None = None,
    ) -> GcReport:
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        report = GcReport(dry_run=dry_run)

        with psycopg.connect(self._dsn) as lock_connection:
            acquired = bool(
                lock_connection.execute(
                    "SELECT pg_try_advisory_lock(%s)",
                    (_GC_LOCK_KEY,),
                ).fetchone()[0]
            )
            if not acquired:
                report.lock_acquired = False
                self._observability.info("retention_gc_skipped", reason="lock_not_acquired")
                return report
            try:
                if dry_run:
                    rows = self._load_dry_run_candidates(current)
                    report.scanned_artifacts = len(rows)
                    for row in rows:
                        report.discovered_targets += len(self._target_specs(row, current))
                    report.events_pruned = self._count_prunable_events(current)
                    self._observability.info("retention_gc_dry_run", **report.as_dict())
                    return report

                rows = self._scan_next_artifacts()
                report.scanned_artifacts = len(rows)
                for row in rows:
                    for kind, uri, eligible_at in self._target_specs(row, current):
                        self._upsert_target(row["id"], kind, uri, eligible_at)
                        report.discovered_targets += 1

                for target in self._load_work(current):
                    report.processed_targets += 1
                    outcome, deleted_objects = self._process_target(target, current)
                    if outcome is GcTargetState.DELETED:
                        report.deleted_targets += 1
                        report.objects_deleted += deleted_objects
                    elif outcome is GcTargetState.BLOCKED:
                        report.blocked_targets += 1
                    elif outcome is GcTargetState.FAILED:
                        report.failed_targets += 1
                    self._observability.metrics.observe_gc_target(
                        target=target.target_kind.value.lower(),
                        outcome=outcome.value.lower(),
                        objects_deleted=deleted_objects,
                    )

                report.events_pruned = self._prune_events(current)
                self._observability.metrics.observe_gc_event_prune(
                    count=report.events_pruned
                )
                self._observability.info("retention_gc_completed", **report.as_dict())
                return report
            finally:
                lock_connection.execute("SELECT pg_advisory_unlock(%s)", (_GC_LOCK_KEY,))

    def list_targets(self) -> list[GcTarget]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT artifact_id, target_kind, uri, eligible_at, state, attempts
                FROM artifact_gc_targets
                ORDER BY eligible_at, artifact_id, target_kind
                """
            ).fetchall()
        return [self._target_from_row(row) for row in rows]

    def _scan_next_artifacts(self) -> list[dict[str, Any]]:
        with self._connect() as connection, connection.transaction():
            cursor = connection.execute(
                """
                SELECT last_created_at, last_artifact_id
                FROM artifact_gc_cursor
                WHERE singleton = TRUE
                FOR UPDATE
                """
            ).fetchone()
            assert cursor is not None
            if cursor["last_created_at"] is None:
                rows = connection.execute(
                    self._artifact_scan_sql(where=""),
                    (self._policy.scan_batch_size,),
                ).fetchall()
            else:
                rows = connection.execute(
                    self._artifact_scan_sql(
                        where="WHERE (a.created_at, a.id) > (%s, %s)"
                    ),
                    (
                        cursor["last_created_at"],
                        cursor["last_artifact_id"],
                        self._policy.scan_batch_size,
                    ),
                ).fetchall()

            if rows:
                last = rows[-1]
                connection.execute(
                    """
                    UPDATE artifact_gc_cursor
                    SET last_created_at = %s, last_artifact_id = %s, updated_at = NOW()
                    WHERE singleton = TRUE
                    """,
                    (last["created_at"], last["id"]),
                )
            else:
                connection.execute(
                    """
                    UPDATE artifact_gc_cursor
                    SET last_created_at = NULL, last_artifact_id = NULL, updated_at = NOW()
                    WHERE singleton = TRUE
                    """
                )
        return rows

    def _load_dry_run_candidates(self, now: datetime) -> list[dict[str, Any]]:
        staging_cutoff = now - timedelta(seconds=self._policy.staging_seconds)
        run_cutoff = now - timedelta(seconds=self._policy.terminal_run_seconds)
        artifact_cutoff = now - timedelta(
            seconds=self._policy.committed_artifact_seconds
        )
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT
                    a.*,
                    r.status AS run_status,
                    r.finished_at AS run_finished_at,
                    ea.status AS attempt_status,
                    ea.finished_at AS attempt_finished_at
                FROM artifacts AS a
                JOIN pipeline_runs AS r ON r.id = a.pipeline_run_id
                JOIN execution_attempts AS ea
                  ON ea.execution_unit_id = a.execution_unit_id
                 AND ea.attempt_number = a.attempt_number
                WHERE (
                    ea.status = ANY(%s)
                    AND ea.finished_at IS NOT NULL
                    AND ea.finished_at <= %s
                ) OR (
                    a.state = 'COMMITTED'
                    AND r.status = ANY(%s)
                    AND r.finished_at IS NOT NULL
                    AND r.finished_at <= %s
                    AND a.committed_at IS NOT NULL
                    AND a.committed_at <= %s
                )
                ORDER BY a.created_at, a.id
                LIMIT %s
                """,
                (
                    _TERMINAL_ATTEMPTS,
                    staging_cutoff,
                    _TERMINAL_RUNS,
                    run_cutoff,
                    artifact_cutoff,
                    self._policy.scan_batch_size,
                ),
            ).fetchall()

    def _target_specs(
        self,
        row: Mapping[str, Any],
        now: datetime,
    ) -> list[tuple[GcTargetKind, str, datetime]]:
        targets: list[tuple[GcTargetKind, str, datetime]] = []
        attempt_finished = row["attempt_finished_at"]
        if row["attempt_status"] in _TERMINAL_ATTEMPTS and attempt_finished is not None:
            eligible_at = attempt_finished + timedelta(seconds=self._policy.staging_seconds)
            if eligible_at <= now:
                targets.append((GcTargetKind.STAGING, row["staging_uri"], eligible_at))

        run_finished = row["run_finished_at"]
        committed_at = row["committed_at"]
        if (
            row["state"] == ArtifactState.COMMITTED.value
            and row["run_status"] in _TERMINAL_RUNS
            and run_finished is not None
            and committed_at is not None
        ):
            eligible_at = max(
                run_finished + timedelta(seconds=self._policy.terminal_run_seconds),
                committed_at
                + timedelta(seconds=self._policy.committed_artifact_seconds),
            )
            if eligible_at <= now:
                targets.append((GcTargetKind.COMMITTED, row["committed_uri"], eligible_at))
        return targets

    def _upsert_target(
        self,
        artifact_id: UUID,
        kind: GcTargetKind,
        uri: str,
        eligible_at: datetime,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO artifact_gc_targets (
                    artifact_id, target_kind, uri, eligible_at
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (artifact_id, target_kind) DO UPDATE
                SET uri = EXCLUDED.uri,
                    eligible_at = EXCLUDED.eligible_at,
                    updated_at = NOW()
                WHERE artifact_gc_targets.state <> 'DELETED'
                """,
                (artifact_id, kind.value, uri, eligible_at),
            )

    def _load_work(self, now: datetime) -> list[GcTarget]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT artifact_id, target_kind, uri, eligible_at, state, attempts
                FROM artifact_gc_targets
                WHERE state <> 'DELETED' AND eligible_at <= %s
                ORDER BY updated_at, eligible_at, artifact_id, target_kind
                LIMIT %s
                """,
                (now, self._policy.work_batch_size),
            ).fetchall()
        return [self._target_from_row(row) for row in rows]

    def _process_target(
        self,
        target: GcTarget,
        now: datetime,
    ) -> tuple[GcTargetState, int]:
        reason = self._blocked_reason(target, now)
        if reason is not None:
            self._set_target_state(target, GcTargetState.BLOCKED, error=reason)
            return GcTargetState.BLOCKED, 0

        try:
            deleted_objects = self._storage.delete_prefix(target.uri)
        except Exception as error:
            self._set_target_state(target, GcTargetState.FAILED, error=str(error), attempt=True)
            self._observability.warning(
                "retention_gc_delete_failed",
                artifact_id=str(target.artifact_id),
                target=target.target_kind.value,
                error=str(error),
            )
            return GcTargetState.FAILED, 0

        self._set_target_state(
            target,
            GcTargetState.DELETED,
            attempt=True,
            deleted=True,
        )
        return GcTargetState.DELETED, deleted_objects

    def _blocked_reason(self, target: GcTarget, now: datetime) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    a.state,
                    a.committed_at,
                    r.status AS run_status,
                    r.finished_at AS run_finished_at,
                    ea.status AS attempt_status,
                    ea.finished_at AS attempt_finished_at
                FROM artifacts AS a
                JOIN pipeline_runs AS r ON r.id = a.pipeline_run_id
                JOIN execution_attempts AS ea
                  ON ea.execution_unit_id = a.execution_unit_id
                 AND ea.attempt_number = a.attempt_number
                WHERE a.id = %s
                """,
                (target.artifact_id,),
            ).fetchone()
            if row is None:
                return "artifact metadata no longer exists"

            if target.target_kind is GcTargetKind.STAGING:
                finished = row["attempt_finished_at"]
                if row["attempt_status"] not in _TERMINAL_ATTEMPTS or finished is None:
                    return "attempt is not terminal"
                if finished + timedelta(seconds=self._policy.staging_seconds) > now:
                    return "staging retention window has not elapsed"
                return None

            run_finished = row["run_finished_at"]
            committed_at = row["committed_at"]
            if row["state"] != ArtifactState.COMMITTED.value:
                return "artifact is not committed"
            if row["run_status"] not in _TERMINAL_RUNS or run_finished is None:
                return "owning run is not terminal"
            if run_finished + timedelta(seconds=self._policy.terminal_run_seconds) > now:
                return "run retention window has not elapsed"
            if (
                committed_at is None
                or committed_at
                + timedelta(seconds=self._policy.committed_artifact_seconds)
                > now
            ):
                return "artifact retention window has not elapsed"
            if self._has_active_reference(connection, target.uri):
                return "committed artifact is referenced by an active run"
            return None

    def _has_active_reference(self, connection: Any, uri: str) -> bool:
        row = connection.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM execution_units AS u
                JOIN pipeline_runs AS r ON r.id = u.pipeline_run_id
                WHERE r.status <> ALL(%s)
                  AND COALESCE(u.plan_json -> 'input_artifacts', '[]'::jsonb) @> %s
                UNION ALL
                SELECT 1
                FROM node_runs AS n
                JOIN pipeline_runs AS r ON r.id = n.pipeline_run_id
                WHERE r.status <> ALL(%s)
                  AND n.input_artifacts_json @> %s
            ) AS referenced
            """,
            (
                _TERMINAL_RUNS,
                Jsonb([{"uri": uri}]),
                _TERMINAL_RUNS,
                Jsonb([{"uri": uri}]),
            ),
        ).fetchone()
        return bool(row["referenced"])

    def _set_target_state(
        self,
        target: GcTarget,
        state: GcTargetState,
        *,
        error: str | None = None,
        attempt: bool = False,
        deleted: bool = False,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE artifact_gc_targets
                SET state = %s,
                    attempts = attempts + %s,
                    last_error = %s,
                    updated_at = NOW(),
                    deleted_at = CASE WHEN %s THEN COALESCE(deleted_at, NOW()) ELSE deleted_at END
                WHERE artifact_id = %s AND target_kind = %s
                """,
                (
                    state.value,
                    1 if attempt else 0,
                    error[:1000] if error else None,
                    deleted,
                    target.artifact_id,
                    target.target_kind.value,
                ),
            )

    def _count_prunable_events(self, now: datetime) -> int:
        event_cutoff = now - timedelta(seconds=self._policy.event_seconds)
        run_cutoff = now - timedelta(seconds=self._policy.terminal_run_seconds)
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM events AS e
                JOIN pipeline_runs AS r ON r.id = e.pipeline_run_id
                WHERE r.status = ANY(%s)
                  AND r.finished_at IS NOT NULL
                  AND r.finished_at <= %s
                  AND e.created_at <= %s
                """,
                (_TERMINAL_RUNS, run_cutoff, event_cutoff),
            ).fetchone()
        return int(row["count"])

    def _prune_events(self, now: datetime) -> int:
        event_cutoff = now - timedelta(seconds=self._policy.event_seconds)
        run_cutoff = now - timedelta(seconds=self._policy.terminal_run_seconds)
        with self._connect() as connection, connection.transaction():
            connection.execute("SET LOCAL dataflow.retention_gc = 'on'")
            result = connection.execute(
                """
                DELETE FROM events AS e
                USING pipeline_runs AS r
                WHERE r.id = e.pipeline_run_id
                  AND r.status = ANY(%s)
                  AND r.finished_at IS NOT NULL
                  AND r.finished_at <= %s
                  AND e.created_at <= %s
                """,
                (_TERMINAL_RUNS, run_cutoff, event_cutoff),
            )
            return result.rowcount

    @staticmethod
    def _artifact_scan_sql(*, where: str) -> str:
        return f"""
            SELECT
                a.*,
                r.status AS run_status,
                r.finished_at AS run_finished_at,
                ea.status AS attempt_status,
                ea.finished_at AS attempt_finished_at
            FROM artifacts AS a
            JOIN pipeline_runs AS r ON r.id = a.pipeline_run_id
            JOIN execution_attempts AS ea
              ON ea.execution_unit_id = a.execution_unit_id
             AND ea.attempt_number = a.attempt_number
            {where}
            ORDER BY a.created_at, a.id
            LIMIT %s
        """

    def _connect(self) -> psycopg.Connection[dict[str, Any]]:
        return psycopg.connect(self._dsn, row_factory=dict_row)

    @staticmethod
    def _target_from_row(row: Mapping[str, Any]) -> GcTarget:
        return GcTarget(
            artifact_id=row["artifact_id"],
            target_kind=GcTargetKind(row["target_kind"]),
            uri=row["uri"],
            eligible_at=row["eligible_at"],
            state=GcTargetState(row["state"]),
            attempts=int(row["attempts"]),
        )


def _env_int(
    environment: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int,
) -> int:
    raw = environment.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if value < minimum:
        qualifier = "non-negative" if minimum == 0 else f"at least {minimum}"
        raise ValueError(f"{name} must be {qualifier}")
    return value


__all__ = [
    "GcReport",
    "GcTarget",
    "GcTargetKind",
    "GcTargetState",
    "PostgresRetentionGc",
    "RetentionPolicy",
]
