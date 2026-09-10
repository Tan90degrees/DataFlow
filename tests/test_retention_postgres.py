from __future__ import annotations

import os
from datetime import UTC, datetime
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from dataflow.metadata.migrations import migrate
from dataflow.retention import (
    GcTargetKind,
    GcTargetState,
    PostgresRetentionGc,
    RetentionPolicy,
)


class _RecordingStorage:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.fail_once: set[str] = set()
        self._failed: set[str] = set()

    def delete_prefix(self, uri: str) -> int:
        self.calls.append(uri)
        if uri in self.fail_once and uri not in self._failed:
            self._failed.add(uri)
            raise RuntimeError("transient object-store failure")
        return 2


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    dsn = os.environ.get("DATAFLOW_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("DATAFLOW_TEST_DATABASE_URL is not configured")
    migrate(dsn)
    return dsn


@pytest.fixture(autouse=True)
def clean_database(postgres_dsn: str) -> None:
    with psycopg.connect(postgres_dsn) as connection, connection.transaction():
        connection.execute(
            """
            TRUNCATE TABLE
                artifact_gc_targets,
                execution_admissions,
                artifacts,
                events,
                node_runs,
                execution_attempts,
                execution_units,
                pipeline_runs,
                pipeline_versions,
                pipelines
            RESTART IDENTITY CASCADE
            """
        )
        connection.execute(
            """
            UPDATE artifact_gc_cursor
            SET last_created_at = NULL, last_artifact_id = NULL, updated_at = NOW()
            WHERE singleton = TRUE
            """
        )


def _seed_artifact(
    dsn: str,
    *,
    run_status: str,
    attempt_status: str,
    artifact_state: str,
    name: str,
) -> tuple[UUID, UUID, str, str]:
    pipeline_id = uuid4()
    version_id = uuid4()
    run_id = uuid4()
    unit_id = uuid4()
    attempt_id = uuid4()
    artifact_id = uuid4()
    staging_uri = f"s3://bucket/runs/{run_id}/artifacts/{name}/attempts/001/data"
    committed_uri = f"s3://bucket/runs/{run_id}/artifacts/{name}/committed"
    run_terminal = run_status in {"SUCCEEDED", "FAILED", "CANCELLED"}
    attempt_terminal = attempt_status in {"SUCCEEDED", "FAILED", "CANCELLED"}
    unit_status = run_status if run_status in {"SUCCEEDED", "FAILED", "CANCELLED"} else "RUNNING"

    with psycopg.connect(dsn) as connection, connection.transaction():
        connection.execute(
            "INSERT INTO pipelines (id, tenant_id, name) VALUES (%s, 'default', %s)",
            (pipeline_id, f"gc-{name}-{pipeline_id}"),
        )
        connection.execute(
            """
            INSERT INTO pipeline_versions (
                id, pipeline_id, version, spec_json, spec_hash
            ) VALUES (%s, %s, 1, %s, %s)
            """,
            (version_id, pipeline_id, Jsonb({}), "0" * 64),
        )
        connection.execute(
            """
            INSERT INTO pipeline_runs (
                id, pipeline_version_id, status, parameters_json,
                created_at, started_at, finished_at
            ) VALUES (
                %s, %s, %s, %s,
                NOW() - INTERVAL '3 hours',
                NOW() - INTERVAL '3 hours',
                CASE WHEN %s THEN NOW() - INTERVAL '2 hours' ELSE NULL END
            )
            """,
            (run_id, version_id, run_status, Jsonb({}), run_terminal),
        )
        connection.execute(
            """
            INSERT INTO execution_units (
                id, pipeline_run_id, unit_key, plan_json, dependencies_json,
                cluster_profile, status, current_attempt,
                created_at, started_at, finished_at
            ) VALUES (
                %s, %s, 'unit-001', %s, %s, 'default', %s, 1,
                NOW() - INTERVAL '3 hours',
                NOW() - INTERVAL '3 hours',
                CASE WHEN %s THEN NOW() - INTERVAL '2 hours' ELSE NULL END
            )
            """,
            (unit_id, run_id, Jsonb({}), Jsonb([]), unit_status, run_terminal),
        )
        connection.execute(
            """
            INSERT INTO execution_attempts (
                id, execution_unit_id, attempt_number, status,
                created_at, started_at, finished_at
            ) VALUES (
                %s, %s, 1, %s,
                NOW() - INTERVAL '3 hours',
                NOW() - INTERVAL '3 hours',
                CASE WHEN %s THEN NOW() - INTERVAL '2 hours' ELSE NULL END
            )
            """,
            (attempt_id, unit_id, attempt_status, attempt_terminal),
        )
        connection.execute(
            """
            INSERT INTO artifacts (
                id, pipeline_run_id, node_id, execution_unit_id, attempt_number,
                format, state, staging_uri, committed_uri, checkpoint,
                created_at, committed_at, aborted_at
            ) VALUES (
                %s, %s, %s, %s, 1,
                'parquet', %s, %s, %s, TRUE,
                NOW() - INTERVAL '3 hours',
                CASE WHEN %s = 'COMMITTED' THEN NOW() - INTERVAL '2 hours' ELSE NULL END,
                CASE WHEN %s = 'ABORTED' THEN NOW() - INTERVAL '2 hours' ELSE NULL END
            )
            """,
            (
                artifact_id,
                run_id,
                name,
                unit_id,
                artifact_state,
                staging_uri,
                committed_uri,
                artifact_state,
                artifact_state,
            ),
        )
    return artifact_id, run_id, staging_uri, committed_uri


def _seed_active_reference(dsn: str, uri: str) -> UUID:
    pipeline_id = uuid4()
    version_id = uuid4()
    run_id = uuid4()
    unit_id = uuid4()
    with psycopg.connect(dsn) as connection, connection.transaction():
        connection.execute(
            "INSERT INTO pipelines (id, tenant_id, name) VALUES (%s, 'default', %s)",
            (pipeline_id, f"active-ref-{pipeline_id}"),
        )
        connection.execute(
            """
            INSERT INTO pipeline_versions (
                id, pipeline_id, version, spec_json, spec_hash
            ) VALUES (%s, %s, 1, %s, %s)
            """,
            (version_id, pipeline_id, Jsonb({}), "1" * 64),
        )
        connection.execute(
            """
            INSERT INTO pipeline_runs (
                id, pipeline_version_id, status, parameters_json, started_at
            ) VALUES (%s, %s, 'RUNNING', %s, NOW())
            """,
            (run_id, version_id, Jsonb({})),
        )
        artifact_ref = {
            "run_id": "upstream",
            "node_id": "source",
            "format": "parquet",
            "uri": uri,
        }
        connection.execute(
            """
            INSERT INTO execution_units (
                id, pipeline_run_id, unit_key, plan_json, dependencies_json,
                cluster_profile, status
            ) VALUES (%s, %s, 'consumer', %s, %s, 'default', 'RUNNING')
            """,
            (
                unit_id,
                run_id,
                Jsonb({"input_artifacts": [artifact_ref]}),
                Jsonb([]),
            ),
        )
    return run_id


def _policy(*, work_batch_size: int = 64) -> RetentionPolicy:
    return RetentionPolicy(
        terminal_run_seconds=0,
        event_seconds=0,
        staging_seconds=0,
        committed_artifact_seconds=0,
        scan_batch_size=64,
        work_batch_size=work_batch_size,
    )


def test_committed_artifact_referenced_by_active_run_is_blocked(
    postgres_dsn: str,
) -> None:
    artifact_id, _, staging_uri, committed_uri = _seed_artifact(
        postgres_dsn,
        run_status="SUCCEEDED",
        attempt_status="SUCCEEDED",
        artifact_state="COMMITTED",
        name="shared",
    )
    _seed_active_reference(postgres_dsn, committed_uri)
    storage = _RecordingStorage()
    gc = PostgresRetentionGc(postgres_dsn, storage, _policy())

    report = gc.run_once(now=datetime.now(UTC))
    targets = gc.list_targets()
    committed = next(
        target
        for target in targets
        if target.artifact_id == artifact_id
        and target.target_kind is GcTargetKind.COMMITTED
    )

    assert report.blocked_targets == 1
    assert committed.state is GcTargetState.BLOCKED
    assert committed_uri not in storage.calls
    assert staging_uri in storage.calls


def test_blocked_committed_artifact_is_deleted_after_reference_finishes(
    postgres_dsn: str,
) -> None:
    artifact_id, _, _, committed_uri = _seed_artifact(
        postgres_dsn,
        run_status="SUCCEEDED",
        attempt_status="SUCCEEDED",
        artifact_state="COMMITTED",
        name="release-reference",
    )
    active_run_id = _seed_active_reference(postgres_dsn, committed_uri)
    storage = _RecordingStorage()
    gc = PostgresRetentionGc(postgres_dsn, storage, _policy())

    first = gc.run_once(now=datetime.now(UTC))
    assert first.blocked_targets == 1

    with psycopg.connect(postgres_dsn) as connection, connection.transaction():
        connection.execute(
            """
            UPDATE pipeline_runs
            SET status = 'SUCCEEDED', finished_at = NOW()
            WHERE id = %s
            """,
            (active_run_id,),
        )

    second = gc.run_once(now=datetime.now(UTC))
    committed = next(
        target
        for target in gc.list_targets()
        if target.artifact_id == artifact_id
        and target.target_kind is GcTargetKind.COMMITTED
    )
    assert second.deleted_targets >= 1
    assert committed.state is GcTargetState.DELETED
    assert storage.calls.count(committed_uri) == 1


@pytest.mark.parametrize(
    ("run_status", "attempt_status"),
    [("FAILED", "FAILED"), ("CANCELLED", "CANCELLED")],
)
def test_terminal_failed_or_cancelled_staging_is_reclaimed(
    postgres_dsn: str,
    run_status: str,
    attempt_status: str,
) -> None:
    artifact_id, _, staging_uri, _ = _seed_artifact(
        postgres_dsn,
        run_status=run_status,
        attempt_status=attempt_status,
        artifact_state="ABORTED",
        name=run_status.lower(),
    )
    storage = _RecordingStorage()
    gc = PostgresRetentionGc(postgres_dsn, storage, _policy())

    report = gc.run_once(now=datetime.now(UTC))
    target = next(target for target in gc.list_targets() if target.artifact_id == artifact_id)

    assert report.deleted_targets == 1
    assert report.objects_deleted == 2
    assert target.target_kind is GcTargetKind.STAGING
    assert target.state is GcTargetState.DELETED
    assert target.attempts == 1
    assert storage.calls == [staging_uri]


def test_transient_storage_failure_is_resumable_and_idempotent(
    postgres_dsn: str,
) -> None:
    artifact_id, _, staging_uri, _ = _seed_artifact(
        postgres_dsn,
        run_status="FAILED",
        attempt_status="FAILED",
        artifact_state="ABORTED",
        name="retry",
    )
    storage = _RecordingStorage()
    storage.fail_once.add(staging_uri)
    gc = PostgresRetentionGc(postgres_dsn, storage, _policy())
    now = datetime.now(UTC)

    first = gc.run_once(now=now)
    first_target = next(
        target for target in gc.list_targets() if target.artifact_id == artifact_id
    )
    assert first.failed_targets == 1
    assert first_target.state is GcTargetState.FAILED
    assert first_target.attempts == 1

    second = gc.run_once(now=now)
    second_target = next(
        target for target in gc.list_targets() if target.artifact_id == artifact_id
    )
    assert second.deleted_targets == 1
    assert second_target.state is GcTargetState.DELETED
    assert second_target.attempts == 2
    assert storage.calls == [staging_uri, staging_uri]

    third = gc.run_once(now=now)
    assert third.deleted_targets == 0
    assert storage.calls == [staging_uri, staging_uri]


def test_dry_run_reports_without_mutating_targets_events_or_storage(
    postgres_dsn: str,
) -> None:
    _, run_id, _, _ = _seed_artifact(
        postgres_dsn,
        run_status="FAILED",
        attempt_status="FAILED",
        artifact_state="ABORTED",
        name="dry-run",
    )
    with psycopg.connect(postgres_dsn) as connection, connection.transaction():
        connection.execute(
            """
            INSERT INTO events (
                pipeline_run_id, aggregate_type, aggregate_id,
                event_type, payload_json, created_at
            ) VALUES (%s, 'pipeline_run', %s, 'OLD_EVENT', %s, NOW() - INTERVAL '2 hours')
            """,
            (run_id, run_id, Jsonb({})),
        )

    storage = _RecordingStorage()
    gc = PostgresRetentionGc(postgres_dsn, storage, _policy())
    report = gc.run_once(dry_run=True, now=datetime.now(UTC))

    assert report.dry_run is True
    assert report.discovered_targets == 1
    assert report.events_pruned == 1
    assert storage.calls == []
    assert gc.list_targets() == []
    with psycopg.connect(postgres_dsn) as connection:
        assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1


def test_event_pruning_uses_narrow_maintenance_exception_only(
    postgres_dsn: str,
) -> None:
    _, run_id, _, _ = _seed_artifact(
        postgres_dsn,
        run_status="FAILED",
        attempt_status="FAILED",
        artifact_state="ABORTED",
        name="events",
    )
    event_id: int
    with psycopg.connect(postgres_dsn) as connection, connection.transaction():
        event_id = connection.execute(
            """
            INSERT INTO events (
                pipeline_run_id, aggregate_type, aggregate_id,
                event_type, payload_json, created_at
            ) VALUES (%s, 'pipeline_run', %s, 'OLD_EVENT', %s, NOW() - INTERVAL '2 hours')
            RETURNING id
            """,
            (run_id, run_id, Jsonb({})),
        ).fetchone()[0]

    storage = _RecordingStorage()
    report = PostgresRetentionGc(postgres_dsn, storage, _policy()).run_once(
        now=datetime.now(UTC)
    )
    assert report.events_pruned == 1

    with psycopg.connect(postgres_dsn) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM events WHERE id = %s", (event_id,)
        ).fetchone()[0] == 0

    with psycopg.connect(postgres_dsn) as connection:
        connection.execute(
            """
            INSERT INTO events (
                pipeline_run_id, aggregate_type, aggregate_id,
                event_type, payload_json
            ) VALUES (%s, 'pipeline_run', %s, 'NEW_EVENT', %s)
            """,
            (run_id, run_id, Jsonb({})),
        )
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            connection.execute("DELETE FROM events WHERE pipeline_run_id = %s", (run_id,))
