from __future__ import annotations

import os
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from dataflow.admission import (
    AdmissionPolicy,
    AdmissionState,
    PostgresAdmissionController,
)
from dataflow.metadata.migrations import migrate


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
            "UPDATE admission_scheduler_state SET next_sequence = 1 WHERE singleton = TRUE"
        )


def _seed_run(
    dsn: str,
    name: str,
    profiles: list[str],
    *,
    status: str = "READY",
) -> tuple[UUID, list[UUID]]:
    pipeline_id = uuid4()
    version_id = uuid4()
    run_id = uuid4()
    unit_ids = [uuid4() for _ in profiles]
    with psycopg.connect(dsn) as connection, connection.transaction():
        connection.execute(
            "INSERT INTO pipelines (id, tenant_id, name) VALUES (%s, 'default', %s)",
            (pipeline_id, name),
        )
        connection.execute(
            """
            INSERT INTO pipeline_versions (
                id, pipeline_id, version, spec_json, spec_hash
            )
            VALUES (%s, %s, 1, %s, %s)
            """,
            (version_id, pipeline_id, Jsonb({}), "0" * 64),
        )
        connection.execute(
            """
            INSERT INTO pipeline_runs (
                id, pipeline_version_id, status, parameters_json, cluster_profile,
                queued_at, started_at
            )
            VALUES (%s, %s, 'RUNNING', %s, %s, NOW(), NOW())
            """,
            (run_id, version_id, Jsonb({}), profiles[0]),
        )
        for index, (unit_id, profile) in enumerate(zip(unit_ids, profiles, strict=True)):
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
                    run_id,
                    f"unit-{index}",
                    Jsonb({}),
                    Jsonb([]),
                    profile,
                    status,
                ),
            )
    return run_id, unit_ids


def _set_unit_status(dsn: str, unit_id: UUID, status: str) -> None:
    with psycopg.connect(dsn) as connection, connection.transaction():
        connection.execute(
            "UPDATE execution_units SET status = %s WHERE id = %s",
            (status, unit_id),
        )


def _event_types(dsn: str) -> list[str]:
    with psycopg.connect(dsn) as connection:
        return [
            str(row[0])
            for row in connection.execute("SELECT event_type FROM events ORDER BY id").fetchall()
        ]


def test_global_and_profile_limits_are_never_exceeded(postgres_dsn: str) -> None:
    _, unit_ids = _seed_run(postgres_dsn, "limits", ["gpu", "gpu", "cpu", "cpu"])
    admission = PostgresAdmissionController(
        postgres_dsn,
        AdmissionPolicy(global_limit=2, profile_limits={"gpu": 1}, max_new_per_pass=10),
    )

    first = admission.reconcile()
    records = admission.list_records()
    admitted = [record for record in records if record.state is AdmissionState.ADMITTED]

    assert first.active_slots == 2
    assert first.profile_slots["gpu"] == 1
    assert len(admitted) == 2
    assert sum(record.cluster_profile == "gpu" for record in admitted) == 1
    assert first.queued_units == 2

    gpu_admitted = next(
        record.execution_unit_id
        for record in admitted
        if record.cluster_profile == "gpu"
    )
    _set_unit_status(postgres_dsn, gpu_admitted, "SUCCEEDED")

    second = admission.reconcile()
    second_records = admission.list_records()
    second_admitted = [
        record for record in second_records if record.state is AdmissionState.ADMITTED
    ]
    assert second.active_slots == 2
    assert second.profile_slots["gpu"] == 1
    assert len(second_admitted) == 2
    assert gpu_admitted not in second.admitted_unit_ids
    assert any(
        unit_id in second.admitted_unit_ids
        for unit_id in unit_ids
        if unit_id != gpu_admitted
    )
    assert "ADMISSION_RELEASED" in _event_types(postgres_dsn)


def test_restart_recovery_and_retry_do_not_leak_or_double_count_slots(
    postgres_dsn: str,
) -> None:
    _, unit_ids = _seed_run(postgres_dsn, "restart", ["cpu", "cpu"])
    policy = AdmissionPolicy(global_limit=1, max_new_per_pass=4)

    first_controller = PostgresAdmissionController(postgres_dsn, policy)
    first = first_controller.reconcile()
    assert len(first.newly_admitted_unit_ids) == 1
    active_unit = first.newly_admitted_unit_ids[0]
    waiting_unit = next(unit_id for unit_id in unit_ids if unit_id != active_unit)
    _set_unit_status(postgres_dsn, active_unit, "RUNNING")

    restarted = PostgresAdmissionController(postgres_dsn, policy).reconcile()
    assert restarted.active_slots == 1
    assert restarted.newly_admitted_unit_ids == ()
    assert restarted.admitted_unit_ids == frozenset({active_unit})

    with psycopg.connect(postgres_dsn) as connection, connection.transaction():
        connection.execute(
            "DELETE FROM execution_admissions WHERE execution_unit_id = %s",
            (active_unit,),
        )

    reconstructed = PostgresAdmissionController(postgres_dsn, policy).reconcile()
    assert reconstructed.recovered_units == 1
    assert reconstructed.active_slots == 1
    assert reconstructed.newly_admitted_unit_ids == ()
    assert reconstructed.admitted_unit_ids == frozenset({active_unit})

    _set_unit_status(postgres_dsn, active_unit, "RETRY_WAIT")
    retry_wait = PostgresAdmissionController(postgres_dsn, policy).reconcile()
    assert retry_wait.active_slots == 1
    assert retry_wait.admitted_unit_ids == frozenset({active_unit})

    _set_unit_status(postgres_dsn, active_unit, "FAILED")
    after_terminal = PostgresAdmissionController(postgres_dsn, policy).reconcile()
    assert after_terminal.released_units == 1
    assert after_terminal.active_slots == 1
    assert after_terminal.newly_admitted_unit_ids == (waiting_unit,)


def test_fair_selection_rotates_across_active_runs(postgres_dsn: str) -> None:
    run_a, units_a = _seed_run(postgres_dsn, "fair-a", ["cpu", "cpu"])
    run_b, units_b = _seed_run(postgres_dsn, "fair-b", ["cpu", "cpu"])
    admission = PostgresAdmissionController(
        postgres_dsn,
        AdmissionPolicy(global_limit=1, max_new_per_pass=1),
    )

    selected_runs: list[UUID] = []
    selected_units: list[UUID] = []
    unit_to_run = {**{unit: run_a for unit in units_a}, **{unit: run_b for unit in units_b}}
    for _ in range(4):
        batch = admission.reconcile()
        assert len(batch.newly_admitted_unit_ids) == 1
        selected = batch.newly_admitted_unit_ids[0]
        selected_units.append(selected)
        selected_runs.append(unit_to_run[selected])
        _set_unit_status(postgres_dsn, selected, "SUCCEEDED")

    assert selected_runs == [run_a, run_b, run_a, run_b]
    assert len(set(selected_units)) == 4


def test_new_admission_work_is_bounded_per_reconcile_pass(postgres_dsn: str) -> None:
    _seed_run(postgres_dsn, "bounded", ["cpu"] * 5)
    admission = PostgresAdmissionController(
        postgres_dsn,
        AdmissionPolicy(max_new_per_pass=2),
    )

    first = admission.reconcile()
    assert len(first.newly_admitted_unit_ids) == 2
    assert first.active_slots == 2
    assert first.queued_units == 3
    assert first.throttled_units == 3

    second = admission.reconcile()
    assert len(second.newly_admitted_unit_ids) == 2
    assert second.active_slots == 4
    assert second.queued_units == 1

    third = admission.reconcile()
    assert len(third.newly_admitted_unit_ids) == 1
    assert third.active_slots == 5
    assert third.queued_units == 0
