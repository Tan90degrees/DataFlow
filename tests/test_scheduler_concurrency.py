from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID, uuid4

from dataflow.metadata.repository import (
    ConcurrentStateChange,
    ExecutionUnitRecord,
    PipelineRunRecord,
)
from dataflow.scheduler import Scheduler
from dataflow.state import ExecutionUnitStatus, PipelineRunStatus


class RacingRepository:
    def __init__(self) -> None:
        now = datetime.now(UTC)
        run_id = uuid4()
        self.run = PipelineRunRecord(
            id=run_id,
            pipeline_version_id=uuid4(),
            status=PipelineRunStatus.RUNNING,
            parameters_json={},
            cluster_profile="default",
            created_at=now,
            queued_at=now,
            started_at=now,
            finished_at=None,
        )
        self.unit = ExecutionUnitRecord(
            id=uuid4(),
            pipeline_run_id=run_id,
            unit_key="unit-001",
            status=ExecutionUnitStatus.PENDING,
            current_attempt=0,
            cluster_profile="default",
            dependencies=[],
            plan_json={},
            created_at=now,
            started_at=None,
            finished_at=None,
        )
        self.injected_race = False

    def get_run(self, run_id: UUID) -> PipelineRunRecord:
        assert run_id == self.run.id
        return self.run

    def get_unit(self, unit_id: UUID) -> ExecutionUnitRecord:
        assert unit_id == self.unit.id
        return self.unit

    def list_units(self, run_id: UUID) -> list[ExecutionUnitRecord]:
        assert run_id == self.run.id
        return [self.unit]

    def transition_unit_status(
        self,
        unit_id: UUID,
        target: ExecutionUnitStatus,
        *,
        expected: ExecutionUnitStatus | None = None,
        payload: dict | None = None,
    ) -> ExecutionUnitRecord:
        assert unit_id == self.unit.id
        assert target is ExecutionUnitStatus.READY
        assert expected is ExecutionUnitStatus.PENDING
        assert payload == {"reason": "ALL_DEPENDENCIES_SUCCEEDED"}

        if not self.injected_race:
            self.injected_race = True
            self.unit = replace(self.unit, status=ExecutionUnitStatus.READY)
            raise ConcurrentStateChange(
                "expected state PENDING, but durable state is READY"
            )
        return self.unit

    def transition_run_status(
        self,
        run_id: UUID,
        target: PipelineRunStatus,
        *,
        expected: PipelineRunStatus | None = None,
        payload: dict | None = None,
    ) -> PipelineRunRecord:
        raise AssertionError("run state must not change in this readiness race")


def test_reconcile_run_tolerates_concurrent_readiness_transition() -> None:
    repository = RacingRepository()

    units = Scheduler(repository).reconcile_run(repository.run.id)

    assert repository.injected_race is True
    assert repository.unit.status is ExecutionUnitStatus.READY
    assert [unit.status for unit in units] == [ExecutionUnitStatus.READY]
