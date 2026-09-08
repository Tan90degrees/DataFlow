"""Dependency-aware scheduling for durable execution units."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from dataflow.metadata.repository import ExecutionUnitRecord, PipelineRunRecord
from dataflow.state import (
    TERMINAL_UNIT_STATUSES,
    ExecutionUnitStatus,
    PipelineRunStatus,
)


class SchedulingRepository(Protocol):
    def get_run(self, run_id: UUID) -> PipelineRunRecord: ...

    def list_units(self, run_id: UUID) -> list[ExecutionUnitRecord]: ...

    def transition_unit_status(
        self,
        unit_id: UUID,
        target: ExecutionUnitStatus,
        *,
        expected: ExecutionUnitStatus | None = None,
        payload: dict | None = None,
    ) -> ExecutionUnitRecord: ...

    def transition_run_status(
        self,
        run_id: UUID,
        target: PipelineRunStatus,
        *,
        expected: PipelineRunStatus | None = None,
        payload: dict | None = None,
    ) -> PipelineRunRecord: ...


class Scheduler:
    """Evaluate static `all_success` dependencies against durable unit state."""

    def __init__(self, repository: SchedulingRepository) -> None:
        self._repository = repository

    def reconcile_run(self, run_id: UUID) -> list[ExecutionUnitRecord]:
        run = self._repository.get_run(run_id)
        units = self._repository.list_units(run_id)
        if not units:
            return units

        if run.status is PipelineRunStatus.CANCELLED:
            return self._cancel_waiting_units(units)
        if run.status is not PipelineRunStatus.RUNNING:
            return units

        units_by_key = {unit.unit_key: unit for unit in units}
        changed = True
        while changed:
            changed = False
            for unit in list(units_by_key.values()):
                if unit.status is not ExecutionUnitStatus.PENDING:
                    continue
                dependencies = self._dependencies(unit, units_by_key)
                if all(dep.status is ExecutionUnitStatus.SUCCEEDED for dep in dependencies):
                    updated = self._repository.transition_unit_status(
                        unit.id,
                        ExecutionUnitStatus.READY,
                        expected=ExecutionUnitStatus.PENDING,
                        payload={"reason": "ALL_DEPENDENCIES_SUCCEEDED"},
                    )
                    units_by_key[unit.unit_key] = updated
                    changed = True
                    continue

                blocked = [
                    dep
                    for dep in dependencies
                    if dep.status in {ExecutionUnitStatus.FAILED, ExecutionUnitStatus.CANCELLED}
                ]
                if blocked:
                    updated = self._repository.transition_unit_status(
                        unit.id,
                        ExecutionUnitStatus.CANCELLED,
                        expected=ExecutionUnitStatus.PENDING,
                        payload={
                            "reason": "UPSTREAM_NOT_SUCCESSFUL",
                            "upstream_units": [dep.unit_key for dep in blocked],
                        },
                    )
                    units_by_key[unit.unit_key] = updated
                    changed = True

        ordered = [units_by_key[unit.unit_key] for unit in units]
        self._converge_run(run, ordered)
        return ordered

    def cancel_run(self, run_id: UUID) -> PipelineRunRecord:
        run = self._repository.get_run(run_id)
        if run.status is not PipelineRunStatus.CANCELLED:
            run = self._repository.transition_run_status(
                run_id,
                PipelineRunStatus.CANCELLED,
                expected=run.status,
                payload={"reason": "CANCELLATION_REQUESTED"},
            )
        self._cancel_waiting_units(self._repository.list_units(run_id))
        return run

    def _cancel_waiting_units(
        self,
        units: list[ExecutionUnitRecord],
    ) -> list[ExecutionUnitRecord]:
        result: list[ExecutionUnitRecord] = []
        waiting = {
            ExecutionUnitStatus.PENDING,
            ExecutionUnitStatus.READY,
            ExecutionUnitStatus.RETRY_WAIT,
        }
        for unit in units:
            if unit.status in waiting:
                unit = self._repository.transition_unit_status(
                    unit.id,
                    ExecutionUnitStatus.CANCELLED,
                    expected=unit.status,
                    payload={"reason": "RUN_CANCELLED"},
                )
            result.append(unit)
        return result

    @staticmethod
    def _dependencies(
        unit: ExecutionUnitRecord,
        units_by_key: dict[str, ExecutionUnitRecord],
    ) -> list[ExecutionUnitRecord]:
        missing = [key for key in unit.dependencies if key not in units_by_key]
        if missing:
            raise ValueError(
                f"execution unit {unit.unit_key!r} references missing dependencies: {missing}"
            )
        return [units_by_key[key] for key in unit.dependencies]

    def _converge_run(
        self,
        run: PipelineRunRecord,
        units: list[ExecutionUnitRecord],
    ) -> None:
        if run.status is not PipelineRunStatus.RUNNING:
            return
        statuses = [unit.status for unit in units]
        if all(status is ExecutionUnitStatus.SUCCEEDED for status in statuses):
            self._repository.transition_run_status(
                run.id,
                PipelineRunStatus.SUCCEEDED,
                expected=PipelineRunStatus.RUNNING,
            )
            return
        if all(status in TERMINAL_UNIT_STATUSES for status in statuses) and any(
            status is ExecutionUnitStatus.FAILED for status in statuses
        ):
            self._repository.transition_run_status(
                run.id,
                PipelineRunStatus.FAILED,
                expected=PipelineRunStatus.RUNNING,
                payload={"reason": "EXECUTION_UNIT_FAILED"},
            )
            return
        if all(status in TERMINAL_UNIT_STATUSES for status in statuses) and any(
            status is ExecutionUnitStatus.CANCELLED for status in statuses
        ):
            self._repository.transition_run_status(
                run.id,
                PipelineRunStatus.FAILED,
                expected=PipelineRunStatus.RUNNING,
                payload={"reason": "EXECUTION_UNIT_CANCELLED"},
            )


__all__ = ["Scheduler", "SchedulingRepository"]
