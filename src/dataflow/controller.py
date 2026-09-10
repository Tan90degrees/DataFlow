"""Top-level orchestration control loop."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Protocol
from uuid import UUID

from dataflow.admission import AdmissionBatch, PostgresAdmissionController
from dataflow.leadership import ControllerLeadership, LeadershipUnavailable
from dataflow.metadata.repository import ExecutionUnitRecord, PipelineRunRecord
from dataflow.observability import DEFAULT_OBSERVABILITY, Observability
from dataflow.reconciler import Reconciler
from dataflow.scheduler import Scheduler


class ControllerRepository(Protocol):
    def list_active_runs(self) -> list[PipelineRunRecord]: ...

    def list_recoverable_units(self) -> list[ExecutionUnitRecord]: ...

    def list_units(self, run_id: UUID) -> list[ExecutionUnitRecord]: ...


class OrchestrationController:
    """Drive scheduler, admission, and reconciler passes from durable PostgreSQL state."""

    def __init__(
        self,
        repository: ControllerRepository,
        scheduler: Scheduler,
        reconciler: Reconciler,
        *,
        admission: PostgresAdmissionController | None = None,
        observability: Observability | None = None,
        max_scheduled_runs_per_pass: int = 128,
    ) -> None:
        if max_scheduled_runs_per_pass <= 0:
            raise ValueError("max_scheduled_runs_per_pass must be positive")
        self._repository = repository
        self._scheduler = scheduler
        self._reconciler = reconciler
        self._admission = admission
        self._observability = observability or DEFAULT_OBSERVABILITY
        self._max_scheduled_runs_per_pass = max_scheduled_runs_per_pass
        self._schedule_cursor = 0

    def reconcile_once(self) -> None:
        active_runs = self._repository.list_active_runs()
        scheduled_runs = self._select_scheduled_runs(active_runs)
        selected_run_ids = {run.id for run in scheduled_runs}
        self._observability.info(
            "controller_reconcile_started",
            active_runs=len(active_runs),
            scheduled_runs=len(scheduled_runs),
            scheduler_run_limit=self._max_scheduled_runs_per_pass,
        )
        for run in scheduled_runs:
            with self._observability.bind(run_id=str(run.id)):
                self._observability.info(
                    "controller_schedule_run",
                    run_status=run.status.value,
                )
                self._scheduler.reconcile_run(run.id)

        admission_batch = self._reconcile_admission()
        touched_runs: set[UUID] = set(selected_run_ids)
        recoverable = self._repository.list_recoverable_units()
        reconciled_units = 0
        for unit in recoverable:
            touched_runs.add(unit.pipeline_run_id)
            if admission_batch is not None and unit.id not in admission_batch.admitted_unit_ids:
                continue
            with self._observability.bind(
                run_id=str(unit.pipeline_run_id),
                unit_id=str(unit.id),
                unit_key=unit.unit_key,
            ):
                self._observability.info(
                    "controller_reconcile_unit",
                    unit_status=unit.status.value,
                )
                self._reconciler.reconcile_unit(unit.id)
                reconciled_units += 1

        active_ids = {run.id for run in self._repository.list_active_runs()}
        reschedule_ids = touched_runs & active_ids & selected_run_ids
        for run_id in sorted(reschedule_ids, key=str):
            with self._observability.bind(run_id=str(run_id)):
                self._scheduler.reconcile_run(run_id)

        self._observability.info(
            "controller_reconcile_finished",
            active_runs=len(active_runs),
            scheduled_runs=len(scheduled_runs),
            scheduler_run_limit=self._max_scheduled_runs_per_pass,
            recoverable_units=len(recoverable),
            reconciled_units=reconciled_units,
            admission_enabled=admission_batch is not None,
            admission_active_slots=(
                admission_batch.active_slots if admission_batch is not None else None
            ),
            admission_queued=(
                admission_batch.queued_units if admission_batch is not None else None
            ),
        )

    def cancel_run(self, run_id: UUID) -> None:
        with self._observability.bind(run_id=str(run_id)):
            self._observability.info("controller_cancel_run")
            self._scheduler.cancel_run(run_id)
            for unit in self._repository.list_units(run_id):
                self._reconciler.reconcile_unit(unit.id)

    def run_forever(
        self,
        *,
        poll_interval_seconds: float = 5.0,
        leadership: ControllerLeadership | None = None,
        standby_poll_interval_seconds: float | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if leadership is None:
            while True:
                self.reconcile_once()
                sleep(poll_interval_seconds)

        standby_seconds = (
            poll_interval_seconds
            if standby_poll_interval_seconds is None
            else standby_poll_interval_seconds
        )
        if standby_seconds <= 0:
            raise ValueError("standby_poll_interval_seconds must be positive")

        standby_reported = False
        while True:
            try:
                acquired = leadership.try_acquire()
            except LeadershipUnavailable as error:
                self._observability.metrics.observe_controller_leadership(is_leader=False)
                if not standby_reported:
                    self._observability.warning(
                        "controller_leadership_unavailable",
                        error=str(error),
                    )
                    standby_reported = True
                sleep(standby_seconds)
                continue

            if not acquired:
                self._observability.metrics.observe_controller_leadership(is_leader=False)
                if not standby_reported:
                    self._observability.info("controller_standby")
                    standby_reported = True
                sleep(standby_seconds)
                continue

            standby_reported = False
            self._observability.metrics.observe_controller_leadership(is_leader=True)
            self._observability.info("controller_leadership_acquired")
            try:
                while leadership.is_current():
                    self.reconcile_once()
                    if not leadership.is_current():
                        self._observability.warning("controller_leadership_lost")
                        break
                    sleep(poll_interval_seconds)
            finally:
                leadership.release()
                self._observability.metrics.observe_controller_leadership(is_leader=False)
                self._observability.info("controller_leadership_released")

    def _reconcile_admission(self) -> AdmissionBatch | None:
        if self._admission is None:
            return None
        return self._admission.reconcile()

    def _select_scheduled_runs(
        self,
        active_runs: list[PipelineRunRecord],
    ) -> list[PipelineRunRecord]:
        """Select a bounded rotating window so active runs cannot starve each other."""
        if not active_runs:
            self._schedule_cursor = 0
            return []
        if len(active_runs) <= self._max_scheduled_runs_per_pass:
            self._schedule_cursor = 0
            return active_runs

        start = self._schedule_cursor % len(active_runs)
        count = self._max_scheduled_runs_per_pass
        selected = [
            active_runs[(start + offset) % len(active_runs)]
            for offset in range(count)
        ]
        self._schedule_cursor = (start + count) % len(active_runs)
        return selected


__all__ = ["ControllerRepository", "OrchestrationController"]
