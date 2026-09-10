from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from dataflow.controller import OrchestrationController


class _Repository:
    def __init__(self, run_ids: list[UUID]) -> None:
        self._runs = [
            SimpleNamespace(
                id=run_id,
                status=SimpleNamespace(value="RUNNING"),
            )
            for run_id in run_ids
        ]

    def list_active_runs(self):
        return list(self._runs)

    def list_recoverable_units(self):
        return []

    def list_units(self, run_id: UUID):
        return []


class _Scheduler:
    def __init__(self) -> None:
        self.calls: list[UUID] = []

    def reconcile_run(self, run_id: UUID) -> None:
        self.calls.append(run_id)

    def cancel_run(self, run_id: UUID) -> None:
        pass


class _Reconciler:
    def reconcile_unit(self, unit_id: UUID) -> None:
        raise AssertionError("no units should be reconciled in this test")


def test_scheduler_work_is_bounded_and_rotates_across_active_runs() -> None:
    run_ids = [uuid4() for _ in range(5)]
    scheduler = _Scheduler()
    controller = OrchestrationController(
        _Repository(run_ids),
        scheduler,
        _Reconciler(),
        max_scheduled_runs_per_pass=2,
    )

    expected_windows = [
        {run_ids[0], run_ids[1]},
        {run_ids[2], run_ids[3]},
        {run_ids[4], run_ids[0]},
    ]
    for expected in expected_windows:
        scheduler.calls.clear()
        controller.reconcile_once()
        assert len(scheduler.calls) == 4
        assert set(scheduler.calls) == expected


def test_scheduler_run_limit_must_be_positive() -> None:
    with pytest.raises(ValueError, match="max_scheduled_runs_per_pass must be positive"):
        OrchestrationController(
            _Repository([]),
            _Scheduler(),
            _Reconciler(),
            max_scheduled_runs_per_pass=0,
        )
