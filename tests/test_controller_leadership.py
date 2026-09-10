from __future__ import annotations

import pytest

from dataflow.controller import OrchestrationController
from dataflow.leadership import LeadershipUnavailable


class StopLoop(RuntimeError):
    pass


class FakeLeadership:
    def __init__(
        self,
        *,
        acquisitions: list[bool | Exception],
        current: list[bool] | None = None,
    ) -> None:
        self.acquisitions = acquisitions
        self.current = current or []
        self.release_calls = 0

    def try_acquire(self) -> bool:
        value = self.acquisitions.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    def is_current(self) -> bool:
        if self.current:
            return self.current.pop(0)
        return True

    def release(self) -> None:
        self.release_calls += 1


class CountingController(OrchestrationController):
    def __init__(self) -> None:
        super().__init__(object(), object(), object())  # type: ignore[arg-type]
        self.reconcile_calls = 0

    def reconcile_once(self) -> None:
        self.reconcile_calls += 1


def _stop_sleep(_seconds: float) -> None:
    raise StopLoop


def test_standby_never_reconciles_without_leadership() -> None:
    controller = CountingController()
    leadership = FakeLeadership(acquisitions=[False])

    with pytest.raises(StopLoop):
        controller.run_forever(
            leadership=leadership,
            poll_interval_seconds=1,
            standby_poll_interval_seconds=1,
            sleep=_stop_sleep,
        )

    assert controller.reconcile_calls == 0
    assert leadership.release_calls == 0


def test_leader_reconciles_and_releases_on_shutdown() -> None:
    controller = CountingController()
    leadership = FakeLeadership(acquisitions=[True], current=[True, True])

    with pytest.raises(StopLoop):
        controller.run_forever(
            leadership=leadership,
            poll_interval_seconds=1,
            sleep=_stop_sleep,
        )

    assert controller.reconcile_calls == 1
    assert leadership.release_calls == 1


def test_leadership_loss_fences_next_reconcile_pass() -> None:
    controller = CountingController()
    leadership = FakeLeadership(
        acquisitions=[True, False],
        current=[True, False],
    )

    with pytest.raises(StopLoop):
        controller.run_forever(
            leadership=leadership,
            poll_interval_seconds=1,
            standby_poll_interval_seconds=1,
            sleep=_stop_sleep,
        )

    assert controller.reconcile_calls == 1
    assert leadership.release_calls == 1


def test_leadership_backend_outage_keeps_controller_in_standby() -> None:
    controller = CountingController()
    leadership = FakeLeadership(
        acquisitions=[LeadershipUnavailable("database unavailable")]
    )

    with pytest.raises(StopLoop):
        controller.run_forever(
            leadership=leadership,
            poll_interval_seconds=1,
            standby_poll_interval_seconds=1,
            sleep=_stop_sleep,
        )

    assert controller.reconcile_calls == 0
    assert leadership.release_calls == 0
