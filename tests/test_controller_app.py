from __future__ import annotations

from typing import Any

from dataflow import controller_app


class FakeController:
    def __init__(self) -> None:
        self.reconcile_calls = 0
        self.poll_intervals: list[float] = []

    def reconcile_once(self) -> None:
        self.reconcile_calls += 1

    def run_forever(self, *, poll_interval_seconds: float) -> None:
        self.poll_intervals.append(poll_interval_seconds)


def _install_fake_controller(monkeypatch: Any) -> FakeController:
    controller = FakeController()
    monkeypatch.setenv("DATAFLOW_JSON_LOGS", "false")
    monkeypatch.setattr(controller_app, "create_controller_from_env", lambda: controller)
    return controller


def test_main_once_reconciles_exactly_once(monkeypatch: Any) -> None:
    controller = _install_fake_controller(monkeypatch)

    controller_app.main(["--once"])

    assert controller.reconcile_calls == 1
    assert controller.poll_intervals == []


def test_main_uses_configured_poll_interval(monkeypatch: Any) -> None:
    controller = _install_fake_controller(monkeypatch)
    monkeypatch.setenv("DATAFLOW_CONTROLLER_POLL_SECONDS", "7.5")

    controller_app.main([])

    assert controller.reconcile_calls == 0
    assert controller.poll_intervals == [7.5]
