from __future__ import annotations

from typing import Any

from dataflow import controller_app


class FakeController:
    def __init__(self) -> None:
        self.reconcile_calls = 0
        self.run_calls: list[dict[str, Any]] = []

    def reconcile_once(self) -> None:
        self.reconcile_calls += 1

    def run_forever(self, **kwargs: Any) -> None:
        self.run_calls.append(kwargs)


def _install_fake_controller(monkeypatch: Any) -> tuple[FakeController, object]:
    controller = FakeController()
    leadership = object()
    monkeypatch.setenv("DATAFLOW_JSON_LOGS", "false")
    monkeypatch.setattr(controller_app, "create_controller_from_env", lambda: controller)
    monkeypatch.setattr(controller_app, "create_leadership_from_env", lambda: leadership)
    return controller, leadership


def test_main_once_reconciles_exactly_once(monkeypatch: Any) -> None:
    controller, _leadership = _install_fake_controller(monkeypatch)

    controller_app.main(["--once"])

    assert controller.reconcile_calls == 1
    assert controller.run_calls == []


def test_main_uses_configured_poll_intervals_and_leadership(monkeypatch: Any) -> None:
    controller, leadership = _install_fake_controller(monkeypatch)
    monkeypatch.setenv("DATAFLOW_CONTROLLER_POLL_SECONDS", "7.5")
    monkeypatch.setenv("DATAFLOW_CONTROLLER_STANDBY_POLL_SECONDS", "3.0")

    controller_app.main([])

    assert controller.reconcile_calls == 0
    assert controller.run_calls == [
        {
            "poll_interval_seconds": 7.5,
            "leadership": leadership,
            "standby_poll_interval_seconds": 3.0,
        }
    ]
