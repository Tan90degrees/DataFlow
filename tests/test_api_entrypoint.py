from __future__ import annotations

import pytest

from dataflow.api_entrypoint import api_listen_port


def test_api_listen_port_defaults_to_8080() -> None:
    assert api_listen_port({}) == 8080


def test_api_listen_port_prefers_new_setting() -> None:
    assert api_listen_port(
        {
            "DATAFLOW_API_LISTEN_PORT": "9090",
            "DATAFLOW_API_PORT": "tcp://10.96.148.50:8080",
        }
    ) == 9090


def test_api_listen_port_keeps_legacy_numeric_setting() -> None:
    assert api_listen_port({"DATAFLOW_API_PORT": "8181"}) == 8181


def test_api_listen_port_ignores_kubernetes_service_env() -> None:
    assert api_listen_port({"DATAFLOW_API_PORT": "tcp://10.96.148.50:8080"}) == 8080


@pytest.mark.parametrize("value", ["invalid", "0", "65536"])
def test_api_listen_port_rejects_invalid_explicit_values(value: str) -> None:
    with pytest.raises(RuntimeError, match="DATAFLOW_API_LISTEN_PORT"):
        api_listen_port({"DATAFLOW_API_LISTEN_PORT": value})
