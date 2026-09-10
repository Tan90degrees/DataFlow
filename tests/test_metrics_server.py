from __future__ import annotations

from urllib.request import urlopen

from dataflow.metrics_server import start_metrics_http_server
from dataflow.observability import NoopMetrics, PrometheusMetrics


def test_metrics_server_exposes_process_local_registry() -> None:
    metrics = PrometheusMetrics()
    metrics.observe_controller_leadership(is_leader=True)
    server = start_metrics_http_server(metrics, host="127.0.0.1", port=0)
    assert server is not None

    try:
        host, port = server.server_address
        with urlopen(f"http://{host}:{port}/metrics", timeout=2) as response:  # noqa: S310
            body = response.read().decode("utf-8")
        assert "dataflow_controller_leader 1.0" in body
    finally:
        server.shutdown()
        server.server_close()


def test_metrics_server_stays_disabled_for_noop_registry() -> None:
    assert start_metrics_http_server(NoopMetrics(), host="127.0.0.1", port=0) is None
