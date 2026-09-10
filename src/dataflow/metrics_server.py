"""Small process-local HTTP surface for control-plane Prometheus metrics."""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from dataflow.observability import Metrics


class MetricsHttpServer(ThreadingHTTPServer):
    daemon_threads = True


def start_metrics_http_server(
    metrics: Metrics,
    *,
    host: str = "0.0.0.0",
    port: int = 9091,
) -> MetricsHttpServer | None:
    """Serve one Metrics registry without relying on a global Prometheus registry."""
    if metrics.render() is None:
        return None
    if not host.strip():
        raise ValueError("metrics host must not be empty")
    if not 0 <= port <= 65535:
        raise ValueError("metrics port must be between 0 and 65535")

    handler = _handler_for(metrics)
    server = MetricsHttpServer((host, port), handler)
    thread = threading.Thread(
        target=server.serve_forever,
        name="dataflow-metrics-http",
        daemon=True,
    )
    thread.start()
    return server


def _handler_for(metrics: Metrics) -> type[BaseHTTPRequestHandler]:
    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            if self.path != "/metrics":
                self.send_response(404)
                self.end_headers()
                return

            rendered = metrics.render()
            if rendered is None:
                self.send_response(404)
                self.end_headers()
                return

            body, content_type = rendered
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            return None

    return MetricsHandler


__all__ = ["MetricsHttpServer", "start_metrics_http_server"]
