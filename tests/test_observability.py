from __future__ import annotations

import logging

from fastapi.testclient import TestClient

from dataflow.api import create_app
from dataflow.observability import NoopMetrics, Observability, PrometheusMetrics


class ReadyService:
    def ready(self) -> bool:
        return True


def test_structured_log_context_correlates_run_unit_attempt_and_job(caplog) -> None:
    logger = logging.getLogger("dataflow.test.observability")
    observability = Observability(metrics=NoopMetrics(), logger=logger)

    with caplog.at_level(logging.INFO, logger=logger.name):
        with observability.bind(
            run_id="run-123",
            unit_id="unit-456",
            attempt_number=2,
        ):
            observability.info(
                "external_job_observed",
                external_job_id="rayjob-789",
                external_job_state="RUNNING",
            )

    record = caplog.records[-1]
    assert record.getMessage() == "external_job_observed"
    assert record.dataflow == {
        "run_id": "run-123",
        "unit_id": "unit-456",
        "attempt_number": 2,
        "external_job_id": "rayjob-789",
        "external_job_state": "RUNNING",
    }


def test_prometheus_metrics_use_low_cardinality_labels() -> None:
    metrics = PrometheusMetrics()
    metrics.observe_api_request(
        method="POST",
        route="/v1/pipeline-runs",
        status_code=201,
        duration_seconds=0.125,
    )
    metrics.observe_run_queue(duration_seconds=0.02)
    metrics.observe_reconciliation(outcome="running", duration_seconds=0.05)
    metrics.observe_reconciliation_error(error_code="API_UNAVAILABLE")
    metrics.observe_unit_retry(error_code="RESOURCE_ERROR")
    metrics.observe_unit_terminal(status="SUCCEEDED", duration_seconds=3.5)
    metrics.observe_artifact_publication(outcome="committed")

    rendered = metrics.render()
    assert rendered is not None
    body = rendered[0].decode("utf-8")
    assert "dataflow_api_requests_total" in body
    assert 'route="/v1/pipeline-runs"' in body
    assert "dataflow_reconciliation_errors_total" in body
    assert "dataflow_execution_unit_retries_total" in body
    assert "dataflow_artifact_publications_total" in body
    assert "run-123" not in body
    assert "unit-456" not in body


def test_api_metrics_endpoint_and_middleware_share_registry() -> None:
    observability = Observability.prometheus()
    client = TestClient(create_app(ReadyService(), observability=observability))

    response = client.get("/healthz")
    assert response.status_code == 200
    metrics = client.get("/metrics")

    assert metrics.status_code == 200
    assert "dataflow_api_requests_total" in metrics.text
    assert 'route="/healthz"' in metrics.text
    assert 'status="200"' in metrics.text


def test_noop_metrics_keep_metrics_endpoint_optional() -> None:
    observability = Observability.noop()
    client = TestClient(create_app(ReadyService(), observability=observability))

    assert client.get("/metrics").status_code == 404


def test_opentelemetry_span_hook_is_safe_without_configured_exporter() -> None:
    observability = Observability.noop()

    with observability.span("dataflow.test", run_id="run-123") as span:
        assert span is not None
