"""Optional structured logging, Prometheus metrics, and OpenTelemetry hooks."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Protocol

_LOG_CONTEXT: ContextVar[dict[str, Any]] = ContextVar("dataflow_log_context", default={})


class Metrics(Protocol):
    enabled: bool

    def observe_api_request(
        self,
        *,
        method: str,
        route: str,
        status_code: int,
        duration_seconds: float,
    ) -> None: ...

    def observe_run_queue(self, *, duration_seconds: float) -> None: ...

    def observe_reconciliation(self, *, outcome: str, duration_seconds: float) -> None: ...

    def observe_reconciliation_error(self, *, error_code: str) -> None: ...

    def observe_unit_retry(self, *, error_code: str | None) -> None: ...

    def observe_unit_terminal(self, *, status: str, duration_seconds: float | None) -> None: ...

    def observe_artifact_publication(self, *, outcome: str) -> None: ...

    def render(self) -> tuple[bytes, str] | None: ...


class NoopMetrics:
    enabled = False

    def observe_api_request(self, **_kwargs: Any) -> None:
        return None

    def observe_run_queue(self, **_kwargs: Any) -> None:
        return None

    def observe_reconciliation(self, **_kwargs: Any) -> None:
        return None

    def observe_reconciliation_error(self, **_kwargs: Any) -> None:
        return None

    def observe_unit_retry(self, **_kwargs: Any) -> None:
        return None

    def observe_unit_terminal(self, **_kwargs: Any) -> None:
        return None

    def observe_artifact_publication(self, **_kwargs: Any) -> None:
        return None

    def render(self) -> tuple[bytes, str] | None:
        return None


class PrometheusMetrics:
    enabled = True

    def __init__(self) -> None:
        try:
            from prometheus_client import CollectorRegistry, Counter, Histogram
        except ImportError as error:  # pragma: no cover - exercised without optional extra
            raise RuntimeError(
                "install DataFlow with the 'observability' extra to enable Prometheus"
            ) from error

        self._registry = CollectorRegistry()
        self._api_requests = Counter(
            "dataflow_api_requests_total",
            "HTTP requests handled by the DataFlow control-plane API.",
            ("method", "route", "status"),
            registry=self._registry,
        )
        self._api_duration = Histogram(
            "dataflow_api_request_duration_seconds",
            "Control-plane API request latency.",
            ("method", "route"),
            registry=self._registry,
        )
        self._run_queue = Histogram(
            "dataflow_run_queue_duration_seconds",
            "Time from run queueing until execution starts.",
            registry=self._registry,
        )
        self._reconciliations = Counter(
            "dataflow_reconciliations_total",
            "Execution-unit reconciliation passes.",
            ("outcome",),
            registry=self._registry,
        )
        self._reconcile_duration = Histogram(
            "dataflow_reconciliation_duration_seconds",
            "Execution-unit reconciliation latency.",
            ("outcome",),
            registry=self._registry,
        )
        self._reconcile_errors = Counter(
            "dataflow_reconciliation_errors_total",
            "Reconciliation errors grouped by bounded error code.",
            ("error_code",),
            registry=self._registry,
        )
        self._unit_retries = Counter(
            "dataflow_execution_unit_retries_total",
            "Workflow-level execution-unit retries.",
            ("error_code",),
            registry=self._registry,
        )
        self._unit_duration = Histogram(
            "dataflow_execution_unit_duration_seconds",
            "Execution-unit wall-clock duration for terminal units.",
            ("status",),
            registry=self._registry,
        )
        self._artifact_publications = Counter(
            "dataflow_artifact_publications_total",
            "Durable artifact publication outcomes.",
            ("outcome",),
            registry=self._registry,
        )

    def observe_api_request(
        self,
        *,
        method: str,
        route: str,
        status_code: int,
        duration_seconds: float,
    ) -> None:
        status = str(status_code)
        self._api_requests.labels(method=method, route=route, status=status).inc()
        self._api_duration.labels(method=method, route=route).observe(duration_seconds)

    def observe_run_queue(self, *, duration_seconds: float) -> None:
        self._run_queue.observe(max(duration_seconds, 0.0))

    def observe_reconciliation(self, *, outcome: str, duration_seconds: float) -> None:
        self._reconciliations.labels(outcome=outcome).inc()
        self._reconcile_duration.labels(outcome=outcome).observe(duration_seconds)

    def observe_reconciliation_error(self, *, error_code: str) -> None:
        self._reconcile_errors.labels(error_code=error_code or "UNKNOWN").inc()

    def observe_unit_retry(self, *, error_code: str | None) -> None:
        self._unit_retries.labels(error_code=error_code or "UNKNOWN").inc()

    def observe_unit_terminal(self, *, status: str, duration_seconds: float | None) -> None:
        if duration_seconds is not None:
            self._unit_duration.labels(status=status).observe(max(duration_seconds, 0.0))

    def observe_artifact_publication(self, *, outcome: str) -> None:
        self._artifact_publications.labels(outcome=outcome).inc()

    def render(self) -> tuple[bytes, str]:
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

        return generate_latest(self._registry), CONTENT_TYPE_LATEST


class JsonLogFormatter(logging.Formatter):
    """Serialize DataFlow log events as compact JSON without changing log APIs."""

    def format(self, record: logging.LogRecord) -> str:
        fields = dict(getattr(record, "dataflow", {}) or {})
        payload: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
            **fields,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), default=str)


def configure_json_logging(*, level: int = logging.INFO) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    root = logging.getLogger("dataflow")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    root.propagate = False


@dataclass(slots=True)
class Observability:
    metrics: Metrics
    logger: logging.Logger
    tracer_name: str = "dataflow"

    @classmethod
    def noop(cls) -> Observability:
        return cls(metrics=NoopMetrics(), logger=logging.getLogger("dataflow"))

    @classmethod
    def prometheus(cls) -> Observability:
        return cls(metrics=PrometheusMetrics(), logger=logging.getLogger("dataflow"))

    @classmethod
    def from_env(cls) -> Observability:
        metrics_enabled = os.environ.get("DATAFLOW_METRICS_ENABLED", "true").lower() not in {
            "0",
            "false",
            "no",
        }
        if metrics_enabled:
            try:
                metrics: Metrics = PrometheusMetrics()
            except RuntimeError:
                metrics = NoopMetrics()
        else:
            metrics = NoopMetrics()
        return cls(metrics=metrics, logger=logging.getLogger("dataflow"))

    @contextmanager
    def bind(self, **fields: Any) -> Iterator[None]:
        current = dict(_LOG_CONTEXT.get())
        current.update({key: value for key, value in fields.items() if value is not None})
        token = _LOG_CONTEXT.set(current)
        try:
            yield
        finally:
            _LOG_CONTEXT.reset(token)

    def log(self, level: int, event: str, **fields: Any) -> None:
        payload = dict(_LOG_CONTEXT.get())
        payload.update({key: value for key, value in fields.items() if value is not None})
        self.logger.log(level, event, extra={"dataflow": payload})

    def info(self, event: str, **fields: Any) -> None:
        self.log(logging.INFO, event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self.log(logging.WARNING, event, **fields)

    def error(self, event: str, **fields: Any) -> None:
        self.log(logging.ERROR, event, **fields)

    @contextmanager
    def span(self, name: str, **attributes: Any) -> Iterator[Any]:
        try:
            from opentelemetry import trace
        except ImportError:  # pragma: no cover - optional dependency
            yield None
            return

        tracer = trace.get_tracer(self.tracer_name)
        with tracer.start_as_current_span(name) as span:
            for key, value in attributes.items():
                if value is not None:
                    span.set_attribute(key, _otel_value(value))
            yield span

    @contextmanager
    def timed_reconciliation(self) -> Iterator[dict[str, str]]:
        started = perf_counter()
        result = {"outcome": "unchanged"}
        try:
            yield result
        except Exception:
            result["outcome"] = "error"
            raise
        finally:
            self.metrics.observe_reconciliation(
                outcome=result["outcome"],
                duration_seconds=perf_counter() - started,
            )


def _otel_value(value: Any) -> Any:
    if isinstance(value, (str, bool, int, float)):
        return value
    return str(value)


DEFAULT_OBSERVABILITY = Observability.noop()


__all__ = [
    "DEFAULT_OBSERVABILITY",
    "JsonLogFormatter",
    "Metrics",
    "NoopMetrics",
    "Observability",
    "PrometheusMetrics",
    "configure_json_logging",
]
