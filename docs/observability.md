# DataFlow observability

English | [简体中文](zh-CN/observability.md)

DataFlow separates high-cardinality correlation from low-cardinality metrics.

## Enable metrics

Install the observability extra alongside the API/control-plane dependencies:

```bash
pip install -e '.[api,control-plane,artifacts,observability]'
```

`dataflow-api` enables the Prometheus registry when the optional Prometheus client is installed and `DATAFLOW_METRICS_ENABLED` is not set to `false`. Scrape:

```text
GET /metrics
```

The core orchestration package uses a no-op metrics backend when observability is not configured, so local tests and embedded usage do not require a telemetry service.

## Metrics

Initial control-plane metrics are:

| Metric | Purpose | Labels |
| --- | --- | --- |
| `dataflow_api_requests_total` | HTTP request count | method, route, status |
| `dataflow_api_request_duration_seconds` | HTTP latency | method, route |
| `dataflow_run_queue_duration_seconds` | queued-to-running latency | none |
| `dataflow_reconciliations_total` | execution-unit reconcile passes | outcome |
| `dataflow_reconciliation_duration_seconds` | reconcile latency | outcome |
| `dataflow_reconciliation_errors_total` | executor/artifact reconcile errors | error_code |
| `dataflow_execution_unit_retries_total` | workflow-level retries | error_code |
| `dataflow_execution_unit_duration_seconds` | terminal unit duration | status |
| `dataflow_artifact_publications_total` | commit/abort/error outcomes | outcome |

Run IDs, unit IDs, attempt IDs, RayJob names, and artifact IDs are intentionally not metric labels.

Useful PromQL starting points:

```promql
sum(rate(dataflow_api_requests_total{status=~"5.."}[5m]))
/
sum(rate(dataflow_api_requests_total[5m]))
```

```promql
histogram_quantile(
  0.95,
  sum by (le, route) (rate(dataflow_api_request_duration_seconds_bucket[5m]))
)
```

```promql
sum by (error_code) (rate(dataflow_reconciliation_errors_total[10m]))
```

```promql
sum by (outcome) (rate(dataflow_artifact_publications_total[10m]))
```

## Structured logs

Set:

```bash
export DATAFLOW_JSON_LOGS=true
```

The built-in JSON formatter preserves correlation fields emitted by the API/controller/reconciler. Depending on the lifecycle point, records include `run_id`, `unit_id`, `unit_key`, `attempt_id`, `attempt_number`, and `external_job_id` plus bounded status/error fields.

Use these IDs for log search; do not convert them into Prometheus labels.

## OpenTelemetry

DataFlow creates OpenTelemetry spans through `opentelemetry-api` but does not install or configure an exporter. Deployment owns provider, sampler, processor, and OTLP exporter configuration. Without a configured SDK/provider, the hooks behave as no-op spans and orchestration semantics are unchanged.

Initial spans include HTTP requests, PipelineRun creation, and ExecutionUnit reconciliation.

## Build identity

Use the public endpoint:

```text
GET /version
```

The response reports the installed package version and, when available, the immutable Git commit
and container image coordinates. Release builds inject `DATAFLOW_BUILD_COMMIT` into both the
control-plane and runtime images. The Helm chart sets `DATAFLOW_BUILD_IMAGE` on API and controller
Pods from the exact rendered image reference, including an operator-supplied tag or digest.

```json
{
  "version": "0.1.0",
  "commit": "0123456789abcdef",
  "image": "ghcr.io/tan90degrees/dataflow-control-plane:0.1.0"
}
```

Source-tree processes that are not installed as a distribution report `0+unknown`; locally built
images without a build argument return `null` for `commit`, and non-Helm processes return `null`
for `image`.

## Run diagnostics

Use:

```text
GET /v1/pipeline-runs/{run_id}/diagnostics
```

or:

```python
client.get_run_diagnostics(run_id)
```

The response correlates pipeline/version/run identity with each ExecutionUnit, all durable attempts, latest known external RayJob ID, artifacts produced by that unit, and recent durable events. It reads PostgreSQL only and does not synchronously call Kubernetes, so a Kubernetes API outage does not make the diagnostic endpoint unavailable.
