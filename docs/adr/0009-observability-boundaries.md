# ADR 0009: Keep correlation identifiers out of metric labels

## Status

Accepted

## Context

DataFlow spans several independently failing layers: HTTP API, PostgreSQL durable state, scheduler/reconciler, KubeRay RayJobs, Ray execution, and durable artifacts. Operators need to navigate from a PipelineRun to its ExecutionUnits, attempts, external jobs, events, and artifacts without querying raw tables or searching unrelated logs.

At the same time, identifiers such as run UUIDs, unit UUIDs, attempt IDs, RayJob names, and artifact IDs have unbounded cardinality. Adding them as Prometheus labels would make the metrics backend itself a scalability risk.

## Decision

DataFlow exposes one optional `Observability` facade to control-plane components.

### Structured logs

Controller and reconciler log records carry correlation fields in structured metadata:

- `run_id`;
- `unit_id` and `unit_key`;
- `attempt_id` and `attempt_number` when available;
- `external_job_id` when an external job is observed;
- bounded status/error fields.

A JSON formatter is provided but is opt-in so embedding applications can choose their own logging configuration.

### Metrics

Prometheus metrics use low-cardinality labels only. IDs are deliberately excluded. Initial metrics cover:

- API request count and latency by method/route/status;
- run queue duration;
- reconciliation count/latency by outcome;
- reconciliation errors by error code;
- workflow retry count by error code;
- terminal execution-unit duration by status;
- artifact publication outcomes.

Each DataFlow process owns an explicit Prometheus registry rather than mutating the global registry. This keeps tests isolated and avoids duplicate collector registration when multiple app instances are created in one process.

### Tracing

OpenTelemetry is an optional hook. API run creation and execution-unit reconciliation create spans with correlation attributes. The core package does not configure an exporter or sampling policy; deployment owns the SDK/provider/exporter configuration.

### Diagnostic read model

`GET /v1/pipeline-runs/{run_id}/diagnostics` returns a control-plane diagnostic view containing:

- pipeline and immutable version identity;
- run state;
- each ExecutionUnit and all durable attempts;
- latest known external job ID per unit;
- artifacts grouped under their producing unit;
- recent durable orchestration events.

This endpoint is a read model over PostgreSQL. It does not query Kubernetes synchronously, so diagnostic API availability is not coupled to Kubernetes availability.

## Consequences

- High-cardinality IDs remain available in logs, traces, and diagnostics without polluting Prometheus labels.
- Metrics and traces are optional for local unit tests.
- Kubernetes/Ray status remains reconciler-observed state rather than an API request-time dependency.
- Operators can navigate from a run to the external job and artifact state using one API response.
- Exporter configuration, dashboards, and alert policy remain deployment concerns.

## Deferred

- direct log aggregation integration;
- OTLP exporter configuration helpers;
- exemplars linking Prometheus samples to traces;
- live Kubernetes/Ray diagnostic fan-out;
- prebuilt Grafana dashboards beyond documented metric names.
