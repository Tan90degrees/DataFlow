# ADR 0006: HTTP API writes durable desired state

## Status

Accepted.

## Context

DataFlow now has a durable PostgreSQL state machine, a dependency scheduler, an idempotent reconciler, KubeRay execution, and durable artifact publication. The next product surface needs an HTTP API for pipeline/version/run lifecycle operations.

A tempting implementation is to let HTTP handlers submit or cancel RayJobs directly. That would couple API availability and request latency to Kubernetes/KubeRay availability, duplicate reconciler behavior, and create additional crash windows between external side effects and durable workflow state.

## Decision

The HTTP API is a thin control-plane adapter over durable orchestration state.

- Pipeline creation persists metadata only.
- Pipeline version creation validates the `PipelineSpec` and performs a compiler preflight before persistence.
- Pipeline run creation loads an immutable version, compiles an `ExecutionGraph`, persists the graph, advances the run through `CREATED -> QUEUED -> PLANNING -> RUNNING`, then evaluates scheduler readiness once.
- The API does not create RayJobs.
- Cancellation records durable cancellation intent through the existing scheduler state machine. A separately running controller/reconciler observes the cancelled run and stops active external jobs.
- Run reads expose durable units, attempts, events, and artifacts. They never expose Ray `ObjectRef` or process-local dataset identity.
- `/readyz` checks PostgreSQL because PostgreSQL is the API's required source of truth. Kubernetes health is not part of API readiness.

The service layer sits between transport and repositories so a future Python SDK, CLI, or alternate transport can reuse the same use cases without duplicating state transitions.

## Consequences

### Positive

- Kubernetes outages do not prevent pipeline/version metadata operations or durable cancellation requests.
- HTTP retries do not introduce a second RayJob submission path.
- The controller remains the only component that converges external execution state.
- API tests can exercise real PostgreSQL state without requiring a Kubernetes cluster.
- SDK work can target a stable HTTP/use-case boundary rather than internal repositories.

### Tradeoffs

- Cancellation of an already running RayJob is asynchronous with respect to the HTTP response. The run becomes durably `CANCELLED` first; the controller stops the external job on its next reconciliation pass.
- Run creation currently performs compiler validation before persistence and graph persistence immediately after creating the run. Database operational failures can still leave a non-terminal run that must be recovered or diagnosed; a future transactional `create_compiled_run` operation may tighten that boundary if needed.
- Authentication, RBAC, tenant authorization, and rate limiting are intentionally deferred.

## Rejected alternatives

### Submit/cancel KubeRay resources in API handlers

Rejected because it duplicates reconciler responsibility and makes API success depend on external control-plane availability.

### Run the controller loop inside the API process

Rejected for V1. API serving and reconciliation have different scaling/failure characteristics and should remain independently deployable.

### Persist serialized Python DAG objects

Rejected. API versions persist canonical `PipelineSpec` JSON so versions remain inspectable, diffable, and compiler-compatible.
