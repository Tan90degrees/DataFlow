# ADR 0007: Python SDK generates specifications, not distributed work

## Status

Accepted.

## Context

DataFlow now exposes a durable HTTP control plane and needs a Python-first authoring experience. A Python SDK could either execute user code while building a graph and persist arbitrary Python objects, or it could produce the same explicit `PipelineSpec` already consumed by the compiler and API.

Persisting closures, lambdas, pickled functions, or process-local objects would make versions difficult to inspect, diff, migrate, secure, and reproduce. It would also introduce a second execution path outside the control plane.

## Decision

The SDK is a deterministic `PipelineSpec` generator plus an HTTP API client.

- `@pipeline` creates a reusable pipeline definition.
- Every `spec()` call creates a fresh `PipelineBuilder` and invokes the user's graph-building function locally.
- Builder operations only append typed nodes and edges; they never initialize Ray or execute datasets.
- User functions and callable classes are persisted only as fully-qualified import paths.
- Lambdas, local/nested functions, `__main__` symbols, and arbitrary callable instances are rejected because they are not reproducibly importable by the current runtime resolver.
- Explicit node IDs are supported; omitted IDs are generated deterministically from operator type and insertion order.
- `checkpoint()` and `hard_boundary()` annotate the next data edge instead of materializing data during authoring.
- Generated specifications use the core `PipelineSpec`, `ResourceSpec`, and `RuntimeSpec` models and the same canonical hashing logic as the control plane.
- `DataFlowClient.submit()` persists a version and creates a run through the public HTTP API. It does not bypass the API to access PostgreSQL, Ray, or Kubernetes directly.

## Consequences

### Positive

- Python-authored and JSON-authored pipelines have identical compiler semantics.
- Pipeline versions remain human-readable and diffable.
- Repeated graph construction with the same inputs produces stable serialization and hashes.
- User code dependencies are explicit: referenced symbols must exist in the immutable runtime image.
- SDK tests can validate compilation without starting Ray.

### Tradeoffs

- Arbitrary notebook closures and lambdas cannot be submitted directly.
- Users must package top-level functions/classes into importable modules for production execution.
- The current runtime resolver supports one top-level symbol after module import, so nested class methods or nested qualified symbols are intentionally rejected.
- The SDK does not yet upload source code or build runtime images.

## Rejected alternatives

### Pickle or cloudpickle the Python DAG

Rejected because it couples durable metadata to Python interpreter state and weakens reproducibility, inspectability, migration, and security.

### Execute Ray Data calls while authoring

Rejected because it confuses control-plane graph construction with execution and creates a second scheduling path.

### SDK writes PostgreSQL directly

Rejected because the HTTP API owns the public lifecycle boundary and future authentication/authorization policy.
