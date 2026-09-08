# ADR 0003: PostgreSQL is the durable orchestration source of truth

## Status

Accepted

## Context

Ray tasks, RayJobs, RayClusters, controller processes, and Kubernetes Pods are all ephemeral from the
workflow engine's perspective. DataFlow must be able to restart its control plane and determine what
was intended, what has already happened, and what still needs reconciliation without depending on a
live Ray object reference or an in-memory scheduler queue.

Retries also require historical attempts to remain inspectable. Overwriting one attempt with the next
would make failure analysis and idempotent reconciliation unreliable.

## Decision

PostgreSQL is the durable source of truth for orchestration metadata. The initial schema persists:

- pipelines and immutable pipeline versions;
- pipeline runs;
- physical execution units;
- append-only execution attempts;
- node-level run projections for UI/observability;
- append-only events for state transitions and lifecycle actions.

Pipeline specs are stored as JSONB together with a canonical SHA-256 hash. Version allocation is
serialized by locking the owning pipeline row before selecting the next version number.

Run, execution-unit, and attempt states use independent state machines. A state transition locks the
aggregate row, validates the transition, updates the aggregate, and appends the corresponding event
in the same database transaction. Callers may provide an expected current state to detect stale
controller decisions.

`pipeline_versions` and `events` are protected by database triggers against UPDATE or DELETE.
Execution attempts always receive a new row and monotonically increasing attempt number; prior
attempts are never overwritten.

Schema changes are shipped as ordered SQL migrations inside the Python package. Migration execution
uses a PostgreSQL advisory transaction lock so multiple control-plane replicas may start safely.

## Consequences

- Scheduler and reconciler code can recover non-terminal execution units after process restart.
- Kubernetes/Ray status can be treated as observed external state and reconciled against PostgreSQL.
- Durable workflow semantics do not depend on Ray `ObjectRef`, driver memory, or Pod lifetime.
- PostgreSQL becomes a required control-plane dependency and is exercised by integration tests in CI.
- Event history is an audit stream, not a mutable projection; derived views should be rebuilt from
  aggregate tables or events instead of rewriting historical events.
