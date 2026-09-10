# ADR 0010: Fence controller reconciliation with PostgreSQL leadership

## Status

Accepted.

## Context

DataFlow keeps desired orchestration state in PostgreSQL and converges that state against KubeRay from a long-lived controller. The reconciler is deliberately idempotent: state transitions use expected-state checks and RayJob names are deterministic per run, unit, and attempt. Those properties make crash recovery safe, but they do not make active/active controller execution desirable.

Running multiple unfenced controllers would cause every replica to scan the same active runs and recoverable units, race on state transitions, and issue redundant reads or writes to Kubernetes. Even when those races converge correctly, they amplify load and enlarge the operational failure surface.

DataFlow already requires PostgreSQL for every controller deployment. Adding a second consensus or lease dependency only for controller leadership would complicate the control plane and make failover harder to test locally.

## Decision

Long-lived controllers use a dedicated PostgreSQL session advisory lock as an exclusive leadership primitive.

The lock uses the two-integer PostgreSQL advisory-lock namespace. Defaults are `441001` and `1`, configurable through `DATAFLOW_CONTROLLER_LOCK_NAMESPACE` and `DATAFLOW_CONTROLLER_LOCK_KEY` for deployments that intentionally need independent controller groups against the same database.

A controller replica behaves as follows:

1. Open a dedicated autocommit PostgreSQL session used only for leadership.
2. Attempt `pg_try_advisory_lock(namespace, key)`.
3. If the lock is not available, remain standby and do not call the scheduler or reconciler.
4. If the lock is acquired, mark the process as leader and run normal durable reconciliation passes.
5. Verify that the leadership session is still alive before each subsequent pass.
6. On session loss, stop entering new reconciliation passes, release local leadership state, and return to acquisition mode.
7. On graceful shutdown, explicitly unlock and close the leadership session. Process death or connection loss also releases the PostgreSQL advisory lock automatically.

`dataflow-controller --once` is an explicit administrative/debug operation and bypasses long-lived leader election. Operators must not run concurrent `--once` commands against a production controller group as a substitute for the managed controller loop.

## Fencing boundary

The advisory-lock session fences entry into reconciliation passes. It cannot cancel Python code that is already executing at the exact instant a network failure destroys the leadership session. DataFlow therefore keeps two existing safety layers for that narrow crash window:

- durable state transitions use expected-state checks, so stale writers lose races instead of silently overwriting newer state;
- external RayJob identity is deterministic per run/unit/attempt, so repeated submission converges on the same Kubernetes object.

A replica that detects a dead leadership session performs no further reconcile pass until it reacquires the lock.

## Observability

Leadership transitions are emitted as structured log events:

- `controller_standby`
- `controller_leadership_acquired`
- `controller_leadership_lost`
- `controller_leadership_released`
- `controller_leadership_unavailable`

The Prometheus registry also exposes `dataflow_controller_leader` as a process-local gauge with value `1` for the elected replica and `0` otherwise. No controller, run, unit, or attempt identifier is added as a metric label.

## Consequences

- Multiple controller replicas can be deployed for failover while only one performs orchestration work.
- Failover has no lease-expiry delay after PostgreSQL observes the old session close.
- Leadership remains testable with PostgreSQL alone; no live Kubernetes cluster is required.
- PostgreSQL availability is now explicitly required both for workflow state and for controller leadership, matching the existing control-plane dependency model.
- This is single-leader HA, not sharded active/active reconciliation. Per-run partitions or multi-region consensus remain separate future designs.
