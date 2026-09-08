# ADR 0004: Durable desired state with idempotent external reconciliation

## Status

Accepted

## Context

DataFlow must survive controller restarts, Kubernetes API retries, duplicate reconcile calls, and external RayJob completion without losing workflow history or creating duplicate work. PostgreSQL already stores immutable pipeline versions, execution units, append-only attempts, and state-transition events. KubeRay RayJobs are external objects whose lifecycle can outlive any individual controller process.

A controller that treats in-memory state or the return value of a Kubernetes create call as authoritative has unavoidable crash windows. For example, a process can successfully create a RayJob and crash before persisting that fact. Retrying with a random object name would create duplicate execution.

## Decision

PostgreSQL remains the durable desired-state source of truth. The scheduler and reconciler have separate responsibilities:

- the scheduler evaluates static DAG dependencies and changes PENDING units to READY only when all upstream units have SUCCEEDED;
- the reconciler observes one durable execution unit and converges it against an external job through the `Executor` interface;
- retries create new append-only execution attempts rather than mutating or reusing a completed attempt;
- run cancellation is durable first, then propagated to waiting units and active external jobs.

KubeRay object identity is deterministic from `(run_id, unit_id, attempt_number)`. The attempt suffix is preserved even when the DNS name must be shortened, with a hash used to keep long identities collision-resistant. `submit` is idempotent: it first observes the deterministic object and only creates it when absent, with conflict recovery for concurrent controllers.

The critical submission sequence is:

1. persist unit state as SUBMITTING;
2. append a durable attempt and persist the attempt as SUBMITTING;
3. submit or observe the deterministic external object;
4. converge attempt and unit state from external status.

If the controller crashes at any point, the next controller derives the same external identity from durable run/unit/attempt data and resumes observation. A missing object before an attempt has ever reached RUNNING may be safely resubmitted under the same attempt identity. A missing object after the attempt has reached RUNNING is treated as an external failure and, when retryable, produces a new attempt.

External API availability errors do not automatically consume a workflow retry. They move the durable attempt/unit to UNKNOWN so reconciliation can re-observe the same deterministic object. Terminal external execution failures use an explicit retry policy and backoff; retries are new attempts with new deterministic RayJob names.

## Consequences

- duplicate reconcile calls do not create duplicate RayJobs for the same attempt;
- controller restart does not require local memory to find an existing RayJob;
- fast jobs may transition directly from SUBMITTING to SUCCEEDED because the first observation can already be terminal;
- cancellation can be retried safely because external delete is idempotent and the durable run is already CANCELLED;
- the control loop can be horizontally replicated later, although stronger work-claiming/leader-election policies may still be added for efficiency;
- artifact commit semantics remain separate. A successful RayJob does not by itself define a durable artifact transaction; that is handled by the artifact/checkpoint milestone.
