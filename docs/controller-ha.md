# Controller high availability

DataFlow controllers use PostgreSQL advisory-lock leadership. You may run multiple long-lived `dataflow-controller` replicas against the same DataFlow database; exactly one replica owns the configured leadership lock and performs scheduler/reconciler work. Other replicas remain standby.

## Recommended deployment

Run at least two controller replicas for failover. Every replica must use the same:

- `DATAFLOW_DATABASE_URL`
- `DATAFLOW_CONTROLLER_LOCK_NAMESPACE`
- `DATAFLOW_CONTROLLER_LOCK_KEY`
- retry policy and external infrastructure configuration

The default lock identifiers are suitable for one DataFlow controller group per database:

```text
DATAFLOW_CONTROLLER_LOCK_NAMESPACE=441001
DATAFLOW_CONTROLLER_LOCK_KEY=1
```

If multiple intentionally independent DataFlow controller groups share one PostgreSQL database, assign each group a different lock pair. Do not change the lock pair for only a subset of replicas in one HA group, because that creates multiple leaders.

## Polling

`DATAFLOW_CONTROLLER_POLL_SECONDS` controls the elected leader's reconcile cadence and defaults to `2` seconds.

`DATAFLOW_CONTROLLER_STANDBY_POLL_SECONDS` controls how often standby replicas retry leadership acquisition. It defaults to the leader poll interval. Both values must be positive.

A shorter standby interval reduces failover pickup latency at the cost of more leadership connection attempts. Advisory locks are released as soon as PostgreSQL observes the leader session close, so there is no fixed lease-expiration delay.

## Failure behavior

If the leader process exits or its dedicated PostgreSQL leadership session is lost, PostgreSQL releases the advisory lock. A standby can then acquire it and resume reconciliation from durable workflow state.

The leadership session gates entry into each reconciliation pass. A connection failure cannot interrupt an external call already executing at that exact instant, so DataFlow continues to rely on two existing idempotency guarantees for that narrow crash window: expected-state PostgreSQL transitions and deterministic RayJob names per run/unit/attempt.

A temporary PostgreSQL outage during acquisition leaves a controller in standby. It does not fall back to unfenced reconciliation.

## Graceful shutdown

Normal controller shutdown releases the advisory lock explicitly. Abrupt process termination also closes the PostgreSQL session, which releases the lock server-side.

`dataflow-controller --once` intentionally bypasses long-lived leader election for debugging and administrative recovery. Do not run concurrent `--once` commands against a live HA controller group unless you are deliberately performing an operator intervention and understand the reconciliation races involved.

## Observability

Structured logs identify leadership transitions with these events:

```text
controller_standby
controller_leadership_acquired
controller_leadership_lost
controller_leadership_released
controller_leadership_unavailable
```

When Prometheus metrics are enabled, every long-lived controller starts a process-local metrics listener. Configure it with:

```text
DATAFLOW_CONTROLLER_METRICS_HOST=0.0.0.0
DATAFLOW_CONTROLLER_METRICS_LISTEN_PORT=9091
```

The listener serves `/metrics` from the same registry used by controller reconciliation and leadership instrumentation. It includes:

```text
dataflow_controller_leader
```

The elected process reports `1`; standby processes report `0`. Leadership metrics contain no run, unit, attempt, or job identifiers. When `DATAFLOW_METRICS_ENABLED=false`, the controller does not start the metrics listener.
