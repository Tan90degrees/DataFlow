# ADR 0012: Durable admission is a separate layer between DAG readiness and reconciliation

English | [简体中文](../zh-CN/adr/0012-durable-admission-and-fairness.md)

- Status: Accepted
- Date: 2026-09-10

## Context

The dependency scheduler currently marks every dependency-satisfied ExecutionUnit `READY`, and
the controller can then hand all recoverable units to the KubeRay reconciler. Under many
simultaneous PipelineRuns this makes control-plane work unbounded and gives DataFlow no durable
place to enforce global or ClusterProfile concurrency policy.

Admission policy must survive controller restarts and remain safe through the narrow overlap
window around HA leader failover. It also needs deterministic forward progress across runs.

## Decision

Keep the existing scheduler responsible only for DAG dependency semantics. Add a separate
PostgreSQL-backed admission controller between `READY` and external reconciliation.

Each logical ExecutionUnit has one current admission record with `WAITING`, `ADMITTED`, or
`RELEASED` state. New admission decisions are serialized with a PostgreSQL transaction advisory
lock and are constrained by an optional global limit, optional ClusterProfile limits, and a
maximum number of new admissions per controller pass.

An admitted logical unit retains its slot across attempts, including `RETRY_WAIT`. This avoids
slot reacquisition races and means one retrying unit cannot be double-counted as multiple active
attempts. Terminal/cancelled work releases the slot. Existing active unit state is sufficient to
reconstruct a missing admission row after controller restart or an upgrade.

Fairness uses a monotonic durable admission sequence. The next candidate comes from the run with
the least-recent admission sequence, with stable creation/id tie breakers; the oldest queued unit
inside that run wins. Capacity-ineligible profiles are skipped rather than causing head-of-line
blocking for other profiles.

## Consequences

- Dependency scheduling remains independent of cluster capacity policy.
- Configured admission decisions cannot exceed global/profile limits, even if two controller
  processes overlap briefly during failover.
- Restarts do not reset fairness order or lose slot ownership.
- Retry backoff intentionally reserves capacity; this trades some utilization for stability and
  simple exactly-once logical slot ownership.
- If limits are lowered below already active work, existing work is recovered rather than killed;
  new admissions stop until usage falls below the configured ceiling.
- A per-pass admission bound limits new external-work fan-out while existing admitted units remain
  observable/reconcilable every pass.
