# ADR 0002: Compile logical DAGs into execution islands

## Status

Accepted

## Context

DataFlow needs a DAG model that is durable and schedulable without replacing Ray Data's internal
execution planner. Mapping every DAG node to one RayJob would force unnecessary materialization and
would discard Ray Data streaming, fusion, backpressure, and actor/task scheduling behavior.

## Decision

DataFlow introduces three representations:

1. `PipelineSpec`: the user-facing static DAG with explicit data and control edges.
2. `LogicalGraph`: a validated deterministic graph used for dependency and topology analysis.
3. `ExecutionGraph`: physical execution islands, each carrying one `ExecutionPlan` for the existing
   Ray Data runtime.

For `v1alpha1`, a data edge is fused when both endpoints are part of a linear data chain, the edge is
not a hard boundary, and runtime image/cluster profile are compatible. Data fan-out creates a durable
staging boundary so each downstream island can read the same upstream result. Data fan-in is retained
in the logical DAG model but rejected by the current physical compiler until multi-input Ray Data
operators such as join are represented in the runtime contract.

A hard data boundary inserts internal Parquet write/read operators using `artifact_base_uri`. This is
an intentionally minimal materialization protocol. Transactional artifact metadata and commit/abort
semantics are a separate concern and will replace the staging convention in the artifact milestone.

Control edges never fuse execution units. They become unit dependencies for the scheduler.

## Consequences

- The scheduler operates on `ExecutionUnit`, not individual Ray Data operators.
- Linear Ray Data pipelines stay inside one RayJob and preserve Ray-native execution behavior.
- Runtime or cluster changes can force a deterministic execution boundary.
- The compiler remains independent of Kubernetes APIs and Ray imports.
- The current staging URI is not yet a durable artifact contract and must not be treated as one by
  external consumers.
