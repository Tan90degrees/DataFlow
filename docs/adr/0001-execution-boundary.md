# ADR-0001: Separate orchestration and Ray Data execution

Status: Accepted

## Context

DataFlow needs DAG orchestration, durable workflow state, retries, cancellation, artifacts, and Kubernetes lifecycle management. Ray Data already owns dataset execution planning and Ray Core owns distributed task scheduling.

Mapping every logical DAG node to a standalone RayJob would force unnecessary materialization between data operators and would discard Ray Data's ability to optimize and stream blocks within one execution.

## Decision

DataFlow introduces three distinct representations:

1. `PipelineSpec`: user-facing logical DAG.
2. `ExecutionGraph`: compiler output containing one or more execution units.
3. `ExecutionPlan`: the versioned physical plan for one Ray-native execution unit.

A contiguous set of compatible Ray Data operators is compiled into one execution unit. The runtime executes that plan inside one RayJob. Durable boundaries, runtime incompatibilities, cross-cluster transitions, or explicit checkpoints split execution units.

The initial `v1alpha1` execution plan is intentionally linear and supports `read_parquet`, `map_batches`, `filter`, and `write_parquet`. DAG branching belongs to the compiler layer and will be added after the runtime contract stabilizes.

## Consequences

- Workflow state is durable and independent of a Ray cluster lifecycle.
- Ray object references are never persisted as workflow recovery state.
- KubeRay remains an execution adapter rather than the DataFlow domain model.
- Ray Data retains ownership of dataset-level optimization and scheduling.
- The execution-plan schema must be versioned and backward compatible.
