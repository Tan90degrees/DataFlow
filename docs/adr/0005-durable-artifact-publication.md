# ADR 0005: Durable artifact publication at execution-unit boundaries

English | [简体中文](../zh-CN/adr/0005-durable-artifact-publication.md)

## Status

Accepted.

## Context

DataFlow keeps compatible Ray Data operators inside one execution island so blocks can flow through Ray's in-memory/object-store execution path. When an edge crosses an execution-unit boundary, that transient representation cannot be used as durable workflow state: a Ray `ObjectRef` is tied to the lifetime and ownership semantics of a Ray cluster/job and is not a recovery contract after cluster loss.

A boundary therefore needs a durable dataset reference that survives controller restarts, RayJob retries, and RayCluster replacement. Publication also has to distinguish a successful computation from a successfully published logical output. A failed attempt must never make partially written data visible to downstream units.

## Decision

DataFlow represents cross-unit data with `ArtifactRef` and `ArtifactOutputSpec`. The initial durable format is Parquet on S3-compatible object storage.

Within an execution island, Dataset/block flow remains transient and is not persisted in workflow metadata. Across an execution-unit boundary, the compiler emits:

- a stable committed URI keyed by `(run_id, node_id)`;
- an attempt-specific staging URI template;
- an `ArtifactOutputSpec` on the producing `ExecutionPlan`;
- an `ArtifactRef` and committed read URI on the consuming `ExecutionPlan`.

The runtime receives `DATAFLOW_ATTEMPT_NUMBER` from the attempt-specific KubeRay RayJob. Internal durable write operators resolve their path to the corresponding staging prefix. They never write directly to the stable committed URI.

Publication has two phases:

1. Ray Data writes Parquet files to the attempt-specific staging prefix.
2. After the RayJob reports success, the control-plane `ArtifactManager` publishes those objects to the stable committed prefix and then transitions the PostgreSQL artifact row from `STAGING` to `COMMITTED`.

The S3-compatible publisher copies data objects first and writes `_dataflow_commit.json` last. PostgreSQL remains the logical visibility source of truth; the marker is an object-store completeness signal, not a replacement for durable metadata.

Artifacts are append-only records with state transitions limited to:

```text
STAGING -> COMMITTED
STAGING -> ABORTED
```

Terminal artifact rows are immutable. The database allows at most one `COMMITTED` artifact for a `(pipeline_run_id, node_id)` logical output, and every artifact is bound to a real `(execution_unit_id, attempt_number)` execution attempt.

## Failure and recovery semantics

If computation fails or is cancelled, DataFlow aborts the staging artifact and best-effort deletes its attempt prefix. It never publishes that attempt as committed.

If computation succeeds but object-store publication fails transiently, the attempt and unit move to `UNKNOWN`. Reconciliation observes the already-successful external RayJob and retries only artifact publication. The Ray computation is not resubmitted.

If publication fails permanently, the staging artifact is aborted and the attempt/unit converge to `FAILED`.

If the controller crashes after object publication but before metadata convergence, publication is retried idempotently. If the PostgreSQL artifact is already `COMMITTED`, the manager returns the existing logical output without rewriting it.

## Checkpoints

`ExecutionBoundary.CHECKPOINT` is an explicit data boundary. It forces a new execution island and marks the produced artifact as a checkpoint. The data path is otherwise the same as any durable cross-unit artifact, which means downstream execution can restart from the committed Parquet URI even if the original Ray cluster no longer exists.

## Consequences

- Persisted workflow state never depends on Ray `ObjectRef` identity.
- Cross-unit recovery is storage-backed and independent of Ray cluster lifetime.
- Logical output identity is stable across retries while staging identity remains attempt-specific.
- Scheduler/UI can read format, schema, byte size, row count, content hash, checkpoint flag, and URI from PostgreSQL without scanning the dataset.
- S3 copy-on-commit adds I/O at execution boundaries, so the compiler should continue to fuse operators aggressively and only materialize when an orchestration boundary requires durability.
- The first implementation supports Parquet and S3-compatible storage; additional formats/storage backends can implement the same `ArtifactStorage` contract later.
