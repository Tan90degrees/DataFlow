# Execution architecture

English | [简体中文](zh-CN/single-cluster.md)

## Responsibilities

| Component | Responsibility |
| --- | --- |
| DataFlow SDK | Generate a serializable DAG without initializing Ray |
| DAG validation | Check references, cycles, supported topology, callables and resource conflicts |
| One driver per synchronous call | Build and consume Ray Data transformations in a single connected cluster |
| Ray Data | Streaming, operator fusion, block-level execution and backpressure |
| Ray Core | Task/actor placement, object storage and node-to-node data transport |
| Hosting application/platform | Cluster lifecycle, dependencies, request state and business persistence |

The spec contains `name`, `nodes` and `edges`, plus version/kind markers. It contains no cluster
provisioning or workflow persistence policy. All calls in an application can share the same
Ray session and workers; operator resources determine execution capacity within that cluster.

## Data and execution

Ordinary linear edges connect lazy Datasets. The sink starts execution and Ray Data pipelines
blocks through operators. DataFlow does not collect the dataset into the driver and does not
insert intermediate Parquet writes, artifact URIs or storage-service calls.

A data node with multiple consumers is materialized once. Its returned MaterializedDataset is
reused by each branch, so independent sinks do not recompute their common prefix during a
successful run. This is an object-store cache and an execution barrier, not a durable checkpoint.
Handles are released after their final downstream consumer is constructed; downstream references
still keep necessary blocks alive. Ray may reconstruct/retry work after failure.

A control edge waits for its source to complete before the target is constructed. A transform
used as a control source is materialized; a write source has already completed its sink action.
Control edges transfer no dataset. A transform/sink still needs one separate data input.

The driver processes nodes and independent sinks in deterministic topological order. Execution
inside a sink is distributed. Different pipeline drivers can run concurrently in the same
cluster. DataFlow does not add a global scheduler or guarantee fairness between drivers.

## Memory and guarantees

Ray stores blocks in its distributed object store; cross-node access involves network transfer.
Compatible same-node representations can support zero-copy reads, while UDFs also allocate heap
memory. Streaming backpressure helps bound working sets. Cached branches require their complete
shared dataset and may cause Ray to spill to its configured local disks under memory pressure.
DataFlow does not configure remote spilling or use S3 between operators.

The supported operators are Parquet read/write, map_batches and filter, with data fan-out and
control dependencies. Data fan-in is currently rejected. Validation occurs before connecting
and before business IO; application imports may execute module initialization code.

This component has no durable run registry, restart recovery, cross-sink atomic transaction or
exactly-once workflow guarantee. Earlier sinks and partial files may remain after failure.
Retry behavior within Ray follows Ray's configuration. The host owns resubmission and idempotency.
