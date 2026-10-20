# Single-cluster execution

English | [简体中文](zh-CN/single-cluster.md)

## Execution contract

Deploy one long-lived Ray cluster. A driver calls `run_pipeline(spec)` and builds the complete
DAG as Ray Data Dataset transformations in that cluster. Multiple drivers/runs share the same
worker resource pool. There is no per-run or per-operator RayCluster lifecycle.

The synchronous SDK and `dataflow-run` do not access PostgreSQL, Kubernetes, an HTTP control
plane, or an artifact registry. They neither create nor publish intermediate storage paths.
Input and final output IO is explicit in the DAG; all intermediate transfers use Ray blocks.

A linear chain remains lazy until its sink executes, letting Ray Data stream blocks, fuse
compatible operators and apply backpressure. It does not wait for a whole dataset between
ordinary operators. Dataset payloads are not collected into the driver.

When one Dataset feeds multiple consumers, DataFlow calls `materialize()` once and reuses the
returned Dataset in the object store. This prevents the shared prefix from being recomputed
for each sink. It is an execution/memory barrier, not a durable checkpoint. Control edges also
materialize their source before proceeding. Dataset handles are released after their final
consumer has been constructed. Ray retains blocks still referenced by downstream work.

Cached branch data can exceed object-store memory and spill to Ray's configured local disks.
This path does not configure S3 spilling. Account for the shared branch working set when sizing
the cluster. Same-node Arrow/NumPy data may use zero-copy access; cross-node transfer still uses
the network, and UDFs can allocate their own heap memory.

## Connect and deploy

```bash
pip install -e '.[runtime]'
dataflow-run examples/single_cluster.json --address auto
# Explicit address on the same network:
dataflow-run examples/single_cluster.json --address ray-head:6379
```

Install the same DataFlow/Ray version and business modules in the driver and worker environment.
Run the driver inside the shared cluster/network, or use a Ray-supported connection address.
An ordinary head GCS address requires worker-to-driver connectivity as well as the head port.
Operators are importable top-level functions or callable classes. No dynamic dependency or
image installation is performed per pipeline.

Use `auto` on a node attached to the cluster, `RAY_ADDRESS`, or an explicit `address=`. A missing
cluster is an error. `local` (including through `RAY_ADDRESS`) is rejected. If the application
already initialized Ray, omit `address` and `namespace`: the existing session is reused and
left connected. Otherwise DataFlow connects once and disconnects in `finally`; the external
cluster stays running. Within one process Ray connection lifecycle is process-global; use one
application-owned session for concurrent callers.

The filesystem paths in the examples must exist on workers. For a multi-node deployment use a
shared filesystem/mount for input and final output. Ray object transfer needs no such mount.
KubeRay can manage the one shared RayCluster independently; no DataFlow controller is required.

## Resources and behavior

Declare `Resources(cpu=..., gpu=..., memory_bytes=..., accelerator_type=...)` per operator.
Read and write task resources are forwarded through `ray_remote_args`; map/filter resources
are passed directly to Ray Data. Accelerator type uses the existing worker's
`ray.io/accelerator-type` label. `batch_size`, `concurrency`, actor class constructor options and
other Ray Data settings remain in operator config. CPU/memory requests are scheduler hints,
not OS-level enforcement. All requested resources must exist on a worker in the shared cluster.

The initial path supports Parquet sources/sinks, map_batches, filter, data fan-out and control
edges. Each transform/sink has exactly one data input; fan-in is rejected before IO. Independent
sinks run in deterministic topological order in the driver, with distributed execution inside
each sink. Concurrent pipeline drivers still share the same cluster. There is no new global
fairness or admission scheduler in DataFlow.

A result contains pipeline name and final output paths. Failures preserve the Ray exception
as the cause and report the node that triggered execution; because execution is lazy, that can
be the sink whose upstream UDF failed. Ray's configured task/actor retry behavior applies.
There is no durable workflow recovery or exactly-once multi-sink commit in this path. A failed
run can leave a completed earlier sink or partial output. Use unique run output directories or
an idempotent business sink when retrying; Parquet writes use Ray's default append semantics.

## Migrate an old pipeline

1. Remove `runtime_image`, Kubernetes service accounts and per-node runtime overrides. Preinstall
   dependencies on the shared workers instead.
2. Remove named `cluster_profile` selections. Operator resources select capacity inside the one cluster.
3. Remove `artifact_base_uri`, `.checkpoint()` and `.hard_boundary()`. Ordinary edges and branches
   transfer Ray data directly. Legacy durable boundaries fail validation rather than silently
   changing their recovery guarantees.
4. Replace the HTTP client's `submit` with `run_pipeline(spec)`, or use `dataflow-run`.
5. Keep only explicit business input/output paths; no MinIO/S3 service is needed between operators.

The old commands and control-plane modules are retained for existing deployments. They do not
implement this execution contract and are not used by the new entrypoints. Their docs and earlier
ADRs describe the legacy architecture. Full control-plane removal and production load/failure
acceptance are separate follow-up work; this path does not claim those guarantees.

## Verification

```bash
pip install -e '.[dev,runtime]'
ruff check .
pytest
DATAFLOW_TEST_RAY=1 pytest tests/test_single_cluster_ray.py
```

The opt-in integration test provisions a head and a worker strictly as test infrastructure,
then runs multiple pipelines against that same cluster. It verifies final data, connection
ownership and unchanged node identity, without an object-store service or Kubernetes.
