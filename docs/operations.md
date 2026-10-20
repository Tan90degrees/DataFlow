# Deployment and operations

English | [简体中文](zh-CN/operations.md)

## Shared environment

The hosting platform provisions one Ray cluster, by native Ray commands or its existing cluster
operator. Preinstall matching Ray/DataFlow versions and business modules on driver and workers.
The provided [Dockerfile](../Dockerfile) extends the Ray 2.58.0 Python 3.11 image with DataFlow;
extend it further with your business dependencies and deploy it to the shared cluster.

A driver attached to that cluster uses `auto`. From a reachable peer, use its GCS address, e.g.
`ray-head:6379`, with bidirectional driver/worker connectivity. An address alone does not tunnel
all traffic. On Kubernetes, run the driver within the cluster network or an existing head pod,
with access to shared input/output mounts. There is no DataFlow service, chart or extra database.

One application process should initialize one Ray session and reuse it for concurrent calls:

```python
import ray
from dataflow.sdk import run_pipeline

ray.init(address="auto", namespace="processing")
try:
    # Reuse this session for your application calls; do not pass connection overrides.
    result = run_pipeline(spec)
finally:
    ray.shutdown()
```

The `spec` is constructed by your host application. Without a preinitialized session, each call
owns its connect/disconnect lifecycle. Do not let independent threads race to initialize or shut
down Ray. Separate driver processes can safely share the same externally managed cluster.

## Data and capacity

Only business sources and final sinks require filesystem access. Mount input/output paths at
the same locations on workers. Avoid `local://` paths for distributed data; those explicitly
refer to driver-local files. Intermediate Dataset blocks require no shared filesystem or S3.

Size CPU resources for UDF work, `memory_bytes` for per-worker heap needs, and `concurrency` for
useful parallelism. Requests must fit an individual eligible worker. Ray memory reservations do
not prevent a UDF from exceeding them; use the platform's process/container limits as appropriate.
Fractional GPU requests are logical reservations and do not physically isolate accelerator memory.

Streaming chains usually need less object-store capacity than full materialization. Branches and
control barriers may retain entire datasets. Monitor worker heap/RSS, object-store usage, local
spill disk, queued tasks and upstream/downstream throughput. A slow sink can backpressure an
otherwise fast pipeline. Do not increase concurrency without measuring these limits.

## Failure and diagnosis

Use unique run-specific final output paths. A failure may leave earlier successful sinks or
partially written files, and resubmitting to the same path appends by default. Host-controlled
retries and external database/vector-store side effects must be idempotent. Ray retries are
configured through the Ray environment/operator options; DataFlow has no retry registry.

Catch `PipelineExecutionError`, record `node_id` and log the complete exception cause chain.
Correlate host request IDs with driver logs and Ray job/task IDs. Use Ray's dashboard, task logs
and dataset stats when enabled by the platform. The component does not run its own metrics
server or store execution history. Terminating a driver is not a durable workflow cancellation
protocol; the host must own shutdown and business cleanup.

A missing cluster fails at connection. Missing business modules fail during driver preflight
or worker execution. Unsatisfiable resource hints can leave Ray tasks pending; verify worker
capacity/labels and actor concurrency. Input path errors usually indicate missing shared mounts.

Cluster/driver restart recovery, retention of final business outputs, admission/fairness and
availability guarantees belong to the hosting system and require workload-specific acceptance.


## Bounded concurrent DAG admission

Prefer one PipelineRunner per application process for high concurrency. Defaults are 8 active
DAGs, 32 physically queued DAGs and 8 globally executing node actions. These are scheduling/
admission limits, not duplicate CPU/memory budgets; actual tasks/actors use operator Resources.
The runner has fixed idle-waiting coordinator threads and a shared action pool, not a thread
pool per file. Queued cancellation does not free physical queue capacity until removal.

Use iter_completed for large input iterables. Keeping every submit Future/result forever in
host code can still consume unbounded host memory. Watch snapshot counters alongside Ray worker,
object-store and spill metrics. Increasing DAG concurrency can worsen memory pressure or hold
more model actors; there is no cross-DAG model cache or automatic adaptive admission yet.

Choose .cache() when saving expensive prefix computation outweighs the full dataset's memory
and barrier cost. Default recomputation requires pure/idempotent transforms. Keyed joins perform
shuffle and can retain inputs; tune num_partitions and aggregator Resources to worker capacity.
Schema discovery may sample/execute transforms, even before a final sink starts.

The runner drains already-started work before disconnecting or returning observed batch failure.
This is graceful cleanup, not immediate cancellation or a rollback. Output directories across
concurrent DAGs/sinks must be distinct unless the business sink explicitly coordinates writes.
