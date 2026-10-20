# Performance experiments

English | [简体中文](zh-CN/performance.md)

## What changed

Ready nodes and independent sinks now execute concurrently, with a fixed global action pool
when using PipelineRunner. DAG admission and input consumption are bounded. Threads wait for
notifications; the component adds no idle polling loop or per-file thread pool in the runner.

The shared-data default changed: fan-out stays lazy and permits recomputation. `.cache()` trades
a whole-dataset barrier/object-store footprint for prefix reuse. This is an explicit policy,
not an adaptive algorithm or streaming broadcast. Union can probe schemas; keyed join uses
Ray's shuffle/materialization path. These costs must be included in workload measurements.

## Reproduce on an existing cluster

Use Linux, matching Ray 2.58.0 on driver/workers, and the repository checkout on the driver.
Preinstall the updated DataFlow package on workers; the benchmark UDF is in dataflow.callables.
Do not run concurrent unrelated loads during a comparison.

```bash
pip install -e '.[runtime]'
python -m benchmarks.dag_throughput --address auto --dags 8 --rows 2048 \
  --batch-size 128 --payload-bytes 512 --delay-seconds 0.01 \
  --output-json test-artifacts/dag-benchmark.json
```

The default temporary work directory is suitable only for a colocated head/worker test. For
physical multi-node runs, pass `--work-dir /shared/unique-benchmark-run` on a shared mount.
Use a new directory each time: final Parquet outputs use append semantics. No intermediate
storage files or S3 are added by DataFlow. The CLI requires an already running cluster.

A separate warmup runs before measurement. Four cases use the same workload and worker pool:

| Case | Active DAG limit | Node-action limit | Shared prefix |
| --- | --- | --- | --- |
| serial-cached | 1 | 1 | Cached |
| parallel-sinks-cached | 1 | 4 | Cached |
| parallel-dags-cached | 4 | 4 | Cached |
| parallel-dags-recompute | 4 | 4 | Recomputed when consumed |

Each input DAG has two output branches. A short sleep in the UDF simulates IO-like work; this is
not an OCR/model/CPU benchmark. Compare sink concurrency separately from multi-DAG concurrency.
Cached/recompute cases expose a sharing tradeoff rather than a universally faster default.

The report records wall time, DAGs/s, input rows/s, per-DAG observed P50/P95, driver RSS snapshots,
observed worker process RSS high-water marks and validated output row counts. Latency starts
when the bounded iterable provides a spec, includes admission wait, and excludes runner/session
construction. Input rows/s does not count the two branch outputs twice.

Memory limits: worker RSS can include prior tasks in reused processes; it is not total cluster
RSS. Driver snapshots are not peak memory. Object-store/spill peaks are not measured here.
Small CI samples cannot establish production P95 or sustained throughput. Measure Ray worker
heap, object-store and spill peaks separately for representative OCR/page/BOX workloads.
The benchmark asserts correctness, never an assumed speedup ratio.

## CI and acceptance

The opt-in head/worker suite runs the same benchmark with four DAGs and a smaller dataset. It
uploads `dag-benchmark.json` as the `dag-benchmark` workflow artifact. Unit tests use explicit
gates/events to prove sink overlap, shared global limits, physical queue backpressure,
cancellation, failure draining and bounded generator consumption without timing-based speed claims.

Production acceptance still needs long-duration mixed workloads, realistic model initialization,
actor reservations, memory/spill pressure, skewed files, worker loss and host-driven recovery.
Limits are local to one runner, not a global resource/fairness policy for every cluster user.
