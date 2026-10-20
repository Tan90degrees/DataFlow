"""Run with an existing Linux Ray cluster: python -m benchmarks.dag_throughput ..."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import statistics
import tempfile
from pathlib import Path
from time import perf_counter

from dataflow.sdk import PipelineRunner, Resources, pipeline


def _driver_rss() -> int | None:
    try:
        pages = int(Path("/proc/self/statm").read_text().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return None


@pipeline(name="dag-benchmark")
def workload(flow, source, left, right, *, cache, batch_size, delay_seconds):
    shared = flow.read_parquet(source, resources=Resources(cpu=0.25), override_num_blocks=2)
    shared = shared.map_batches(
        "dataflow.callables.identity_batch", resources=Resources(cpu=0.25), batch_size=batch_size
    )
    if cache:
        shared = shared.cache()
    for target in (left, right):
        shared.map_batches(
            "dataflow.callables.profiled_batch",
            resources=Resources(cpu=0.25),
            batch_format="numpy",
            batch_size=batch_size,
            fn_kwargs={"delay_seconds": delay_seconds},
        ).write_parquet(target, resources=Resources(cpu=0.25))


def benchmark(
    root: Path,
    *,
    dags: int = 8,
    rows: int = 2048,
    batch_size: int = 128,
    payload_bytes: int = 512,
    delay_seconds: float = 0.01,
) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import ray

    if not ray.is_initialized():
        raise RuntimeError("benchmark requires an initialized existing Ray session")
    if min(dags, rows, batch_size) < 1 or payload_bytes < 0 or delay_seconds < 0:
        raise ValueError("invalid benchmark workload size")
    root.mkdir(parents=True, exist_ok=True)
    source = root / "input.parquet"
    pq.write_table(
        pa.table({"id": list(range(rows)), "payload": ["x" * payload_bytes] * rows}), source
    )
    output = {
        "environment": {
            "python": platform.python_version(),
            "ray": ray.__version__,
            "platform": platform.platform(),
            "resources": ray.cluster_resources(),
        },
        "workload": {
            "dags": dags,
            "input_rows_per_dag": rows,
            "batch_size": batch_size,
            "payload_bytes": payload_bytes,
            "delay_seconds_per_batch": delay_seconds,
        },
        "cases": [],
    }
    # Warm remote imports/workers separately; do not present process startup as throughput.
    with PipelineRunner(max_concurrent_dags=1, max_queued_dags=1, max_concurrent_nodes=1) as runner:
        runner.submit(
            workload.spec(
                str(source),
                str(root / "warm-left"),
                str(root / "warm-right"),
                cache=False,
                batch_size=batch_size,
                delay_seconds=0,
            )
        ).result()
    for name, dag_limit, node_limit, cache in (
        ("serial-cached", 1, 1, True),
        ("parallel-sinks-cached", 1, 4, True),
        ("parallel-dags-cached", 4, 4, True),
        ("parallel-dags-recompute", 4, 4, False),
    ):
        submitted, latency, outputs = {}, [], []

        def specs(case_name=name, use_cache=cache, timestamps=submitted):
            for index in range(dags):
                timestamps[index] = perf_counter()
                yield workload.spec(
                    str(source),
                    str(root / case_name / f"{index}-left"),
                    str(root / case_name / f"{index}-right"),
                    cache=use_cache,
                    batch_size=batch_size,
                    delay_seconds=delay_seconds,
                )

        rss_before = _driver_rss()
        with PipelineRunner(
            max_concurrent_dags=dag_limit,
            max_queued_dags=dag_limit,
            max_concurrent_nodes=node_limit,
        ) as runner:
            started = perf_counter()
            for index, result in runner.iter_completed(specs()):
                latency.append(perf_counter() - submitted[index])
                outputs.extend(result.output_paths.values())
            elapsed = perf_counter() - started
            state = runner.snapshot()
        rss_after = _driver_rss()
        # Validate/read metrics after the timed region, so observation does not throttle sinks.
        count, worker_rss, pids = 0, 0, set()
        for path in outputs:
            table = pq.read_table(path, columns=["_worker_peak_rss_bytes", "_worker_pid"])
            count += table.num_rows
            worker_rss = max(worker_rss, max(table.column("_worker_peak_rss_bytes").to_pylist()))
            pids.update(table.column("_worker_pid").to_pylist())
        assert count == dags * rows * 2
        case = {
            "name": name,
            "max_concurrent_dags": dag_limit,
            "max_concurrent_nodes": node_limit,
            "cache": cache,
            "elapsed_seconds": elapsed,
            "dags_per_second": dags / elapsed,
            "input_rows_per_second": dags * rows / elapsed,
            "latency_p50_seconds": statistics.median(latency),
            "latency_p95_seconds": sorted(latency)[math.ceil(len(latency) * 0.95) - 1],
            "driver_rss_before_bytes": rss_before,
            "driver_rss_after_bytes": rss_after,
            "observed_worker_peak_rss_bytes": worker_rss,
            "worker_processes_observed": len(pids),
            "validated_output_rows": count,
            "completed_dags": state.completed_dags,
        }
        output["cases"].append(case)
        print(json.dumps(case), flush=True)
    output["limitations"] = [
        "Synthetic IO-like workload; no universal throughput guarantee or production acceptance.",
        "Worker RSS is a process high-water mark including prior tasks, not cluster RSS.",
        "Driver RSS snapshots are not peak memory; object-store/spill peaks are not measured.",
        "Latency includes admission wait from source consumption; excludes session setup.",
        "Small samples do not establish production P95; recomputed prefixes require pure UDFs.",
    ]
    return output


def main() -> None:
    import ray

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", default="auto")
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--dags", type=int, default=8)
    parser.add_argument("--rows", type=int, default=2048)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--payload-bytes", type=int, default=512)
    parser.add_argument("--delay-seconds", type=float, default=0.01)
    args = parser.parse_args()
    if not args.address.strip() or args.address.strip().lower() == "local":
        parser.error("select an existing Ray cluster")
    # Unlike the application API, this standalone benchmark owns its process/session.
    ray.init(address=args.address)
    try:
        with tempfile.TemporaryDirectory(prefix="dataflow-benchmark-") as temp:
            result = benchmark(
                args.work_dir or Path(temp),
                dags=args.dags,
                rows=args.rows,
                batch_size=args.batch_size,
                payload_bytes=args.payload_bytes,
                delay_seconds=args.delay_seconds,
            )
            args.output_json.parent.mkdir(parents=True, exist_ok=True)
            args.output_json.write_text(json.dumps(result, indent=2) + "\n")
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
