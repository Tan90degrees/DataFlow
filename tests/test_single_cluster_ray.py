"""Opt-in real Ray tests: one head/worker cluster, no intermediate storage service."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("DATAFLOW_TEST_RAY") != "1", reason="set DATAFLOW_TEST_RAY=1 for real Ray"
)


@pytest.fixture(scope="module")
def cluster():
    import ray
    from ray.cluster_utils import Cluster

    cluster = Cluster()
    try:
        # Driver normalization and GCS binding must use the same actual node address.
        cluster.add_node(num_cpus=0, include_dashboard=False, object_store_memory=256 * 1024**2)
        cluster.add_node(num_cpus=2, object_store_memory=256 * 1024**2)
        cluster.wait_for_nodes()
        print(f"Test cluster ready: {cluster.address}", flush=True)
        yield cluster
    finally:
        ray.shutdown()
        cluster.shutdown()


def test_multiple_pipelines_share_one_cluster_and_transfer_blocks(tmp_path, cluster):
    import pyarrow as pa
    import pyarrow.parquet as pq
    import ray

    from dataflow.sdk import Resources, pipeline, run_pipeline

    source = tmp_path / "input.parquet"
    pq.write_table(pa.table({"value": [1, 2, 3, 4]}), source)

    @pipeline(name="real-single-cluster")
    def demo(flow, left, right):
        dataset = flow.read_parquet(str(source), resources=Resources(cpu=0.25))
        dataset = dataset.map_batches(
            "dataflow.callables.identity_batch", resources=Resources(cpu=0.5), batch_size=2
        )
        dataset.write_parquet(str(left), resources=Resources(cpu=0.25))
        dataset.filter("dataflow.callables.keep_all").write_parquet(str(right))

    for index in range(2):
        left, right = tmp_path / f"left-{index}", tmp_path / f"right-{index}"
        result = run_pipeline(demo.spec(left, right), address=cluster.address)
        assert not result.materialized_nodes
        assert not ray.is_initialized()
        for output in (left, right):
            assert sorted(pq.read_table(output).column("value").to_pylist()) == [1, 2, 3, 4]
    ray.init(address=cluster.address)
    try:
        before = {n["NodeID"] for n in ray.nodes() if n["Alive"]}
        run_pipeline(demo.spec(tmp_path / "borrow-left", tmp_path / "borrow-right"))
        assert ray.is_initialized()
        assert {n["NodeID"] for n in ray.nodes() if n["Alive"]} == before
        assert len(before) == 2
    finally:
        ray.shutdown()


def test_union_and_keyed_join_have_explicit_data_semantics(tmp_path, cluster):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from dataflow.sdk import Resources, pipeline, run_pipeline

    left, right = tmp_path / "left.parquet", tmp_path / "right.parquet"
    pq.write_table(pa.table({"id": [1, 2, 3], "left_value": [10, 20, 30]}), left)
    pq.write_table(pa.table({"id": [1, 3], "right_value": [100, 300]}), right)

    @pipeline(name="join")
    def joined(f):
        left_data = f.read_parquet(str(left), node_id="z-left", resources=Resources(cpu=0.25))
        right_data = f.read_parquet(str(right), node_id="a-right", resources=Resources(cpu=0.25))
        left_data.join(
            right_data,
            on="id",
            num_partitions=1,
            join_type="left_outer",
            resources=Resources(cpu=0.25),
        ).write_parquet(str(tmp_path / "joined"))

    run_pipeline(joined.spec(), address=cluster.address)
    records = sorted(pq.read_table(tmp_path / "joined").to_pylist(), key=lambda row: row["id"])
    assert [(r["id"], r["left_value"], r["right_value"]) for r in records] == [
        (1, 10, 100),
        (2, 20, None),
        (3, 30, 300),
    ]

    @pipeline(name="union")
    def union(f):
        source = f.read_parquet(str(left), resources=Resources(cpu=0.25)).cache()
        left_data = source.map_batches(
            "dataflow.callables.identity_batch", resources=Resources(cpu=0.25)
        )
        right_data = source.filter("dataflow.callables.keep_all", resources=Resources(cpu=0.25))
        left_data.union(right_data).write_parquet(str(tmp_path / "union"))

    result = run_pipeline(union.spec(), address=cluster.address)
    assert len(result.materialized_nodes) == 1
    assert sorted(pq.read_table(tmp_path / "union").column("id").to_pylist()) == [1, 1, 2, 2, 3, 3]


def test_bounded_multi_dag_runner_and_reproducible_benchmark(tmp_path, cluster):
    import ray

    from benchmarks.dag_throughput import benchmark

    ray.init(address=cluster.address)
    try:
        before = {n["NodeID"] for n in ray.nodes() if n["Alive"]}
        report = benchmark(
            tmp_path, dags=4, rows=512, batch_size=64, payload_bytes=128, delay_seconds=0.01
        )
        assert all(c["completed_dags"] == 4 for c in report["cases"])
        assert {n["NodeID"] for n in ray.nodes() if n["Alive"]} == before
        target = Path("test-artifacts/dag-benchmark.json")
        target.parent.mkdir(exist_ok=True)
        target.write_text(json.dumps(report, indent=2) + "\n")
    finally:
        ray.shutdown()
