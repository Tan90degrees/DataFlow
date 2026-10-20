"""Opt-in integration test: a real head and worker, no Kubernetes or object storage."""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("DATAFLOW_TEST_RAY") != "1", reason="set DATAFLOW_TEST_RAY=1 for real Ray"
)


def test_multiple_pipelines_share_one_cluster_and_transfer_blocks(tmp_path):
    print("Importing Ray and Arrow", flush=True)
    ray = pytest.importorskip("ray")
    import pyarrow as pa
    import pyarrow.parquet as pq
    from ray.cluster_utils import Cluster

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

    print("Creating test cluster", flush=True)
    cluster = Cluster()
    try:
        # Use Ray's actual node address: ray.init normalizes loopback to the LAN IP.
        print("Starting head", flush=True)
        cluster.add_node(
            num_cpus=0,
            include_dashboard=False,
            object_store_memory=100 * 1024**2,
        )
        print("Starting worker", flush=True)
        cluster.add_node(num_cpus=2, object_store_memory=100 * 1024**2)
        cluster.wait_for_nodes()
        print(f"Connecting drivers to {cluster.address}", flush=True)
        # The runner must connect and disconnect; the pre-existing cluster remains alive.
        for index in range(2):
            print(f"Running pipeline {index}", flush=True)
            left, right = tmp_path / f"left-{index}", tmp_path / f"right-{index}"
            result = run_pipeline(demo.spec(left, right), address=cluster.address)
            assert len(result.output_paths) == 2
            assert not ray.is_initialized()
            for output in (left, right):
                assert sorted(pq.read_table(output).column("value").to_pylist()) == [1, 2, 3, 4]
        # Also exercise an application-owned session.
        print("Reusing application-owned session", flush=True)
        ray.init(address=cluster.address)
        nodes_before = {node["NodeID"] for node in ray.nodes() if node["Alive"]}
        run_pipeline(demo.spec(tmp_path / "borrowed-left", tmp_path / "borrowed-right"))
        assert ray.is_initialized()
        assert {node["NodeID"] for node in ray.nodes() if node["Alive"]} == nodes_before
        assert len(nodes_before) == 2
    finally:
        print("Stopping test cluster", flush=True)
        ray.shutdown()
        cluster.shutdown()
