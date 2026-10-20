"""Run with: python examples/single_cluster.py /shared/input /shared/output."""

from __future__ import annotations

import argparse

from dataflow.sdk import Resources, pipeline, run_pipeline


@pipeline(name="single-cluster")
def process(flow, input_path: str, output_path: str):
    dataset = flow.read_parquet(input_path, resources=Resources(cpu=0.25))
    dataset = dataset.map_batches(
        "dataflow.callables.identity_batch",
        resources=Resources(cpu=1, memory_bytes=256 * 1024**2),
        batch_size=128,
    )
    dataset.write_parquet(output_path, resources=Resources(cpu=0.25))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_path")
    parser.add_argument("output_path")
    parser.add_argument("--address", default="auto")
    args = parser.parse_args()
    result = run_pipeline(process.spec(args.input_path, args.output_path), address=args.address)
    print(result.output_paths)
