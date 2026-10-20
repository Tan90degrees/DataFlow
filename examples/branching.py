"""Reuse a shared prefix in Ray's object store, with two explicit final sinks."""

from __future__ import annotations

import argparse

from dataflow.sdk import Resources, pipeline, run_pipeline


@pipeline(name="branching")
def branch(flow, source: str, left: str, right: str):
    dataset = flow.read_parquet(source, resources=Resources(cpu=0.25))
    shared = dataset.map_batches(
        "dataflow.callables.identity_batch", resources=Resources(cpu=1), batch_size=128
    )
    shared.write_parquet(left, node_id="left")
    shared.filter("dataflow.callables.keep_all").write_parquet(right, node_id="right")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source")
    parser.add_argument("left")
    parser.add_argument("right")
    parser.add_argument("--address", default="auto")
    args = parser.parse_args()
    print(run_pipeline(branch.spec(args.source, args.left, args.right), address=args.address))
