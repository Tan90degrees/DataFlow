"""Bounded file DAG admission in one shared cluster; each DAG has two output branches."""

from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

from dataflow.sdk import PipelineRunner, Resources, pipeline


@pipeline(name="file-dag")
def file_dag(flow, source: str, output: str, *, cache_prefix: bool):
    dataset = flow.read_parquet(source, resources=Resources(cpu=0.25))
    shared = dataset.map_batches(
        "dataflow.callables.identity_batch", resources=Resources(cpu=1), batch_size=128
    )
    if cache_prefix:
        shared = shared.cache()
    shared.write_parquet(f"{output}/left", node_id="left")
    shared.filter("dataflow.callables.keep_all").write_parquet(f"{output}/right", node_id="right")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-glob", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--address", default="auto")
    parser.add_argument("--max-dags", type=int, default=8)
    parser.add_argument("--max-nodes", type=int, default=8)
    parser.add_argument("--cache-prefix", action="store_true")
    args = parser.parse_args()

    def specs():
        for index, source in enumerate(glob.iglob(args.input_glob)):
            output = Path(args.output_root) / f"{index:06d}-{Path(source).stem}"
            yield file_dag.spec(source, str(output), cache_prefix=args.cache_prefix)

    with PipelineRunner(
        address=args.address,
        max_concurrent_dags=args.max_dags,
        max_queued_dags=args.max_dags * 2,
        max_concurrent_nodes=args.max_nodes,
    ) as runner:
        for index, result in runner.iter_completed(specs()):
            print(
                json.dumps(
                    {
                        "index": index,
                        "outputs": result.output_paths,
                        "duration_seconds": result.duration_seconds,
                    }
                ),
                flush=True,
            )


if __name__ == "__main__":
    main()
