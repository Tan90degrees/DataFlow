"""Execute a JSON pipeline against an existing Ray cluster."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from dataflow.graph import PipelineSpec
from dataflow.single_cluster import run_pipeline


def run_pipeline_cli() -> None:
    parser = argparse.ArgumentParser(description="Execute a pipeline in one existing Ray cluster")
    parser.add_argument("pipeline", help="JSON pipeline specification")
    parser.add_argument("--address", help="Existing Ray address (default: auto / RAY_ADDRESS)")
    parser.add_argument("--namespace", help="Ray namespace for a new connection")
    parser.add_argument("--max-concurrent-nodes", type=int, default=4)
    args = parser.parse_args()
    spec = PipelineSpec.model_validate_json(Path(args.pipeline).read_text(encoding="utf-8"))
    result = run_pipeline(
        spec,
        address=args.address,
        namespace=args.namespace,
        max_concurrent_nodes=args.max_concurrent_nodes,
    )
    print(json.dumps({"pipeline_name": result.pipeline_name, "output_paths": result.output_paths}))


if __name__ == "__main__":
    run_pipeline_cli()
