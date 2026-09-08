"""Command-line entrypoints for the DataFlow runtime."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import yaml

from dataflow.contracts import ExecutionPlan
from dataflow.kuberay import render_rayjob as build_rayjob
from dataflow.runtime import execute_with_ray


def _load_plan(path: str) -> ExecutionPlan:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return ExecutionPlan.model_validate(data)


def run_plan() -> None:
    parser = argparse.ArgumentParser(description="Execute a DataFlow ExecutionPlan with Ray Data")
    parser.add_argument("plan")
    args = parser.parse_args()
    execute_with_ray(_load_plan(args.plan))


def render_rayjob() -> None:
    parser = argparse.ArgumentParser(description="Render a KubeRay RayJob for a DataFlow plan")
    parser.add_argument("plan")
    args = parser.parse_args()
    print(yaml.safe_dump(build_rayjob(_load_plan(args.plan)), sort_keys=False))


def run_inline_plan() -> None:
    raw = os.environ.get("DATAFLOW_EXECUTION_PLAN")
    if not raw:
        raise RuntimeError("DATAFLOW_EXECUTION_PLAN is required")
    execute_with_ray(ExecutionPlan.model_validate_json(raw))


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m dataflow.cli")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("run-inline-plan")
    args = parser.parse_args()

    if args.command == "run-inline-plan":
        run_inline_plan()


if __name__ == "__main__":
    main()
