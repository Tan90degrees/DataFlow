# DataFlow

Ray-native distributed data processing orchestration framework built on Ray, Ray Data, KubeRay, and Kubernetes.

## Engineering direction

DataFlow owns workflow state, DAG compilation, retries, artifacts, and lifecycle. Ray Data owns dataset execution planning; Ray owns distributed task scheduling; KubeRay owns Ray cluster/job lifecycle on Kubernetes.

The execution path is now:

```text
PipelineSpec -> LogicalGraph -> ExecutionGraph -> ExecutionPlan -> Ray Data -> KubeRay RayJob
```

The compiler groups compatible linear data operators into execution islands instead of creating one RayJob per DAG node. Hard boundaries, runtime changes, cluster-profile changes, and data fan-out materialize through an internal Parquet staging path. Transactional artifact semantics are intentionally deferred to the artifact milestone.

## Repository layout

```text
src/dataflow/       Core contracts, DAG compiler, runtime, and KubeRay adapter
examples/           Pipeline and execution-plan examples
tests/              Unit tests
docs/               Architecture decisions
```

## Development

Python 3.11+ is required.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
ruff check .
pytest
```

Ray is an optional runtime dependency for local unit tests. Install the runtime extras to execute real Ray Data plans:

```bash
pip install -e '.[runtime]'
```

Compile a pipeline into physical execution units:

```bash
dataflow-compile examples/basic_pipeline.json --run-id run-001
```

Run a physical plan locally against an existing Ray environment:

```bash
dataflow-runtime examples/basic_plan.json
```

Render the equivalent KubeRay `RayJob` manifest:

```bash
dataflow-render-rayjob examples/basic_plan.json
```

## Runtime image

The RayJob entrypoint imports the `dataflow` package, so production plans must reference an image that contains this repository's runtime package. Build the development image with:

```bash
docker build -t dataflow-runtime:dev .
```

Push that image to a registry reachable by the Kubernetes cluster and set `runtime.image` in the execution plan or pipeline to the pushed immutable tag. Do not point production execution plans at a stock Ray image unless DataFlow is supplied through an explicit Ray runtime environment.

## Current milestone

- [x] Versioned execution-plan contract
- [x] Ray Data runtime driver
- [x] KubeRay RayJob renderer
- [x] Runtime container definition
- [x] PipelineSpec and deterministic LogicalGraph
- [x] Execution-island compiler
- [x] Hard-boundary materialization convention
- [ ] Scheduler and state machine
- [ ] PostgreSQL metadata store
- [ ] Kubernetes reconciler
- [ ] Durable artifacts/checkpoints
