# DataFlow

Ray-native distributed data processing orchestration framework built on Ray, Ray Data, KubeRay, and Kubernetes.

## Engineering direction

DataFlow owns workflow state, DAG compilation, retries, artifacts, and lifecycle. Ray Data owns dataset execution planning; Ray owns distributed task scheduling; KubeRay owns Ray cluster/job lifecycle on Kubernetes.

The first vertical slice is:

```text
ExecutionPlan -> Runtime Driver -> Ray Data -> KubeRay RayJob
```

## Repository layout

```text
src/dataflow/       Core contracts, runtime, and KubeRay adapter
examples/           Executable plan examples
tests/              Unit tests
docs/               Architecture decisions
```

## Development

Python 3.11+ is required.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest
```

Ray is an optional runtime dependency for local unit tests. Install the runtime extras to execute real Ray Data plans:

```bash
pip install -e '.[runtime]'
```

Run a plan locally against an existing Ray environment:

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

Push that image to a registry reachable by the Kubernetes cluster and set `runtime.image` in the execution plan to the pushed immutable tag. Do not point production execution plans at a stock Ray image unless DataFlow is supplied through an explicit Ray runtime environment.

## Current milestone

- [x] Versioned execution-plan contract
- [x] Ray Data runtime driver
- [x] KubeRay RayJob renderer
- [x] Unit-testable operator compiler
- [x] Runtime container definition
- [ ] Pipeline/DAG compiler
- [ ] Scheduler and state machine
- [ ] PostgreSQL metadata store
- [ ] Kubernetes reconciler
- [ ] Durable artifacts/checkpoints
