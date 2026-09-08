# DataFlow

Ray-native distributed data processing orchestration framework built on Ray, Ray Data, KubeRay, and Kubernetes.

## Engineering direction

DataFlow owns workflow state, DAG compilation, retries, artifacts, and lifecycle. Ray Data owns dataset execution planning; Ray owns distributed task scheduling; KubeRay owns Ray cluster/job lifecycle on Kubernetes.

The execution path is now:

```text
PipelineSpec -> LogicalGraph -> ExecutionGraph -> Durable State -> Scheduler/Reconciler -> ExecutionPlan -> Ray Data -> KubeRay RayJob
```

The compiler groups compatible linear data operators into execution islands instead of creating one RayJob per DAG node. Hard boundaries, runtime changes, cluster-profile changes, and data fan-out materialize through an internal Parquet staging path. Transactional artifact semantics are intentionally deferred to the artifact milestone.

PostgreSQL is the durable orchestration source of truth. Pipeline versions are immutable, execution attempts are append-only, and run/unit/attempt state transitions append an event in the same transaction. Ray and Kubernetes status are external observed state that the reconciler converges against durable desired state.

The scheduler uses `all_success` dependency semantics: a PENDING execution unit becomes READY only after every upstream unit succeeds. The reconciler submits READY units through an `Executor` interface, tracks attempt-specific external jobs, retries recoverable failures as new attempts with backoff, and propagates cancellation without creating new downstream work. KubeRay RayJob names are deterministic per run, unit, and attempt so repeated reconcile calls and controller restarts converge on the same Kubernetes object.

## Repository layout

```text
src/dataflow/                    Core contracts, compiler, scheduler, reconciler, runtime, and KubeRay adapter
src/dataflow/metadata/           PostgreSQL repository and packaged migrations
examples/                        Pipeline and execution-plan examples
tests/                           Unit and PostgreSQL integration tests
docs/                            Architecture decisions
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

PostgreSQL metadata and control-plane integration tests run when `DATAFLOW_TEST_DATABASE_URL` is configured. Apply packaged migrations manually with:

```bash
dataflow-migrate --dsn postgresql://postgres:postgres@localhost:5432/dataflow
```

Ray is an optional runtime dependency for local unit tests. Install the runtime extras to execute real Ray Data plans:

```bash
pip install -e '.[runtime]'
```

Install the Kubernetes client when running the live KubeRay control plane:

```bash
pip install -e '.[control-plane]'
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
- [x] Durable orchestration state machine
- [x] PostgreSQL metadata store and migrations
- [x] Scheduler
- [x] Idempotent Kubernetes/KubeRay reconciler
- [ ] Durable artifacts/checkpoints
