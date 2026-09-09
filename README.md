# DataFlow

Ray-native distributed data processing orchestration framework built on Ray, Ray Data, KubeRay, and Kubernetes.

## Engineering direction

DataFlow owns workflow state, DAG compilation, retries, artifacts, resource policy, and lifecycle. Ray Data owns dataset execution planning; Ray owns distributed task scheduling; KubeRay owns Ray cluster/job lifecycle on Kubernetes.

The execution path is now:

```text
Python SDK / HTTP API
        ↓
PipelineSpec -> LogicalGraph -> ExecutionGraph -> Durable State
             -> ClusterProfile snapshot -> Scheduler/Reconciler
             -> ExecutionPlan -> Ray Data -> KubeRay RayJob
                              -> Durable Artifact -> downstream ExecutionPlan
```

The Python SDK is deliberately a specification generator and API client. Building a pipeline never initializes Ray. Python callables are stored only as reproducibly importable `module.symbol` references, so persisted versions remain JSON, diffable, inspectable, and executable from immutable runtime images.

The compiler groups compatible linear data operators into execution islands instead of creating one RayJob per DAG node. Hard boundaries, explicit checkpoint boundaries, runtime changes, cluster-profile changes, and data fan-out materialize through durable Parquet artifacts.

ClusterProfiles keep infrastructure policy out of PipelineSpecs. Pipelines reference a profile by name; when a PipelineRun is created, DataFlow resolves the current immutable profile revision and embeds its full snapshot in each ExecutionPlan. Later profile edits therefore affect new runs only. CPU/GPU/memory/accelerator requests are checked against worker-group capacity before execution, and the pinned snapshot deterministically drives KubeRay namespace, service account, pod resources, worker groups, placement and autoscaling.

Inside one execution island, Ray Dataset blocks remain transient and are never persisted as workflow state. Across an execution-unit boundary, DataFlow persists an `ArtifactRef` that points to a stable committed URI. Ray `ObjectRef` identity is deliberately not part of the durable orchestration contract.

PostgreSQL is the durable orchestration source of truth. Pipeline versions and ClusterProfile revisions are immutable, execution attempts are append-only, artifact rows are append-only after terminal publication, and run/unit/attempt state transitions append an event in the same transaction. Ray and Kubernetes status are external observed state that the reconciler converges against durable desired state.

The HTTP API is also a durable-state adapter: request handlers create pipeline metadata, immutable versions, ClusterProfile revisions and compiled runs, but they do not submit RayJobs directly. This keeps API availability independent from transient Kubernetes failures. A separately deployed controller/reconciler converges RUNNING and CANCELLED desired state against KubeRay.

The scheduler uses `all_success` dependency semantics: a PENDING execution unit becomes READY only after every upstream unit succeeds. The reconciler submits READY units through an `Executor` interface, tracks attempt-specific external jobs, retries recoverable failures as new attempts with backoff, and propagates cancellation without creating new downstream work. KubeRay RayJob names are deterministic per run, unit, and attempt so repeated reconcile calls and controller restarts converge on the same Kubernetes object.

Durable outputs use two-phase publication. Ray Data writes to an attempt-specific S3 staging prefix. After the RayJob succeeds, the control plane publishes that staging data to the stable `(run_id, node_id)` committed URI, writes an object-store commit marker, and transitions the PostgreSQL artifact row from `STAGING` to `COMMITTED`. Failed or cancelled attempts are marked `ABORTED` and never become downstream-visible outputs. A transient publication failure moves the unit to `UNKNOWN` and retries publication of the already-successful RayJob instead of rerunning the computation.

## Repository layout

```text
src/dataflow/sdk/                       Python authoring DSL and HTTP API client
src/dataflow/api*.py                    HTTP application, service layer, and API repository
src/dataflow/cluster_profiles.py        ClusterProfile resource and placement contracts
src/dataflow/cluster_profile_repository.py  Immutable PostgreSQL profile revisions
src/dataflow/                           Compiler, scheduler, reconciler, runtime, KubeRay adapter
src/dataflow/artifacts.py               Artifact contracts and S3-compatible publication backend
src/dataflow/artifact_*.py              Durable artifact manager and PostgreSQL registry
src/dataflow/metadata/                  PostgreSQL repository and packaged migrations
examples/                               Pipeline, profile, execution-plan, and SDK examples
tests/                                  Unit and PostgreSQL integration tests
docs/                                   Architecture decisions
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

### Python SDK

Install the SDK client dependency:

```bash
pip install -e '.[sdk]'
```

Author a pipeline with the same core `PipelineSpec` used by the API and compiler:

```python
from dataflow.sdk import DataFlowClient, Resources, pipeline


def preprocess(batch):
    return batch


class Predictor:
    def __call__(self, batch):
        return batch


@pipeline(
    name="image-inference",
    runtime_image="registry.example.com/dataflow-runtime:sha-abc123",
    cluster_profile="gpu-medium",
    artifact_base_uri="s3://my-bucket/dataflow",
)
def image_pipeline(flow, input_path: str, output_path: str):
    dataset = flow.read_parquet(input_path)
    dataset = dataset.map_batches(
        preprocess,
        resources=Resources(cpu=2),
        batch_size=128,
    )
    dataset = dataset.checkpoint().map_batches(
        Predictor,
        resources=Resources(cpu=2, gpu=1, accelerator_type="A100"),
    )
    dataset.write_parquet(output_path)


spec = image_pipeline.spec("s3://my-bucket/input", "s3://my-bucket/output")

with DataFlowClient("http://dataflow-api:8080") as client:
    submission = client.submit(spec, parameters={"request_id": "demo"})
    print(submission.run["id"])
```

`@pipeline` executes only the graph-building function locally. It never calls Ray or reads the input dataset. Functions and callable classes referenced by `map_batches`/`filter` must be top-level symbols in importable modules available inside the runtime image. Lambdas, nested/local functions, `__main__` symbols, and arbitrary callable instances are rejected instead of being pickled into pipeline metadata.

See `examples/sdk_pipeline.py` for a complete example.

### HTTP API

Install the API server extra:

```bash
pip install -e '.[api]'
```

Configure PostgreSQL and start the control-plane API:

```bash
export DATAFLOW_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/dataflow
dataflow-api
```

The server listens on `0.0.0.0:8080` by default. Override with `DATAFLOW_API_HOST` and `DATAFLOW_API_PORT`.

Current API surface:

```text
POST /v1/cluster-profiles
GET  /v1/cluster-profiles
GET  /v1/cluster-profiles/{name}
PUT  /v1/cluster-profiles/{name}
POST /v1/pipelines
GET  /v1/pipelines
GET  /v1/pipelines/{pipeline_id}
POST /v1/pipelines/{pipeline_id}/versions
POST /v1/pipeline-runs
GET  /v1/pipeline-runs/{run_id}
POST /v1/pipeline-runs/{run_id}/cancel
GET  /v1/pipeline-runs/{run_id}/events
GET  /v1/pipeline-runs/{run_id}/artifacts
GET  /healthz
GET  /readyz
```

Creating a run persists the compiled execution graph, advances the run through `CREATED -> QUEUED -> PLANNING -> RUNNING`, and evaluates readiness once. The API does not run the long-lived controller loop inside the web process.

### ClusterProfiles

A PipelineSpec references a ClusterProfile by name rather than embedding Kubernetes scheduling policy. The built-in `default` profile is revision `0`; administrators can create a persisted `default` profile or additional named profiles through the API.

A profile revision is immutable. `PUT /v1/cluster-profiles/{name}` appends a new revision. At run creation time, every referenced profile is resolved and the complete revision snapshot is embedded in the persisted `ExecutionPlan`. Retrying or reconciling that run never re-resolves the mutable current profile.

The profile controls:

- Ray version and Kubernetes namespace;
- service account and optional Kubernetes PriorityClass;
- head/worker CPU and memory resources;
- GPU capacity and Kubernetes GPU resource name;
- accelerator labels used by Ray `label_selector` scheduling;
- worker replica/min/max settings and Ray Autoscaler v1/v2 configuration;
- node selectors and tolerations;
- optional Kueue LocalQueue metadata.

See `examples/gpu_cluster_profile.json` for a GPU profile. A pipeline requesting, for example, `Resources(gpu=1, accelerator_type="A100")` is rejected during version creation if none of the selected profile's worker groups can satisfy that request.

Ray is an optional runtime dependency for local unit tests. Install the runtime extras to execute real Ray Data plans:

```bash
pip install -e '.[runtime]'
```

Install the Kubernetes client when running the live KubeRay control plane:

```bash
pip install -e '.[control-plane]'
```

Install the S3-compatible artifact publisher in the control plane with:

```bash
pip install -e '.[artifacts]'
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

## Artifact path convention

For an artifact base URI such as `s3://bucket/dataflow`, DataFlow derives stable logical and attempt-specific paths:

```text
committed:
s3://bucket/dataflow/runs/<run-id>/artifacts/<node-id>/committed

staging:
s3://bucket/dataflow/runs/<run-id>/artifacts/<node-id>/attempts/<NNN>/data
```

Downstream execution units read only the committed URI. The attempt-specific staging URI is resolved by the runtime from `DATAFLOW_ATTEMPT_NUMBER`, which KubeRay injects into each RayJob.

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
- [x] Durable artifacts/checkpoints
- [x] HTTP control-plane API
- [x] Python SDK
- [x] ClusterProfile/resource policy
- [ ] Control-plane observability
- [ ] Real KubeRay end-to-end suite
