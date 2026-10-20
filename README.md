# DataFlow

English | [简体中文](README.zh-CN.md) · [Documentation](docs/README.md)

A thin pipeline layer over **one existing, long-lived Ray cluster**. One driver executes the
whole DAG with Ray Data. Operators exchange Dataset blocks through Ray's object store and
node-to-node transport. DataFlow does not write intermediate Parquet files or use S3 to connect
operators.

Ray Data owns streaming execution, fusion and backpressure. Ray owns task/actor placement and
operator resource scheduling. DataFlow owns the declarative DAG, validation and execution entrypoint.
Kubernetes/KubeRay may provision the shared cluster externally; DataFlow does not create clusters.

## Run a pipeline

Python 3.11+; install the runtime on the driver and workers:

```bash
pip install -e '.[runtime]'
```

Connect to a cluster that is already running (on its head, `auto` discovers the existing cluster):

```python
from dataflow.sdk import Resources, pipeline, run_pipeline

@pipeline(name="preprocess")
def preprocess(flow, input_path, output_path):
    dataset = flow.read_parquet(input_path, resources=Resources(cpu=0.25))
    dataset = dataset.map_batches(
        "dataflow.callables.identity_batch",
        resources=Resources(cpu=1, memory_bytes=256 * 1024**2),
        batch_size=128,
    )
    dataset.write_parquet(output_path, resources=Resources(cpu=0.25))

result = run_pipeline(preprocess.spec("/shared/input", "/shared/output"))
print(result.output_paths)
```

Or run a JSON DAG:

```bash
dataflow-run examples/single_cluster.json --address auto
```

There is no required API server, PostgreSQL, MinIO, ClusterProfile, runtime image declaration,
controller or KubeRay operator in this path. The worker environment is installed when deploying
the shared cluster. Set `RAY_ADDRESS`, pass `address=`, or reuse an application-owned `ray.init()`
session. `local` is rejected; missing clusters cause connection failure instead of creating one.

See [single-cluster execution](docs/single-cluster.md) for branching, resources, deployment and
migration. [examples/single_cluster.py](examples/single_cluster.py) is an executable SDK example.
Input and final output paths must be accessible to the workers; a shared filesystem is sufficient.

## Development

```bash
pip install -e '.[dev]'
ruff check .
pytest
# Optional real Ray head + worker test:
pip install -e '.[runtime]'
DATAFLOW_TEST_RAY=1 pytest tests/test_single_cluster_ray.py
```

## Legacy control plane

The earlier PostgreSQL/API/KubeRay implementation remains for migration compatibility. Its
execution-island compiler, per-unit RayJobs, ClusterProfiles and durable artifact publication
are **not used by `run_pipeline` or `dataflow-run`**. Existing deployments keep their explicit
legacy commands; see [legacy documentation](docs/README.md). This is the first migration step,
not removal of those modules or production acceptance of the new path.
