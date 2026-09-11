# Getting started with DataFlow

English | [简体中文](zh-CN/getting-started.md)

This guide takes a new contributor from a source checkout to a running pipeline. For the shortest
path, use the isolated Kind environment: it runs PostgreSQL, MinIO, KubeRay, the DataFlow API and
controller, and real Ray workloads without changing your default Kubernetes context.

## 1. Choose a workflow

| Goal | Recommended path |
| --- | --- |
| Explore the API and run a real pipeline | [Local Kubernetes environment](#3-start-the-local-kubernetes-environment) |
| Change Python code and run fast checks | [Python development](#2-set-up-python-development) |
| Validate failure recovery end to end | [KubeRay E2E suite](../e2e/kuberay/README.md) |
| Deploy a shared environment | [Production Helm deployment](kubernetes-deployment.md) |

## 2. Set up Python development

Python 3.11 or newer is required.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
ruff check .
pytest
```

PostgreSQL integration tests are enabled when `DATAFLOW_TEST_DATABASE_URL` is set. The local Kind
environment exposes its database on port `15432`, so after it starts you can run:

```bash
export DATAFLOW_TEST_DATABASE_URL=postgresql://dataflow:dataflow@127.0.0.1:15432/dataflow
pytest
```

## 3. Start the local Kubernetes environment

Prerequisites are Docker, `kubectl`, Helm 3, kind v0.33.0, `curl`, and `jq`. Start or rebuild the
environment from the repository root:

```bash
bash scripts/local-dev-up.sh
```

The command creates the dedicated `dataflow-dev` Kind cluster, rebuilds the runtime and control-plane
images from the working tree, and deploys the complete stack. It writes an isolated kubeconfig to
`.dataflow-dev/kubeconfig`; your normal Kubernetes context is not replaced.

Verify the deployment:

```bash
bash scripts/local-dev-status.sh
curl -fsS http://127.0.0.1:18080/readyz
curl -fsS http://127.0.0.1:18080/version | jq
```

Useful endpoints:

| Service | Address | Local credentials |
| --- | --- | --- |
| DataFlow API | `http://127.0.0.1:18080` | authentication disabled |
| OpenAPI UI | `http://127.0.0.1:18080/docs` | none |
| MinIO console | `http://127.0.0.1:19001` | `dataflow` / `dataflow-secret` |
| PostgreSQL | `127.0.0.1:15432` | `dataflow` / `dataflow`, database `dataflow` |

These values are test credentials for the isolated cluster only.

## 4. Submit a pipeline through the API

The local fixture already contains the KubeRay service account, S3 credentials, input Parquet data,
and matching runtime image. Create the ClusterProfile, pipeline, immutable version, and run:

```bash
export DATAFLOW_API_URL=http://127.0.0.1:18080

curl -fsS -X POST "$DATAFLOW_API_URL/v1/cluster-profiles" \
  -H 'content-type: application/json' \
  --data-binary @e2e/kuberay/cluster-profile.json | jq

PIPELINE_ID=$(curl -fsS -X POST "$DATAFLOW_API_URL/v1/pipelines" \
  -H 'content-type: application/json' \
  -d '{"name":"kuberay-e2e","tenant_id":"local"}' | jq -r '.id')

VERSION_ID=$(curl -fsS -X POST \
  "$DATAFLOW_API_URL/v1/pipelines/$PIPELINE_ID/versions" \
  -H 'content-type: application/json' \
  --data-binary @e2e/kuberay/pipeline.json | jq -r '.id')

RUN_ID=$(curl -fsS -X POST "$DATAFLOW_API_URL/v1/pipeline-runs" \
  -H 'content-type: application/json' \
  -d "{\"pipeline_version_id\":\"$VERSION_ID\",\"created_by\":\"local-debug\"}" \
  | jq -r '.id')

echo "$RUN_ID"
```

The example deliberately includes a 90-second operator so there is time to inspect the RayJob and
controller behavior while it is active.

## 5. Observe, cancel, and debug the run

Read the durable diagnostic view:

```bash
curl -fsS "$DATAFLOW_API_URL/v1/pipeline-runs/$RUN_ID/diagnostics" | jq
curl -fsS "$DATAFLOW_API_URL/v1/pipeline-runs/$RUN_ID/events" | jq
curl -fsS "$DATAFLOW_API_URL/v1/pipeline-runs/$RUN_ID/artifacts" | jq
```

Inspect live Kubernetes resources and logs with the isolated kubeconfig:

```bash
export KUBECONFIG="$PWD/.dataflow-dev/kubeconfig"
kubectl get pods,jobs,rayjobs,rayclusters -n dataflow-e2e
kubectl logs -n dataflow-e2e deployment/dataflow-controller -f
kubectl logs -n dataflow-e2e deployment/dataflow-api -f
```

Cancellation is durable first and reaches the external RayJob asynchronously:

```bash
curl -fsS -X POST "$DATAFLOW_API_URL/v1/pipeline-runs/$RUN_ID/cancel" | jq
```

Common checks:

- `readyz` fails: inspect the API log and PostgreSQL Pod; readiness requires PostgreSQL.
- no RayJob appears: inspect controller leadership and admission logs, then read run diagnostics.
- a RayJob cannot start: inspect the RayJob, RayCluster, Pod events, runtime image, service account,
  and selected ClusterProfile resources.
- artifact publication fails: verify MinIO/S3 endpoint and credentials in both controller and Ray
  Pods; computation success and artifact commit are separate states.
- after source changes: rerun `bash scripts/local-dev-up.sh` to rebuild and reload both images. The
  script resets the local fixture namespace and therefore discards its PostgreSQL and MinIO data.

## 6. Use the Python SDK

Install the client extra and point it at the API:

```bash
pip install -e '.[sdk]'
```

```python
from dataflow.callables import identity_batch
from dataflow.sdk import DataFlowClient, Resources, pipeline


@pipeline(
    name="example",
    runtime_image="dataflow-runtime:e2e",
    cluster_profile="e2e-cpu",
    artifact_base_uri="s3://dataflow-e2e/checkpoints",
)
def example(flow, input_path: str, output_path: str):
    data = flow.read_parquet(input_path)
    data.map_batches(identity_batch, resources=Resources(cpu=1)).write_parquet(output_path)


spec = example.spec(
    "s3://dataflow-e2e/input/data.parquet",
    "s3://dataflow-e2e/sdk-output",
)
with DataFlowClient("http://127.0.0.1:18080") as client:
    result = client.submit(spec, created_by="sdk-local")
    print(result.run["id"])
```

Building a specification does not initialize Ray or read data. Production callables must be
top-level symbols in importable modules included in the immutable runtime image. See
`examples/sdk_pipeline.py` for a larger example.

## 7. Shut down or move toward production

Delete only the dedicated local cluster and release its ports:

```bash
bash scripts/local-dev-down.sh
```

For a shared deployment, continue with [Kubernetes deployment and controller HA](kubernetes-deployment.md),
then configure [API authentication](api-auth.md), [observability](observability.md), admission limits,
and [retention/garbage collection](retention-gc.md). Do not reuse the local database, S3 credentials,
runtime image tags, or authentication-disabled configuration in production.
