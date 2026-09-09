# Real KubeRay end-to-end suite

This suite validates DataFlow against a real Kubernetes control plane, KubeRay operator, RayCluster/RayJob lifecycle, Ray Data execution, PostgreSQL state, and S3-compatible durable artifacts.

## Pinned test stack

- kind: `v0.33.0`
- Kubernetes node image: `kindest/node:v1.36.1`
- KubeRay Helm chart/operator: `1.6.2`
- Ray runtime: `2.58.0`
- PostgreSQL: `16-alpine`
- MinIO: `RELEASE.2025-09-07T16-13-09Z`

The GitHub Actions workflow pins the kind binary checksum and the kind node-image digest. These pins are test infrastructure, not DataFlow runtime compatibility promises.

## What the scenario proves

`run.sh` drives the public HTTP API and observes the durable diagnostics read model while a separate controller reconciles real KubeRay resources.

The primary run verifies, in order:

1. ClusterProfile creation and immutable PipelineVersion persistence through the public API.
2. Scheduler/reconciler submission of a real KubeRay RayJob.
3. Controller restart while the first attempt is active without creating a duplicate RayJob.
4. RayJob deletion as failure injection, producing a failed first workflow attempt.
5. The failed attempt cannot expose a committed durable artifact; its artifact record remains `ABORTED`.
6. A second attempt succeeds and publishes a checkpoint through MinIO.
7. The controller is stopped before downstream submission, then the upstream RayJob/RayCluster is removed.
8. After the controller restarts, the downstream execution unit reads the committed checkpoint and the PipelineRun reaches `SUCCEEDED`.
9. The checkpoint contains Parquet data plus the DataFlow commit marker, and the final sink contains Parquet output.
10. A second real run is cancelled while active; cancellation reaches KubeRay and does not publish a committed artifact.
11. The API Prometheus surface remains available during the scenario.

An optional worker-pod restart stress step can be enabled with `DATAFLOW_E2E_WORKER_RESTART=1`. It is intentionally not part of the blocking CI path because Ray may legitimately recover the worker without a workflow-level retry; the blocking suite focuses on deterministic orchestration semantics.

## Local reproduction

Prerequisites:

- Docker
- `kubectl`
- Helm 3
- kind `v0.33.0`
- `curl` and `jq`

Create the cluster and install KubeRay:

```bash
kind create cluster \
  --name dataflow-e2e \
  --image kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5

helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator \
  --version 1.6.2 \
  --namespace kuberay-system \
  --create-namespace \
  --wait
```

Build and load the DataFlow images:

```bash
docker build -t dataflow-runtime:e2e -f Dockerfile .
docker build -t dataflow-control-plane:e2e -f Dockerfile.control-plane .
kind load docker-image --name dataflow-e2e dataflow-runtime:e2e
a kind load docker-image --name dataflow-e2e dataflow-control-plane:e2e
```

Run the suite:

```bash
bash e2e/kuberay/run.sh
```

For the optional worker restart stress path:

```bash
DATAFLOW_E2E_WORKER_RESTART=1 bash e2e/kuberay/run.sh
```

On failure, the script prints pod/job/RayJob/RayCluster state plus DataFlow controller, API, and KubeRay operator logs before exiting non-zero.

## Storage behavior

The Ray pods receive MinIO endpoint configuration from the pinned ClusterProfile and credentials through a Kubernetes Secret reference. DataFlow never stores the credential values in PipelineSpec or ExecutionPlan metadata.

When `DATAFLOW_S3_ENDPOINT_URL` is configured, the runtime builds a PyArrow `S3FileSystem` and converts `s3://bucket/key` into the filesystem-qualified path `bucket/key` before calling Ray Data. The controller separately uses the same S3-compatible endpoint through boto3 for two-phase artifact publication.
