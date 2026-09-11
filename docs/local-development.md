# Local Kubernetes development environment

English | [简体中文](zh-CN/local-development.md)

The local debug environment runs the complete DataFlow execution path in an isolated Kind cluster:
KubeRay, PostgreSQL, MinIO, the DataFlow API and controller, and RayJob/RayCluster workloads. It
uses a dedicated kubeconfig under `.dataflow-dev/` and does not replace the user's default
Kubernetes context.

## Prerequisites

- Docker with a running daemon
- `kubectl`
- Helm 3
- kind v0.33.0
- `curl`

The repository expects the kind binary at `.dataflow-dev/bin/kind` by default. It can be overridden
with `DATAFLOW_DEV_KIND_BIN=/path/to/kind`.

## Start or rebuild

```bash
bash scripts/local-dev-up.sh
```

The script creates the `dataflow-dev` cluster when needed, installs KubeRay 1.6.2, rebuilds both
DataFlow images from the current working tree, loads them into Kind, and deploys the existing real
KubeRay fixture. It resets only the dedicated `dataflow-e2e` namespace on each run, so PostgreSQL
and MinIO data from the previous local debug deployment is intentionally discarded.

Local endpoints remain available through Kind's localhost-only port mappings; no background
port-forward process is required:

| Component | Endpoint | Credentials |
| --- | --- | --- |
| DataFlow API | `http://127.0.0.1:18080` | authentication disabled in the local fixture |
| OpenAPI UI | `http://127.0.0.1:18080/docs` | none |
| MinIO console | `http://127.0.0.1:19001` | `dataflow` / `dataflow-secret` |
| PostgreSQL | `127.0.0.1:15432` | `dataflow` / `dataflow`, database `dataflow` |

These credentials are intentionally local-only test values and must never be reused outside the
dedicated debug cluster.

## Inspect and debug

```bash
bash scripts/local-dev-status.sh

export KUBECONFIG="$PWD/.dataflow-dev/kubeconfig"
kubectl get pods,jobs,rayjobs,rayclusters -n dataflow-e2e
kubectl logs -n dataflow-e2e deployment/dataflow-controller -f
kubectl logs -n dataflow-e2e deployment/dataflow-api -f
```

The pipeline and ClusterProfile examples under `e2e/kuberay/` can be submitted through the API.
Running `bash e2e/kuberay/run.sh` remains the destructive full lifecycle test; it should not be used
when preserving an interactive debugging session matters.

## Stop and remove

```bash
bash scripts/local-dev-down.sh
```

This deletes only the `dataflow-dev` Kind cluster and releases its localhost ports.
