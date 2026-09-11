#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${DATAFLOW_DEV_STATE_DIR:-$ROOT_DIR/.dataflow-dev}"
KUBECONFIG_PATH="${DATAFLOW_DEV_KUBECONFIG:-$STATE_DIR/kubeconfig}"
KIND_BIN="${DATAFLOW_DEV_KIND_BIN:-$STATE_DIR/bin/kind}"
KIND_CONFIG="$ROOT_DIR/scripts/kind-local-dev.yaml"
CLUSTER_NAME="${DATAFLOW_DEV_CLUSTER:-dataflow-dev}"
NAMESPACE="${DATAFLOW_DEV_NAMESPACE:-dataflow-e2e}"
API_PORT="${DATAFLOW_DEV_API_PORT:-18080}"
MINIO_PORT="${DATAFLOW_DEV_MINIO_PORT:-19001}"
POSTGRES_PORT="${DATAFLOW_DEV_POSTGRES_PORT:-15432}"
KIND_NODE_IMAGE="kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5"

log() {
  printf '\n[local-dev] %s\n' "$*"
}

fail() {
  printf '\n[local-dev] ERROR: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

wait_http() {
  local url=$1
  local timeout=${2:-90}
  local deadline=$((SECONDS + timeout))
  until curl -fsS "$url" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || fail "timed out waiting for $url"
    sleep 1
  done
}

require_command docker
require_command kubectl
require_command helm
require_command curl
[[ -x "$KIND_BIN" ]] || fail "kind binary not found at $KIND_BIN; see docs/local-development.md"
mkdir -p "$STATE_DIR"

if ! "$KIND_BIN" get clusters 2>/dev/null | grep -Fxq "$CLUSTER_NAME"; then
  log "create isolated Kind cluster $CLUSTER_NAME"
  "$KIND_BIN" create cluster \
    --name "$CLUSTER_NAME" \
    --image "$KIND_NODE_IMAGE" \
    --config "$KIND_CONFIG" \
    --kubeconfig "$KUBECONFIG_PATH"
elif [[ ! -s "$KUBECONFIG_PATH" ]]; then
  log "export kubeconfig for existing Kind cluster $CLUSTER_NAME"
  "$KIND_BIN" export kubeconfig --name "$CLUSTER_NAME" --kubeconfig "$KUBECONFIG_PATH"
fi

log "install or reconcile KubeRay operator"
helm repo add kuberay https://ray-project.github.io/kuberay-helm/ --force-update
helm repo update kuberay
helm upgrade --install kuberay-operator kuberay/kuberay-operator \
  --kubeconfig "$KUBECONFIG_PATH" \
  --version 1.6.2 \
  --namespace kuberay-system \
  --create-namespace \
  --wait \
  --timeout 5m

BUILD_COMMIT="$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || true)"
if [[ -n "$(git -C "$ROOT_DIR" status --porcelain 2>/dev/null || true)" ]]; then
  BUILD_COMMIT="${BUILD_COMMIT:-unknown}-dirty"
fi
log "build DataFlow runtime and control-plane images"
docker build \
  --build-arg "DATAFLOW_BUILD_COMMIT=$BUILD_COMMIT" \
  -t dataflow-runtime:e2e \
  -f "$ROOT_DIR/Dockerfile" "$ROOT_DIR"
docker build \
  --build-arg "DATAFLOW_BUILD_COMMIT=$BUILD_COMMIT" \
  -t dataflow-control-plane:e2e \
  -f "$ROOT_DIR/Dockerfile.control-plane" "$ROOT_DIR"

log "load images into Kind"
"$KIND_BIN" load docker-image --name "$CLUSTER_NAME" dataflow-runtime:e2e
"$KIND_BIN" load docker-image --name "$CLUSTER_NAME" dataflow-control-plane:e2e

log "reset the dedicated debug namespace and deploy dependencies plus DataFlow"
kubectl --kubeconfig "$KUBECONFIG_PATH" delete namespace "$NAMESPACE" \
  --ignore-not-found --wait=true
kubectl --kubeconfig "$KUBECONFIG_PATH" apply -f "$ROOT_DIR/e2e/kuberay/infrastructure.yaml"
kubectl --kubeconfig "$KUBECONFIG_PATH" apply -f "$ROOT_DIR/e2e/kuberay/local-services.yaml"
kubectl --kubeconfig "$KUBECONFIG_PATH" rollout status -n "$NAMESPACE" \
  deployment/postgres --timeout=180s
kubectl --kubeconfig "$KUBECONFIG_PATH" rollout status -n "$NAMESPACE" \
  deployment/minio --timeout=180s
kubectl --kubeconfig "$KUBECONFIG_PATH" wait -n "$NAMESPACE" \
  --for=condition=complete job/dataflow-migrate --timeout=180s
kubectl --kubeconfig "$KUBECONFIG_PATH" wait -n "$NAMESPACE" \
  --for=condition=complete job/seed-parquet --timeout=180s
kubectl --kubeconfig "$KUBECONFIG_PATH" rollout status -n "$NAMESPACE" \
  deployment/dataflow-api --timeout=180s
kubectl --kubeconfig "$KUBECONFIG_PATH" rollout status -n "$NAMESPACE" \
  deployment/dataflow-controller --timeout=180s

log "verify persistent localhost endpoints"
wait_http "http://127.0.0.1:$API_PORT/readyz"
wait_http "http://127.0.0.1:$MINIO_PORT/"

log "debug environment is ready"
printf 'API:           http://127.0.0.1:%s\n' "$API_PORT"
printf 'API docs:      http://127.0.0.1:%s/docs\n' "$API_PORT"
printf 'MinIO console: http://127.0.0.1:%s  (dataflow / dataflow-secret)\n' "$MINIO_PORT"
printf 'PostgreSQL:    postgresql://dataflow:dataflow@127.0.0.1:%s/dataflow\n' "$POSTGRES_PORT"
printf 'Kubeconfig:    %s\n' "$KUBECONFIG_PATH"
printf 'Status:        bash scripts/local-dev-status.sh\n'
printf 'Stop:          bash scripts/local-dev-down.sh\n'
