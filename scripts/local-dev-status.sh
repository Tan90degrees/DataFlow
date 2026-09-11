#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${DATAFLOW_DEV_STATE_DIR:-$ROOT_DIR/.dataflow-dev}"
KUBECONFIG_PATH="${DATAFLOW_DEV_KUBECONFIG:-$STATE_DIR/kubeconfig}"
NAMESPACE="${DATAFLOW_DEV_NAMESPACE:-dataflow-e2e}"
API_PORT="${DATAFLOW_DEV_API_PORT:-18080}"

[[ -s "$KUBECONFIG_PATH" ]] || {
  printf 'local debug environment is not configured\n' >&2
  exit 1
}

kubectl --kubeconfig "$KUBECONFIG_PATH" get pods,jobs,rayjobs,rayclusters \
  -n "$NAMESPACE" -o wide
printf '\nAPI health\n'
curl -fsS "http://127.0.0.1:$API_PORT/healthz"
printf '\n\nBuild identity\n'
curl -fsS "http://127.0.0.1:$API_PORT/version"
printf '\n'
