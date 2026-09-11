#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${DATAFLOW_DEV_STATE_DIR:-$ROOT_DIR/.dataflow-dev}"
KIND_BIN="${DATAFLOW_DEV_KIND_BIN:-$STATE_DIR/bin/kind}"
CLUSTER_NAME="${DATAFLOW_DEV_CLUSTER:-dataflow-dev}"

if [[ -x "$KIND_BIN" ]] && "$KIND_BIN" get clusters 2>/dev/null | grep -Fxq "$CLUSTER_NAME"; then
  "$KIND_BIN" delete cluster --name "$CLUSTER_NAME"
fi

printf 'local debug environment stopped\n'
