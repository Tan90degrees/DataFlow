#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck disable=SC1091
source "$BUNDLE_ROOT/bundle.env"

usage() {
  cat <<'EOF'
Usage: kind-load-images.sh --cluster <kind-name> --registry <synthetic-prefix>

Loads all bundle image archives, retags them to the supplied prefix, and
injects those exact tags into a Kind cluster. This is primarily an offline
acceptance-test helper; no registry server is required because Kubernetes can
run the preloaded images with imagePullPolicy=Never.
EOF
}

CLUSTER=""
REGISTRY=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --cluster)
      CLUSTER="${2:-}"
      shift 2
      ;;
    --registry)
      REGISTRY="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done
if [[ -z "$CLUSTER" || -z "$REGISTRY" ]]; then
  echo "--cluster and --registry are required" >&2
  exit 2
fi
REGISTRY="${REGISTRY%/}"

for command_name in docker kind; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "$command_name is required" >&2
    exit 1
  }
done

load_and_inject() {
  local archive="$1"
  local source="$2"
  local target="$3"
  docker load --input "$BUNDLE_ROOT/$archive" >/dev/null
  docker tag "$source" "$target"
  kind load docker-image --name "$CLUSTER" "$target"
}

load_and_inject images/dataflow-runtime.tar \
  "$DATAFLOW_RUNTIME_SOURCE" \
  "$REGISTRY/dataflow-runtime:$DATAFLOW_VERSION"
load_and_inject images/dataflow-control-plane.tar \
  "$DATAFLOW_CONTROL_PLANE_SOURCE" \
  "$REGISTRY/dataflow-control-plane:$DATAFLOW_VERSION"
load_and_inject images/kuberay-operator.tar \
  "$KUBERAY_OPERATOR_SOURCE" \
  "$REGISTRY/kuberay-operator:v$KUBERAY_VERSION"
load_and_inject images/postgres.tar \
  "$POSTGRES_SOURCE" \
  "$REGISTRY/postgres:16-alpine"
load_and_inject images/minio.tar \
  "$MINIO_SOURCE" \
  "$REGISTRY/minio:RELEASE.2025-09-07T16-13-09Z"

echo "[offline] injected all $DATAFLOW_ARCH images into Kind cluster $CLUSTER"
