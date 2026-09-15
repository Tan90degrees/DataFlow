#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck disable=SC1091
source "$BUNDLE_ROOT/bundle.env"

usage() {
  cat <<'EOF'
Usage: load-images.sh --registry <internal-registry[/prefix]>

Loads every image archive in the bundle into Docker, retags it under the
provided internal registry prefix, and pushes it there. No public network
access is required.
EOF
}

REGISTRY=""
while [[ $# -gt 0 ]]; do
  case "$1" in
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

if [[ -z "$REGISTRY" ]]; then
  echo "--registry is required" >&2
  exit 2
fi
REGISTRY="${REGISTRY%/}"

command -v docker >/dev/null 2>&1 || {
  echo "docker is required" >&2
  exit 1
}

load_and_push() {
  local archive="$1"
  local source="$2"
  local target="$3"
  echo "[offline] loading $archive"
  docker load --input "$BUNDLE_ROOT/$archive" >/dev/null
  docker tag "$source" "$target"
  docker push "$target"
}

load_and_push images/dataflow-runtime.tar \
  "$DATAFLOW_RUNTIME_SOURCE" \
  "$REGISTRY/dataflow-runtime:$DATAFLOW_VERSION"
load_and_push images/dataflow-control-plane.tar \
  "$DATAFLOW_CONTROL_PLANE_SOURCE" \
  "$REGISTRY/dataflow-control-plane:$DATAFLOW_VERSION"
load_and_push images/kuberay-operator.tar \
  "$KUBERAY_OPERATOR_SOURCE" \
  "$REGISTRY/kuberay-operator:v$KUBERAY_VERSION"
load_and_push images/postgres.tar \
  "$POSTGRES_SOURCE" \
  "$REGISTRY/postgres:16-alpine"
load_and_push images/minio.tar \
  "$MINIO_SOURCE" \
  "$REGISTRY/minio:RELEASE.2025-09-07T16-13-09Z"

cat <<EOF
[offline] pushed all bundle images to $REGISTRY
[offline] runtime image for PipelineSpec:
  $REGISTRY/dataflow-runtime:$DATAFLOW_VERSION
EOF
