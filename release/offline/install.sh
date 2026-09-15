#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck disable=SC1091
source "$BUNDLE_ROOT/bundle.env"

usage() {
  cat <<'EOF'
Usage: install.sh \
  --registry <internal-registry[/prefix]> \
  --database-secret <secret-name> \
  --s3-secret <secret-name> \
  --s3-endpoint <url> \
  [--namespace dataflow] \
  [--database-key database-url] \
  [--s3-region us-east-1] \
  [--pull-policy IfNotPresent|Never] \
  [--skip-kuberay]

Installs KubeRay and DataFlow exclusively from the charts in this bundle.
Container images are resolved only from the supplied internal registry prefix.
The referenced database and S3 Secrets must already exist in the target
namespace. The script runs DataFlow database migrations before installing the
API/controller Deployments.
EOF
}

REGISTRY=""
NAMESPACE="dataflow"
DATABASE_SECRET=""
DATABASE_KEY="database-url"
S3_SECRET=""
S3_ENDPOINT=""
S3_REGION="us-east-1"
PULL_POLICY="IfNotPresent"
SKIP_KUBERAY="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry)
      REGISTRY="${2:-}"
      shift 2
      ;;
    --namespace)
      NAMESPACE="${2:-}"
      shift 2
      ;;
    --database-secret)
      DATABASE_SECRET="${2:-}"
      shift 2
      ;;
    --database-key)
      DATABASE_KEY="${2:-}"
      shift 2
      ;;
    --s3-secret)
      S3_SECRET="${2:-}"
      shift 2
      ;;
    --s3-endpoint)
      S3_ENDPOINT="${2:-}"
      shift 2
      ;;
    --s3-region)
      S3_REGION="${2:-}"
      shift 2
      ;;
    --pull-policy)
      PULL_POLICY="${2:-}"
      shift 2
      ;;
    --skip-kuberay)
      SKIP_KUBERAY="true"
      shift
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

for required in REGISTRY DATABASE_SECRET S3_SECRET S3_ENDPOINT; do
  if [[ -z "${!required}" ]]; then
    echo "--$(echo "$required" | tr '[:upper:]_' '[:lower:]-') is required" >&2
    exit 2
  fi
done
if [[ "$PULL_POLICY" != "IfNotPresent" && "$PULL_POLICY" != "Never" ]]; then
  echo "--pull-policy must be IfNotPresent or Never" >&2
  exit 2
fi
REGISTRY="${REGISTRY%/}"

for command_name in kubectl helm; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "$command_name is required" >&2
    exit 1
  }
done

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" get secret "$DATABASE_SECRET" >/dev/null
kubectl -n "$NAMESPACE" get secret "$S3_SECRET" >/dev/null

if [[ "$SKIP_KUBERAY" != "true" ]]; then
  helm upgrade --install kuberay-operator "$BUNDLE_ROOT/charts/kuberay-operator.tgz" \
    --namespace kuberay-system \
    --create-namespace \
    --set-string image.repository="$REGISTRY/kuberay-operator" \
    --set-string image.tag="v$KUBERAY_VERSION" \
    --set-string image.pullPolicy="$PULL_POLICY" \
    --wait \
    --timeout 5m
fi

kubectl -n "$NAMESPACE" create serviceaccount dataflow-ray \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NAMESPACE" delete job dataflow-migrate --ignore-not-found >/dev/null
cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: dataflow-migrate
  namespace: $NAMESPACE
spec:
  backoffLimit: 6
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: $REGISTRY/dataflow-control-plane:$DATAFLOW_VERSION
          imagePullPolicy: $PULL_POLICY
          command: ["dataflow-migrate"]
          args: ["--dsn", "\$(DATAFLOW_DATABASE_URL)"]
          env:
            - name: DATAFLOW_DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: $DATABASE_SECRET
                  key: $DATABASE_KEY
EOF
kubectl -n "$NAMESPACE" wait --for=condition=complete job/dataflow-migrate --timeout=5m

helm upgrade --install dataflow "$BUNDLE_ROOT/charts/dataflow.tgz" \
  --namespace "$NAMESPACE" \
  --set-string image.repository="$REGISTRY/dataflow-control-plane" \
  --set-string image.tag="$DATAFLOW_VERSION" \
  --set-string image.pullPolicy="$PULL_POLICY" \
  --set-string database.existingSecret="$DATABASE_SECRET" \
  --set-string database.urlKey="$DATABASE_KEY" \
  --set-string s3.existingSecret="$S3_SECRET" \
  --set-string s3.endpointUrl="$S3_ENDPOINT" \
  --set-string s3.region="$S3_REGION" \
  --wait \
  --timeout 5m

cat <<EOF
[offline] DataFlow $DATAFLOW_VERSION installed in namespace $NAMESPACE
[offline] control-plane image: $REGISTRY/dataflow-control-plane:$DATAFLOW_VERSION
[offline] runtime image for PipelineSpec: $REGISTRY/dataflow-runtime:$DATAFLOW_VERSION
EOF
