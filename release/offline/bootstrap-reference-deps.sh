#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck disable=SC1091
source "$BUNDLE_ROOT/bundle.env"

usage() {
  cat <<'EOF'
Usage: bootstrap-reference-deps.sh \
  --registry <internal-registry[/prefix]> \
  [--namespace dataflow] \
  [--pull-policy IfNotPresent|Never]

Creates a NON-PRODUCTION reference PostgreSQL + MinIO deployment and the
DataFlow database/S3 Secrets. This is intended for isolated demos and offline
acceptance tests. Production installations should use durable platform-managed
PostgreSQL and S3-compatible storage.
EOF
}

REGISTRY=""
NAMESPACE="dataflow"
PULL_POLICY="IfNotPresent"
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
    --pull-policy)
      PULL_POLICY="${2:-}"
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
if [[ "$PULL_POLICY" != "IfNotPresent" && "$PULL_POLICY" != "Never" ]]; then
  echo "--pull-policy must be IfNotPresent or Never" >&2
  exit 2
fi
REGISTRY="${REGISTRY%/}"

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" create secret generic dataflow-database \
  --from-literal=database-url="postgresql://dataflow:dataflow@postgres:5432/dataflow" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" create secret generic dataflow-s3 \
  --from-literal=AWS_ACCESS_KEY_ID=dataflow \
  --from-literal=AWS_SECRET_ACCESS_KEY=dataflow-secret \
  --dry-run=client -o yaml | kubectl apply -f -

cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: $NAMESPACE
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dataflow-offline-postgres
  template:
    metadata:
      labels:
        app: dataflow-offline-postgres
    spec:
      containers:
        - name: postgres
          image: $REGISTRY/postgres:16-alpine
          imagePullPolicy: $PULL_POLICY
          env:
            - name: POSTGRES_DB
              value: dataflow
            - name: POSTGRES_USER
              value: dataflow
            - name: POSTGRES_PASSWORD
              value: dataflow
          ports:
            - containerPort: 5432
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "dataflow", "-d", "dataflow"]
            initialDelaySeconds: 2
            periodSeconds: 2
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: $NAMESPACE
spec:
  selector:
    app: dataflow-offline-postgres
  ports:
    - port: 5432
      targetPort: 5432
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: $NAMESPACE
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dataflow-offline-minio
  template:
    metadata:
      labels:
        app: dataflow-offline-minio
    spec:
      containers:
        - name: minio
          image: $REGISTRY/minio:RELEASE.2025-09-07T16-13-09Z
          imagePullPolicy: $PULL_POLICY
          args: ["server", "/data", "--console-address", ":9001"]
          env:
            - name: MINIO_ROOT_USER
              value: dataflow
            - name: MINIO_ROOT_PASSWORD
              value: dataflow-secret
          ports:
            - containerPort: 9000
          readinessProbe:
            httpGet:
              path: /minio/health/ready
              port: 9000
            initialDelaySeconds: 2
            periodSeconds: 2
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: $NAMESPACE
spec:
  selector:
    app: dataflow-offline-minio
  ports:
    - name: s3
      port: 9000
      targetPort: 9000
EOF

kubectl -n "$NAMESPACE" rollout status deployment/postgres --timeout=3m
kubectl -n "$NAMESPACE" rollout status deployment/minio --timeout=3m

cat <<EOF
[offline] reference dependencies are ready in namespace $NAMESPACE
[offline] database secret: dataflow-database
[offline] s3 secret: dataflow-s3
[offline] s3 endpoint: http://minio.$NAMESPACE.svc.cluster.local:9000
WARNING: this reference dependency deployment is not production-grade.
EOF
