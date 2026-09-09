#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_DIR="$ROOT_DIR/e2e/kuberay"
NAMESPACE="${DATAFLOW_E2E_NAMESPACE:-dataflow-e2e}"
API_PORT="${DATAFLOW_E2E_API_PORT:-18080}"
API_URL="http://127.0.0.1:${API_PORT}"
PF_PID=""

log() {
  printf '\n[e2e] %s\n' "$*"
}

fail() {
  printf '\n[e2e] ERROR: %s\n' "$*" >&2
  exit 1
}

cleanup() {
  rc=$?
  if [[ -n "$PF_PID" ]]; then
    kill "$PF_PID" >/dev/null 2>&1 || true
  fi
  if [[ $rc -ne 0 ]]; then
    log "dumping diagnostics after failure"
    kubectl get pods,jobs,rayjobs,rayclusters -A -o wide || true
    kubectl logs -n "$NAMESPACE" deployment/dataflow-controller --tail=250 || true
    kubectl logs -n "$NAMESPACE" job/dataflow-controller-once --tail=250 || true
    kubectl logs -n "$NAMESPACE" deployment/dataflow-api --tail=150 || true
    kubectl logs -n kuberay-system deployment/kuberay-operator --tail=250 || true
    kubectl get rayjobs -n "$NAMESPACE" -o yaml || true
  fi
  exit "$rc"
}
trap cleanup EXIT

wait_http() {
  local url=$1
  local timeout=${2:-120}
  local deadline=$((SECONDS + timeout))
  until curl -fsS "$url" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      fail "timed out waiting for $url"
    fi
    sleep 1
  done
}

run_diagnostics() {
  local run_id=$1
  curl -fsS "$API_URL/v1/pipeline-runs/$run_id/diagnostics"
}

wait_diag() {
  local run_id=$1
  local expression=$2
  local description=$3
  local timeout=${4:-600}
  local deadline=$((SECONDS + timeout))
  local body=""
  while (( SECONDS < deadline )); do
    body="$(run_diagnostics "$run_id" 2>/dev/null || true)"
    if [[ -n "$body" ]] && jq -e "$expression" >/dev/null 2>&1 <<<"$body"; then
      printf '%s\n' "$body"
      return 0
    fi
    sleep 1
  done
  printf '%s\n' "$body" >&2
  fail "timed out waiting for $description"
}

rayjob_count() {
  local run_id=$1
  local unit_id=${2:-}
  local selector="dataflow.io/run-id=$run_id"
  if [[ -n "$unit_id" ]]; then
    selector+=",dataflow.io/unit-id=$unit_id"
  fi
  kubectl get rayjobs -n "$NAMESPACE" -l "$selector" -o json | jq '.items | length'
}

cleanup_run_ray_resources() {
  local run_id=$1
  local selector="dataflow.io/run-id=$run_id"
  local clusters=""
  clusters="$(kubectl get rayjobs -n "$NAMESPACE" -l "$selector" -o json 2>/dev/null \
    | jq -r '.items[].status.rayClusterName // empty' \
    | sort -u)"

  kubectl delete rayjobs -n "$NAMESPACE" -l "$selector" --ignore-not-found --wait=true

  local cluster
  while IFS= read -r cluster; do
    [[ -n "$cluster" ]] || continue
    for _ in $(seq 1 90); do
      if ! kubectl get raycluster -n "$NAMESPACE" "$cluster" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
    kubectl get raycluster -n "$NAMESPACE" "$cluster" >/dev/null 2>&1 \
      && fail "RayCluster $cluster still exists after RayJob cleanup"
  done <<<"$clusters"
}

wait_rayjob_succeeded() {
  local job_name=$1
  local timeout=${2:-600}
  local deadline=$((SECONDS + timeout))
  local body=""
  while (( SECONDS < deadline )); do
    body="$(kubectl get rayjob -n "$NAMESPACE" "$job_name" -o json 2>/dev/null || true)"
    if [[ -n "$body" ]]; then
      local job_status
      local deployment_status
      job_status="$(jq -r '.status.jobStatus // ""' <<<"$body")"
      deployment_status="$(jq -r '.status.jobDeploymentStatus // ""' <<<"$body")"
      if [[ "$job_status" == "SUCCEEDED" && "$deployment_status" == "Complete" ]]; then
        return 0
      fi
      if [[ "$job_status" == "FAILED" || "$deployment_status" == "Failed" ]]; then
        printf '%s\n' "$body" >&2
        fail "RayJob $job_name failed while the durable controller was stopped"
      fi
    fi
    sleep 1
  done
  printf '%s\n' "$body" >&2
  fail "timed out waiting for RayJob $job_name to succeed"
}

run_controller_once() {
  kubectl delete job -n "$NAMESPACE" dataflow-controller-once --ignore-not-found --wait=true
  cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: dataflow-controller-once
  namespace: $NAMESPACE
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: dataflow-controller-once
    spec:
      restartPolicy: Never
      serviceAccountName: dataflow-controller
      containers:
        - name: controller
          image: dataflow-control-plane:e2e
          imagePullPolicy: IfNotPresent
          command: ["dataflow-controller", "--once"]
          envFrom:
            - secretRef:
                name: dataflow-s3
          env:
            - name: DATAFLOW_DATABASE_URL
              value: postgresql://dataflow:dataflow@postgres:5432/dataflow
            - name: DATAFLOW_S3_ENDPOINT_URL
              value: http://minio:9000
            - name: AWS_DEFAULT_REGION
              value: us-east-1
            - name: DATAFLOW_RETRY_MAX_ATTEMPTS
              value: "3"
            - name: DATAFLOW_RETRY_INITIAL_BACKOFF_SECONDS
              value: "0"
            - name: DATAFLOW_METRICS_ENABLED
              value: "false"
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              memory: 512Mi
EOF
  if ! kubectl wait -n "$NAMESPACE" --for=condition=complete job/dataflow-controller-once --timeout=180s; then
    kubectl logs -n "$NAMESPACE" job/dataflow-controller-once --tail=300 || true
    fail "one-shot controller reconciliation failed"
  fi
  kubectl logs -n "$NAMESPACE" job/dataflow-controller-once --tail=100 || true
}

assert_s3_outputs() {
  local run_id=$1
  kubectl exec -i -n "$NAMESPACE" deployment/dataflow-controller -- python - "$run_id" <<'PY'
import os
import sys
import boto3

run_id = sys.argv[1]
client = boto3.client(
    "s3",
    endpoint_url=os.environ["DATAFLOW_S3_ENDPOINT_URL"],
    region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
)

def keys(prefix: str) -> list[str]:
    response = client.list_objects_v2(Bucket="dataflow-e2e", Prefix=prefix)
    return [item["Key"] for item in response.get("Contents", [])]

checkpoint_prefix = f"checkpoints/runs/{run_id}/artifacts/slow/committed/"
checkpoint_keys = keys(checkpoint_prefix)
assert any(key.endswith(".parquet") for key in checkpoint_keys), checkpoint_keys
assert checkpoint_prefix + "_dataflow_commit.json" in checkpoint_keys, checkpoint_keys

final_keys = keys("final-output/")
assert any(key.endswith(".parquet") for key in final_keys), final_keys
print("checkpoint objects:", checkpoint_keys)
print("final objects:", final_keys)
PY
}

log "reset namespace"
kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=true
kubectl apply -f "$E2E_DIR/infrastructure.yaml"

log "wait for PostgreSQL and S3-compatible storage"
kubectl rollout status -n "$NAMESPACE" deployment/postgres --timeout=180s
kubectl rollout status -n "$NAMESPACE" deployment/minio --timeout=180s
kubectl wait -n "$NAMESPACE" --for=condition=complete job/dataflow-migrate --timeout=180s
kubectl wait -n "$NAMESPACE" --for=condition=complete job/seed-parquet --timeout=180s

log "wait for DataFlow API/controller"
kubectl rollout status -n "$NAMESPACE" deployment/dataflow-api --timeout=180s
kubectl rollout status -n "$NAMESPACE" deployment/dataflow-controller --timeout=180s

kubectl port-forward -n "$NAMESPACE" service/dataflow-api "$API_PORT:8080" \
  >/tmp/dataflow-e2e-port-forward.log 2>&1 &
PF_PID=$!
wait_http "$API_URL/readyz" 60

log "create ClusterProfile and immutable PipelineVersion through public API"
curl -fsS -X POST "$API_URL/v1/cluster-profiles" \
  -H 'content-type: application/json' \
  --data-binary "@$E2E_DIR/cluster-profile.json" >/tmp/dataflow-e2e-profile.json

PIPELINE_JSON="$(curl -fsS -X POST "$API_URL/v1/pipelines" \
  -H 'content-type: application/json' \
  -d '{"name":"kuberay-e2e","tenant_id":"e2e"}')"
PIPELINE_ID="$(jq -r '.id' <<<"$PIPELINE_JSON")"

VERSION_JSON="$(curl -fsS -X POST "$API_URL/v1/pipelines/$PIPELINE_ID/versions" \
  -H 'content-type: application/json' \
  --data-binary "@$E2E_DIR/pipeline.json")"
VERSION_ID="$(jq -r '.id' <<<"$VERSION_JSON")"

RUN_JSON="$(curl -fsS -X POST "$API_URL/v1/pipeline-runs" \
  -H 'content-type: application/json' \
  -d "{\"pipeline_version_id\":\"$VERSION_ID\",\"created_by\":\"kuberay-e2e\"}")"
RUN_ID="$(jq -r '.id' <<<"$RUN_JSON")"
log "run $RUN_ID created"

DIAG="$(wait_diag "$RUN_ID" '.units[0].unit.status == "RUNNING"' 'unit-001 RUNNING' 600)"
JOB1="$(jq -r '.units[0].latest_external_job_id' <<<"$DIAG")"
[[ -n "$JOB1" && "$JOB1" != "null" ]] || fail "unit-001 has no external RayJob id"
[[ "$(rayjob_count "$RUN_ID" unit-001)" == "1" ]] || fail "expected exactly one RayJob"

log "restart controller while attempt 1 is active and verify idempotence"
kubectl rollout restart -n "$NAMESPACE" deployment/dataflow-controller
kubectl rollout status -n "$NAMESPACE" deployment/dataflow-controller --timeout=180s
DIAG="$(wait_diag "$RUN_ID" '.units[0].unit.status == "RUNNING" and (.units[0].unit.attempts | length) == 1' 'same attempt after controller restart' 120)"
JOB_AFTER_RESTART="$(jq -r '.units[0].latest_external_job_id' <<<"$DIAG")"
[[ "$JOB_AFTER_RESTART" == "$JOB1" ]] || fail "controller restart changed RayJob identity"
[[ "$(rayjob_count "$RUN_ID" unit-001)" == "1" ]] || fail "controller restart duplicated RayJob"

log "delete active RayJob to inject an external failure and force workflow retry"
kubectl delete rayjob -n "$NAMESPACE" "$JOB1" --wait=true
DIAG="$(wait_diag "$RUN_ID" '(.units[0].unit.attempts | length) >= 2' 'workflow retry attempt 2' 300)"
FIRST_ATTEMPT_STATUS="$(jq -r '.units[0].unit.attempts[0].status' <<<"$DIAG")"
[[ "$FIRST_ATTEMPT_STATUS" == "FAILED" ]] || fail "attempt 1 should be FAILED, got $FIRST_ATTEMPT_STATUS"
if jq -e '.units[0].artifacts | any(.attempt_number == 1 and .state == "COMMITTED")' <<<"$DIAG" >/dev/null; then
  fail "failed attempt exposed a committed artifact"
fi
jq -e '.units[0].artifacts | any(.attempt_number == 1 and .state == "ABORTED")' <<<"$DIAG" >/dev/null \
  || fail "failed attempt should retain an ABORTED artifact record"

DIAG="$(wait_diag "$RUN_ID" '.units[0].unit.status == "RUNNING" and .units[0].unit.current_attempt == 2' 'attempt 2 RUNNING' 300)"
JOB2="$(jq -r '.units[0].latest_external_job_id' <<<"$DIAG")"
[[ "$JOB2" != "$JOB1" ]] || fail "retry must use a new attempt-specific RayJob"

if [[ "${DATAFLOW_E2E_WORKER_RESTART:-0}" == "1" ]]; then
  log "optional stress: delete one Ray worker pod during attempt 2"
  WORKER_POD="$(kubectl get pods -n "$NAMESPACE" \
    -l "dataflow.io/run-id=$RUN_ID,ray.io/node-type=worker" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "$WORKER_POD" ]]; then
    kubectl delete pod -n "$NAMESPACE" "$WORKER_POD" --wait=false
  fi
fi

log "freeze the durable controller while attempt 2 is still active"
kubectl scale -n "$NAMESPACE" deployment/dataflow-controller --replicas=0
kubectl wait -n "$NAMESPACE" --for=delete pod -l app=dataflow-controller --timeout=120s
[[ "$(rayjob_count "$RUN_ID" unit-002)" == "0" ]] || fail "downstream RayJob started before upstream success"

log "let the real RayJob finish without the DataFlow controller"
wait_rayjob_succeeded "$JOB2" 600
[[ "$(rayjob_count "$RUN_ID" unit-002)" == "0" ]] || fail "downstream RayJob appeared while controller was stopped"

log "run one durable reconciliation pass to commit the checkpoint and expose downstream READY"
run_controller_once
DIAG="$(wait_diag "$RUN_ID" '.units[0].unit.status == "SUCCEEDED" and .units[1].unit.status == "READY"' 'checkpoint committed and downstream READY' 120)"
jq -e '.units[0].artifacts | any(.attempt_number == 2 and .state == "COMMITTED" and .checkpoint == true)' <<<"$DIAG" >/dev/null \
  || fail "attempt 2 checkpoint was not committed"
[[ "$(rayjob_count "$RUN_ID" unit-002)" == "0" ]] || fail "one-shot reconcile must not submit downstream work"

UPSTREAM_CLUSTER="$(kubectl get rayjob -n "$NAMESPACE" "$JOB2" -o jsonpath='{.status.rayClusterName}' 2>/dev/null || true)"
[[ -n "$UPSTREAM_CLUSTER" ]] || fail "upstream RayJob did not expose its RayCluster name"
log "delete upstream RayJob/cluster after checkpoint publication"
kubectl delete rayjob -n "$NAMESPACE" "$JOB2" --ignore-not-found --wait=true
for _ in $(seq 1 90); do
  if ! kubectl get raycluster -n "$NAMESPACE" "$UPSTREAM_CLUSTER" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
kubectl get raycluster -n "$NAMESPACE" "$UPSTREAM_CLUSTER" >/dev/null 2>&1 \
  && fail "upstream RayCluster still exists after RayJob deletion"

log "restart controller after upstream cluster is gone; downstream must read committed checkpoint"
kubectl scale -n "$NAMESPACE" deployment/dataflow-controller --replicas=1
kubectl rollout status -n "$NAMESPACE" deployment/dataflow-controller --timeout=180s
wait_diag "$RUN_ID" '.status == "SUCCEEDED"' 'pipeline SUCCEEDED from durable checkpoint' 600 >/tmp/dataflow-e2e-success.json
assert_s3_outputs "$RUN_ID"

log "release completed Ray resources before starting cancellation scenario"
cleanup_run_ray_resources "$RUN_ID"

log "verify cancellation propagates to an active real RayJob"
CANCEL_RUN_JSON="$(curl -fsS -X POST "$API_URL/v1/pipeline-runs" \
  -H 'content-type: application/json' \
  -d "{\"pipeline_version_id\":\"$VERSION_ID\",\"created_by\":\"kuberay-e2e-cancel\"}")"
CANCEL_RUN_ID="$(jq -r '.id' <<<"$CANCEL_RUN_JSON")"
CANCEL_DIAG="$(wait_diag "$CANCEL_RUN_ID" '.units[0].unit.status == "RUNNING"' 'cancellation run active' 600)"
CANCEL_JOB="$(jq -r '.units[0].latest_external_job_id' <<<"$CANCEL_DIAG")"
curl -fsS -X POST "$API_URL/v1/pipeline-runs/$CANCEL_RUN_ID/cancel" >/tmp/dataflow-e2e-cancel.json
CANCEL_DIAG="$(wait_diag "$CANCEL_RUN_ID" '.status == "CANCELLED" and .units[0].unit.status == "CANCELLED"' 'cancellation convergence' 300)"
if jq -e '.units[0].artifacts | any(.state == "COMMITTED")' <<<"$CANCEL_DIAG" >/dev/null; then
  fail "cancelled attempt exposed a committed artifact"
fi
for _ in $(seq 1 90); do
  if ! kubectl get rayjob -n "$NAMESPACE" "$CANCEL_JOB" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
kubectl get rayjob -n "$NAMESPACE" "$CANCEL_JOB" >/dev/null 2>&1 \
  && fail "cancelled RayJob still exists"

log "verify Prometheus surface remains available"
curl -fsS "$API_URL/metrics" | grep -q '^dataflow_api_requests_total'

log "PASS: real KubeRay/Ray Data lifecycle, retry, checkpoint recovery, and cancellation verified"
