#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BASE_E2E_DIR="$ROOT_DIR/e2e/kuberay"
NAMESPACE="${DATAFLOW_HA_E2E_NAMESPACE:-dataflow-e2e}"
RELEASE="${DATAFLOW_HA_E2E_RELEASE:-dataflow-ha}"
API_PORT="${DATAFLOW_HA_E2E_API_PORT:-18081}"
API_URL="http://127.0.0.1:${API_PORT}"
CONTROLLER_SELECTOR="app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=controller"
PF_PID=""

log() { printf '\n[ha-e2e] %s\n' "$*"; }
fail() { printf '\n[ha-e2e] ERROR: %s\n' "$*" >&2; exit 1; }

cleanup() {
  local rc=$?
  if [[ -n "$PF_PID" ]]; then
    kill "$PF_PID" >/dev/null 2>&1 || true
  fi
  if [[ $rc -ne 0 ]]; then
    bash "$ROOT_DIR/e2e/kuberay-ha/dump-diagnostics.sh" || true
  fi
  exit "$rc"
}
trap cleanup EXIT

wait_http() {
  local url=$1
  local timeout=${2:-120}
  local deadline=$((SECONDS + timeout))
  until curl -fsS "$url" >/dev/null 2>&1; do
    (( SECONDS < deadline )) || fail "timed out waiting for $url"
    sleep 1
  done
}

run_diagnostics() {
  curl -fsS "$API_URL/v1/pipeline-runs/$1/diagnostics"
}

wait_diag() {
  local run_id=$1 expression=$2 description=$3 timeout=${4:-600}
  local deadline=$((SECONDS + timeout)) body=""
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
  local run_id=$1 unit_id=$2
  kubectl get rayjobs -n "$NAMESPACE" \
    -l "dataflow.io/run-id=$run_id,dataflow.io/unit-id=$unit_id" \
    -o json | jq '.items | length'
}

controller_pods_json() {
  kubectl get pods -n "$NAMESPACE" -l "$CONTROLLER_SELECTOR" -o json
}

postgres_leader_ip() {
  kubectl exec -n "$NAMESPACE" deployment/postgres -- \
    psql -U dataflow -d dataflow -tA -c \
    "SELECT client_addr::text FROM pg_stat_activity WHERE application_name = 'dataflow-controller-leader-election' ORDER BY backend_start LIMIT 1" \
    2>/dev/null | sed -n '1p' | tr -d '\r\n ' || true
}

leader_pod() {
  local ip resource pod pod_ip
  ip="$(postgres_leader_ip)"
  [[ -n "$ip" ]] || return 1
  while IFS= read -r resource; do
    [[ -n "$resource" ]] || continue
    pod="${resource#pod/}"
    pod_ip="$(kubectl get pod -n "$NAMESPACE" "$pod" -o jsonpath='{.status.podIP}' 2>/dev/null || true)"
    if [[ "$pod_ip" == "$ip" ]]; then
      printf '%s\n' "$pod"
      return 0
    fi
  done < <(kubectl get pods -n "$NAMESPACE" -l "$CONTROLLER_SELECTOR" -o name 2>/dev/null || true)
  return 1
}

print_leader_mapping_debug() {
  local resource pod pod_ip
  printf '[ha-e2e] PostgreSQL leader IP: %s\n' "$(postgres_leader_ip)" >&2
  while IFS= read -r resource; do
    [[ -n "$resource" ]] || continue
    pod="${resource#pod/}"
    pod_ip="$(kubectl get pod -n "$NAMESPACE" "$pod" -o jsonpath='{.status.podIP}' 2>/dev/null || true)"
    printf '[ha-e2e] controller pod/IP: %s %s\n' "$pod" "${pod_ip:-none}" >&2
  done < <(kubectl get pods -n "$NAMESPACE" -l "$CONTROLLER_SELECTOR" -o name 2>/dev/null || true)
}

wait_for_leader() {
  local timeout=${1:-60}
  local deadline=$((SECONDS + timeout))
  local leader=""
  while (( SECONDS < deadline )); do
    leader="$(leader_pod || true)"
    if [[ -n "$leader" ]]; then
      printf '%s\n' "$leader"
      return 0
    fi
    sleep 0.5
  done
  print_leader_mapping_debug
  fail "no chart controller acquired PostgreSQL leadership"
}

wait_for_specific_leader() {
  local expected=$1
  local timeout=${2:-60}
  local deadline=$((SECONDS + timeout))
  local actual=""
  while (( SECONDS < deadline )); do
    actual="$(leader_pod || true)"
    if [[ "$actual" == "$expected" ]]; then
      printf '%s\n' "$actual"
      return 0
    fi
    sleep 0.25
  done
  print_leader_mapping_debug
  fail "expected standby $expected to take leadership; current leader is ${actual:-none}"
}

log "reset namespace and bring up external dependencies"
kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=true
kubectl apply -f "$BASE_E2E_DIR/infrastructure.yaml"
kubectl rollout status -n "$NAMESPACE" deployment/postgres --timeout=180s
kubectl rollout status -n "$NAMESPACE" deployment/minio --timeout=180s
kubectl wait -n "$NAMESPACE" --for=condition=complete job/dataflow-migrate --timeout=180s
kubectl wait -n "$NAMESPACE" --for=condition=complete job/seed-parquet --timeout=180s

log "remove the legacy single-controller fixture before installing the production chart"
kubectl scale -n "$NAMESPACE" deployment/dataflow-controller deployment/dataflow-api --replicas=0
kubectl wait -n "$NAMESPACE" --for=delete pod -l app=dataflow-controller --timeout=120s || true
kubectl wait -n "$NAMESPACE" --for=delete pod -l app=dataflow-api --timeout=120s || true

kubectl create secret generic dataflow-ha-database -n "$NAMESPACE" \
  --from-literal=database-url='postgresql://dataflow:dataflow@postgres:5432/dataflow' \
  --dry-run=client -o yaml | kubectl apply -f -

log "install DataFlow through Helm with two controller replicas"
helm upgrade --install "$RELEASE" "$ROOT_DIR/charts/dataflow" \
  --namespace "$NAMESPACE" \
  --set fullnameOverride=dataflow-ha \
  --set image.repository=dataflow-control-plane \
  --set image.tag=e2e \
  --set image.pullPolicy=IfNotPresent \
  --set database.existingSecret=dataflow-ha-database \
  --set s3.existingSecret=dataflow-s3 \
  --set s3.endpointUrl=http://minio:9000 \
  --set controller.replicaCount=2 \
  --set controller.pollSeconds=0.5 \
  --set controller.standbyPollSeconds=0.25 \
  --set controller.retry.initialBackoffSeconds=0 \
  --set controller.resources.requests.cpu=50m \
  --set api.replicaCount=1 \
  --wait --timeout 4m

kubectl rollout status -n "$NAMESPACE" deployment/dataflow-ha-controller --timeout=180s
kubectl rollout status -n "$NAMESPACE" deployment/dataflow-ha-api --timeout=180s
[[ "$(controller_pods_json | jq '[.items[] | select(.status.phase == "Running")] | length')" == "2" ]] \
  || fail "expected two running chart controller pods"

INITIAL_LEADER="$(wait_for_leader 60)"
mapfile -t CONTROLLERS < <(controller_pods_json | jq -r '.items[] | select(.status.phase == "Running") | .metadata.name' | sort)
[[ ${#CONTROLLERS[@]} -eq 2 ]] || fail "expected exactly two controller pods"
if [[ "${CONTROLLERS[0]}" == "$INITIAL_LEADER" ]]; then
  INITIAL_STANDBY="${CONTROLLERS[1]}"
else
  INITIAL_STANDBY="${CONTROLLERS[0]}"
fi
log "initial leader=$INITIAL_LEADER standby=$INITIAL_STANDBY"

kubectl logs -n "$NAMESPACE" "$INITIAL_LEADER" --tail=200 | grep -q controller_leadership_acquired \
  || fail "leader pod did not report leadership acquisition"
kubectl logs -n "$NAMESPACE" "$INITIAL_STANDBY" --tail=200 | grep -q controller_standby \
  || fail "standby pod did not report standby state"

kubectl port-forward -n "$NAMESPACE" service/dataflow-ha-api "$API_PORT:8080" \
  >/tmp/dataflow-ha-e2e-port-forward.log 2>&1 &
PF_PID=$!
wait_http "$API_URL/readyz" 60

log "create profile, pipeline version and a long-running real Ray workload"
curl -fsS -X POST "$API_URL/v1/cluster-profiles" \
  -H 'content-type: application/json' \
  --data-binary "@$BASE_E2E_DIR/cluster-profile.json" >/tmp/dataflow-ha-profile.json
PIPELINE_JSON="$(curl -fsS -X POST "$API_URL/v1/pipelines" \
  -H 'content-type: application/json' \
  -d '{"name":"kuberay-ha-e2e","tenant_id":"e2e"}')"
PIPELINE_ID="$(jq -r '.id' <<<"$PIPELINE_JSON")"
VERSION_JSON="$(curl -fsS -X POST "$API_URL/v1/pipelines/$PIPELINE_ID/versions" \
  -H 'content-type: application/json' \
  --data-binary "@$BASE_E2E_DIR/pipeline.json")"
VERSION_ID="$(jq -r '.id' <<<"$VERSION_JSON")"
RUN_JSON="$(curl -fsS -X POST "$API_URL/v1/pipeline-runs" \
  -H 'content-type: application/json' \
  -d "{\"pipeline_version_id\":\"$VERSION_ID\",\"created_by\":\"kuberay-ha-e2e\"}")"
RUN_ID="$(jq -r '.id' <<<"$RUN_JSON")"

DIAG="$(wait_diag "$RUN_ID" '.units[0].unit.status == "RUNNING" and (.units[0].unit.attempts | length) == 1' 'upstream unit RUNNING' 600)"
JOB1="$(jq -r '.units[0].latest_external_job_id' <<<"$DIAG")"
[[ -n "$JOB1" && "$JOB1" != "null" ]] || fail "active unit has no RayJob identity"
[[ "$(rayjob_count "$RUN_ID" unit-001)" == "1" ]] || fail "expected one upstream RayJob before failover"

log "kill the elected controller during active execution"
kubectl delete pod -n "$NAMESPACE" "$INITIAL_LEADER" --wait=true
TAKEOVER_LEADER="$(wait_for_specific_leader "$INITIAL_STANDBY" 60)"
log "standby takeover confirmed: $TAKEOVER_LEADER"
kubectl logs -n "$NAMESPACE" "$TAKEOVER_LEADER" --tail=300 | grep -q controller_leadership_acquired \
  || fail "standby did not log leadership acquisition"

log "prove failover did not duplicate or retry the active RayJob"
for _ in $(seq 1 20); do
  [[ "$(rayjob_count "$RUN_ID" unit-001)" == "1" ]] || fail "controller failover duplicated upstream RayJob"
  sleep 0.5
done
DIAG="$(wait_diag "$RUN_ID" '.units[0].unit.status == "RUNNING" and (.units[0].unit.attempts | length) == 1' 'same active attempt after takeover' 60)"
JOB_AFTER_FAILOVER="$(jq -r '.units[0].latest_external_job_id' <<<"$DIAG")"
[[ "$JOB_AFTER_FAILOVER" == "$JOB1" ]] || fail "failover changed active RayJob identity"

log "wait for the in-flight run to complete under the new leader"
DIAG="$(wait_diag "$RUN_ID" '.status == "SUCCEEDED"' 'pipeline SUCCEEDED after leader failover' 600)"
jq -e 'all(.units[]; (.unit.attempts | length) == 1)' <<<"$DIAG" >/dev/null \
  || fail "HA failover introduced an unexpected workflow retry"
[[ "$(rayjob_count "$RUN_ID" unit-001)" == "1" ]] || fail "upstream RayJob count changed after completion"

log "wait for Deployment to restore two replicas and smoke-test an idempotent Helm upgrade"
kubectl rollout status -n "$NAMESPACE" deployment/dataflow-ha-controller --timeout=180s
[[ "$(controller_pods_json | jq '[.items[] | select(.status.phase == "Running")] | length')" == "2" ]] \
  || fail "controller Deployment did not restore two replicas"
helm upgrade "$RELEASE" "$ROOT_DIR/charts/dataflow" \
  --namespace "$NAMESPACE" \
  --reuse-values \
  --set revisionHistoryLimit=4 \
  --wait --timeout 4m
wait_http "$API_URL/readyz" 60

log "HA E2E succeeded: leader termination preserved one attempt and one upstream RayJob"
