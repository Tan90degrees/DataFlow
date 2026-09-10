#!/usr/bin/env bash
set +e

NAMESPACE="${DATAFLOW_E2E_NAMESPACE:-dataflow-e2e}"
OUT_DIR="${DATAFLOW_E2E_DIAGNOSTICS_DIR:-.artifacts/kuberay-diagnostics}"
SUMMARY="$OUT_DIR/summary.txt"
mkdir -p "$OUT_DIR"
: >"$SUMMARY"

summary() {
  printf '%s\n' "$*" | tee -a "$SUMMARY"
}

summary "[e2e] Ray/KubeRay diagnostics for namespace $NAMESPACE"
summary ""
summary "===== RayJob status ====="
kubectl get rayjobs -n "$NAMESPACE" -o json 2>/dev/null | jq -r '
  if (.items | length) == 0 then
    "(no RayJobs)"
  else
    .items[]
    | [
        "rayjob/" + .metadata.name,
        "status=" + (.status.jobStatus // "<none>"),
        "deployment=" + (.status.jobDeploymentStatus // "<none>"),
        "cluster=" + (.status.rayClusterName // "<none>"),
        "message=" + (.status.message // "")
      ]
    | join(" ")
  end
' | tee -a "$SUMMARY" || true

summary ""
summary "===== Ray pod/container termination summary ====="
kubectl get pods -n "$NAMESPACE" -o json 2>/dev/null | jq -r '
  .items[]
  | select(any(.spec.containers[]?; .name == "ray-head" or .name == "ray-worker"))
  | .metadata.name as $pod
  | .status.phase as $phase
  | (.status.containerStatuses // [])[]
  | select(.name == "ray-head" or .name == "ray-worker")
  | [
      "pod=" + $pod,
      "phase=" + ($phase // "<none>"),
      "container=" + .name,
      "ready=" + (.ready | tostring),
      "restarts=" + (.restartCount | tostring),
      "state=" + (
        if .state.running then "running"
        elif .state.waiting then "waiting:" + (.state.waiting.reason // "<none>")
        elif .state.terminated then "terminated:" + (.state.terminated.reason // "<none>") + ":exit=" + (.state.terminated.exitCode | tostring)
        else "<none>" end
      ),
      "last=" + (
        if .lastState.terminated then
          "terminated:" + (.lastState.terminated.reason // "<none>") + ":exit=" + (.lastState.terminated.exitCode | tostring)
        elif .lastState.waiting then "waiting:" + (.lastState.waiting.reason // "<none>")
        elif .lastState.running then "running"
        else "<none>" end
      )
    ] | join(" ")
' | tee -a "$SUMMARY" || true

# Keep full Kubernetes state in the artifact so the console remains compact.
kubectl get pods,jobs,rayjobs,rayclusters -n "$NAMESPACE" -o wide \
  >"$OUT_DIR/resources-wide.txt" 2>&1 || true
kubectl get events -n "$NAMESPACE" --sort-by=.lastTimestamp \
  >"$OUT_DIR/events.txt" 2>&1 || true
kubectl get rayjobs -n "$NAMESPACE" -o yaml \
  >"$OUT_DIR/rayjobs.yaml" 2>&1 || true
kubectl get rayclusters -n "$NAMESPACE" -o yaml \
  >"$OUT_DIR/rayclusters.yaml" 2>&1 || true
kubectl describe pods -n "$NAMESPACE" \
  >"$OUT_DIR/pods-describe.txt" 2>&1 || true
kubectl logs -n kuberay-system deployment/kuberay-operator --tail=1000 \
  >"$OUT_DIR/kuberay-operator.log" 2>&1 || true
kubectl logs -n "$NAMESPACE" deployment/dataflow-controller --tail=1000 \
  >"$OUT_DIR/dataflow-controller.log" 2>&1 || true
kubectl logs -n "$NAMESPACE" deployment/dataflow-api --tail=500 \
  >"$OUT_DIR/dataflow-api.log" 2>&1 || true

while IFS=$'\t' read -r pod container restarts; do
  [[ -n "$pod" && -n "$container" ]] || continue
  safe_name="${pod}-${container}"
  kubectl logs -n "$NAMESPACE" "$pod" -c "$container" --tail=1000 \
    >"$OUT_DIR/${safe_name}.log" 2>&1 || true

  if [[ "${restarts:-0}" =~ ^[0-9]+$ ]] && (( restarts > 0 )); then
    kubectl logs -n "$NAMESPACE" "$pod" -c "$container" --previous --tail=1000 \
      >"$OUT_DIR/${safe_name}.previous.log" 2>&1 || true
    summary ""
    summary "===== previous log tail: pod/$pod container/$container ====="
    tail -n 60 "$OUT_DIR/${safe_name}.previous.log" | tee -a "$SUMMARY" || true
  fi

done < <(
  kubectl get pods -n "$NAMESPACE" -o json 2>/dev/null | jq -r '
    .items[]
    | .metadata.name as $pod
    | (.status.containerStatuses // [])[]
    | select(.name == "ray-head" or .name == "ray-worker")
    | [$pod, .name, (.restartCount | tostring)] | @tsv
  '
)

summary ""
summary "[e2e] full diagnostics saved under $OUT_DIR"
