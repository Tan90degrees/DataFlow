#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${DATAFLOW_HA_E2E_NAMESPACE:-dataflow-e2e}"
RELEASE="${DATAFLOW_HA_E2E_RELEASE:-dataflow-ha}"
SELECTOR="app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=controller"

printf '\n[ha-e2e diagnostics] resources\n'
kubectl get pods,jobs,deployments,services,rayjobs,rayclusters -n "$NAMESPACE" -o wide || true

printf '\n[ha-e2e diagnostics] controller leadership sessions\n'
kubectl exec -n "$NAMESPACE" deployment/postgres -- \
  psql -U dataflow -d dataflow -c \
  "SELECT pid, application_name, client_addr, backend_start, state FROM pg_stat_activity WHERE application_name = 'dataflow-controller-leader-election' ORDER BY backend_start" \
  || true

printf '\n[ha-e2e diagnostics] chart controller logs\n'
for pod in $(kubectl get pods -n "$NAMESPACE" -l "$SELECTOR" -o name 2>/dev/null); do
  printf '\n--- %s ---\n' "$pod"
  kubectl logs -n "$NAMESPACE" "$pod" --all-containers --tail=400 || true
done

printf '\n[ha-e2e diagnostics] api logs\n'
kubectl logs -n "$NAMESPACE" deployment/dataflow-ha-api --tail=250 || true

printf '\n[ha-e2e diagnostics] Ray resources\n'
kubectl get rayjobs -n "$NAMESPACE" -o yaml || true
kubectl get rayclusters -n "$NAMESPACE" -o yaml || true

printf '\n[ha-e2e diagnostics] namespace events\n'
kubectl get events -n "$NAMESPACE" --sort-by=.lastTimestamp || true
