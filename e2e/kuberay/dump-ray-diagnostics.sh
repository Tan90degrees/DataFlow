#!/usr/bin/env bash
set +e

NAMESPACE="${DATAFLOW_E2E_NAMESPACE:-dataflow-e2e}"

printf '\n[e2e] Ray/KubeRay diagnostics for namespace %s\n' "$NAMESPACE"
kubectl get pods,jobs,rayjobs,rayclusters -n "$NAMESPACE" -o wide || true

for pod in $(kubectl get pods -n "$NAMESPACE" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null); do
  containers="$(kubectl get pod -n "$NAMESPACE" "$pod" -o jsonpath='{.spec.containers[*].name}' 2>/dev/null)"
  if [[ "$containers" != *"ray-head"* && "$containers" != *"ray-worker"* ]]; then
    continue
  fi

  printf '\n===== pod/%s describe =====\n' "$pod"
  kubectl describe pod -n "$NAMESPACE" "$pod" || true

  printf '\n===== pod/%s container states =====\n' "$pod"
  kubectl get pod -n "$NAMESPACE" "$pod" -o json | jq '{
    pod: .metadata.name,
    phase: .status.phase,
    reason: .status.reason,
    message: .status.message,
    containerStatuses: [.status.containerStatuses[]? | {
      name,
      ready,
      restartCount,
      state,
      lastState
    }]
  }' || true

  for container in ray-head ray-worker; do
    if [[ " $containers " != *" $container "* ]]; then
      continue
    fi

    printf '\n===== pod/%s container/%s current logs =====\n' "$pod" "$container"
    kubectl logs -n "$NAMESPACE" "$pod" -c "$container" --tail=400 || true

    printf '\n===== pod/%s container/%s previous logs =====\n' "$pod" "$container"
    kubectl logs -n "$NAMESPACE" "$pod" -c "$container" --previous --tail=400 || true
  done
done

printf '\n===== RayJob YAML =====\n'
kubectl get rayjobs -n "$NAMESPACE" -o yaml || true

printf '\n===== RayCluster YAML =====\n'
kubectl get rayclusters -n "$NAMESPACE" -o yaml || true

printf '\n===== KubeRay operator tail =====\n'
kubectl logs -n kuberay-system deployment/kuberay-operator --tail=400 || true
