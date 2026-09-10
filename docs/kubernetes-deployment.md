# Kubernetes deployment and controller HA

The `charts/dataflow` Helm chart packages the DataFlow API and durable controller for production-oriented Kubernetes deployments. PostgreSQL and S3-compatible storage are external dependencies and are intentionally not installed by the chart.

## Prerequisites

- Kubernetes 1.28+
- Helm 3
- KubeRay operator compatible with the configured `ClusterProfile` resources
- PostgreSQL reachable from API/controller Pods
- S3-compatible object storage reachable from controller and Ray Pods
- a runtime service account referenced by each `ClusterProfile`; this is separate from the chart-managed control-plane service accounts

Run database migrations before first install and before upgrading to an application version that contains new migrations:

```bash
dataflow-migrate --dsn "$DATAFLOW_DATABASE_URL"
```

Store the database URL in a Secret instead of values files:

```bash
kubectl create secret generic dataflow-database \
  --namespace dataflow \
  --from-literal=database-url="$DATAFLOW_DATABASE_URL"
```

Store object-store credentials in a separate Secret using the normal AWS credential environment names:

```bash
kubectl create secret generic dataflow-s3 \
  --namespace dataflow \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"
```

## Install

A minimal production install keeps state outside the chart and runs two controller replicas:

```bash
helm upgrade --install dataflow charts/dataflow \
  --namespace dataflow \
  --create-namespace \
  --set image.repository=registry.example.com/dataflow-control-plane \
  --set image.tag=0.1.0 \
  --set database.existingSecret=dataflow-database \
  --set s3.existingSecret=dataflow-s3 \
  --set s3.endpointUrl=https://s3.example.com \
  --set controller.replicaCount=2
```

The default controller PodDisruptionBudget keeps at least one of two replicas available during voluntary disruption. Both replicas run the same durable reconciliation loop, but PostgreSQL advisory-lock leader election fences mutation work to one controller at a time. Standby replicas keep retrying the same lock and can take over as soon as the leader process/session disappears.

The API defaults to two replicas. API authentication remains configurable through `api.auth`; `static` mode requires `api.auth.existingSecret` containing the configured `staticTokensKey`. For internet-facing deployments, do not expose the API with authentication disabled.

## Controller permissions

The chart creates separate API and controller service accounts by default. The controller role is namespace-scoped and grants only the RayJob operations needed by the control plane. Ray head/worker Pods use the service account configured in the selected `ClusterProfile`; the chart does not grant runtime Pods control-plane privileges.

Set `serviceAccount.create=false` and provide `serviceAccount.apiName` / `serviceAccount.controllerName` when platform-managed identities are required.

## Upgrade sequence

1. Review release notes and render the new chart with the existing production values.
2. Apply any new PostgreSQL migrations before rolling API/controller Pods.
3. Run `helm upgrade` with the same external Secret references.
4. Wait for the API and controller Deployments to become available.
5. Confirm one controller reports leadership and the other reports standby.
6. Verify active runs retain their attempt and RayJob identities across the rollout.

The chart uses rolling Deployments and does not recreate PostgreSQL, object storage, pipeline metadata, attempts, or RayJobs. Durable reconciliation plus leader fencing makes controller replacement safe, while the PDB protects against voluntary loss of all controller replicas.

## HA verification

The repository's `e2e-kuberay-ha` workflow installs the production chart into Kind with two controllers, creates a real slow Ray Data pipeline, discovers the elected controller from PostgreSQL `pg_stat_activity`, kills that leader while the RayJob is active, and requires the pre-existing standby Pod to acquire leadership. It then asserts that the upstream unit keeps exactly one attempt and one RayJob identity through takeover and that the pipeline completes successfully.

This test deliberately identifies leadership from the PostgreSQL lock-owning session rather than inferring it from Pod readiness. On failure, diagnostics capture controller logs, PostgreSQL leadership sessions, RayJobs/RayClusters and namespace events.

## Operational signals

Controller logs emit `controller_standby`, `controller_leadership_acquired`, `controller_leadership_lost`, `controller_leadership_released` and `controller_leadership_unavailable`. Prometheus leadership metrics should show exactly one active leader for a healthy two-replica deployment.

A controller Pod can be terminated without cancelling an already submitted RayJob. The replacement leader reconstructs durable state from PostgreSQL and observes the existing RayJob instead of creating a duplicate. If PostgreSQL is unavailable, controllers remain unable to acquire leadership and mutation work stops rather than running unfenced.
