# Offline and multi-architecture deployment

English | [简体中文](zh-CN/offline-deployment.md)

DataFlow release bundles support air-gapped Kubernetes environments on
`linux/amd64` and `linux/arm64`. The build pipeline produces one bundle per
architecture so an operator can transfer the required images and charts across
an isolation boundary once, verify them, and install without contacting a
public container registry or Helm repository.

## What a bundle contains

A release such as `v0.1.0` produces:

```text
dataflow-offline-0.1.0-amd64.tar.gz
dataflow-offline-0.1.0-amd64.sha256
dataflow-offline-0.1.0-arm64.tar.gz
dataflow-offline-0.1.0-arm64.sha256
```

Each archive contains:

- the DataFlow Ray runtime image for the selected architecture;
- the DataFlow API/controller image for the selected architecture;
- the pinned KubeRay operator `v1.6.2` image;
- the packaged DataFlow Helm chart;
- the packaged KubeRay operator Helm chart;
- PostgreSQL 16 and MinIO reference images used only for standalone/offline
  bootstrap testing;
- `bundle.env` and `manifest.json` describing exact versions and source image
  names;
- `SHA256SUMS` covering every payload in the bundle;
- image loading, Kind loading, installation, and reference dependency scripts.

The production DataFlow chart still treats PostgreSQL and S3-compatible object
storage as external services. The reference PostgreSQL and MinIO images are in
the bundle so an isolated environment can prove a full installation without
public network access; they are not a production durability recommendation.

## Verify after crossing the air gap

Verify the outer archive before extracting it:

```bash
sha256sum -c dataflow-offline-0.1.0-amd64.sha256
tar -xzf dataflow-offline-0.1.0-amd64.tar.gz
cd dataflow-offline-0.1.0-amd64
sha256sum -c SHA256SUMS
```

Do not install a bundle whose outer or inner checksum verification fails.

## Production flow with an internal registry

The target environment needs Docker access to the internal registry on the
transfer host, plus `kubectl` and Helm 3 configured for the target cluster.
These tools are prerequisites and are not downloaded by the bundle scripts.

Load every image archive and push it to an internal registry prefix:

```bash
./scripts/load-images.sh \
  --registry registry.internal.example/dataflow
```

The command publishes these image names inside the isolated environment:

```text
registry.internal.example/dataflow/dataflow-runtime:0.1.0
registry.internal.example/dataflow/dataflow-control-plane:0.1.0
registry.internal.example/dataflow/kuberay-operator:v1.6.2
registry.internal.example/dataflow/postgres:16-alpine
registry.internal.example/dataflow/minio:RELEASE.2025-09-07T16-13-09Z
```

For production, create the database and S3 Secrets using platform-managed
services reachable inside the isolated network:

```bash
kubectl create namespace dataflow

kubectl -n dataflow create secret generic dataflow-database \
  --from-literal=database-url='postgresql://USER:PASSWORD@postgres.internal:5432/dataflow'

kubectl -n dataflow create secret generic dataflow-s3 \
  --from-literal=AWS_ACCESS_KEY_ID='...' \
  --from-literal=AWS_SECRET_ACCESS_KEY='...'
```

Install KubeRay and DataFlow exclusively from the local chart archives:

```bash
./scripts/install.sh \
  --registry registry.internal.example/dataflow \
  --namespace dataflow \
  --database-secret dataflow-database \
  --s3-secret dataflow-s3 \
  --s3-endpoint http://s3.internal:9000
```

The installer runs `dataflow-migrate` from the bundled control-plane image
before it rolls out the API/controller chart. It also creates the
`dataflow-ray` service account used by the reference ClusterProfile path.

If the platform already operates a compatible KubeRay installation, add
`--skip-kuberay`.

Pipeline specifications in the air-gapped environment must use the mirrored
runtime image:

```text
registry.internal.example/dataflow/dataflow-runtime:0.1.0
```

## Fully standalone reference bootstrap

For demos and acceptance testing, the bundle can create non-production
PostgreSQL and MinIO services using only bundled images:

```bash
./scripts/bootstrap-reference-deps.sh \
  --registry registry.internal.example/dataflow \
  --namespace dataflow \
  --pull-policy IfNotPresent

./scripts/install.sh \
  --registry registry.internal.example/dataflow \
  --namespace dataflow \
  --database-secret dataflow-database \
  --s3-secret dataflow-s3 \
  --s3-endpoint http://minio.dataflow.svc.cluster.local:9000
```

The bootstrap script uses fixed demonstration credentials and ephemeral
storage. Do not use it for production data.

## Registry-free Kind acceptance path

For a local Kind cluster, the bundle can inject images directly into the node
under a synthetic registry prefix. No registry process is required:

```bash
./scripts/kind-load-images.sh \
  --cluster dataflow-offline \
  --registry offline.invalid/dataflow
```

Run the bootstrap and install scripts with `--pull-policy Never`. Kubernetes
will fail immediately if any required image is missing instead of reaching a
registry. The release workflow uses this mode for the offline smoke test and
also checks that no `Pulling` events occur in the DataFlow or KubeRay
namespaces during installation.

## Architecture selection

Use the bundle matching the Kubernetes node architecture:

- `amd64` for `linux/amd64`;
- `arm64` for `linux/arm64`.

The release workflow builds both application images for both platforms. The
normal GHCR version and `sha-<commit>` tags are multi-platform manifest lists,
so connected environments can use the same tag on either architecture.

Do not mix an `amd64` bundle onto `arm64` nodes or the reverse. The bundle
manifest records both `architecture` and `platform` so deployment automation
can reject a mismatch before loading images.

## Upgrade and rollback

For an offline upgrade:

1. transfer and verify the new architecture-specific bundle;
2. load/push the new image tags into the internal registry;
3. run `scripts/install.sh` with the same namespace, Secret names, and S3
   endpoint;
4. verify both controller replicas, API readiness, and KubeRay health;
5. update ClusterProfiles to the matching new runtime image when desired.

Release versions and SHA tags are immutable. Keep the previous bundle and
internal image tags available until the upgrade has passed workload validation.
