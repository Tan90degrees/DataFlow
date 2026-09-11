# Releasing DataFlow

English | [简体中文](zh-CN/releases.md)

DataFlow releases are driven by immutable Git tags. A release version has one source-of-truth contract across the Python package and Helm chart, and publication is performed by GitHub Actions with the repository `GITHUB_TOKEN` rather than long-lived registry credentials.

## Version contract

Before cutting a release, set the same stable semantic version in both locations:

- `pyproject.toml`: `project.version`
- `charts/dataflow/Chart.yaml`: `version` and `appVersion`

Validate the repository before tagging:

```bash
dataflow-check-release
# Or validate the intended tag explicitly:
dataflow-check-release v0.1.0
```

Only canonical `vX.Y.Z` tags are accepted. A tag is rejected if any of the three embedded versions differ.

The Helm chart deliberately leaves `image.tag` empty by default. The templates resolve an empty tag to `Chart.appVersion`, so upgrading the chart version contract also upgrades the default control-plane image without a second version value in `values.yaml`.

## Pull-request validation

Changes to release-sensitive files run the `release` workflow in validation-only mode. It builds, but does not publish:

- the Python wheel and source distribution;
- the packaged Helm chart;
- the Ray runtime image;
- the control-plane image;
- SHA256 checksums for distributable archives.

This is in addition to the normal unit, Helm rendering, KubeRay and HA gates. PR validation has read-only repository permissions.

## Cutting a release

After the release commit is on `main` and all required checks are green, create and push the matching annotated or lightweight Git tag, for example:

```bash
git switch main
git pull --ff-only
dataflow-check-release v0.1.0
git tag v0.1.0
git push origin v0.1.0
```

The tag-triggered workflow validates the tag again before publication. If validation or any build fails, nothing is published by the publish job.

For `v0.1.0`, publication coordinates are:

```text
ghcr.io/tan90degrees/dataflow-runtime:0.1.0
ghcr.io/tan90degrees/dataflow-runtime:sha-<git-sha>
ghcr.io/tan90degrees/dataflow-control-plane:0.1.0
ghcr.io/tan90degrees/dataflow-control-plane:sha-<git-sha>
oci://ghcr.io/tan90degrees/charts/dataflow:0.1.0
```

The workflow intentionally does not publish a mutable `latest` tag. Deployments should pin a release version or an image digest.

The GitHub Release for the tag contains the Python wheel, Python source distribution, packaged Helm chart and `SHA256SUMS` generated from the exact validated archives.

## Installing a released chart

Create the external database and S3 credential Secrets described in `kubernetes-deployment.md`, then install the OCI chart:

```bash
helm upgrade --install dataflow \
  oci://ghcr.io/tan90degrees/charts/dataflow \
  --version 0.1.0 \
  --namespace dataflow \
  --create-namespace \
  --set database.existingSecret=dataflow-db \
  --set s3.existingSecret=dataflow-s3
```

The chart defaults to `ghcr.io/tan90degrees/dataflow-control-plane:<appVersion>`. Override `image.repository` or `image.tag` only when intentionally consuming a mirrored or custom build.

The Ray runtime image is separate from the control plane. ClusterProfiles should reference the matching released runtime image, for example `ghcr.io/tan90degrees/dataflow-runtime:0.1.0`.

## Failure and retry semantics

A failed tag workflow should be diagnosed and fixed in a new commit; do not move an already published release tag. Before any artifact is published, retrying the failed workflow is safe. After publication begins, treat version tags as immutable: bump the patch version for corrected artifacts instead of overwriting released images or charts.
