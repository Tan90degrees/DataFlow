# DataFlow roadmap

English | [简体中文](zh-CN/roadmap.md)

This roadmap starts after the v0.1.0 implementation milestone. It deliberately describes
observable acceptance gates rather than a list of loosely defined features. An item is complete
only when its behavior is automated where practical and its operational procedure is documented.

## Release v0.1.0

Goal: publish the first immutable, installable release from the repository's existing release
contract.

- [ ] Run the unit, PostgreSQL integration, Helm render, KubeRay, HA, and release validation gates
  against the intended release commit.
- [ ] Review the generated Python, Helm, and container coordinates and their checksums.
- [ ] Create immutable tag `v0.1.0` from the validated `main` commit.
- [ ] Verify the GitHub Release, GHCR images, and OCI Helm chart from a clean environment.
- [ ] Install the published chart and submit a pipeline using the published runtime image.

The tag is the release boundary. Failed validation should be fixed in a new commit; a published tag
must not be moved or overwritten.

## Production validation

Goal: demonstrate that durable orchestration semantics survive normal production operations.

- [ ] Add an automated migration-and-rolling-upgrade test from the previous released version.
- [ ] Add a rollback test that proves existing PipelineRuns retain attempt and RayJob identity.
- [ ] Document and exercise PostgreSQL backup/restore and artifact-store recovery procedures.
- [ ] Add a controller/API soak test with concurrent runs, retries, cancellation, and GC activity.
- [ ] Publish baseline capacity measurements for scheduler throughput and reconciliation latency.

## Security baseline

Goal: provide a deployment profile that fails closed without requiring operators to discover every
hardening option independently.

- [ ] Add a production values example with API authentication enabled and credentials sourced only
  from Kubernetes Secrets.
- [ ] Add optional Kubernetes NetworkPolicies for API, controller, PostgreSQL, object storage, and
  RayJob traffic.
- [ ] Document TLS termination, bearer-token rotation, and least-privilege runtime identities.
- [ ] Add container and dependency scanning as release-blocking checks with a documented exception
  process.

## Operability

Goal: make failures diagnosable and routine maintenance safe for an on-call operator.

- [ ] Define alerting rules and example dashboards for leadership, reconciliation, admission,
  retries, artifact publication, and garbage collection.
- [ ] Add runbooks for stuck runs, PostgreSQL unavailability, object-store failures, and exhausted
  admission capacity.
- [x] Expose control-plane build/version identity and preserve runtime image coordinates in run
  diagnostics so running components can be correlated with immutable release artifacts.
- [ ] Add a compatibility policy covering Kubernetes, KubeRay, Ray, PostgreSQL, and S3-compatible
  stores.

## Planning rule

Work enters a release milestone only with a linked acceptance test or an explicit explanation of
why automated verification is not practical. New durable state requires a migration, rollback
consideration, and an architecture decision record when it changes an existing invariant.
