# DataFlow documentation

English | [简体中文](zh-CN/README.md)

Use this page as the documentation entry point. The English documents keep their existing paths;
every document has a matching Simplified Chinese version under `docs/zh-CN/`.

## Start here

- [Getting started](getting-started.md): install dependencies, start the isolated local Kubernetes
  environment, submit a pipeline, inspect it, and choose the next deployment path.
- [Local Kubernetes development](local-development.md): local cluster lifecycle, endpoints, and
  debugging commands.
- [Kubernetes deployment and controller HA](kubernetes-deployment.md): production-oriented Helm
  installation and upgrades.
- [API authentication and RBAC](api-auth.md): authentication modes, roles, and SDK credentials.

## Use and operate DataFlow

- [Durable artifacts and checkpoints](ARTIFACTS.md)
- [Scheduling admission control](admission-control.md)
- [Controller high availability](controller-ha.md)
- [Observability and diagnostics](observability.md)
- [Retention and artifact garbage collection](retention-gc.md)
- [Releasing DataFlow](releases.md)
- [Project roadmap](roadmap.md)
- [Real KubeRay end-to-end suite](../e2e/kuberay/README.md)

## Architecture decisions

- [ADR-0001: Separate orchestration and Ray Data execution](adr/0001-execution-boundary.md)
- [ADR-0002: Compile logical DAGs into execution islands](adr/0002-execution-islands.md)
- [ADR-0003: PostgreSQL is the durable orchestration source of truth](adr/0003-durable-metadata.md)
- [ADR-0004: Durable desired state with idempotent external reconciliation](adr/0004-idempotent-reconciliation.md)
- [ADR-0005: Durable artifact publication at execution-unit boundaries](adr/0005-durable-artifact-publication.md)
- [ADR-0006: HTTP API writes durable desired state](adr/0006-api-desired-state-boundary.md)
- [ADR-0007: Python SDK generates specifications, not distributed work](adr/0007-sdk-spec-generation.md)
- [ADR-0008: Version ClusterProfiles and pin snapshots into execution plans](adr/0008-cluster-profile-snapshots.md)
- [ADR-0009: Keep correlation identifiers out of metric labels](adr/0009-observability-boundaries.md)
- [ADR-0010: Fence controller reconciliation with PostgreSQL leadership](adr/0010-controller-leader-election.md)
- [ADR-0011: Keep API authentication outside orchestration identity](adr/0011-api-authentication-boundary.md)
- [ADR-0012: Durable admission is separate from DAG readiness](adr/0012-durable-admission-and-fairness.md)
- [ADR-0013: Retention and resumable artifact garbage collection](adr/0013-retention-gc.md)

## Documentation convention

When adding or renaming an English file under `docs/`, add or rename the file at the same relative
path under `docs/zh-CN/`. Keep commands, configuration keys, API paths, code, and version numbers
identical across both languages. The test suite checks the pairing and language-switch links.
