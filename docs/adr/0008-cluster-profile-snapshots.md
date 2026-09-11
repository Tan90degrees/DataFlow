# ADR 0008: Version ClusterProfiles and pin snapshots into execution plans

English | [简体中文](../zh-CN/adr/0008-cluster-profile-snapshots.md)

## Status

Accepted

## Context

`PipelineSpec` must express logical resource intent without embedding Kubernetes pod templates in every pipeline. The control plane also needs to validate resource requests before a RayJob is submitted, while keeping already-created runs reproducible if an administrator later changes cluster sizing, placement, or autoscaling policy.

Ray Data and Ray Core own task/actor scheduling inside a Ray cluster. KubeRay owns the Kubernetes representation of that cluster. DataFlow therefore needs a resource-policy layer between logical pipeline resources and the KubeRay manifest, but it must not turn mutable administrator configuration into mutable workflow history.

## Decision

DataFlow introduces an admin-managed `ClusterProfile` contract.

A profile defines:

- Ray version and Kubernetes namespace;
- service account and optional Kubernetes priority class;
- head-pod CPU/memory and placement;
- one or more worker groups with CPU, memory, GPU capacity, replicas and autoscaling bounds;
- optional accelerator type and GPU Kubernetes resource name;
- node selectors and tolerations;
- Ray Autoscaler settings;
- an optional Kueue LocalQueue name.

`PipelineSpec` and node overrides continue to reference a profile by **name only**. They do not persist Kubernetes pod templates.

ClusterProfile revisions are append-only in PostgreSQL. Updating a profile creates a new immutable revision rather than modifying an existing revision.

When a `PipelineRun` is created, the control plane resolves every referenced profile name to its current revision and passes those immutable snapshots to the compiler. Each physical `ExecutionPlan` embeds the full `ClusterProfileSnapshot` containing profile name, revision, hash and spec. The persisted execution-unit plan is therefore self-contained and does not re-resolve a mutable profile during reconciliation or retry.

The compiler validates every node's `ResourceSpec` against at least one worker group in its selected profile. CPU, GPU and memory must fit on a single worker pod. If `accelerator_type` is requested, a compatible worker group with the same accelerator type must exist. Invalid resource requests fail before a PipelineVersion is persisted by the API.

For accelerator-specific Ray Data operators, DataFlow maps `ResourceSpec.accelerator_type` to a Ray `label_selector` using the reserved node label `ray.io/accelerator-type`. The corresponding KubeRay worker group publishes that Ray node label. Kubernetes node selection remains explicit in the profile through `nodeSelector` and tolerations.

The KubeRay renderer uses the snapshot embedded in the `ExecutionPlan`, not the current profile in PostgreSQL. It renders namespace, service account, pod resources, worker groups, placement, autoscaling and optional Kueue metadata deterministically from that snapshot.

The reserved `default` profile is available as an in-process revision `0` fallback so existing PipelineSpecs keep working before an administrator creates a persisted default profile. A persisted profile named `default` supersedes the built-in fallback for future runs.

## Consequences

- Updating a ClusterProfile affects only runs created after the update.
- Retries and controller restarts for an existing run render the same RayJob resource policy.
- Resource-policy validation is testable without a live Kubernetes cluster.
- Pipeline metadata stays portable and does not become a Kubernetes manifest store.
- A profile revision can be audited independently from PipelineVersion history.
- Runtime images remain a Pipeline/Runtime concern; ClusterProfiles describe compute and placement policy rather than code packaging.
- Cross-cluster admission, quota and Kueue policy can evolve without changing the logical DAG contract.

## Deferred

- shared/per-run pre-created RayCluster pools;
- Kueue ClusterQueue/ResourceFlavor lifecycle management;
- cost-aware profile selection;
- per-tenant authorization for profile administration;
- profile deletion and retention policy;
- live Kubernetes capability discovery.
