# Migration to 0.2

English | [简体中文](zh-CN/migration.md)

The single-cluster cleanup removes the old implementation. There is no compatibility runtime,
historical deployment chart or second execution path in the package. Use Git history if you
need to inspect the earlier system.

| Removed surface | Replacement / ownership |
| --- | --- |
| HTTP client/API, PostgreSQL schema/migrations | Host application handles request state; call `run_pipeline(spec)` |
| ExecutionPlan, island compiler and per-unit jobs | One PipelineSpec and one driver in the shared Ray cluster |
| ClusterProfile, image/service-account/runtime overrides | Platform deploys one shared worker environment |
| S3 artifact registry, publication, retention and checkpoints | Ray Data blocks for intermediates; explicit business final sinks |
| Controller, leadership/HA, admission and auth services | Host/platform policies; no replacement service in DataFlow |
| Helm, local multi-service scripts and offline vendor bundles | Existing cluster and optional small runtime Dockerfile |
| Other DataFlow console commands | `dataflow-run` or `python -m dataflow.cli` |

Create pipelines with `@pipeline(name="...")`. Remove `runtime`, `runtime_image`,
`cluster_profile`, `artifact_base_uri`, per-node runtime/profile arguments and edge `boundary`
fields. `.checkpoint()` and `.hard_boundary()` are removed. Old JSON fields are rejected rather
than silently retaining the former behavior. `Runtime`, `DataFlowClient`, compiler and durable
artifact/state APIs are no longer exported.

Connection settings are execution arguments (`address`, `namespace`) or an application-owned
Ray session, outside the pipeline spec. Import contracts/topology from `dataflow` or
`dataflow.graph`, and authoring/execution from `dataflow.sdk`.

Install the new package into a clean environment or rebuild your cluster image. Uninstall old
service deployments through the platform's normal maintenance process. This source change does
not stop deployed services, migrate a running database, or delete external business data.
Validate idempotent outputs and required failure/recovery behavior before switching production.
