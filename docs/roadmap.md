# Roadmap and acceptance boundaries

English | [简体中文](zh-CN/roadmap.md)

Current implementation: a serializable DAG/SDK, preflight validation, per-worker resource hints,
one existing Ray cluster, lazy linear execution, cached data fan-out, control dependencies,
synchronous results and errors, and a small JSON CLI. Sources/sinks are currently Parquet.
The source tree and documentation describe this one execution path only.

Next acceptance work is workload-driven: real cluster stability, sustained throughput and tail
latency, mixed CPU/IO/model stages, object-store/spill pressure, worker loss, driver failure,
resubmission and host-owned idempotency. A green unit suite is not production acceptance.

Potential functional work includes more source/sink types, explicit fan-in semantics, efficient
streaming fan-out and better host-facing progress/cancellation integration. Independent sinks
currently execute sequentially per driver. These are not implemented commitments or performance
claims, and should be justified by a concrete use case.

Multi-cluster orchestration, a standalone database/control plane, auth/audit services and
storage-backed intermediate artifact protocols are outside the current component's scope.
