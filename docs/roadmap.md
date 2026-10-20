# Roadmap and acceptance boundaries

English | [简体中文](zh-CN/roadmap.md)

Implemented: serializable SDK/JSON DAGs, data/control dependencies, bounded independent-node and
sink concurrency, explicit cache barriers, same-schema union and keyed shuffle join, and a
long-lived runner with bounded DAG admission and completion-order streaming of results.
All execute in one existing Ray cluster, with no storage-backed intermediate protocol.

Unit tests exercise concurrency overlap/bounds, queue backpressure, cancellation, failure draining,
input windows, mutable spec snapshots and merge ports. Real Ray tests and a reproducible synthetic
benchmark validate actual execution separately. Benchmark results are workload-specific; a green
suite is not production acceptance or a global-optimality claim.

Next work: representative OCR/page/BOX workloads, sustained throughput and latency under memory
pressure, object-store/spill peak measurement, worker/driver failure acceptance, smarter sharing
policies, model actor reuse across DAGs, and optional host-facing progress/cancellation integration.
Default fan-out may recompute and join may materialize/shuffle; zero-copy streaming broadcast,
automatic memory-aware admission and cluster-wide fairness are not implemented.

Cluster lifecycle, durable request history, host resubmission and business idempotency remain
host responsibilities. Multi-cluster orchestration, standalone auth/audit/database/control-plane
services and S3 intermediate artifact protocols remain outside this component's scope.
