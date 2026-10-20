"""Execute a complete pipeline in one existing Ray cluster, without a control plane."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dataflow.compiler import EdgeKind, ExecutionBoundary, LogicalGraph, PipelineSpec
from dataflow.contracts import OperatorKind, OperatorSpec
from dataflow.runtime import (
    DatasetLike,
    FilesystemResolver,
    PlanExecutor,
    RayDataBackend,
    resolve_symbol,
)


@dataclass(frozen=True)
class PipelineResult:
    pipeline_name: str
    output_paths: dict[str, str]


class PipelineExecutionError(RuntimeError):
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        super().__init__(f"pipeline execution failed at node {node_id!r}")


def validate_single_cluster(spec: PipelineSpec) -> LogicalGraph:
    """Reject incompatible legacy semantics before connecting or performing any IO."""

    graph = LogicalGraph(spec)
    if spec.cluster_profile != "default" or any(
        node.cluster_profile not in {None, "default"} for node in spec.nodes
    ):
        raise ValueError("single-cluster execution does not use ClusterProfiles")
    if spec.runtime.image is not None or spec.runtime.service_account is not None:
        raise ValueError(
            "single-cluster execution uses the existing worker environment; remove "
            "runtime.image and runtime.service_account"
        )
    if spec.artifact_base_uri is not None:
        raise ValueError("single-cluster execution does not use artifact_base_uri")
    for edge in spec.edges:
        if edge.boundary in {ExecutionBoundary.HARD, ExecutionBoundary.CHECKPOINT}:
            raise ValueError(
                "single-cluster execution does not support legacy hard/checkpoint "
                "boundaries; remove them or write an explicit output"
            )
    for node_id in graph.topological_order:
        node = graph.nodes[node_id]
        if node.runtime is not None and node.runtime != spec.runtime:
            raise ValueError(f"node {node_id!r} cannot change runtime in one Ray cluster")
        incoming = graph.data_incoming(node_id)
        outgoing = graph.data_outgoing(node_id)
        if node.kind is OperatorKind.READ_PARQUET:
            if incoming:
                raise ValueError(f"read node {node_id!r} cannot have a data input")
        elif len(incoming) != 1:
            raise ValueError(
                f"node {node_id!r} requires exactly one data input; fan-in is unsupported"
            )
        if node.kind is OperatorKind.WRITE_PARQUET:
            if outgoing:
                raise ValueError(f"write node {node_id!r} cannot have a data output")
        elif not graph.outgoing[node_id]:
            raise ValueError(f"terminal node {node_id!r} must write an output")
        if node.kind in {OperatorKind.READ_PARQUET, OperatorKind.WRITE_PARQUET}:
            if not isinstance(node.config.get("path"), str) or not node.config["path"].strip():
                raise ValueError(f"node {node_id!r} requires a non-empty config.path")
        else:
            symbol = node.config.get("callable")
            if not isinstance(symbol, str) or not callable(resolve_symbol(symbol)):
                raise ValueError(f"node {node_id!r} requires an importable config.callable")
    if not any(node.kind is OperatorKind.WRITE_PARQUET for node in spec.nodes):
        raise ValueError("pipeline requires at least one write_parquet output")
    return graph


class SingleClusterExecutor:
    """Build lazy Ray Data chains; materialize shared data once in the object store."""

    def __init__(
        self,
        backend: RayDataBackend,
        *,
        filesystem_resolver: FilesystemResolver | None = None,
    ) -> None:
        self._operators = PlanExecutor(backend, filesystem_resolver=filesystem_resolver)

    def execute(self, spec: PipelineSpec) -> PipelineResult:
        graph = validate_single_cluster(spec)
        datasets: dict[str, DatasetLike] = {}
        remaining = {node_id: len(graph.data_outgoing(node_id)) for node_id in graph.nodes}
        outputs: dict[str, str] = {}
        for node_id in graph.topological_order:
            node = graph.nodes[node_id]
            incoming = graph.data_incoming(node_id)
            dataset = datasets[incoming[0].from_node] if incoming else None
            try:
                result = self._operators.apply_operator(
                    dataset,
                    OperatorSpec(
                        id=node.id, kind=node.kind, config=node.config, resources=node.resources
                    ),
                )
                if node.kind is OperatorKind.WRITE_PARQUET:
                    outputs[node_id] = node.config["path"]
                elif result is not None:
                    # A lazy Dataset reused by two sinks would otherwise execute twice.
                    # Control edges also require completed work, not just a lazy handle.
                    if remaining[node_id] > 1 or any(
                        edge.kind is EdgeKind.CONTROL for edge in graph.outgoing[node_id]
                    ):
                        result = result.materialize()
                    if remaining[node_id]:
                        datasets[node_id] = result
            except Exception as error:
                raise PipelineExecutionError(node_id) from error
            for edge in incoming:
                remaining[edge.from_node] -= 1
                if not remaining[edge.from_node]:
                    datasets.pop(edge.from_node, None)
        return PipelineResult(spec.name, outputs)


def run_pipeline(
    spec: PipelineSpec,
    *,
    address: str | None = None,
    namespace: str | None = None,
) -> PipelineResult:
    """Run synchronously in an existing cluster. Never start a new Ray cluster.

    An initialized Ray session belongs to the caller and is left connected. Otherwise
    this function connects for the duration of the run and disconnects on completion.
    """

    validate_single_cluster(spec)
    import ray
    import ray.data

    owns_connection = not ray.is_initialized()
    if not owns_connection:
        if address is not None or namespace is not None or spec.runtime.ray_address != "auto":
            raise ValueError(
                "Ray is already initialized; omit address/namespace to reuse its session"
            )
    else:
        requested = address if address is not None else spec.runtime.ray_address
        if requested == "auto":
            requested = os.environ.get("RAY_ADDRESS") or "auto"
        if not requested.strip() or requested.strip().lower() == "local":
            raise ValueError("address must select an existing Ray cluster; 'local' is not allowed")
        ray.init(address=requested, namespace=namespace or spec.runtime.namespace)
    try:
        return SingleClusterExecutor(ray.data).execute(spec)
    finally:
        if owns_connection:
            ray.shutdown()


__all__ = ["PipelineExecutionError", "PipelineResult", "SingleClusterExecutor", "run_pipeline"]
