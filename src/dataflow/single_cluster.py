"""Bounded dependency execution inside one existing Ray cluster."""

from __future__ import annotations

import heapq
import os
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, field
from time import perf_counter

from dataflow.contracts import OperatorKind
from dataflow.graph import EdgeKind, LogicalGraph, PipelineSpec
from dataflow.runtime import (
    DatasetLike,
    OperatorExecutor,
    RayDataBackend,
    operator_config,
    resolve_symbol,
)


@dataclass(frozen=True)
class PipelineResult:
    pipeline_name: str
    output_paths: dict[str, str]
    duration_seconds: float = field(default=0, compare=False)
    peak_in_flight_nodes: int = field(default=0, compare=False)
    materialized_nodes: tuple[str, ...] = field(default=(), compare=False)


class PipelineExecutionError(RuntimeError):
    def __init__(self, node_id: str, pipeline_name: str | None = None) -> None:
        self.node_id, self.pipeline_name = node_id, pipeline_name
        super().__init__(f"pipeline {pipeline_name!r} execution failed at node {node_id!r}")


def positive_integer(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def validate_single_cluster(spec: PipelineSpec) -> LogicalGraph:
    """Validate dependencies/configuration before connecting or performing business IO."""
    graph = LogicalGraph(spec)
    for node_id in graph.topological_order:
        node = graph.nodes[node_id]
        operator_config(node)
        incoming = graph.data_incoming(node_id)
        indexes = [edge.input_index for edge in incoming]
        if indexes != list(range(len(incoming))):
            raise ValueError(f"node {node_id!r} requires unique contiguous data input_index ports")
        outgoing = graph.data_outgoing(node_id)
        if node.kind is OperatorKind.READ_PARQUET:
            if incoming:
                raise ValueError(f"read node {node_id!r} cannot have a data input")
        elif node.kind is OperatorKind.UNION:
            if len(incoming) < 2:
                raise ValueError(f"union node {node_id!r} requires at least two data inputs")
        elif node.kind is OperatorKind.JOIN:
            if len(incoming) != 2:
                raise ValueError(f"join node {node_id!r} requires left/right data inputs")
        elif len(incoming) != 1:
            raise ValueError(f"node {node_id!r} requires exactly one data input")
        if node.kind is OperatorKind.WRITE_PARQUET:
            if outgoing or node.cache:
                raise ValueError(f"write node {node_id!r} cannot have data outputs or cache")
        elif not graph.outgoing[node_id]:
            raise ValueError(f"terminal node {node_id!r} must write an output")
        if node.kind in {OperatorKind.READ_PARQUET, OperatorKind.WRITE_PARQUET}:
            if not isinstance(node.config.get("path"), str) or not node.config["path"].strip():
                raise ValueError(f"node {node_id!r} requires a non-empty config.path")
        elif node.kind in {OperatorKind.MAP_BATCHES, OperatorKind.FILTER}:
            symbol = node.config.get("callable")
            if not isinstance(symbol, str) or not callable(resolve_symbol(symbol)):
                raise ValueError(f"node {node_id!r} requires an importable config.callable")
    if not any(node.kind is OperatorKind.WRITE_PARQUET for node in spec.nodes):
        raise ValueError("pipeline requires at least one write_parquet output")
    return graph


class SingleClusterExecutor:
    """Schedule ready nodes without occupying action threads while waiting for dependencies."""

    def __init__(
        self,
        backend: RayDataBackend,
        *,
        max_concurrency: int = 4,
        action_pool: ThreadPoolExecutor | None = None,
    ) -> None:
        self._operators = OperatorExecutor(backend)
        self._max_concurrency = positive_integer("max_concurrency", max_concurrency)
        self._action_pool = action_pool

    def execute(self, spec: PipelineSpec) -> PipelineResult:
        return self._execute_graph(validate_single_cluster(spec))

    def _execute_graph(self, graph: LogicalGraph) -> PipelineResult:
        start = perf_counter()
        order = graph.topological_order
        rank = {node_id: index for index, node_id in enumerate(order)}
        indegree = {node_id: len(graph.incoming[node_id]) for node_id in order}
        ready = [node_id for node_id in order if not indegree[node_id]]
        heapq.heapify(ready)
        datasets: dict[str, DatasetLike] = {}
        remaining = {node_id: len(graph.data_outgoing(node_id)) for node_id in order}
        barriers = {
            node_id
            for node_id in order
            if graph.nodes[node_id].cache
            or any(edge.kind is EdgeKind.CONTROL for edge in graph.outgoing[node_id])
        }
        outputs: dict[str, str] = {}
        running: dict[Future, str] = {}
        pool = self._action_pool or ThreadPoolExecutor(
            max_workers=self._max_concurrency, thread_name_prefix="dataflow-node"
        )
        peak = 0
        try:
            while ready or running:
                while ready and len(running) < self._max_concurrency:
                    node_id = heapq.heappop(ready)
                    incoming = graph.data_incoming(node_id)
                    inputs = [datasets[edge.from_node] for edge in incoming]
                    future = pool.submit(
                        self._execute_node, graph, node_id, inputs, node_id in barriers
                    )
                    running[future] = node_id
                    for edge in incoming:
                        remaining[edge.from_node] -= 1
                        if not remaining[edge.from_node]:
                            datasets.pop(edge.from_node, None)
                peak = max(peak, len(running))
                completed, _ = wait(running, return_when=FIRST_COMPLETED)
                completed = sorted(completed, key=lambda f: rank[running[f]])
                # Inspect the entire completed batch before admitting descendants.
                for future in completed:
                    if future.exception() is not None:
                        raise PipelineExecutionError(
                            running[future], graph.spec.name
                        ) from future.exception()
                for future in completed:
                    node_id = running.pop(future)
                    result = future.result()
                    node = graph.nodes[node_id]
                    if node.kind is OperatorKind.WRITE_PARQUET:
                        outputs[node_id] = node.config["path"]
                    elif remaining[node_id]:
                        datasets[node_id] = result
                    for edge in graph.outgoing[node_id]:
                        indegree[edge.to_node] -= 1
                        if not indegree[edge.to_node]:
                            heapq.heappush(ready, edge.to_node)
            return PipelineResult(
                graph.spec.name,
                {n: outputs[n] for n in order if n in outputs},
                perf_counter() - start,
                peak,
                tuple(n for n in order if n in barriers),
            )
        finally:
            # Stop admitting descendants on failure; cancel queued actions and drain started
            # writes before returning an error or disconnecting the Ray session.
            for future in running:
                future.cancel()
            wait(running)
            if self._action_pool is None:
                pool.shutdown(wait=True, cancel_futures=True)

    def _execute_node(
        self,
        graph: LogicalGraph,
        node_id: str,
        inputs: list[DatasetLike],
        materialize: bool,
    ) -> DatasetLike:
        result = self._operators.apply_operator(
            inputs[0] if inputs else None,
            graph.nodes[node_id],
            inputs=inputs,
        )
        return result.copy(result).materialize() if materialize else result


@contextmanager
def ray_connection(address: str | None = None, namespace: str | None = None):
    import ray
    import ray.data

    owns = not ray.is_initialized()
    if not owns:
        if address is not None or namespace is not None:
            raise ValueError(
                "Ray is already initialized; omit address/namespace to reuse its session"
            )
    else:
        requested = address if address is not None else "auto"
        if requested == "auto":
            requested = os.environ.get("RAY_ADDRESS") or "auto"
        if not requested.strip() or requested.strip().lower() == "local":
            raise ValueError("address must select an existing Ray cluster; 'local' is not allowed")
        ray.init(address=requested, namespace=namespace if namespace is not None else "default")
    try:
        yield ray.data
    finally:
        if owns:
            ray.shutdown()


def run_pipeline(
    spec: PipelineSpec,
    *,
    address: str | None = None,
    namespace: str | None = None,
    max_concurrent_nodes: int = 4,
) -> PipelineResult:
    """Execute synchronously, with bounded concurrent nodes in an existing Ray cluster."""
    positive_integer("max_concurrent_nodes", max_concurrent_nodes)
    # Snapshot caller-owned mutable data before worker threads can observe it.
    snapshot = spec.model_copy(deep=True)
    validate_single_cluster(snapshot)
    with ray_connection(address, namespace) as backend:
        return SingleClusterExecutor(backend, max_concurrency=max_concurrent_nodes).execute(
            snapshot
        )
