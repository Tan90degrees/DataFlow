"""Logical DAG contracts and execution-island compiler."""

from __future__ import annotations

import heapq
from collections import defaultdict
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dataflow.artifacts import (
    ArtifactDurability,
    ArtifactFormat,
    ArtifactOutputSpec,
    ArtifactRef,
    committed_artifact_uri,
    staging_artifact_uri_template,
)
from dataflow.contracts import ExecutionPlan, OperatorKind, OperatorSpec, ResourceSpec, RuntimeSpec


class EdgeKind(StrEnum):
    DATA = "data"
    CONTROL = "control"


class ExecutionBoundary(StrEnum):
    NONE = "none"
    SOFT = "soft"
    HARD = "hard"
    CHECKPOINT = "checkpoint"


class PipelineNodeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1)
    kind: OperatorKind
    config: dict = Field(default_factory=dict)
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    runtime: RuntimeSpec | None = None
    cluster_profile: str | None = None


class PipelineEdgeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    from_node: str = Field(alias="from", min_length=1)
    to_node: str = Field(alias="to", min_length=1)
    kind: EdgeKind = EdgeKind.DATA
    boundary: ExecutionBoundary = ExecutionBoundary.NONE


class PipelineSpec(BaseModel):
    """Versioned static DAG definition owned by the orchestration layer."""

    model_config = ConfigDict(extra="forbid")

    api_version: Literal["dataflow.io/v1alpha1"] = "dataflow.io/v1alpha1"
    kind: Literal["Pipeline"] = "Pipeline"
    name: str = Field(min_length=1)
    runtime: RuntimeSpec
    cluster_profile: str = Field(default="default", min_length=1)
    artifact_base_uri: str | None = None
    nodes: list[PipelineNodeSpec] = Field(min_length=1)
    edges: list[PipelineEdgeSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_references(self) -> PipelineSpec:
        ids = [node.id for node in self.nodes]
        if len(ids) != len(set(ids)):
            raise ValueError("pipeline node ids must be unique")
        if any(node_id.startswith("__dataflow_") for node_id in ids):
            raise ValueError("node ids starting with '__dataflow_' are reserved")

        known = set(ids)
        seen_edges: set[tuple[str, str, EdgeKind]] = set()
        for edge in self.edges:
            if edge.from_node not in known or edge.to_node not in known:
                raise ValueError(
                    f"edge references unknown node: {edge.from_node} -> {edge.to_node}"
                )
            if edge.from_node == edge.to_node:
                raise ValueError("self edges are not allowed")
            key = (edge.from_node, edge.to_node, edge.kind)
            if key in seen_edges:
                raise ValueError(f"duplicate edge: {edge.from_node} -> {edge.to_node}")
            seen_edges.add(key)
            if edge.kind is EdgeKind.CONTROL and edge.boundary is ExecutionBoundary.CHECKPOINT:
                raise ValueError("checkpoint boundaries are only valid on data edges")
        return self


class ExecutionUnit(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    node_ids: list[str]
    dependencies: list[str] = Field(default_factory=list)
    cluster_profile: str
    plan: ExecutionPlan


class ExecutionGraph(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    pipeline_name: str
    units: list[ExecutionUnit]


class LogicalGraph:
    """Validated static DAG with deterministic topology helpers."""

    def __init__(self, spec: PipelineSpec):
        self.spec = spec
        self.nodes = {node.id: node for node in spec.nodes}
        self.outgoing: dict[str, list[PipelineEdgeSpec]] = defaultdict(list)
        self.incoming: dict[str, list[PipelineEdgeSpec]] = defaultdict(list)
        for edge in spec.edges:
            self.outgoing[edge.from_node].append(edge)
            self.incoming[edge.to_node].append(edge)
        self._topological_order = self._build_topological_order()

    @property
    def topological_order(self) -> list[str]:
        return list(self._topological_order)

    def data_incoming(self, node_id: str) -> list[PipelineEdgeSpec]:
        return sorted(
            (edge for edge in self.incoming[node_id] if edge.kind is EdgeKind.DATA),
            key=lambda edge: (edge.from_node, edge.to_node),
        )

    def data_outgoing(self, node_id: str) -> list[PipelineEdgeSpec]:
        return sorted(
            (edge for edge in self.outgoing[node_id] if edge.kind is EdgeKind.DATA),
            key=lambda edge: (edge.from_node, edge.to_node),
        )

    def _build_topological_order(self) -> list[str]:
        indegree = {node_id: 0 for node_id in self.nodes}
        for edge in self.spec.edges:
            indegree[edge.to_node] += 1

        ready = [node_id for node_id, degree in indegree.items() if degree == 0]
        heapq.heapify(ready)
        result: list[str] = []

        while ready:
            node_id = heapq.heappop(ready)
            result.append(node_id)
            for edge in sorted(
                self.outgoing[node_id], key=lambda item: (item.to_node, item.kind.value)
            ):
                indegree[edge.to_node] -= 1
                if indegree[edge.to_node] == 0:
                    heapq.heappush(ready, edge.to_node)

        if len(result) != len(self.nodes):
            raise ValueError("pipeline graph contains a cycle")
        return result


class PipelineCompiler:
    """Compile a PipelineSpec into deterministic linear Ray Data execution islands."""

    def compile(self, spec: PipelineSpec, *, run_id: str) -> ExecutionGraph:
        graph = LogicalGraph(spec)
        self._validate_supported_data_topology(graph)

        unit_for_node: dict[str, str] = {}
        unit_nodes: dict[str, list[str]] = {}
        next_unit = 1

        for node_id in graph.topological_order:
            incoming_data = graph.data_incoming(node_id)
            fused = False
            if len(incoming_data) == 1:
                edge = incoming_data[0]
                predecessor = edge.from_node
                if self._can_fuse(graph, predecessor, node_id, edge):
                    unit_id = unit_for_node[predecessor]
                    unit_for_node[node_id] = unit_id
                    unit_nodes[unit_id].append(node_id)
                    fused = True

            if not fused:
                unit_id = f"unit-{next_unit:03d}"
                next_unit += 1
                unit_for_node[node_id] = unit_id
                unit_nodes[unit_id] = [node_id]

        dependencies: dict[str, set[str]] = defaultdict(set)
        incoming_data_edges: dict[str, list[PipelineEdgeSpec]] = defaultdict(list)
        outgoing_data_edges: dict[str, list[PipelineEdgeSpec]] = defaultdict(list)
        for edge in spec.edges:
            source_unit = unit_for_node[edge.from_node]
            target_unit = unit_for_node[edge.to_node]
            if source_unit == target_unit:
                continue
            dependencies[target_unit].add(source_unit)
            if edge.kind is EdgeKind.DATA:
                incoming_data_edges[target_unit].append(edge)
                outgoing_data_edges[source_unit].append(edge)

        if any(
            len({unit_for_node[edge.from_node] for edge in edges}) > 1
            for edges in incoming_data_edges.values()
        ):
            raise ValueError("v1alpha1 runtime does not support data fan-in across execution units")

        units: list[ExecutionUnit] = []
        ordered_units = sorted(unit_nodes, key=lambda value: int(value.split("-")[1]))
        for unit_id in ordered_units:
            node_ids = unit_nodes[unit_id]
            first_node = graph.nodes[node_ids[0]]
            runtime = first_node.runtime or spec.runtime
            cluster_profile = first_node.cluster_profile or spec.cluster_profile

            operators: list[OperatorSpec] = []
            input_artifacts: list[ArtifactRef] = []
            input_edges = incoming_data_edges[unit_id]
            if input_edges:
                source_nodes = {edge.from_node for edge in input_edges}
                if len(source_nodes) != 1:
                    raise ValueError(
                        "v1alpha1 execution unit cannot consume multiple durable artifacts"
                    )
                source_node = next(iter(source_nodes))
                committed_uri = self._committed_artifact_uri(spec, run_id, source_node)
                input_ref = ArtifactRef(
                    run_id=run_id,
                    node_id=source_node,
                    format=ArtifactFormat.PARQUET,
                    uri=committed_uri,
                )
                input_artifacts.append(input_ref)
                operators.append(
                    OperatorSpec(
                        id=f"__dataflow_read_{source_node}",
                        kind=OperatorKind.READ_PARQUET,
                        config={"path": committed_uri},
                    )
                )

            for node_id in node_ids:
                node = graph.nodes[node_id]
                operators.append(
                    OperatorSpec(
                        id=node.id,
                        kind=node.kind,
                        config=node.config,
                        resources=node.resources,
                    )
                )

            output_artifacts: list[ArtifactOutputSpec] = []
            output_edges = outgoing_data_edges[unit_id]
            if output_edges:
                source_nodes = {edge.from_node for edge in output_edges}
                if len(source_nodes) != 1:
                    raise ValueError(
                        "v1alpha1 execution unit cannot publish multiple durable artifacts"
                    )
                source_node = next(iter(source_nodes))
                operator_id = f"__dataflow_write_{source_node}"
                operators.append(
                    OperatorSpec(
                        id=operator_id,
                        kind=OperatorKind.WRITE_PARQUET,
                        config={},
                    )
                )
                output_artifacts.append(
                    ArtifactOutputSpec(
                        run_id=run_id,
                        node_id=source_node,
                        operator_id=operator_id,
                        format=ArtifactFormat.PARQUET,
                        durability=ArtifactDurability.DURABLE,
                        committed_uri=self._committed_artifact_uri(
                            spec,
                            run_id,
                            source_node,
                        ),
                        staging_uri_template=self._staging_artifact_uri_template(
                            spec,
                            run_id,
                            source_node,
                        ),
                        checkpoint=any(
                            edge.boundary is ExecutionBoundary.CHECKPOINT
                            for edge in output_edges
                        ),
                    )
                )

            units.append(
                ExecutionUnit(
                    id=unit_id,
                    node_ids=node_ids,
                    dependencies=sorted(dependencies[unit_id]),
                    cluster_profile=cluster_profile,
                    plan=ExecutionPlan(
                        run_id=run_id,
                        unit_id=unit_id,
                        operators=operators,
                        runtime=runtime,
                        input_artifacts=input_artifacts,
                        output_artifacts=output_artifacts,
                    ),
                )
            )

        return ExecutionGraph(run_id=run_id, pipeline_name=spec.name, units=units)

    def _can_fuse(
        self,
        graph: LogicalGraph,
        source_id: str,
        target_id: str,
        edge: PipelineEdgeSpec,
    ) -> bool:
        if edge.kind is not EdgeKind.DATA or edge.boundary in {
            ExecutionBoundary.HARD,
            ExecutionBoundary.CHECKPOINT,
        }:
            return False
        if len(graph.data_outgoing(source_id)) != 1 or len(graph.data_incoming(target_id)) != 1:
            return False

        source = graph.nodes[source_id]
        target = graph.nodes[target_id]
        source_runtime = source.runtime or graph.spec.runtime
        target_runtime = target.runtime or graph.spec.runtime
        source_cluster = source.cluster_profile or graph.spec.cluster_profile
        target_cluster = target.cluster_profile or graph.spec.cluster_profile
        return source_runtime == target_runtime and source_cluster == target_cluster

    def _committed_artifact_uri(self, spec: PipelineSpec, run_id: str, node_id: str) -> str:
        base_uri = self._require_artifact_base_uri(spec)
        return committed_artifact_uri(base_uri, run_id, node_id)

    def _staging_artifact_uri_template(
        self,
        spec: PipelineSpec,
        run_id: str,
        node_id: str,
    ) -> str:
        base_uri = self._require_artifact_base_uri(spec)
        return staging_artifact_uri_template(base_uri, run_id, node_id)

    @staticmethod
    def _require_artifact_base_uri(spec: PipelineSpec) -> str:
        if not spec.artifact_base_uri:
            raise ValueError(
                "artifact_base_uri is required when data crosses execution-unit boundaries"
            )
        return spec.artifact_base_uri

    def _validate_supported_data_topology(self, graph: LogicalGraph) -> None:
        for node_id in graph.topological_order:
            if len(graph.data_incoming(node_id)) > 1:
                raise ValueError(
                    f"v1alpha1 runtime does not support data fan-in at node {node_id!r}"
                )


def compile_pipeline(spec: PipelineSpec, *, run_id: str) -> ExecutionGraph:
    return PipelineCompiler().compile(spec, run_id=run_id)
