"""Declarative DAG contracts and deterministic topology, independent of Ray."""

from __future__ import annotations

import heapq
from collections import defaultdict
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dataflow.contracts import OperatorSpec


class EdgeKind(StrEnum):
    DATA = "data"
    CONTROL = "control"


class PipelineEdgeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    from_node: str = Field(alias="from", min_length=1)
    to_node: str = Field(alias="to", min_length=1)
    kind: EdgeKind = EdgeKind.DATA


class PipelineSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    api_version: Literal["dataflow.io/v1alpha1"] = "dataflow.io/v1alpha1"
    kind: Literal["Pipeline"] = "Pipeline"
    name: str = Field(min_length=1)
    nodes: list[OperatorSpec] = Field(min_length=1)
    edges: list[PipelineEdgeSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_references(self) -> PipelineSpec:
        ids = [node.id for node in self.nodes]
        if len(ids) != len(set(ids)):
            raise ValueError("pipeline node ids must be unique")
        known = set(ids)
        seen: set[tuple[str, str, EdgeKind]] = set()
        for edge in self.edges:
            if edge.from_node not in known or edge.to_node not in known:
                raise ValueError(
                    f"edge references unknown node: {edge.from_node} -> {edge.to_node}"
                )
            if edge.from_node == edge.to_node:
                raise ValueError("self edges are not allowed")
            key = edge.from_node, edge.to_node, edge.kind
            if key in seen:
                raise ValueError(f"duplicate edge: {edge.from_node} -> {edge.to_node}")
            seen.add(key)
        return self


class LogicalGraph:
    """A static DAG with data and control dependency indexes."""

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
        result = []
        while ready:
            node_id = heapq.heappop(ready)
            result.append(node_id)
            for edge in sorted(self.outgoing[node_id], key=lambda item: (item.to_node, item.kind)):
                indegree[edge.to_node] -= 1
                if indegree[edge.to_node] == 0:
                    heapq.heappush(ready, edge.to_node)
        if len(result) != len(self.nodes):
            raise ValueError("pipeline graph contains a cycle")
        return result
