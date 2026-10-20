"""Python authoring DSL that emits a serializable pipeline without initializing Ray."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from inspect import isclass, isfunction
from typing import Any, Generic, ParamSpec

from dataflow.contracts import OperatorKind, OperatorSpec, ResourceSpec
from dataflow.graph import PipelineEdgeSpec, PipelineSpec
from dataflow.spec_hash import canonical_spec_hash

P = ParamSpec("P")


@dataclass(frozen=True, slots=True)
class NodeHandle:
    builder: PipelineBuilder
    node_id: str


@dataclass(frozen=True, slots=True)
class DatasetNode(NodeHandle):
    def cache(self) -> DatasetNode:
        """Materialize this node once per run, reusing blocks across consumers."""
        self.builder._require_owned(self)
        self.builder._nodes_by_id[self.node_id].cache = True
        return self

    def union(self, *others: DatasetNode, node_id: str | None = None) -> DatasetNode:
        """Append rows of same-schema datasets in input order; never align or deduplicate."""
        return self.builder._merge(OperatorKind.UNION, (self, *others), node_id=node_id)

    def join(
        self,
        right: DatasetNode,
        *,
        on: str | list[str],
        num_partitions: int,
        join_type: str = "inner",
        node_id: str | None = None,
        resources: ResourceSpec | None = None,
        **config: Any,
    ) -> DatasetNode:
        """Key-based shuffle join with explicit partition count and left/right ports."""
        config.update(
            on=[on] if isinstance(on, str) else on,
            num_partitions=num_partitions,
            join_type=join_type,
        )
        return self.builder._merge(
            OperatorKind.JOIN, (self, right), node_id=node_id, resources=resources, config=config
        )

    def map_batches(
        self,
        fn: str | Callable[..., Any] | type,
        *,
        node_id: str | None = None,
        resources: ResourceSpec | None = None,
        **config: Any,
    ) -> DatasetNode:
        config["callable"] = callable_ref(fn)
        node_id = self.builder._add(
            OperatorKind.MAP_BATCHES,
            node_id=node_id,
            config=config,
            resources=resources,
            source=self,
        )
        return DatasetNode(self.builder, node_id)

    def filter(
        self,
        fn: str | Callable[..., Any],
        *,
        node_id: str | None = None,
        resources: ResourceSpec | None = None,
        **config: Any,
    ) -> DatasetNode:
        config["callable"] = callable_ref(fn)
        node_id = self.builder._add(
            OperatorKind.FILTER,
            node_id=node_id,
            config=config,
            resources=resources,
            source=self,
        )
        return DatasetNode(self.builder, node_id)

    def write_parquet(
        self,
        path: str,
        *,
        node_id: str | None = None,
        resources: ResourceSpec | None = None,
        **config: Any,
    ) -> NodeHandle:
        config["path"] = path
        node_id = self.builder._add(
            OperatorKind.WRITE_PARQUET,
            node_id=node_id,
            config=config,
            resources=resources,
            source=self,
        )
        return NodeHandle(self.builder, node_id)


class PipelineBuilder:
    def __init__(self, *, name: str) -> None:
        self.name = name
        self._nodes: list[OperatorSpec] = []
        self._edges: list[PipelineEdgeSpec] = []
        self._nodes_by_id: dict[str, OperatorSpec] = {}
        self._node_ids: set[str] = set()
        self._sequence = 0

    def read_parquet(
        self,
        path: str,
        *,
        node_id: str | None = None,
        resources: ResourceSpec | None = None,
        **config: Any,
    ) -> DatasetNode:
        config["path"] = path
        node_id = self._add(
            OperatorKind.READ_PARQUET,
            node_id=node_id,
            config=config,
            resources=resources,
        )
        return DatasetNode(self, node_id)

    def to_spec(self) -> PipelineSpec:
        spec = PipelineSpec(name=self.name, nodes=list(self._nodes), edges=list(self._edges))
        json.dumps(spec.model_dump(mode="json", by_alias=True), sort_keys=True)
        return spec

    def _add(
        self,
        kind: OperatorKind,
        *,
        node_id: str | None,
        config: dict[str, Any],
        resources: ResourceSpec | None,
        source: DatasetNode | None = None,
    ) -> str:
        if source is not None:
            self._require_owned(source)
        if node_id is None:
            self._sequence += 1
            node_id = f"{kind.value.replace('_', '-')}-{self._sequence:03d}"
        if node_id in self._node_ids:
            raise ValueError(f"duplicate SDK node id: {node_id!r}")
        self._nodes.append(
            OperatorSpec(
                id=node_id,
                kind=kind,
                config=config,
                resources=resources or ResourceSpec(),
            )
        )
        self._node_ids.add(node_id)
        self._nodes_by_id[node_id] = self._nodes[-1]
        if source is not None:
            self._edges.append(PipelineEdgeSpec(from_node=source.node_id, to_node=node_id))
        return node_id

    def _require_owned(self, node: NodeHandle) -> None:
        if node.builder is not self:
            raise ValueError("cannot connect nodes from different PipelineBuilder instances")
        if node.node_id not in self._node_ids:
            raise ValueError(f"unknown SDK node: {node.node_id!r}")

    def after(self, source: NodeHandle, target: NodeHandle) -> None:
        """Require completed source work before constructing the target's data plan."""
        self._require_owned(source)
        self._require_owned(target)
        self._edges.append(
            PipelineEdgeSpec(from_node=source.node_id, to_node=target.node_id, kind="control")
        )

    def _merge(
        self,
        kind: OperatorKind,
        inputs: tuple[DatasetNode, ...],
        *,
        node_id: str | None,
        resources: ResourceSpec | None = None,
        config: dict[str, Any] | None = None,
    ) -> DatasetNode:
        if len(inputs) < 2:
            raise ValueError("a merge requires at least two datasets")
        for node in inputs:
            self._require_owned(node)
        if len({node.node_id for node in inputs}) != len(inputs):
            raise ValueError("merge inputs must be distinct nodes")
        target = self._add(kind, node_id=node_id, resources=resources, config=config or {})
        self._edges.extend(
            PipelineEdgeSpec(from_node=node.node_id, to_node=target, input_index=index)
            for index, node in enumerate(inputs)
        )
        return DatasetNode(self, target)


class PipelineDefinition(Generic[P]):
    def __init__(self, factory: Callable[..., Any], *, name: str) -> None:
        self._factory = factory
        self.name = name
        self.__name__ = getattr(factory, "__name__", name)
        self.__doc__ = getattr(factory, "__doc__", None)

    def spec(self, *args: P.args, **kwargs: P.kwargs) -> PipelineSpec:
        builder = PipelineBuilder(name=self.name)
        self._factory(builder, *args, **kwargs)
        return builder.to_spec()

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> PipelineSpec:
        return self.spec(*args, **kwargs)

    def to_dict(self, *args: P.args, **kwargs: P.kwargs) -> dict[str, Any]:
        return self.spec(*args, **kwargs).model_dump(mode="json", by_alias=True)

    def spec_hash(self, *args: P.args, **kwargs: P.kwargs) -> str:
        return canonical_spec_hash(self.to_dict(*args, **kwargs))


def pipeline(*, name: str) -> Callable[[Callable[..., Any]], PipelineDefinition[Any]]:
    """Decorate a graph-building function. Authoring performs no distributed execution."""

    return partial(PipelineDefinition, name=name)


def callable_ref(value: str | Callable[..., Any] | type) -> str:
    if isinstance(value, str):
        module, separator, symbol = value.rpartition(".")
        if not separator or not module or not symbol:
            raise ValueError(f"callable must be a fully-qualified symbol: {value!r}")
        return value
    if not (isfunction(value) or isclass(value)):
        raise TypeError("callable must be a fully-qualified string, top-level function, or class")
    module, qualname = getattr(value, "__module__", None), getattr(value, "__qualname__", None)
    if not module or not qualname:
        raise ValueError("callable does not expose an importable module and name")
    if module == "__main__":
        raise ValueError("callables defined in __main__ are not reproducibly importable")
    if "<locals>" in qualname or "<lambda>" in qualname or "." in qualname:
        raise ValueError("callable must be a top-level function or class")
    return f"{module}.{qualname}"
