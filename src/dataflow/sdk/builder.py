"""Python-first authoring surface that emits canonical PipelineSpec objects."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from inspect import isclass, isfunction
from typing import Any, Generic, ParamSpec, TypeVar

from dataflow.compiler import (
    ExecutionBoundary,
    PipelineEdgeSpec,
    PipelineNodeSpec,
    PipelineSpec,
)
from dataflow.contracts import OperatorKind, ResourceSpec, RuntimeSpec
from dataflow.metadata.repository import canonical_spec_hash

P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True, slots=True)
class NodeHandle:
    builder: PipelineBuilder
    node_id: str


@dataclass(frozen=True, slots=True)
class DatasetNode(NodeHandle):
    boundary: ExecutionBoundary = ExecutionBoundary.NONE

    def checkpoint(self) -> DatasetNode:
        """Force the next data edge to become a durable checkpoint boundary."""

        return DatasetNode(self.builder, self.node_id, ExecutionBoundary.CHECKPOINT)

    def hard_boundary(self) -> DatasetNode:
        """Force the next data edge into a separate execution island."""

        return DatasetNode(self.builder, self.node_id, ExecutionBoundary.HARD)

    def map_batches(
        self,
        fn: str | Callable[..., Any] | type,
        *,
        node_id: str | None = None,
        resources: ResourceSpec | None = None,
        runtime: RuntimeSpec | None = None,
        cluster_profile: str | None = None,
        **config: Any,
    ) -> DatasetNode:
        config = dict(config)
        config["callable"] = callable_ref(fn)
        return self.builder._append_dataset_node(
            self,
            kind=OperatorKind.MAP_BATCHES,
            node_id=node_id,
            config=config,
            resources=resources,
            runtime=runtime,
            cluster_profile=cluster_profile,
        )

    def filter(
        self,
        fn: str | Callable[..., Any],
        *,
        node_id: str | None = None,
        resources: ResourceSpec | None = None,
        runtime: RuntimeSpec | None = None,
        cluster_profile: str | None = None,
        **config: Any,
    ) -> DatasetNode:
        config = dict(config)
        config["callable"] = callable_ref(fn)
        return self.builder._append_dataset_node(
            self,
            kind=OperatorKind.FILTER,
            node_id=node_id,
            config=config,
            resources=resources,
            runtime=runtime,
            cluster_profile=cluster_profile,
        )

    def write_parquet(
        self,
        path: str,
        *,
        node_id: str | None = None,
        resources: ResourceSpec | None = None,
        runtime: RuntimeSpec | None = None,
        cluster_profile: str | None = None,
        **config: Any,
    ) -> NodeHandle:
        config = dict(config)
        config["path"] = path
        return self.builder._append_sink_node(
            self,
            kind=OperatorKind.WRITE_PARQUET,
            node_id=node_id,
            config=config,
            resources=resources,
            runtime=runtime,
            cluster_profile=cluster_profile,
        )


class PipelineBuilder:
    """Mutable graph builder used only while a PipelineDefinition is evaluated."""

    def __init__(
        self,
        *,
        name: str,
        runtime: RuntimeSpec,
        cluster_profile: str = "default",
        artifact_base_uri: str | None = None,
    ) -> None:
        self.name = name
        self.runtime = runtime
        self.cluster_profile = cluster_profile
        self.artifact_base_uri = artifact_base_uri
        self._nodes: list[PipelineNodeSpec] = []
        self._edges: list[PipelineEdgeSpec] = []
        self._node_ids: set[str] = set()
        self._sequence = 0

    def read_parquet(
        self,
        path: str,
        *,
        node_id: str | None = None,
        resources: ResourceSpec | None = None,
        runtime: RuntimeSpec | None = None,
        cluster_profile: str | None = None,
        **config: Any,
    ) -> DatasetNode:
        config = dict(config)
        config["path"] = path
        node = self._new_node(
            kind=OperatorKind.READ_PARQUET,
            node_id=node_id,
            config=config,
            resources=resources,
            runtime=runtime,
            cluster_profile=cluster_profile,
        )
        return DatasetNode(self, node.id)

    def to_spec(self) -> PipelineSpec:
        spec = PipelineSpec(
            name=self.name,
            runtime=self.runtime,
            cluster_profile=self.cluster_profile,
            artifact_base_uri=self.artifact_base_uri,
            nodes=list(self._nodes),
            edges=list(self._edges),
        )
        # Fail authoring early if arbitrary config values cannot be serialized as API JSON.
        json.dumps(spec.model_dump(mode="json", by_alias=True), sort_keys=True)
        return spec

    def _append_dataset_node(
        self,
        source: DatasetNode,
        *,
        kind: OperatorKind,
        node_id: str | None,
        config: dict[str, Any],
        resources: ResourceSpec | None,
        runtime: RuntimeSpec | None,
        cluster_profile: str | None,
    ) -> DatasetNode:
        self._require_owned(source)
        node = self._new_node(
            kind=kind,
            node_id=node_id,
            config=config,
            resources=resources,
            runtime=runtime,
            cluster_profile=cluster_profile,
        )
        self._connect(source, node.id)
        return DatasetNode(self, node.id)

    def _append_sink_node(
        self,
        source: DatasetNode,
        *,
        kind: OperatorKind,
        node_id: str | None,
        config: dict[str, Any],
        resources: ResourceSpec | None,
        runtime: RuntimeSpec | None,
        cluster_profile: str | None,
    ) -> NodeHandle:
        self._require_owned(source)
        node = self._new_node(
            kind=kind,
            node_id=node_id,
            config=config,
            resources=resources,
            runtime=runtime,
            cluster_profile=cluster_profile,
        )
        self._connect(source, node.id)
        return NodeHandle(self, node.id)

    def _new_node(
        self,
        *,
        kind: OperatorKind,
        node_id: str | None,
        config: dict[str, Any],
        resources: ResourceSpec | None,
        runtime: RuntimeSpec | None,
        cluster_profile: str | None,
    ) -> PipelineNodeSpec:
        resolved_id = node_id or self._next_node_id(kind)
        if resolved_id in self._node_ids:
            raise ValueError(f"duplicate SDK node id: {resolved_id!r}")
        if resolved_id.startswith("__dataflow_"):
            raise ValueError("node ids starting with '__dataflow_' are reserved")
        node = PipelineNodeSpec(
            id=resolved_id,
            kind=kind,
            config=config,
            resources=resources or ResourceSpec(),
            runtime=runtime,
            cluster_profile=cluster_profile,
        )
        self._nodes.append(node)
        self._node_ids.add(node.id)
        return node

    def _connect(self, source: DatasetNode, target_id: str) -> None:
        self._edges.append(
            PipelineEdgeSpec.model_validate(
                {
                    "from": source.node_id,
                    "to": target_id,
                    "kind": "data",
                    "boundary": source.boundary,
                }
            )
        )

    def _next_node_id(self, kind: OperatorKind) -> str:
        self._sequence += 1
        prefix = kind.value.replace("_", "-")
        return f"{prefix}-{self._sequence:03d}"

    def _require_owned(self, node: NodeHandle) -> None:
        if node.builder is not self:
            raise ValueError("cannot connect nodes from different PipelineBuilder instances")
        if node.node_id not in self._node_ids:
            raise ValueError(f"unknown SDK node: {node.node_id!r}")


class PipelineDefinition(Generic[P]):
    """Reusable pipeline factory produced by the `@pipeline` decorator."""

    def __init__(
        self,
        factory: Callable[..., Any],
        *,
        name: str,
        runtime: RuntimeSpec,
        cluster_profile: str,
        artifact_base_uri: str | None,
    ) -> None:
        self._factory = factory
        self.name = name
        self.runtime = runtime
        self.cluster_profile = cluster_profile
        self.artifact_base_uri = artifact_base_uri
        self.__name__ = getattr(factory, "__name__", name)
        self.__doc__ = getattr(factory, "__doc__", None)

    def spec(self, *args: P.args, **kwargs: P.kwargs) -> PipelineSpec:
        builder = PipelineBuilder(
            name=self.name,
            runtime=self.runtime,
            cluster_profile=self.cluster_profile,
            artifact_base_uri=self.artifact_base_uri,
        )
        self._factory(builder, *args, **kwargs)
        return builder.to_spec()

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> PipelineSpec:
        return self.spec(*args, **kwargs)

    def to_dict(self, *args: P.args, **kwargs: P.kwargs) -> dict[str, Any]:
        return self.spec(*args, **kwargs).model_dump(mode="json", by_alias=True)

    def spec_hash(self, *args: P.args, **kwargs: P.kwargs) -> str:
        return canonical_spec_hash(self.to_dict(*args, **kwargs))


def pipeline(
    *,
    name: str,
    runtime: RuntimeSpec | None = None,
    runtime_image: str | None = None,
    cluster_profile: str = "default",
    artifact_base_uri: str | None = None,
) -> Callable[[Callable[..., Any]], PipelineDefinition[Any]]:
    """Decorate a graph-building function without executing distributed work."""

    resolved_runtime = _resolve_runtime(runtime, runtime_image)
    return partial(
        PipelineDefinition,
        name=name,
        runtime=resolved_runtime,
        cluster_profile=cluster_profile,
        artifact_base_uri=artifact_base_uri,
    )


def callable_ref(value: str | Callable[..., Any] | type) -> str:
    """Return a runtime-importable top-level symbol path."""

    if isinstance(value, str):
        module, separator, symbol = value.rpartition(".")
        if not separator or not module or not symbol:
            raise ValueError(f"callable must be a fully-qualified symbol: {value!r}")
        return value

    if not (isfunction(value) or isclass(value)):
        raise TypeError("callable must be a fully-qualified string, top-level function, or class")

    module = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)
    if not module or not qualname:
        raise ValueError("callable does not expose an importable module and name")
    if module == "__main__":
        raise ValueError("callables defined in __main__ are not reproducibly importable")
    if "<locals>" in qualname or "<lambda>" in qualname or "." in qualname:
        raise ValueError("callable must be a top-level function or class")
    return f"{module}.{qualname}"


def _resolve_runtime(
    runtime: RuntimeSpec | None,
    runtime_image: str | None,
) -> RuntimeSpec:
    if runtime is not None and runtime_image is not None:
        raise ValueError("specify either runtime or runtime_image, not both")
    if runtime is not None:
        return runtime
    if runtime_image is not None:
        return RuntimeSpec(image=runtime_image)
    raise ValueError("pipeline requires runtime or runtime_image")


__all__ = [
    "DatasetNode",
    "NodeHandle",
    "PipelineBuilder",
    "PipelineDefinition",
    "callable_ref",
    "pipeline",
]
