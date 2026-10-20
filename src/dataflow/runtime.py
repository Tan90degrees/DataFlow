"""Thin adapters from operator declarations to Ray Data calls."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any, Protocol

from dataflow.contracts import JoinConfig, OperatorKind, OperatorSpec, ResourceSpec


class DatasetLike(Protocol):
    @staticmethod
    def copy(dataset: DatasetLike) -> DatasetLike: ...
    def materialize(self) -> DatasetLike: ...
    def union(self, *others: DatasetLike) -> DatasetLike: ...
    def join(self, other: DatasetLike, **kwargs: Any) -> DatasetLike: ...
    def schema(self) -> Any: ...
    def map_batches(self, fn: Callable[..., Any] | type, **kwargs: Any) -> DatasetLike: ...
    def filter(self, fn: Callable[..., Any], **kwargs: Any) -> DatasetLike: ...
    def write_parquet(self, path: str, **kwargs: Any) -> Any: ...


class RayDataBackend(Protocol):
    def read_parquet(self, path: str, **kwargs: Any) -> DatasetLike: ...


def resolve_symbol(path: str) -> Any:
    module_name, separator, symbol_name = path.rpartition(".")
    if not separator:
        raise ValueError(f"callable must be a fully-qualified symbol: {path!r}")
    return getattr(importlib.import_module(module_name), symbol_name)


def apply_resources(config: dict[str, Any], resources: ResourceSpec) -> None:
    """Forward scheduler hints without allowing contradictory duplicate declarations."""

    for key, value in (
        ("num_cpus", resources.cpu),
        ("num_gpus", resources.gpu),
        ("memory", resources.memory_bytes),
    ):
        if value is not None:
            if key in config and config[key] != value:
                raise ValueError(f"config.{key} conflicts with operator resources")
            config[key] = value
    if resources.accelerator_type is not None:
        selector = dict(config.get("label_selector") or {})
        key = "ray.io/accelerator-type"
        if key in selector and selector[key] != resources.accelerator_type:
            raise ValueError("config.label_selector conflicts with operator accelerator_type")
        selector[key] = resources.accelerator_type
        config["label_selector"] = selector


def operator_config(operator: OperatorSpec) -> dict[str, Any]:
    config = dict(operator.config)
    if operator.kind is OperatorKind.UNION:
        if config or operator.resources.model_dump(exclude_none=True):
            raise ValueError("union takes no config/resources; configure its input operators")
        return config
    if operator.kind is OperatorKind.JOIN:
        remote = dict(config.pop("aggregator_ray_remote_args", None) or {})
        apply_resources(remote, operator.resources)
        if remote:
            config["aggregator_ray_remote_args"] = remote
        return JoinConfig.model_validate(config).model_dump(exclude_none=True)
    if operator.kind in {OperatorKind.READ_PARQUET, OperatorKind.WRITE_PARQUET}:
        remote = dict(config.pop("ray_remote_args", None) or {})
        apply_resources(remote, operator.resources)
        # read_parquet also accepts direct resource arguments; reject contradictory hints.
        if operator.kind is OperatorKind.READ_PARQUET:
            for key in ("num_cpus", "num_gpus", "memory"):
                if key in config and key in remote and config[key] != remote[key]:
                    raise ValueError(f"config.{key} conflicts with operator resources")
        if remote:
            config["ray_remote_args"] = remote
    else:
        apply_resources(config, operator.resources)
    return config


class OperatorExecutor:
    def __init__(self, backend: RayDataBackend) -> None:
        self._backend = backend

    def apply_operator(
        self,
        dataset: DatasetLike | None,
        operator: OperatorSpec,
        *,
        inputs: list[DatasetLike] | None = None,
    ) -> DatasetLike:
        config = operator_config(operator)
        if operator.kind is OperatorKind.UNION:
            assert inputs is not None
            inputs = [item.copy(item) for item in inputs]
            schemas = [item.schema() for item in inputs]
            if any(schema is None for schema in schemas):
                raise ValueError("union requires known input schemas")
            if any(
                (schema.names, schema.types) != (schemas[0].names, schemas[0].types)
                for schema in schemas[1:]
            ):
                raise ValueError("union inputs must have identical column names and types")
            return inputs[0].union(*inputs[1:])
        if operator.kind is OperatorKind.JOIN:
            assert inputs is not None
            return inputs[0].copy(inputs[0]).join(inputs[1].copy(inputs[1]), **config)
        if operator.kind is OperatorKind.READ_PARQUET:
            return self._backend.read_parquet(config.pop("path"), **config)
        if dataset is None:
            raise RuntimeError(f"operator {operator.id!r} has no input dataset")
        # Ray writes attach mutable execution state to Dataset instances. Copy handles,
        # not blocks, so concurrent branches do not race on the same Python Dataset.
        dataset = dataset.copy(dataset)
        if operator.kind is OperatorKind.MAP_BATCHES:
            return dataset.map_batches(resolve_symbol(config.pop("callable")), **config)
        if operator.kind is OperatorKind.FILTER:
            return dataset.filter(resolve_symbol(config.pop("callable")), **config)
        if operator.kind is OperatorKind.WRITE_PARQUET:
            dataset.write_parquet(config.pop("path"), **config)
            return dataset
        raise ValueError(f"unsupported operator kind: {operator.kind}")
