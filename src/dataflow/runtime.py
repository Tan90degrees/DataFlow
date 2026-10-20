"""Thin adapters from operator declarations to Ray Data calls."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any, Protocol

from dataflow.contracts import OperatorKind, OperatorSpec, ResourceSpec


class DatasetLike(Protocol):
    def materialize(self) -> DatasetLike: ...
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
    if operator.kind in {OperatorKind.READ_PARQUET, OperatorKind.WRITE_PARQUET}:
        remote = dict(config.pop("ray_remote_args", {}))
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

    def apply_operator(self, dataset: DatasetLike | None, operator: OperatorSpec) -> DatasetLike:
        config = operator_config(operator)
        if operator.kind is OperatorKind.READ_PARQUET:
            return self._backend.read_parquet(config.pop("path"), **config)
        if dataset is None:
            raise RuntimeError(f"operator {operator.id!r} has no input dataset")
        if operator.kind is OperatorKind.MAP_BATCHES:
            return dataset.map_batches(resolve_symbol(config.pop("callable")), **config)
        if operator.kind is OperatorKind.FILTER:
            return dataset.filter(resolve_symbol(config.pop("callable")), **config)
        if operator.kind is OperatorKind.WRITE_PARQUET:
            dataset.write_parquet(config.pop("path"), **config)
            return dataset
        raise ValueError(f"unsupported operator kind: {operator.kind}")
