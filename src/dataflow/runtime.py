"""Execution of versioned DataFlow plans against Ray Data."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any, Protocol

from dataflow.artifacts import ArtifactOutputSpec
from dataflow.contracts import ExecutionPlan, OperatorKind, OperatorSpec


class DatasetLike(Protocol):
    def map_batches(self, fn: Callable[..., Any] | type, **kwargs: Any) -> DatasetLike: ...
    def filter(self, fn: Callable[..., Any], **kwargs: Any) -> DatasetLike: ...
    def write_parquet(self, path: str, **kwargs: Any) -> Any: ...


class RayDataBackend(Protocol):
    def read_parquet(self, path: str, **kwargs: Any) -> DatasetLike: ...


def resolve_symbol(path: str) -> Any:
    module_name, separator, symbol_name = path.rpartition(".")
    if not separator:
        raise ValueError(f"callable must be a fully-qualified symbol: {path!r}")
    module = importlib.import_module(module_name)
    return getattr(module, symbol_name)


class PlanExecutor:
    def __init__(self, backend: RayDataBackend) -> None:
        self._backend = backend

    def execute(self, plan: ExecutionPlan, *, attempt_number: int | None = None) -> None:
        dataset: DatasetLike | None = None
        artifact_outputs = {output.operator_id: output for output in plan.output_artifacts}
        for operator in plan.operators:
            dataset = self._apply(
                dataset,
                operator,
                artifact_output=artifact_outputs.get(operator.id),
                attempt_number=attempt_number,
            )

    def _apply(
        self,
        dataset: DatasetLike | None,
        operator: OperatorSpec,
        *,
        artifact_output: ArtifactOutputSpec | None = None,
        attempt_number: int | None = None,
    ) -> DatasetLike | None:
        config = dict(operator.config)

        if operator.kind is OperatorKind.READ_PARQUET:
            path = _pop_required(config, "path", operator)
            return self._backend.read_parquet(path, **config)

        if dataset is None:
            raise RuntimeError(f"operator {operator.id!r} has no input dataset")

        if operator.kind is OperatorKind.MAP_BATCHES:
            callable_path = _pop_required(config, "callable", operator)
            fn = resolve_symbol(callable_path)
            if operator.resources.cpu is not None:
                config.setdefault("num_cpus", operator.resources.cpu)
            if operator.resources.gpu is not None:
                config.setdefault("num_gpus", operator.resources.gpu)
            return dataset.map_batches(fn, **config)

        if operator.kind is OperatorKind.FILTER:
            callable_path = _pop_required(config, "callable", operator)
            return dataset.filter(resolve_symbol(callable_path), **config)

        if operator.kind is OperatorKind.WRITE_PARQUET:
            if artifact_output is not None:
                if attempt_number is None:
                    raise RuntimeError(
                        f"durable artifact operator {operator.id!r} requires an attempt number"
                    )
                config.pop("path", None)
                path = artifact_output.staging_uri(attempt_number)
            else:
                path = _pop_required(config, "path", operator)
            dataset.write_parquet(path, **config)
            return dataset

        raise ValueError(f"unsupported operator kind: {operator.kind}")


def _pop_required(config: dict[str, Any], key: str, operator: OperatorSpec) -> Any:
    try:
        return config.pop(key)
    except KeyError as exc:
        raise ValueError(f"operator {operator.id!r} requires config.{key}") from exc


def execute_with_ray(plan: ExecutionPlan, *, attempt_number: int | None = None) -> None:
    import ray
    import ray.data

    ray.init(address=plan.runtime.ray_address, namespace=plan.runtime.namespace)
    try:
        PlanExecutor(ray.data).execute(plan, attempt_number=attempt_number)
    finally:
        ray.shutdown()
