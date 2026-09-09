"""Execution of versioned DataFlow plans against Ray Data."""

from __future__ import annotations

import importlib
import os
from collections.abc import Callable
from typing import Any, Protocol
from urllib.parse import urlparse

from dataflow.artifacts import ArtifactOutputSpec
from dataflow.contracts import ExecutionPlan, OperatorKind, OperatorSpec, ResourceSpec


class DatasetLike(Protocol):
    def map_batches(self, fn: Callable[..., Any] | type, **kwargs: Any) -> DatasetLike: ...
    def filter(self, fn: Callable[..., Any], **kwargs: Any) -> DatasetLike: ...
    def write_parquet(self, path: str, **kwargs: Any) -> Any: ...


class RayDataBackend(Protocol):
    def read_parquet(self, path: str, **kwargs: Any) -> DatasetLike: ...


FilesystemResolver = Callable[[str], Any | None]


def resolve_symbol(path: str) -> Any:
    module_name, separator, symbol_name = path.rpartition(".")
    if not separator:
        raise ValueError(f"callable must be a fully-qualified symbol: {path!r}")
    module = importlib.import_module(module_name)
    return getattr(module, symbol_name)


class PlanExecutor:
    def __init__(
        self,
        backend: RayDataBackend,
        *,
        filesystem_resolver: FilesystemResolver | None = None,
    ) -> None:
        self._backend = backend
        self._filesystem_resolver = filesystem_resolver or filesystem_from_env

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
            self._apply_filesystem(path, config)
            return self._backend.read_parquet(path, **config)

        if dataset is None:
            raise RuntimeError(f"operator {operator.id!r} has no input dataset")

        if operator.kind is OperatorKind.MAP_BATCHES:
            callable_path = _pop_required(config, "callable", operator)
            _apply_worker_resources(config, operator.resources)
            return dataset.map_batches(resolve_symbol(callable_path), **config)

        if operator.kind is OperatorKind.FILTER:
            callable_path = _pop_required(config, "callable", operator)
            _apply_worker_resources(config, operator.resources)
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
            self._apply_filesystem(path, config)
            dataset.write_parquet(path, **config)
            return dataset

        raise ValueError(f"unsupported operator kind: {operator.kind}")

    def _apply_filesystem(self, path: str, config: dict[str, Any]) -> None:
        if "filesystem" in config:
            return
        filesystem = self._filesystem_resolver(path)
        if filesystem is not None:
            config["filesystem"] = filesystem


def _apply_worker_resources(config: dict[str, Any], resources: ResourceSpec) -> None:
    if resources.cpu is not None:
        config.setdefault("num_cpus", resources.cpu)
    if resources.gpu is not None:
        config.setdefault("num_gpus", resources.gpu)
    if resources.memory_bytes is not None:
        config.setdefault("memory", resources.memory_bytes)
    if resources.accelerator_type is not None:
        selector = dict(config.get("label_selector") or {})
        selector.setdefault("ray.io/accelerator-type", resources.accelerator_type)
        config["label_selector"] = selector


def filesystem_from_env(path: str) -> Any | None:
    """Build a PyArrow S3 filesystem for S3-compatible endpoints when configured."""

    if not path.startswith("s3://"):
        return None
    endpoint_url = os.environ.get("DATAFLOW_S3_ENDPOINT_URL")
    if not endpoint_url:
        return None

    parsed = urlparse(endpoint_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.path not in {"", "/"}:
        raise RuntimeError(
            "DATAFLOW_S3_ENDPOINT_URL must be an http(s) endpoint such as http://minio:9000"
        )

    try:
        from pyarrow import fs
    except ImportError as error:  # pragma: no cover - Ray runtime includes PyArrow.
        raise RuntimeError("PyArrow is required for S3-compatible Ray Data storage") from error

    return fs.S3FileSystem(
        access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
        secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        session_token=os.environ.get("AWS_SESSION_TOKEN"),
        region=os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-east-1",
        scheme=parsed.scheme,
        endpoint_override=parsed.netloc,
        force_virtual_addressing=False,
    )


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
