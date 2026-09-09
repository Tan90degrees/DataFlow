"""Standalone durable controller process for Kubernetes deployments."""

from __future__ import annotations

import os

from dataflow.api_repository import PostgresApiRepository
from dataflow.artifact_manager import ArtifactManager
from dataflow.artifacts import Boto3S3ObjectClient, S3ParquetArtifactStorage
from dataflow.controller import OrchestrationController
from dataflow.kuberay import KubeRayExecutor, KubernetesRayJobClient
from dataflow.observability import Observability, configure_json_logging
from dataflow.reconciler import Reconciler
from dataflow.scheduler import Scheduler


def create_controller_from_env() -> OrchestrationController:
    dsn = os.environ.get("DATAFLOW_DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATAFLOW_DATABASE_URL is required")

    repository = PostgresApiRepository(dsn)
    observability = Observability.from_env()
    endpoint_url = os.environ.get("DATAFLOW_S3_ENDPOINT_URL")
    region_name = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-east-1"
    client_kwargs: dict[str, str] = {"region_name": region_name}
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    storage = S3ParquetArtifactStorage(
        Boto3S3ObjectClient.from_default_config(**client_kwargs)
    )
    executor = KubeRayExecutor(KubernetesRayJobClient.from_default_config())
    reconciler = Reconciler(
        repository,
        executor,
        artifact_manager=ArtifactManager(repository, storage),
        observability=observability,
    )
    return OrchestrationController(
        repository,
        Scheduler(repository),
        reconciler,
        observability=observability,
    )


def main() -> None:
    if os.environ.get("DATAFLOW_JSON_LOGS", "true").lower() not in {"0", "false", "no"}:
        configure_json_logging()
    poll_seconds = float(os.environ.get("DATAFLOW_CONTROLLER_POLL_SECONDS", "2"))
    create_controller_from_env().run_forever(poll_interval_seconds=poll_seconds)


__all__ = ["create_controller_from_env", "main"]
