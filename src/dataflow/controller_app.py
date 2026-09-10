"""Standalone durable controller process for Kubernetes deployments."""

from __future__ import annotations

import argparse
import os
from collections.abc import Mapping, Sequence

from dataflow.admission import AdmissionPolicy, PostgresAdmissionController
from dataflow.api_repository import PostgresApiRepository
from dataflow.artifact_manager import ArtifactManager
from dataflow.artifacts import Boto3S3ObjectClient, S3ParquetArtifactStorage
from dataflow.controller import OrchestrationController
from dataflow.executor import RetryPolicy
from dataflow.kuberay import KubeRayExecutor, KubernetesRayJobClient
from dataflow.leadership import (
    DEFAULT_LOCK_KEY,
    DEFAULT_LOCK_NAMESPACE,
    PostgresControllerLeadership,
)
from dataflow.metrics_server import start_metrics_http_server
from dataflow.observability import Observability, configure_json_logging
from dataflow.reconciler import Reconciler
from dataflow.scheduler import Scheduler


def create_controller_from_env(
    *,
    observability: Observability | None = None,
) -> OrchestrationController:
    dsn = _database_url()
    repository = PostgresApiRepository(dsn)
    obs = observability or Observability.from_env()
    endpoint_url = os.environ.get("DATAFLOW_S3_ENDPOINT_URL")
    region_name = (
        os.environ.get("AWS_DEFAULT_REGION")
        or os.environ.get("AWS_REGION")
        or "us-east-1"
    )
    client_kwargs: dict[str, str] = {"region_name": region_name}
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    storage = S3ParquetArtifactStorage(
        Boto3S3ObjectClient.from_default_config(**client_kwargs)
    )
    executor = KubeRayExecutor(KubernetesRayJobClient.from_default_config())
    retry_policy = RetryPolicy(
        max_attempts=int(os.environ.get("DATAFLOW_RETRY_MAX_ATTEMPTS", "3")),
        initial_backoff_seconds=float(
            os.environ.get("DATAFLOW_RETRY_INITIAL_BACKOFF_SECONDS", "30")
        ),
        multiplier=float(os.environ.get("DATAFLOW_RETRY_MULTIPLIER", "2")),
        max_backoff_seconds=float(os.environ.get("DATAFLOW_RETRY_MAX_BACKOFF_SECONDS", "600")),
    )
    reconciler = Reconciler(
        repository,
        executor,
        retry_policy=retry_policy,
        artifact_manager=ArtifactManager(repository, storage),
        observability=obs,
    )
    admission = PostgresAdmissionController(
        dsn,
        AdmissionPolicy.from_env(os.environ),
        observability=obs,
    )
    return OrchestrationController(
        repository,
        Scheduler(repository),
        reconciler,
        admission=admission,
        observability=obs,
    )


def create_leadership_from_env() -> PostgresControllerLeadership:
    return PostgresControllerLeadership(
        _database_url(),
        lock_namespace=int(
            os.environ.get("DATAFLOW_CONTROLLER_LOCK_NAMESPACE", str(DEFAULT_LOCK_NAMESPACE))
        ),
        lock_key=int(os.environ.get("DATAFLOW_CONTROLLER_LOCK_KEY", str(DEFAULT_LOCK_KEY))),
    )


def controller_metrics_listen_port(environment: Mapping[str, str]) -> int:
    raw = environment.get("DATAFLOW_CONTROLLER_METRICS_LISTEN_PORT", "9091")
    try:
        port = int(raw)
    except ValueError as error:
        raise RuntimeError(
            "DATAFLOW_CONTROLLER_METRICS_LISTEN_PORT must be an integer"
        ) from error
    if not 1 <= port <= 65535:
        raise RuntimeError(
            "DATAFLOW_CONTROLLER_METRICS_LISTEN_PORT must be between 1 and 65535"
        )
    return port


def _database_url() -> str:
    dsn = os.environ.get("DATAFLOW_DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATAFLOW_DATABASE_URL is required")
    return dsn


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the DataFlow orchestration controller")
    parser.add_argument(
        "--once",
        action="store_true",
        help="perform one durable reconciliation pass and exit",
    )
    args = parser.parse_args(argv)

    if os.environ.get("DATAFLOW_JSON_LOGS", "true").lower() not in {"0", "false", "no"}:
        configure_json_logging()

    observability = Observability.from_env()
    controller = create_controller_from_env(observability=observability)
    if args.once:
        controller.reconcile_once()
        return

    metrics_host = os.environ.get("DATAFLOW_CONTROLLER_METRICS_HOST", "0.0.0.0")
    metrics_port = controller_metrics_listen_port(os.environ)
    metrics_server = start_metrics_http_server(
        observability.metrics,
        host=metrics_host,
        port=metrics_port,
    )
    if metrics_server is not None:
        observability.info(
            "controller_metrics_listening",
            host=metrics_host,
            port=metrics_port,
        )

    poll_seconds = float(os.environ.get("DATAFLOW_CONTROLLER_POLL_SECONDS", "2"))
    standby_seconds = float(
        os.environ.get("DATAFLOW_CONTROLLER_STANDBY_POLL_SECONDS", str(poll_seconds))
    )
    try:
        controller.run_forever(
            poll_interval_seconds=poll_seconds,
            leadership=create_leadership_from_env(),
            standby_poll_interval_seconds=standby_seconds,
        )
    finally:
        if metrics_server is not None:
            metrics_server.shutdown()
            metrics_server.server_close()


__all__ = [
    "controller_metrics_listen_port",
    "create_controller_from_env",
    "create_leadership_from_env",
    "main",
]
