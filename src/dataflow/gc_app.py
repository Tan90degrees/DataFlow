"""One-shot retention garbage collector entry point."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping, Sequence

from dataflow.artifacts import Boto3S3ObjectClient, S3ParquetArtifactStorage
from dataflow.observability import Observability, configure_json_logging
from dataflow.retention import PostgresRetentionGc, RetentionPolicy


def create_gc_from_env(
    environment: Mapping[str, str] | None = None,
    *,
    observability: Observability | None = None,
) -> PostgresRetentionGc:
    env = os.environ if environment is None else environment
    dsn = env.get("DATAFLOW_DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATAFLOW_DATABASE_URL is required")

    region_name = (
        env.get("AWS_DEFAULT_REGION")
        or env.get("AWS_REGION")
        or "us-east-1"
    )
    client_kwargs: dict[str, str] = {"region_name": region_name}
    endpoint_url = env.get("DATAFLOW_S3_ENDPOINT_URL")
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url

    storage = S3ParquetArtifactStorage(
        Boto3S3ObjectClient.from_default_config(**client_kwargs)
    )
    return PostgresRetentionGc(
        dsn,
        storage,
        RetentionPolicy.from_env(env),
        observability=observability or Observability.from_env(),
    )


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run one bounded DataFlow retention garbage-collection pass"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report currently eligible work without changing PostgreSQL or object storage",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="pretty-print the JSON report",
    )
    args = parser.parse_args(argv)

    if os.environ.get("DATAFLOW_JSON_LOGS", "true").lower() not in {"0", "false", "no"}:
        configure_json_logging()

    collector = create_gc_from_env()
    report = collector.run_once(dry_run=args.dry_run)
    print(
        json.dumps(
            report.as_dict(),
            sort_keys=True,
            indent=2 if args.pretty else None,
            separators=None if args.pretty else (",", ":"),
        )
    )


__all__ = ["create_gc_from_env", "main"]
