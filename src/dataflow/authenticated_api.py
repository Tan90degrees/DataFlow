"""Authenticated production factory for the DataFlow control-plane API."""

from __future__ import annotations

import os
from collections.abc import Mapping

from fastapi import FastAPI

from dataflow.api import create_app as create_core_app
from dataflow.api_repository import PostgresApiRepository
from dataflow.api_service import ControlPlaneService
from dataflow.auth import ApiAuthSettings, install_api_auth
from dataflow.observability import Observability


def create_app(
    service: ControlPlaneService,
    *,
    auth_settings: ApiAuthSettings,
    observability: Observability | None = None,
) -> FastAPI:
    """Create the public API and install the configured authentication boundary."""

    obs = observability or Observability.from_env()
    app = create_core_app(service, observability=obs)
    return install_api_auth(app, auth_settings, observability=obs)


def create_app_from_env(environ: Mapping[str, str] | None = None) -> FastAPI:
    """Create the production API using database, observability and auth environment."""

    env = os.environ if environ is None else environ
    dsn = env.get("DATAFLOW_DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATAFLOW_DATABASE_URL is required")
    observability = Observability.from_env()
    service = ControlPlaneService(PostgresApiRepository(dsn), observability=observability)
    return create_app(
        service,
        auth_settings=ApiAuthSettings.from_env(env),
        observability=observability,
    )


__all__ = ["create_app", "create_app_from_env"]
