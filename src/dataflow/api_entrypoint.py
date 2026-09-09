"""Process entrypoint for the DataFlow HTTP control plane."""

from __future__ import annotations

import os
from collections.abc import Mapping

from dataflow.observability import configure_json_logging


def api_listen_port(environ: Mapping[str, str] | None = None) -> int:
    """Resolve the HTTP listen port without colliding with Kubernetes Service env vars."""

    env = os.environ if environ is None else environ
    preferred = env.get("DATAFLOW_API_LISTEN_PORT")
    if preferred:
        return _parse_port("DATAFLOW_API_LISTEN_PORT", preferred)

    legacy = env.get("DATAFLOW_API_PORT")
    if legacy:
        # Kubernetes injects <SERVICE>_PORT=tcp://<cluster-ip>:<port> for Services.
        # A Service named dataflow-api therefore owns DATAFLOW_API_PORT unless users
        # explicitly disable service links. Treat that value as infrastructure metadata,
        # not as a DataFlow listen-port override.
        if legacy.startswith("tcp://"):
            return 8080
        return _parse_port("DATAFLOW_API_PORT", legacy)

    return 8080


def _parse_port(name: str, value: str) -> int:
    try:
        port = int(value)
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer TCP port, got {value!r}") from error
    if not 1 <= port <= 65535:
        raise RuntimeError(f"{name} must be between 1 and 65535, got {port}")
    return port


def main() -> None:
    try:
        import uvicorn
    except ImportError as error:
        raise RuntimeError("install DataFlow with the 'api' extra to run the API server") from error

    if os.environ.get("DATAFLOW_JSON_LOGS", "false").lower() in {"1", "true", "yes"}:
        configure_json_logging()
    uvicorn.run(
        "dataflow.api:create_app_from_env",
        factory=True,
        host=os.environ.get("DATAFLOW_API_HOST", "0.0.0.0"),
        port=api_listen_port(),
    )


__all__ = ["api_listen_port", "main"]
