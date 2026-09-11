"""Runtime build identity for correlating deployments with release artifacts."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version

_DISTRIBUTION = "dataflow-orchestrator"


def package_version() -> str:
    """Return the installed distribution version, with a source-tree fallback."""

    try:
        return distribution_version(_DISTRIBUTION)
    except PackageNotFoundError:
        return "0+unknown"


@dataclass(frozen=True, slots=True)
class BuildInfo:
    """Identity embedded in, or injected into, a running DataFlow process."""

    version: str
    commit: str | None = None
    image: str | None = None

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> BuildInfo:
        env = os.environ if environ is None else environ
        return cls(
            version=package_version(),
            commit=_optional_value(env.get("DATAFLOW_BUILD_COMMIT")),
            image=_optional_value(env.get("DATAFLOW_BUILD_IMAGE")),
        )


def _optional_value(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


__all__ = ["BuildInfo", "package_version"]
