"""Installed package version, without runtime or service dependencies."""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version


def package_version() -> str:
    try:
        return distribution_version("dataflow-orchestrator")
    except PackageNotFoundError:
        return "0+unknown"
