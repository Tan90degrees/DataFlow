"""Public Python SDK for authoring and submitting DataFlow pipelines."""

from dataflow.contracts import ResourceSpec as Resources
from dataflow.contracts import RuntimeSpec as Runtime
from dataflow.sdk.builder import (
    DatasetNode,
    NodeHandle,
    PipelineBuilder,
    PipelineDefinition,
    callable_ref,
    pipeline,
)
from dataflow.single_cluster import PipelineExecutionError, PipelineResult, run_pipeline


def __getattr__(name: str):
    # The default execution path needs neither HTTP nor PostgreSQL dependencies.
    if name in {"DataFlowApiError", "DataFlowClient", "Submission"}:
        from dataflow.sdk import client

        return getattr(client, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "DataFlowApiError",
    "DataFlowClient",
    "DatasetNode",
    "NodeHandle",
    "PipelineBuilder",
    "PipelineDefinition",
    "PipelineExecutionError",
    "PipelineResult",
    "Resources",
    "Runtime",
    "Submission",
    "callable_ref",
    "pipeline",
    "run_pipeline",
]
