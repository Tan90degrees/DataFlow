"""DataFlow core package."""

from dataflow.compiler import (
    EdgeKind,
    ExecutionBoundary,
    ExecutionGraph,
    ExecutionUnit,
    LogicalGraph,
    PipelineCompiler,
    PipelineEdgeSpec,
    PipelineNodeSpec,
    PipelineSpec,
    compile_pipeline,
)
from dataflow.contracts import ExecutionPlan, OperatorKind, OperatorSpec, ResourceSpec, RuntimeSpec
from dataflow.version import package_version

__version__ = package_version()

__all__ = [
    "EdgeKind",
    "ExecutionBoundary",
    "ExecutionGraph",
    "ExecutionPlan",
    "ExecutionUnit",
    "LogicalGraph",
    "OperatorKind",
    "OperatorSpec",
    "PipelineCompiler",
    "PipelineEdgeSpec",
    "PipelineNodeSpec",
    "PipelineSpec",
    "ResourceSpec",
    "RuntimeSpec",
    "__version__",
    "compile_pipeline",
]
