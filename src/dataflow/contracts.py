"""Core execution-plan contracts for DataFlow."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dataflow.artifacts import ArtifactOutputSpec, ArtifactRef
from dataflow.cluster_profiles import ClusterProfileSnapshot


class OperatorKind(StrEnum):
    READ_PARQUET = "read_parquet"
    MAP_BATCHES = "map_batches"
    FILTER = "filter"
    WRITE_PARQUET = "write_parquet"


class ResourceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cpu: float | None = Field(default=None, ge=0)
    gpu: float | None = Field(default=None, ge=0)
    memory_bytes: int | None = Field(default=None, ge=0)
    accelerator_type: str | None = Field(default=None, min_length=1)


class OperatorSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1)
    kind: OperatorKind
    config: dict[str, Any] = Field(default_factory=dict)
    resources: ResourceSpec = Field(default_factory=ResourceSpec)


class RuntimeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    image: str = Field(min_length=1)
    ray_address: str = "auto"
    namespace: str = "default"
    # Retained for backward-compatible standalone plans. ClusterProfile.service_account
    # is authoritative for newly compiled control-plane runs.
    service_account: str | None = None


class ExecutionPlan(BaseModel):
    """Versioned physical plan executed by one Ray Data execution unit."""

    model_config = ConfigDict(extra="forbid")

    api_version: Literal["dataflow.io/v1alpha1"] = "dataflow.io/v1alpha1"
    kind: Literal["ExecutionPlan"] = "ExecutionPlan"
    run_id: str = Field(min_length=1)
    unit_id: str = Field(min_length=1)
    operators: list[OperatorSpec] = Field(min_length=2)
    runtime: RuntimeSpec
    cluster_profile: ClusterProfileSnapshot | None = None
    input_artifacts: list[ArtifactRef] = Field(default_factory=list)
    output_artifacts: list[ArtifactOutputSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_operator_chain(self) -> ExecutionPlan:
        ids = [op.id for op in self.operators]
        if len(ids) != len(set(ids)):
            raise ValueError("operator ids must be unique")
        if self.operators[0].kind is not OperatorKind.READ_PARQUET:
            raise ValueError("v1alpha1 execution plan must start with read_parquet")
        if self.operators[-1].kind is not OperatorKind.WRITE_PARQUET:
            raise ValueError("v1alpha1 execution plan must end with write_parquet")

        operators = {operator.id: operator for operator in self.operators}
        output_ids: set[str] = set()
        for output in self.output_artifacts:
            if output.run_id != self.run_id:
                raise ValueError("artifact output run_id must match execution plan run_id")
            if output.operator_id in output_ids:
                raise ValueError("artifact output operator ids must be unique")
            output_ids.add(output.operator_id)
            operator = operators.get(output.operator_id)
            if operator is None or operator.kind is not OperatorKind.WRITE_PARQUET:
                raise ValueError(
                    "artifact output operator_id must reference a write_parquet operator"
                )
        return self
