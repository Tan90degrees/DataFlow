"""Operator contracts for execution in a shared Ray cluster."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class OperatorKind(StrEnum):
    READ_PARQUET = "read_parquet"
    MAP_BATCHES = "map_batches"
    FILTER = "filter"
    UNION = "union"
    JOIN = "join"
    WRITE_PARQUET = "write_parquet"


class ResourceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

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
    cache: bool = False


class JoinConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    on: list[str] = Field(min_length=1)
    num_partitions: int = Field(gt=0)
    join_type: Literal[
        "inner",
        "left_outer",
        "right_outer",
        "full_outer",
        "left_semi",
        "right_semi",
        "left_anti",
        "right_anti",
    ] = "inner"
    right_on: list[str] | None = None
    left_suffix: str | None = None
    right_suffix: str | None = None
    validate_schemas: bool = True
    aggregator_ray_remote_args: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_keys(self) -> JoinConfig:
        if any(not key for key in self.on) or len(set(self.on)) != len(self.on):
            raise ValueError("join keys must be non-empty and unique")
        if self.right_on is not None and (
            len(self.right_on) != len(self.on)
            or any(not key for key in self.right_on)
            or len(set(self.right_on)) != len(self.right_on)
        ):
            raise ValueError("right_on must have the same number of non-empty unique keys as on")
        return self
