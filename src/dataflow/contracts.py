"""Operator contracts for execution in a shared Ray cluster."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class OperatorKind(StrEnum):
    READ_PARQUET = "read_parquet"
    MAP_BATCHES = "map_batches"
    FILTER = "filter"
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
