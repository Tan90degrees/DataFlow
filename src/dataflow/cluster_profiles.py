"""ClusterProfile contracts and resource-policy validation."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

if TYPE_CHECKING:
    from dataflow.contracts import ResourceSpec


class NodeCapacity(BaseModel):
    """Per-Pod capacity advertised to Kubernetes and Ray."""

    model_config = ConfigDict(extra="forbid")

    cpu: float = Field(default=1, gt=0)
    memory_bytes: int = Field(default=2 * 1024**3, gt=0)
    gpu: int = Field(default=0, ge=0)


class TolerationSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: str | None = None
    operator: Literal["Exists", "Equal"] = "Equal"
    value: str | None = None
    effect: Literal["NoSchedule", "PreferNoSchedule", "NoExecute"] | None = None

    @model_validator(mode="after")
    def validate_operator(self) -> TolerationSpec:
        if self.operator == "Equal" and self.key is not None and self.value is None:
            raise ValueError("Equal tolerations with a key require a value")
        return self


class PlacementSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_selector: dict[str, str] = Field(default_factory=dict)
    tolerations: list[TolerationSpec] = Field(default_factory=list)


class HeadGroupProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resources: NodeCapacity = Field(
        default_factory=lambda: NodeCapacity(cpu=1, memory_bytes=2 * 1024**3)
    )
    placement: PlacementSpec = Field(default_factory=PlacementSpec)


class WorkerGroupProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=63)
    replicas: int = Field(default=0, ge=0)
    min_replicas: int = Field(default=0, ge=0)
    max_replicas: int = Field(default=8, ge=0)
    resources: NodeCapacity = Field(default_factory=NodeCapacity)
    accelerator_type: str | None = Field(default=None, min_length=1)
    gpu_resource_name: str = Field(default="nvidia.com/gpu", min_length=1)
    placement: PlacementSpec = Field(default_factory=PlacementSpec)
    autoscaler_priority: int = 0

    @model_validator(mode="after")
    def validate_replicas(self) -> WorkerGroupProfile:
        if self.min_replicas > self.max_replicas:
            raise ValueError("min_replicas cannot exceed max_replicas")
        if not self.min_replicas <= self.replicas <= self.max_replicas:
            raise ValueError("replicas must be between min_replicas and max_replicas")
        if self.accelerator_type is not None and self.resources.gpu < 1:
            raise ValueError("accelerator_type requires at least one GPU on the worker")
        return self


class AutoscalingProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    version: Literal["v1", "v2"] = "v2"
    idle_timeout_seconds: int = Field(default=60, ge=0)


class ClusterProfileSpec(BaseModel):
    """Admin-managed resource and placement policy referenced by PipelineSpec."""

    model_config = ConfigDict(extra="forbid")

    api_version: Literal["dataflow.io/v1alpha1"] = "dataflow.io/v1alpha1"
    kind: Literal["ClusterProfile"] = "ClusterProfile"
    name: str = Field(min_length=1, max_length=128)
    ray_version: str = Field(default="2.58.0", min_length=1)
    kubernetes_namespace: str = Field(default="default", min_length=1)
    service_account: str | None = Field(default=None, min_length=1)
    priority_class_name: str | None = Field(default=None, min_length=1)
    queue: str | None = Field(default=None, min_length=1)
    head: HeadGroupProfile = Field(default_factory=HeadGroupProfile)
    worker_groups: list[WorkerGroupProfile] = Field(min_length=1)
    autoscaling: AutoscalingProfile = Field(default_factory=AutoscalingProfile)

    @model_validator(mode="after")
    def validate_groups(self) -> ClusterProfileSpec:
        names = [group.name for group in self.worker_groups]
        if len(names) != len(set(names)):
            raise ValueError("worker group names must be unique")
        if self.autoscaling.enabled and not any(
            group.max_replicas > 0 for group in self.worker_groups
        ):
            raise ValueError("autoscaling profiles require at least one scalable worker group")
        return self

    def validate_request(self, request: ResourceSpec, *, node_id: str) -> None:
        if any(_worker_satisfies(group, request) for group in self.worker_groups):
            return
        accelerator = (
            f", accelerator_type={request.accelerator_type!r}"
            if request.accelerator_type
            else ""
        )
        raise ValueError(
            f"node {node_id!r} requests cpu={request.cpu}, gpu={request.gpu}, "
            f"memory_bytes={request.memory_bytes}{accelerator}, but ClusterProfile "
            f"{self.name!r} has no compatible worker group"
        )

    def snapshot(self, *, revision: int) -> ClusterProfileSnapshot:
        payload = self.model_dump(mode="json")
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return ClusterProfileSnapshot(
            name=self.name,
            revision=revision,
            spec_hash=digest,
            spec=self,
        )


class ClusterProfileSnapshot(BaseModel):
    """Immutable profile revision embedded in a physical ExecutionPlan."""

    model_config = ConfigDict(extra="forbid")

    name: str
    revision: int = Field(ge=0)
    spec_hash: str = Field(min_length=64, max_length=64)
    spec: ClusterProfileSpec

    @model_validator(mode="after")
    def validate_identity(self) -> ClusterProfileSnapshot:
        if self.spec.name != self.name:
            raise ValueError("ClusterProfile snapshot name must match embedded spec")
        return self


def default_cluster_profile() -> ClusterProfileSnapshot:
    """Built-in fallback used when a pipeline references the reserved `default` profile."""

    spec = ClusterProfileSpec(
        name="default",
        worker_groups=[
            WorkerGroupProfile(
                name="cpu-workers",
                replicas=1,
                min_replicas=0,
                max_replicas=8,
                resources=NodeCapacity(cpu=4, memory_bytes=16 * 1024**3),
            )
        ],
    )
    return spec.snapshot(revision=0)


def _worker_satisfies(group: WorkerGroupProfile, request: ResourceSpec) -> bool:
    cpu = request.cpu or 0
    gpu = request.gpu or 0
    memory = request.memory_bytes or 0
    if cpu > group.resources.cpu or gpu > group.resources.gpu:
        return False
    if memory > group.resources.memory_bytes:
        return False
    if request.accelerator_type is not None:
        return group.accelerator_type == request.accelerator_type
    return True


def pod_resource_requirements(
    resources: NodeCapacity,
    *,
    gpu_resource_name: str = "nvidia.com/gpu",
) -> dict[str, dict[str, str]]:
    quantities = {
        "cpu": _cpu_quantity(resources.cpu),
        "memory": str(resources.memory_bytes),
    }
    if resources.gpu:
        quantities[gpu_resource_name] = str(resources.gpu)
    return {"requests": dict(quantities), "limits": dict(quantities)}


def placement_fields(
    placement: PlacementSpec,
    *,
    priority_class_name: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if placement.node_selector:
        result["nodeSelector"] = dict(placement.node_selector)
    if placement.tolerations:
        result["tolerations"] = [
            item.model_dump(mode="json", exclude_none=True) for item in placement.tolerations
        ]
    if priority_class_name:
        result["priorityClassName"] = priority_class_name
    return result


def _cpu_quantity(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return str(value)


__all__ = [
    "AutoscalingProfile",
    "ClusterProfileSnapshot",
    "ClusterProfileSpec",
    "HeadGroupProfile",
    "NodeCapacity",
    "PlacementSpec",
    "TolerationSpec",
    "WorkerGroupProfile",
    "default_cluster_profile",
    "placement_fields",
    "pod_resource_requirements",
]
