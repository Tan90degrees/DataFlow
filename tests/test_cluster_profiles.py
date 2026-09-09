from __future__ import annotations

import pytest

from dataflow.cluster_profiles import (
    AutoscalingProfile,
    ClusterProfileSpec,
    HeadGroupProfile,
    NodeCapacity,
    PlacementSpec,
    TolerationSpec,
    WorkerGroupProfile,
)
from dataflow.contracts import ExecutionPlan, ResourceSpec
from dataflow.kuberay import kubernetes_namespace, render_rayjob
from dataflow.runtime import PlanExecutor
from tests.test_runtime import FakeBackend, make_plan


def make_gpu_profile(*, namespace: str = "ml-v1") -> ClusterProfileSpec:
    return ClusterProfileSpec(
        name="gpu-prod",
        kubernetes_namespace=namespace,
        service_account="dataflow-runner",
        priority_class_name="dataflow-high",
        queue="gpu-queue",
        head=HeadGroupProfile(
            resources=NodeCapacity(cpu=1, memory_bytes=2 * 1024**3),
            placement=PlacementSpec(node_selector={"node-pool": "control"}),
        ),
        worker_groups=[
            WorkerGroupProfile(
                name="a100-workers",
                replicas=0,
                min_replicas=0,
                max_replicas=4,
                resources=NodeCapacity(cpu=8, memory_bytes=32 * 1024**3, gpu=1),
                accelerator_type="A100",
                placement=PlacementSpec(
                    node_selector={"accelerator": "a100"},
                    tolerations=[
                        TolerationSpec(
                            key="nvidia.com/gpu",
                            operator="Exists",
                            effect="NoSchedule",
                        )
                    ],
                ),
                autoscaler_priority=10,
            )
        ],
        autoscaling=AutoscalingProfile(
            enabled=True,
            version="v2",
            idle_timeout_seconds=120,
        ),
    )


def test_cluster_profile_validates_worker_capacity_and_accelerator() -> None:
    profile = make_gpu_profile()

    profile.validate_request(
        ResourceSpec(
            cpu=2,
            gpu=1,
            memory_bytes=8 * 1024**3,
            accelerator_type="A100",
        ),
        node_id="predict",
    )

    with pytest.raises(ValueError, match="no compatible worker group"):
        profile.validate_request(
            ResourceSpec(gpu=1, accelerator_type="T4"),
            node_id="predict-t4",
        )

    with pytest.raises(ValueError, match="no compatible worker group"):
        profile.validate_request(
            ResourceSpec(cpu=16),
            node_id="too-large",
        )


def test_runtime_uses_ray_label_selector_for_accelerator_type() -> None:
    plan = make_plan()
    plan.operators[1].resources.accelerator_type = "A100"
    plan.operators[1].resources.memory_bytes = 4 * 1024**3
    backend = FakeBackend()

    PlanExecutor(backend).execute(plan)

    map_kwargs = backend.calls[1][2]
    assert map_kwargs["num_cpus"] == 2
    assert map_kwargs["num_gpus"] == 1
    assert map_kwargs["memory"] == 4 * 1024**3
    assert map_kwargs["label_selector"] == {"ray.io/accelerator-type": "A100"}


def test_kuberay_manifest_is_rendered_from_pinned_profile_snapshot() -> None:
    data = make_plan().model_dump(mode="json")
    data["cluster_profile"] = make_gpu_profile().snapshot(revision=3).model_dump(mode="json")
    plan = ExecutionPlan.model_validate(data)

    manifest = render_rayjob(plan, attempt_number=2)

    assert kubernetes_namespace(plan) == "ml-v1"
    assert manifest["metadata"]["namespace"] == "ml-v1"
    assert manifest["metadata"]["labels"]["dataflow.io/cluster-profile"] == "gpu-prod"
    assert manifest["metadata"]["labels"]["dataflow.io/cluster-profile-revision"] == "3"
    assert manifest["metadata"]["labels"]["kueue.x-k8s.io/queue-name"] == "gpu-queue"
    assert manifest["metadata"]["annotations"]["kueue.x-k8s.io/elastic-job"] == "true"

    cluster = manifest["spec"]["rayClusterSpec"]
    assert cluster["rayVersion"] == "2.58.0"
    assert cluster["enableInTreeAutoscaling"] is True
    assert cluster["autoscalerOptions"] == {
        "version": "v2",
        "idleTimeoutSeconds": 120,
    }

    head_spec = cluster["headGroupSpec"]["template"]["spec"]
    assert head_spec["serviceAccountName"] == "dataflow-runner"
    assert head_spec["priorityClassName"] == "dataflow-high"
    assert head_spec["nodeSelector"] == {"node-pool": "control"}

    worker = cluster["workerGroupSpecs"][0]
    assert worker["groupName"] == "a100-workers"
    assert worker["replicas"] == 0
    assert worker["minReplicas"] == 0
    assert worker["maxReplicas"] == 4
    assert worker["priority"] == 10
    assert worker["labels"] == {"ray.io/accelerator-type": "A100"}
    assert worker["template"]["metadata"]["labels"]["ray.io/accelerator-type"] == "A100"

    worker_spec = worker["template"]["spec"]
    assert worker_spec["nodeSelector"] == {"accelerator": "a100"}
    assert worker_spec["tolerations"] == [
        {
            "key": "nvidia.com/gpu",
            "operator": "Exists",
            "effect": "NoSchedule",
        }
    ]
    resources = worker_spec["containers"][0]["resources"]
    assert resources["requests"]["cpu"] == "8"
    assert resources["requests"]["memory"] == str(32 * 1024**3)
    assert resources["requests"]["nvidia.com/gpu"] == "1"
    assert resources["limits"] == resources["requests"]
