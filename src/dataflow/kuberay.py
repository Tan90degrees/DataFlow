"""KubeRay RayJob manifest generation."""

from __future__ import annotations

import json

from dataflow.contracts import ExecutionPlan


def render_rayjob(plan: ExecutionPlan) -> dict:
    name = _dns_name(f"dataflow-{plan.run_id}-{plan.unit_id}")
    labels = {
        "app.kubernetes.io/name": "dataflow",
        "dataflow.io/run-id": plan.run_id,
        "dataflow.io/unit-id": plan.unit_id,
    }

    pod_spec: dict = {
        "containers": [
            {
                "name": "ray-head",
                "image": plan.runtime.image,
            }
        ]
    }
    if plan.runtime.service_account:
        pod_spec["serviceAccountName"] = plan.runtime.service_account

    plan_json = json.dumps(plan.model_dump(mode="json"), separators=(",", ":"))

    return {
        "apiVersion": "ray.io/v1",
        "kind": "RayJob",
        "metadata": {
            "name": name,
            "namespace": plan.runtime.namespace,
            "labels": labels,
        },
        "spec": {
            "entrypoint": "python -m dataflow.cli run-inline-plan",
            "runtimeEnvYAML": "env_vars:\n  DATAFLOW_EXECUTION_PLAN: '" + plan_json.replace("'", "''") + "'\n",
            "shutdownAfterJobFinishes": True,
            "ttlSecondsAfterFinished": 300,
            "rayClusterSpec": {
                "rayVersion": "2.58.0",
                "headGroupSpec": {
                    "rayStartParams": {},
                    "template": {
                        "metadata": {"labels": labels},
                        "spec": pod_spec,
                    },
                },
                "workerGroupSpecs": [
                    {
                        "groupName": "cpu-workers",
                        "replicas": 1,
                        "minReplicas": 0,
                        "maxReplicas": 8,
                        "rayStartParams": {},
                        "template": {
                            "metadata": {"labels": labels},
                            "spec": {
                                "containers": [
                                    {
                                        "name": "ray-worker",
                                        "image": plan.runtime.image,
                                    }
                                ]
                            },
                        },
                    }
                ],
            },
        },
    }


def _dns_name(value: str) -> str:
    normalized = "".join(ch.lower() if ch.isalnum() or ch == "-" else "-" for ch in value)
    return normalized.strip("-")[:63].rstrip("-")
