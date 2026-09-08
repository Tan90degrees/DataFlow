"""KubeRay RayJob rendering and idempotent external execution."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Protocol

from dataflow.contracts import ExecutionPlan
from dataflow.executor import ExecutorError, ExternalJob, ExternalJobState

RAY_GROUP = "ray.io"
RAY_VERSION = "v1"
RAYJOB_PLURAL = "rayjobs"


class RayJobClient(Protocol):
    def create(self, namespace: str, body: dict[str, Any]) -> dict[str, Any]: ...

    def get(self, namespace: str, name: str) -> dict[str, Any]: ...

    def delete(self, namespace: str, name: str) -> None: ...


class KubernetesRayJobClient:
    """Small adapter around Kubernetes CustomObjectsApi for KubeRay RayJobs."""

    def __init__(self, custom_objects_api: Any) -> None:
        self._api = custom_objects_api

    @classmethod
    def from_default_config(cls) -> KubernetesRayJobClient:
        try:
            from kubernetes import client, config
        except ImportError as error:
            raise RuntimeError(
                "Kubernetes client is required for live KubeRay reconciliation; "
                "install the 'kubernetes' package"
            ) from error

        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()
        return cls(client.CustomObjectsApi())

    def create(self, namespace: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._api.create_namespaced_custom_object(
            group=RAY_GROUP,
            version=RAY_VERSION,
            namespace=namespace,
            plural=RAYJOB_PLURAL,
            body=body,
        )

    def get(self, namespace: str, name: str) -> dict[str, Any]:
        return self._api.get_namespaced_custom_object(
            group=RAY_GROUP,
            version=RAY_VERSION,
            namespace=namespace,
            plural=RAYJOB_PLURAL,
            name=name,
        )

    def delete(self, namespace: str, name: str) -> None:
        self._api.delete_namespaced_custom_object(
            group=RAY_GROUP,
            version=RAY_VERSION,
            namespace=namespace,
            plural=RAYJOB_PLURAL,
            name=name,
        )


class KubeRayExecutor:
    """Idempotent Executor implementation backed by attempt-specific RayJobs."""

    def __init__(self, client: RayJobClient) -> None:
        self._client = client

    def submit(self, plan: ExecutionPlan, *, attempt_number: int) -> ExternalJob:
        existing = self.get(plan, attempt_number=attempt_number)
        if existing is not None:
            return existing

        namespace = plan.runtime.namespace
        body = render_rayjob(plan, attempt_number=attempt_number)
        try:
            resource = self._client.create(namespace, body)
        except Exception as error:
            if _api_status(error) == 409:
                try:
                    resource = self._client.get(
                        namespace,
                        rayjob_name(plan, attempt_number),
                    )
                except Exception as get_error:
                    raise _executor_error(get_error) from get_error
            else:
                raise _executor_error(error) from error
        return _external_job(resource)

    def get(self, plan: ExecutionPlan, *, attempt_number: int) -> ExternalJob | None:
        namespace = plan.runtime.namespace
        name = rayjob_name(plan, attempt_number)
        try:
            resource = self._client.get(namespace, name)
        except Exception as error:
            if _api_status(error) == 404:
                return None
            raise _executor_error(error) from error
        return _external_job(resource)

    def cancel(self, plan: ExecutionPlan, *, attempt_number: int) -> None:
        namespace = plan.runtime.namespace
        name = rayjob_name(plan, attempt_number)
        try:
            self._client.delete(namespace, name)
        except Exception as error:
            if _api_status(error) == 404:
                return
            raise _executor_error(error) from error


def rayjob_name(plan: ExecutionPlan, attempt_number: int) -> str:
    if attempt_number < 1:
        raise ValueError("attempt_number must be at least 1")
    suffix = f"-a{attempt_number:03d}"
    base = _dns_name(f"dataflow-{plan.run_id}-{plan.unit_id}", limit=None)
    maximum = 63 - len(suffix)
    if len(base) > maximum:
        digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:8]
        prefix_length = maximum - len(digest) - 1
        base = f"{base[:prefix_length].rstrip('-')}-{digest}"
    return f"{base}{suffix}"


def render_rayjob(
    plan: ExecutionPlan,
    *,
    attempt_number: int | None = None,
) -> dict[str, Any]:
    if attempt_number is None:
        name = _dns_name(f"dataflow-{plan.run_id}-{plan.unit_id}")
    else:
        name = rayjob_name(plan, attempt_number)

    labels = {
        "app.kubernetes.io/name": "dataflow",
        "dataflow.io/run-id": plan.run_id,
        "dataflow.io/unit-id": plan.unit_id,
    }
    if attempt_number is not None:
        labels["dataflow.io/attempt"] = str(attempt_number)

    pod_spec: dict[str, Any] = {
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
    env_lines = [
        "env_vars:",
        "  DATAFLOW_EXECUTION_PLAN: '" + plan_json.replace("'", "''") + "'",
    ]
    if attempt_number is not None:
        env_lines.append(f"  DATAFLOW_ATTEMPT_NUMBER: '{attempt_number}'")
    runtime_env_yaml = "\n".join(env_lines) + "\n"

    return {
        "apiVersion": f"{RAY_GROUP}/{RAY_VERSION}",
        "kind": "RayJob",
        "metadata": {
            "name": name,
            "namespace": plan.runtime.namespace,
            "labels": labels,
        },
        "spec": {
            "entrypoint": "python -m dataflow.cli run-inline-plan",
            "runtimeEnvYAML": runtime_env_yaml,
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


def _external_job(resource: dict[str, Any]) -> ExternalJob:
    metadata = resource.get("metadata", {})
    status = resource.get("status", {})
    raw_status = str(
        status.get("jobStatus") or status.get("jobDeploymentStatus") or ""
    ).upper()
    state = _external_state(raw_status)
    reason = status.get("reason")
    message = status.get("message") or status.get("jobStatusMessage")
    error_code = str(reason) if reason else None
    if error_code is None and state is ExternalJobState.FAILED:
        error_code = "RAY_JOB_FAILED"
    return ExternalJob(
        id=str(metadata.get("name", "")),
        state=state,
        error_code=error_code,
        error_message=str(message) if message is not None else None,
    )


def _external_state(raw_status: str) -> ExternalJobState:
    if raw_status in {"PENDING", "WAITING", "NEW", "INITIALIZING"}:
        return ExternalJobState.PENDING
    if raw_status == "RUNNING":
        return ExternalJobState.RUNNING
    if raw_status in {"SUCCEEDED", "SUCCESS", "COMPLETE", "COMPLETED"}:
        return ExternalJobState.SUCCEEDED
    if raw_status in {"FAILED", "FAILURE"}:
        return ExternalJobState.FAILED
    if raw_status in {"STOPPED", "CANCELLED", "CANCELED"}:
        return ExternalJobState.CANCELLED
    return ExternalJobState.UNKNOWN


def _executor_error(error: Exception) -> ExecutorError:
    status = _api_status(error)
    retryable = status is None or status in {408, 429} or status >= 500
    error_code = "API_UNAVAILABLE" if retryable else "KUBERNETES_API_ERROR"
    if status == 422:
        error_code = "INVALID_PLAN"
    return ExecutorError(str(error), error_code=error_code, retryable=retryable)


def _api_status(error: Exception) -> int | None:
    status = getattr(error, "status", None)
    return status if isinstance(status, int) else None


def _dns_name(value: str, *, limit: int | None = 63) -> str:
    normalized = "".join(
        ch.lower() if ch.isalnum() or ch == "-" else "-" for ch in value
    ).strip("-")
    normalized = normalized or "dataflow"
    if limit is None:
        return normalized
    return normalized[:limit].rstrip("-")


__all__ = [
    "KubeRayExecutor",
    "KubernetesRayJobClient",
    "RayJobClient",
    "rayjob_name",
    "render_rayjob",
]
