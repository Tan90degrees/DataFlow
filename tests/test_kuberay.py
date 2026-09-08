from __future__ import annotations

from copy import deepcopy
from typing import Any

from dataflow.executor import ExternalJobState
from dataflow.kuberay import KubeRayExecutor, rayjob_name, render_rayjob
from tests.test_runtime import make_plan


class FakeApiError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"api status {status}")
        self.status = status


class FakeRayJobClient:
    def __init__(self) -> None:
        self.resources: dict[tuple[str, str], dict[str, Any]] = {}
        self.create_count = 0
        self.delete_count = 0

    def create(self, namespace: str, body: dict[str, Any]) -> dict[str, Any]:
        name = body["metadata"]["name"]
        key = (namespace, name)
        if key in self.resources:
            raise FakeApiError(409)
        self.create_count += 1
        resource = deepcopy(body)
        resource["status"] = {"jobStatus": "PENDING"}
        self.resources[key] = resource
        return resource

    def get(self, namespace: str, name: str) -> dict[str, Any]:
        try:
            return self.resources[(namespace, name)]
        except KeyError as error:
            raise FakeApiError(404) from error

    def delete(self, namespace: str, name: str) -> None:
        key = (namespace, name)
        if key not in self.resources:
            raise FakeApiError(404)
        self.delete_count += 1
        del self.resources[key]


def test_render_rayjob_contains_traceable_identity_and_runtime() -> None:
    plan = make_plan()
    plan.runtime.namespace = "dataflow-system"
    plan.runtime.service_account = "dataflow-runner"

    manifest = render_rayjob(plan, attempt_number=2)

    assert manifest["apiVersion"] == "ray.io/v1"
    assert manifest["kind"] == "RayJob"
    assert manifest["metadata"]["namespace"] == "dataflow-system"
    assert manifest["metadata"]["labels"]["dataflow.io/run-id"] == "run-1"
    assert manifest["metadata"]["labels"]["dataflow.io/attempt"] == "2"
    assert manifest["metadata"]["name"].endswith("-a002")
    assert manifest["spec"]["shutdownAfterJobFinishes"] is True
    head = manifest["spec"]["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]
    assert head["serviceAccountName"] == "dataflow-runner"
    runtime_env = manifest["spec"]["runtimeEnvYAML"]
    assert "DATAFLOW_EXECUTION_PLAN" in runtime_env
    assert "DATAFLOW_ATTEMPT_NUMBER: '2'" in runtime_env


def test_attempt_names_remain_unique_when_identity_is_long() -> None:
    plan = make_plan()
    plan.run_id = "run-" + "x" * 80
    plan.unit_id = "unit-" + "y" * 80

    first = rayjob_name(plan, 1)
    second = rayjob_name(plan, 2)

    assert len(first) <= 63
    assert len(second) <= 63
    assert first.endswith("-a001")
    assert second.endswith("-a002")
    assert first != second


def test_submit_is_idempotent_for_same_attempt() -> None:
    client = FakeRayJobClient()
    executor = KubeRayExecutor(client)
    plan = make_plan()

    first = executor.submit(plan, attempt_number=1)
    second = executor.submit(plan, attempt_number=1)

    assert first.id == second.id == rayjob_name(plan, 1)
    assert first.state is ExternalJobState.PENDING
    assert client.create_count == 1
    assert len(client.resources) == 1


def test_existing_terminal_rayjob_is_observed_without_recreation() -> None:
    client = FakeRayJobClient()
    executor = KubeRayExecutor(client)
    plan = make_plan()
    executor.submit(plan, attempt_number=1)
    key = (plan.runtime.namespace, rayjob_name(plan, 1))
    client.resources[key]["status"] = {"jobStatus": "SUCCEEDED"}

    observed = executor.submit(plan, attempt_number=1)

    assert observed.state is ExternalJobState.SUCCEEDED
    assert client.create_count == 1
