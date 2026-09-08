from dataflow.kuberay import render_rayjob
from tests.test_runtime import make_plan


def test_render_rayjob_contains_traceable_identity_and_runtime() -> None:
    plan = make_plan()
    plan.runtime.namespace = "dataflow-system"
    plan.runtime.service_account = "dataflow-runner"

    manifest = render_rayjob(plan)

    assert manifest["apiVersion"] == "ray.io/v1"
    assert manifest["kind"] == "RayJob"
    assert manifest["metadata"]["namespace"] == "dataflow-system"
    assert manifest["metadata"]["labels"]["dataflow.io/run-id"] == "run-1"
    assert manifest["spec"]["shutdownAfterJobFinishes"] is True
    head = manifest["spec"]["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]
    assert head["serviceAccountName"] == "dataflow-runner"
    assert "DATAFLOW_EXECUTION_PLAN" in manifest["spec"]["runtimeEnvYAML"]
