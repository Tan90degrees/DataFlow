from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import yaml


def _resources(path: Path) -> list[dict[str, Any]]:
    return [item for item in yaml.safe_load_all(path.read_text()) if item]


def _find(resources: list[dict[str, Any]], kind: str, suffix: str) -> dict[str, Any]:
    matches = [
        item
        for item in resources
        if item.get("kind") == kind and item.get("metadata", {}).get("name", "").endswith(suffix)
    ]
    assert len(matches) == 1, (kind, suffix, matches)
    return matches[0]


def _env(container: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {item["name"]: item for item in container.get("env", [])}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--static-auth", action="store_true")
    args = parser.parse_args()
    resources = _resources(args.manifest)

    api = _find(resources, "Deployment", "-api")
    controller = _find(resources, "Deployment", "-controller")
    pdb = _find(resources, "PodDisruptionBudget", "-controller")
    role = _find(resources, "Role", "-rayjob-controller")
    binding = _find(resources, "RoleBinding", "-rayjob-controller")

    assert api["spec"]["replicas"] == 2
    assert controller["spec"]["replicas"] == 2
    assert pdb["spec"]["minAvailable"] == 1
    verbs = role["rules"][0]["verbs"]
    assert set(verbs) == {"get", "list", "watch", "create", "delete"}
    assert binding["subjects"][0]["kind"] == "ServiceAccount"

    api_pod = api["spec"]["template"]["spec"]
    controller_pod = controller["spec"]["template"]["spec"]
    assert api_pod["serviceAccountName"] != controller_pod["serviceAccountName"]

    api_container = api_pod["containers"][0]
    controller_container = controller_pod["containers"][0]
    expected_image = "ghcr.io/tan90degrees/dataflow-control-plane:0.1.0"
    for container in (api_container, controller_container):
        assert container["image"] == expected_image
        assert container["readinessProbe"]
        assert container["livenessProbe"]
        assert container["resources"]["requests"]["cpu"]
        assert container["resources"]["requests"]["memory"]

    api_env = _env(api_container)
    controller_env = _env(controller_container)

    assert api_env["DATAFLOW_BUILD_IMAGE"]["value"] == expected_image
    assert controller_env["DATAFLOW_BUILD_IMAGE"]["value"] == expected_image

    db_ref = api_env["DATAFLOW_DATABASE_URL"]["valueFrom"]["secretKeyRef"]
    assert db_ref == {"name": "dataflow-db", "key": "database-url"}
    assert controller_env["DATAFLOW_DATABASE_URL"]["valueFrom"]["secretKeyRef"] == db_ref
    assert controller_env["DATAFLOW_CONTROLLER_LOCK_NAMESPACE"]["value"] == "1145132097"
    assert controller_env["DATAFLOW_CONTROLLER_LOCK_KEY"]["value"] == "1"

    secret_kinds = [item for item in resources if item.get("kind") == "Secret"]
    assert secret_kinds == [], "the production chart must not bundle credentials"

    if args.static_auth:
        auth_ref = api_env["DATAFLOW_API_AUTH_STATIC_TOKENS"]["valueFrom"]["secretKeyRef"]
        assert auth_ref == {"name": "dataflow-api-auth", "key": "static-tokens"}
    else:
        assert "DATAFLOW_API_AUTH_STATIC_TOKENS" not in api_env


if __name__ == "__main__":
    main()
