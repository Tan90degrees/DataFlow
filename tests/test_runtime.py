from __future__ import annotations

import pytest

from dataflow.contracts import OperatorKind, OperatorSpec, ResourceSpec
from dataflow.runtime import OperatorExecutor, operator_config
from tests.test_single_cluster import LazyBackend


@pytest.mark.parametrize("kind", list(OperatorKind))
def test_resource_hints_and_accelerator_labels_reach_each_ray_operator(kind):
    config = (
        {"path": "/data"}
        if kind in {OperatorKind.READ_PARQUET, OperatorKind.WRITE_PARQUET}
        else {"callable": "dataflow.callables.identity_batch"}
    )
    operator = OperatorSpec(
        id="op",
        kind=kind,
        config=config,
        resources=ResourceSpec(
            cpu=0.5, gpu=1, memory_bytes=1024, accelerator_type="test-accelerator"
        ),
    )
    original = operator.model_dump()
    backend = LazyBackend()
    dataset = backend.read_parquet("/in") if kind is not OperatorKind.READ_PARQUET else None
    OperatorExecutor(backend).apply_operator(dataset, operator)
    expected = {
        "num_cpus": 0.5,
        "num_gpus": 1,
        "memory": 1024,
        "label_selector": {"ray.io/accelerator-type": "test-accelerator"},
    }
    actual = backend.calls[-1][2]
    if kind in {OperatorKind.READ_PARQUET, OperatorKind.WRITE_PARQUET}:
        actual = actual["ray_remote_args"]
    assert actual == expected
    assert operator.model_dump() == original


def test_conflicting_resource_declarations_fail_instead_of_overriding_budget():
    op = OperatorSpec(
        id="map", kind="map_batches", config={"num_cpus": 8}, resources=ResourceSpec(cpu=1)
    )
    with pytest.raises(ValueError, match="conflicts"):
        operator_config(op)


def test_io_resource_options_preserve_existing_ray_remote_arguments():
    op = OperatorSpec(
        id="read",
        kind="read_parquet",
        config={"path": "/input", "ray_remote_args": {"max_retries": 2}},
        resources=ResourceSpec(cpu=0.25),
    )
    config = operator_config(op)
    assert config["ray_remote_args"] == {"max_retries": 2, "num_cpus": 0.25}
    assert op.config["ray_remote_args"] == {"max_retries": 2}
