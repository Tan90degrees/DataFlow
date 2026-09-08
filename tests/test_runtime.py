from __future__ import annotations

from typing import Any

import pytest

from dataflow.contracts import ExecutionPlan
from dataflow.runtime import PlanExecutor


class FakeDataset:
    def __init__(self, calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]]) -> None:
        self.calls = calls

    def map_batches(self, fn, **kwargs):
        self.calls.append(("map_batches", (fn,), kwargs))
        return self

    def filter(self, fn, **kwargs):
        self.calls.append(("filter", (fn,), kwargs))
        return self

    def write_parquet(self, path: str, **kwargs):
        self.calls.append(("write_parquet", (path,), kwargs))


class FakeBackend:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def read_parquet(self, path: str, **kwargs):
        self.calls.append(("read_parquet", (path,), kwargs))
        return FakeDataset(self.calls)


def make_plan() -> ExecutionPlan:
    return ExecutionPlan.model_validate(
        {
            "run_id": "run-1",
            "unit_id": "unit-1",
            "operators": [
                {"id": "read", "kind": "read_parquet", "config": {"path": "s3://in"}},
                {
                    "id": "map",
                    "kind": "map_batches",
                    "config": {"callable": "dataflow.callables.identity_batch", "batch_size": 128},
                    "resources": {"cpu": 2, "gpu": 1},
                },
                {
                    "id": "filter",
                    "kind": "filter",
                    "config": {"callable": "dataflow.callables.keep_all"},
                },
                {"id": "write", "kind": "write_parquet", "config": {"path": "s3://out"}},
            ],
        }
    )


def test_executor_preserves_operator_order_and_resources() -> None:
    backend = FakeBackend()
    PlanExecutor(backend).execute(make_plan())

    assert [call[0] for call in backend.calls] == [
        "read_parquet",
        "map_batches",
        "filter",
        "write_parquet",
    ]
    assert backend.calls[1][2]["num_cpus"] == 2
    assert backend.calls[1][2]["num_gpus"] == 1
    assert backend.calls[1][2]["batch_size"] == 128


def test_plan_rejects_duplicate_operator_ids() -> None:
    data = make_plan().model_dump(mode="json")
    data["operators"][1]["id"] = "read"

    with pytest.raises(ValueError, match="operator ids must be unique"):
        ExecutionPlan.model_validate(data)


def test_plan_requires_read_and_write_boundaries() -> None:
    data = make_plan().model_dump(mode="json")
    data["operators"][0]["kind"] = "filter"

    with pytest.raises(ValueError, match="must start with read_parquet"):
        ExecutionPlan.model_validate(data)
