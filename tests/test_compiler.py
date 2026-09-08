from __future__ import annotations

import pytest

from dataflow.compiler import (
    EdgeKind,
    ExecutionBoundary,
    LogicalGraph,
    PipelineCompiler,
    PipelineEdgeSpec,
    PipelineNodeSpec,
    PipelineSpec,
)
from dataflow.contracts import OperatorKind, RuntimeSpec


RUNTIME = RuntimeSpec(image="ghcr.io/example/dataflow:dev")


def _edge(
    source: str,
    target: str,
    *,
    kind: EdgeKind = EdgeKind.DATA,
    boundary: ExecutionBoundary = ExecutionBoundary.NONE,
) -> PipelineEdgeSpec:
    return PipelineEdgeSpec.model_validate(
        {
            "from": source,
            "to": target,
            "kind": kind,
            "boundary": boundary,
        }
    )


def _linear_spec(*, boundary: ExecutionBoundary = ExecutionBoundary.NONE) -> PipelineSpec:
    return PipelineSpec(
        name="linear",
        runtime=RUNTIME,
        artifact_base_uri="s3://bucket/dataflow",
        nodes=[
            PipelineNodeSpec(
                id="read",
                kind=OperatorKind.READ_PARQUET,
                config={"path": "s3://bucket/input"},
            ),
            PipelineNodeSpec(
                id="map",
                kind=OperatorKind.MAP_BATCHES,
                config={"callable": "dataflow.callables.identity"},
            ),
            PipelineNodeSpec(
                id="filter",
                kind=OperatorKind.FILTER,
                config={"callable": "dataflow.callables.always_true"},
            ),
            PipelineNodeSpec(
                id="write",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "s3://bucket/output"},
            ),
        ],
        edges=[
            _edge("read", "map"),
            _edge("map", "filter", boundary=boundary),
            _edge("filter", "write"),
        ],
    )


def test_linear_pipeline_compiles_to_one_execution_unit() -> None:
    graph = PipelineCompiler().compile(_linear_spec(), run_id="run-1")

    assert [unit.id for unit in graph.units] == ["unit-001"]
    assert graph.units[0].node_ids == ["read", "map", "filter", "write"]
    assert [op.id for op in graph.units[0].plan.operators] == [
        "read",
        "map",
        "filter",
        "write",
    ]
    assert graph.units[0].dependencies == []


def test_hard_boundary_materializes_between_execution_units() -> None:
    graph = PipelineCompiler().compile(
        _linear_spec(boundary=ExecutionBoundary.HARD),
        run_id="run-2",
    )

    assert [unit.node_ids for unit in graph.units] == [
        ["read", "map"],
        ["filter", "write"],
    ]
    first, second = graph.units
    assert second.dependencies == [first.id]
    assert first.plan.operators[-1].kind is OperatorKind.WRITE_PARQUET
    assert first.plan.operators[-1].config["path"] == (
        "s3://bucket/dataflow/run-2/unit-001"
    )
    assert second.plan.operators[0].kind is OperatorKind.READ_PARQUET
    assert second.plan.operators[0].config["path"] == (
        "s3://bucket/dataflow/run-2/unit-001"
    )


def test_runtime_change_is_a_hard_physical_boundary() -> None:
    spec = _linear_spec()
    spec.nodes[2].runtime = RuntimeSpec(image="ghcr.io/example/dataflow:other")

    graph = PipelineCompiler().compile(spec, run_id="run-runtime")

    assert [unit.node_ids for unit in graph.units] == [
        ["read", "map"],
        ["filter", "write"],
    ]
    assert graph.units[1].plan.runtime.image == "ghcr.io/example/dataflow:other"


def test_data_fan_out_materializes_once_and_reuses_upstream_artifact() -> None:
    spec = PipelineSpec(
        name="fan-out",
        runtime=RUNTIME,
        artifact_base_uri="s3://bucket/dataflow",
        nodes=[
            PipelineNodeSpec(
                id="read",
                kind=OperatorKind.READ_PARQUET,
                config={"path": "s3://bucket/input"},
            ),
            PipelineNodeSpec(
                id="left",
                kind=OperatorKind.FILTER,
                config={"callable": "dataflow.callables.always_true"},
            ),
            PipelineNodeSpec(
                id="left_write",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "s3://bucket/left"},
            ),
            PipelineNodeSpec(
                id="right",
                kind=OperatorKind.FILTER,
                config={"callable": "dataflow.callables.always_true"},
            ),
            PipelineNodeSpec(
                id="right_write",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "s3://bucket/right"},
            ),
        ],
        edges=[
            _edge("read", "left"),
            _edge("left", "left_write"),
            _edge("read", "right"),
            _edge("right", "right_write"),
        ],
    )

    graph = PipelineCompiler().compile(spec, run_id="fan-out-run")

    assert [unit.node_ids for unit in graph.units] == [
        ["read"],
        ["left", "left_write"],
        ["right", "right_write"],
    ]
    stage_uri = "s3://bucket/dataflow/fan-out-run/unit-001"
    assert graph.units[0].plan.operators[-1].config["path"] == stage_uri
    assert graph.units[1].plan.operators[0].config["path"] == stage_uri
    assert graph.units[2].plan.operators[0].config["path"] == stage_uri


def test_logical_graph_topological_order_is_deterministic_for_diamond() -> None:
    spec = PipelineSpec(
        name="control-diamond",
        runtime=RUNTIME,
        nodes=[
            PipelineNodeSpec(
                id="a",
                kind=OperatorKind.READ_PARQUET,
                config={"path": "x"},
            ),
            PipelineNodeSpec(
                id="b",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "b"},
            ),
            PipelineNodeSpec(
                id="c",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "c"},
            ),
            PipelineNodeSpec(
                id="d",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "d"},
            ),
        ],
        edges=[
            _edge("a", "b", kind=EdgeKind.CONTROL),
            _edge("a", "c", kind=EdgeKind.CONTROL),
            _edge("b", "d", kind=EdgeKind.CONTROL),
            _edge("c", "d", kind=EdgeKind.CONTROL),
        ],
    )

    assert LogicalGraph(spec).topological_order == ["a", "b", "c", "d"]


def test_cycle_is_rejected() -> None:
    spec = PipelineSpec(
        name="cycle",
        runtime=RUNTIME,
        nodes=[
            PipelineNodeSpec(
                id="a",
                kind=OperatorKind.READ_PARQUET,
                config={"path": "x"},
            ),
            PipelineNodeSpec(
                id="b",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "y"},
            ),
        ],
        edges=[
            _edge("a", "b", kind=EdgeKind.CONTROL),
            _edge("b", "a", kind=EdgeKind.CONTROL),
        ],
    )

    with pytest.raises(ValueError, match="cycle"):
        LogicalGraph(spec)


def test_dangling_edge_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown node"):
        PipelineSpec(
            name="dangling",
            runtime=RUNTIME,
            nodes=[
                PipelineNodeSpec(
                    id="a",
                    kind=OperatorKind.READ_PARQUET,
                    config={"path": "x"},
                )
            ],
            edges=[_edge("a", "missing")],
        )


def test_data_fan_in_is_explicitly_rejected_in_v1_runtime() -> None:
    spec = PipelineSpec(
        name="fan-in",
        runtime=RUNTIME,
        artifact_base_uri="s3://bucket/dataflow",
        nodes=[
            PipelineNodeSpec(
                id="a",
                kind=OperatorKind.READ_PARQUET,
                config={"path": "a"},
            ),
            PipelineNodeSpec(
                id="b",
                kind=OperatorKind.READ_PARQUET,
                config={"path": "b"},
            ),
            PipelineNodeSpec(
                id="c",
                kind=OperatorKind.MAP_BATCHES,
                config={"callable": "dataflow.callables.identity"},
            ),
            PipelineNodeSpec(
                id="d",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "d"},
            ),
        ],
        edges=[
            _edge("a", "c"),
            _edge("b", "c"),
            _edge("c", "d"),
        ],
    )

    assert LogicalGraph(spec).topological_order == ["a", "b", "c", "d"]
    with pytest.raises(ValueError, match="fan-in"):
        PipelineCompiler().compile(spec, run_id="run-fanin")


def test_compile_output_is_deterministic() -> None:
    compiler = PipelineCompiler()
    first = compiler.compile(_linear_spec(), run_id="same-run")
    second = compiler.compile(_linear_spec(), run_id="same-run")

    assert first.model_dump(mode="json") == second.model_dump(mode="json")
