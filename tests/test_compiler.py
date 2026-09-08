from __future__ import annotations

import pytest

from dataflow import compiler
from dataflow.contracts import OperatorKind, RuntimeSpec

RUNTIME = RuntimeSpec(image="ghcr.io/example/dataflow:dev")


def _edge(
    source: str,
    target: str,
    *,
    kind: compiler.EdgeKind = compiler.EdgeKind.DATA,
    boundary: compiler.ExecutionBoundary = compiler.ExecutionBoundary.NONE,
) -> compiler.PipelineEdgeSpec:
    return compiler.PipelineEdgeSpec.model_validate(
        {
            "from": source,
            "to": target,
            "kind": kind,
            "boundary": boundary,
        }
    )


def _linear_spec(
    *, boundary: compiler.ExecutionBoundary = compiler.ExecutionBoundary.NONE
) -> compiler.PipelineSpec:
    return compiler.PipelineSpec(
        name="linear",
        runtime=RUNTIME,
        artifact_base_uri="s3://bucket/dataflow",
        nodes=[
            compiler.PipelineNodeSpec(
                id="read",
                kind=OperatorKind.READ_PARQUET,
                config={"path": "s3://bucket/input"},
            ),
            compiler.PipelineNodeSpec(
                id="map",
                kind=OperatorKind.MAP_BATCHES,
                config={"callable": "dataflow.callables.identity"},
            ),
            compiler.PipelineNodeSpec(
                id="filter",
                kind=OperatorKind.FILTER,
                config={"callable": "dataflow.callables.always_true"},
            ),
            compiler.PipelineNodeSpec(
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
    graph = compiler.PipelineCompiler().compile(_linear_spec(), run_id="run-1")

    assert [unit.id for unit in graph.units] == ["unit-001"]
    assert graph.units[0].node_ids == ["read", "map", "filter", "write"]
    assert [op.id for op in graph.units[0].plan.operators] == [
        "read",
        "map",
        "filter",
        "write",
    ]
    assert graph.units[0].dependencies == []
    assert graph.units[0].plan.output_artifacts == []


def test_hard_boundary_materializes_durable_artifact() -> None:
    graph = compiler.PipelineCompiler().compile(
        _linear_spec(boundary=compiler.ExecutionBoundary.HARD),
        run_id="run-2",
    )

    assert [unit.node_ids for unit in graph.units] == [
        ["read", "map"],
        ["filter", "write"],
    ]
    first, second = graph.units
    assert second.dependencies == [first.id]
    assert first.plan.operators[-1].kind is OperatorKind.WRITE_PARQUET
    assert first.plan.operators[-1].config == {}
    output = first.plan.output_artifacts[0]
    assert output.node_id == "map"
    assert output.committed_uri == "s3://bucket/dataflow/runs/run-2/artifacts/map/committed"
    assert output.staging_uri(3) == (
        "s3://bucket/dataflow/runs/run-2/artifacts/map/attempts/003/data"
    )
    assert output.checkpoint is False
    assert second.plan.operators[0].kind is OperatorKind.READ_PARQUET
    assert second.plan.operators[0].config["path"] == output.committed_uri
    assert second.plan.input_artifacts[0].uri == output.committed_uri


def test_checkpoint_boundary_is_explicit_durable_artifact() -> None:
    graph = compiler.PipelineCompiler().compile(
        _linear_spec(boundary=compiler.ExecutionBoundary.CHECKPOINT),
        run_id="checkpoint-run",
    )

    assert [unit.node_ids for unit in graph.units] == [
        ["read", "map"],
        ["filter", "write"],
    ]
    assert graph.units[0].plan.output_artifacts[0].checkpoint is True


def test_runtime_override_is_node_scoped_and_creates_boundaries() -> None:
    spec = _linear_spec()
    spec.nodes[2].runtime = RuntimeSpec(image="ghcr.io/example/dataflow:other")

    graph = compiler.PipelineCompiler().compile(spec, run_id="run-runtime")

    assert [unit.node_ids for unit in graph.units] == [
        ["read", "map"],
        ["filter"],
        ["write"],
    ]
    assert graph.units[1].plan.runtime.image == "ghcr.io/example/dataflow:other"
    assert graph.units[2].plan.runtime.image == RUNTIME.image


def test_data_fan_out_materializes_once_and_reuses_upstream_artifact() -> None:
    spec = compiler.PipelineSpec(
        name="fan-out",
        runtime=RUNTIME,
        artifact_base_uri="s3://bucket/dataflow",
        nodes=[
            compiler.PipelineNodeSpec(
                id="read",
                kind=OperatorKind.READ_PARQUET,
                config={"path": "s3://bucket/input"},
            ),
            compiler.PipelineNodeSpec(
                id="left",
                kind=OperatorKind.FILTER,
                config={"callable": "dataflow.callables.always_true"},
            ),
            compiler.PipelineNodeSpec(
                id="left_write",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "s3://bucket/left"},
            ),
            compiler.PipelineNodeSpec(
                id="right",
                kind=OperatorKind.FILTER,
                config={"callable": "dataflow.callables.always_true"},
            ),
            compiler.PipelineNodeSpec(
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

    graph = compiler.PipelineCompiler().compile(spec, run_id="fan-out-run")

    assert [unit.node_ids for unit in graph.units] == [
        ["read"],
        ["left", "left_write"],
        ["right", "right_write"],
    ]
    committed_uri = "s3://bucket/dataflow/runs/fan-out-run/artifacts/read/committed"
    assert graph.units[0].plan.output_artifacts[0].committed_uri == committed_uri
    assert graph.units[1].plan.operators[0].config["path"] == committed_uri
    assert graph.units[2].plan.operators[0].config["path"] == committed_uri


def test_logical_graph_topological_order_is_deterministic_for_diamond() -> None:
    spec = compiler.PipelineSpec(
        name="control-diamond",
        runtime=RUNTIME,
        nodes=[
            compiler.PipelineNodeSpec(
                id="a",
                kind=OperatorKind.READ_PARQUET,
                config={"path": "x"},
            ),
            compiler.PipelineNodeSpec(
                id="b",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "b"},
            ),
            compiler.PipelineNodeSpec(
                id="c",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "c"},
            ),
            compiler.PipelineNodeSpec(
                id="d",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "d"},
            ),
        ],
        edges=[
            _edge("a", "b", kind=compiler.EdgeKind.CONTROL),
            _edge("a", "c", kind=compiler.EdgeKind.CONTROL),
            _edge("b", "d", kind=compiler.EdgeKind.CONTROL),
            _edge("c", "d", kind=compiler.EdgeKind.CONTROL),
        ],
    )

    assert compiler.LogicalGraph(spec).topological_order == ["a", "b", "c", "d"]


def test_checkpoint_is_rejected_on_control_edge() -> None:
    with pytest.raises(ValueError, match="checkpoint"):
        compiler.PipelineSpec(
            name="invalid-checkpoint",
            runtime=RUNTIME,
            nodes=[
                compiler.PipelineNodeSpec(
                    id="a",
                    kind=OperatorKind.READ_PARQUET,
                    config={"path": "a"},
                ),
                compiler.PipelineNodeSpec(
                    id="b",
                    kind=OperatorKind.WRITE_PARQUET,
                    config={"path": "b"},
                ),
            ],
            edges=[
                _edge(
                    "a",
                    "b",
                    kind=compiler.EdgeKind.CONTROL,
                    boundary=compiler.ExecutionBoundary.CHECKPOINT,
                )
            ],
        )


def test_cycle_is_rejected() -> None:
    spec = compiler.PipelineSpec(
        name="cycle",
        runtime=RUNTIME,
        nodes=[
            compiler.PipelineNodeSpec(
                id="a",
                kind=OperatorKind.READ_PARQUET,
                config={"path": "x"},
            ),
            compiler.PipelineNodeSpec(
                id="b",
                kind=OperatorKind.WRITE_PARQUET,
                config={"path": "y"},
            ),
        ],
        edges=[
            _edge("a", "b", kind=compiler.EdgeKind.CONTROL),
            _edge("b", "a", kind=compiler.EdgeKind.CONTROL),
        ],
    )

    with pytest.raises(ValueError, match="cycle"):
        compiler.LogicalGraph(spec)


def test_dangling_edge_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown node"):
        compiler.PipelineSpec(
            name="dangling",
            runtime=RUNTIME,
            nodes=[
                compiler.PipelineNodeSpec(
                    id="a",
                    kind=OperatorKind.READ_PARQUET,
                    config={"path": "x"},
                )
            ],
            edges=[_edge("a", "missing")],
        )


def test_data_fan_in_is_explicitly_rejected_in_v1_runtime() -> None:
    spec = compiler.PipelineSpec(
        name="fan-in",
        runtime=RUNTIME,
        artifact_base_uri="s3://bucket/dataflow",
        nodes=[
            compiler.PipelineNodeSpec(
                id="a",
                kind=OperatorKind.READ_PARQUET,
                config={"path": "a"},
            ),
            compiler.PipelineNodeSpec(
                id="b",
                kind=OperatorKind.READ_PARQUET,
                config={"path": "b"},
            ),
            compiler.PipelineNodeSpec(
                id="c",
                kind=OperatorKind.MAP_BATCHES,
                config={"callable": "dataflow.callables.identity"},
            ),
            compiler.PipelineNodeSpec(
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

    assert compiler.LogicalGraph(spec).topological_order == ["a", "b", "c", "d"]
    with pytest.raises(ValueError, match="fan-in"):
        compiler.PipelineCompiler().compile(spec, run_id="run-fanin")


def test_compile_output_is_deterministic() -> None:
    pipeline_compiler = compiler.PipelineCompiler()
    first = pipeline_compiler.compile(_linear_spec(), run_id="same-run")
    second = pipeline_compiler.compile(_linear_spec(), run_id="same-run")

    assert first.model_dump(mode="json") == second.model_dump(mode="json")
