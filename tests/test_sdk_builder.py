from __future__ import annotations

import json

import pytest

from dataflow.compiler import ExecutionBoundary, PipelineCompiler
from dataflow.metadata.repository import canonical_spec_hash
from dataflow.sdk import Resources, callable_ref, pipeline


def preprocess(batch):
    return batch


class Predictor:
    def __call__(self, batch):
        return batch


def keep_all(row):
    return True


@pipeline(
    name="image-pipeline",
    runtime_image="dataflow-runtime:test",
    cluster_profile="gpu-test",
    artifact_base_uri="s3://bucket/dataflow",
)
def image_pipeline(flow, input_path: str, output_path: str) -> None:
    dataset = flow.read_parquet(input_path, node_id="read")
    dataset = dataset.map_batches(
        preprocess,
        node_id="preprocess",
        resources=Resources(cpu=2, memory_bytes=4 * 1024**3),
        batch_size=128,
    )
    dataset = dataset.checkpoint().map_batches(
        Predictor,
        node_id="predict",
        resources=Resources(cpu=2, gpu=1),
    )
    dataset = dataset.filter(keep_all, node_id="filter")
    dataset.write_parquet(output_path, node_id="write")


def test_decorator_builds_deterministic_serializable_pipeline_spec() -> None:
    first = image_pipeline.spec("s3://bucket/input", "s3://bucket/output")
    second = image_pipeline.spec("s3://bucket/input", "s3://bucket/output")

    assert first.model_dump(mode="json", by_alias=True) == second.model_dump(
        mode="json", by_alias=True
    )
    assert [node.id for node in first.nodes] == [
        "read",
        "preprocess",
        "predict",
        "filter",
        "write",
    ]
    assert first.nodes[1].config["callable"] == "tests.test_sdk_builder.preprocess"
    assert first.nodes[2].config["callable"] == "tests.test_sdk_builder.Predictor"
    assert first.nodes[1].resources.cpu == 2
    assert first.nodes[2].resources.gpu == 1
    assert first.edges[1].boundary is ExecutionBoundary.CHECKPOINT

    payload = image_pipeline.to_dict("s3://bucket/input", "s3://bucket/output")
    json.dumps(payload, sort_keys=True)
    assert image_pipeline.spec_hash("s3://bucket/input", "s3://bucket/output") == (
        canonical_spec_hash(payload)
    )


def test_sdk_spec_compiles_through_the_same_execution_island_compiler() -> None:
    spec = image_pipeline("s3://bucket/input", "s3://bucket/output")

    graph = PipelineCompiler().compile(spec, run_id="sdk-run")

    assert [unit.node_ids for unit in graph.units] == [
        ["read", "preprocess"],
        ["predict", "filter", "write"],
    ]
    assert graph.units[0].plan.output_artifacts[0].checkpoint is True
    assert graph.units[1].plan.input_artifacts[0].node_id == "preprocess"


def test_auto_generated_node_ids_are_stable_and_support_fan_out() -> None:
    @pipeline(
        name="fan-out",
        runtime_image="dataflow-runtime:test",
        artifact_base_uri="s3://bucket/dataflow",
    )
    def fan_out(flow) -> None:
        source = flow.read_parquet("s3://bucket/input")
        left = source.filter(keep_all)
        right = source.filter(keep_all)
        left.write_parquet("s3://bucket/left")
        right.write_parquet("s3://bucket/right")

    spec = fan_out.spec()

    assert [node.id for node in spec.nodes] == [
        "read-parquet-001",
        "filter-002",
        "filter-003",
        "write-parquet-004",
        "write-parquet-005",
    ]
    assert [(edge.from_node, edge.to_node) for edge in spec.edges] == [
        ("read-parquet-001", "filter-002"),
        ("read-parquet-001", "filter-003"),
        ("filter-002", "write-parquet-004"),
        ("filter-003", "write-parquet-005"),
    ]


def test_callable_ref_rejects_non_reproducible_python_objects() -> None:
    assert callable_ref(preprocess) == "tests.test_sdk_builder.preprocess"
    assert callable_ref("package.module.symbol") == "package.module.symbol"

    with pytest.raises(ValueError, match="top-level"):
        callable_ref(lambda value: value)

    def local(value):
        return value

    with pytest.raises(ValueError, match="top-level"):
        callable_ref(local)

    with pytest.raises(ValueError, match="fully-qualified"):
        callable_ref("not-qualified")


def test_duplicate_explicit_node_ids_are_rejected_during_authoring() -> None:
    @pipeline(name="duplicate", runtime_image="dataflow-runtime:test")
    def duplicate(flow) -> None:
        source = flow.read_parquet("input", node_id="same")
        source.filter(keep_all, node_id="same")

    with pytest.raises(ValueError, match="duplicate SDK node id"):
        duplicate.spec()
