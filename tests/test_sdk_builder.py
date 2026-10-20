from __future__ import annotations

import json

import pytest

from dataflow.sdk import PipelineBuilder, Resources, callable_ref, pipeline
from dataflow.spec_hash import canonical_spec_hash


def preprocess(batch):
    return batch


class Predictor:
    def __call__(self, batch):
        return batch


@pipeline(name="inference")
def inference(flow, source, target):
    dataset = flow.read_parquet(source, node_id="read")
    dataset = dataset.map_batches(preprocess, node_id="prepare", resources=Resources(cpu=2))
    dataset.map_batches(Predictor, node_id="predict").write_parquet(target, node_id="write")


def test_sdk_builds_deterministic_serializable_spec_without_infrastructure_policy():
    first = inference.to_dict("/input", "/output")
    assert first == inference.to_dict("/input", "/output")
    assert set(first) == {"api_version", "kind", "name", "nodes", "edges"}
    json.dumps(first)
    assert inference.spec_hash("/input", "/output") == canonical_spec_hash(first)
    assert first["nodes"][1]["config"]["callable"] == "tests.test_sdk_builder.preprocess"
    assert first["nodes"][1]["resources"]["cpu"] == 2


def test_stable_generated_ids_and_fanout():
    @pipeline(name="branch")
    def branch(flow):
        source = flow.read_parquet("/input")
        source.filter("dataflow.callables.keep_all").write_parquet("/left")
        source.write_parquet("/right")

    spec = branch.spec()
    assert [n.id for n in spec.nodes] == [
        "read-parquet-001",
        "filter-002",
        "write-parquet-003",
        "write-parquet-004",
    ]
    assert [(e.from_node, e.to_node) for e in spec.edges] == [
        ("read-parquet-001", "filter-002"),
        ("filter-002", "write-parquet-003"),
        ("read-parquet-001", "write-parquet-004"),
    ]


def test_rejects_duplicate_and_foreign_node_handles():
    first, second = PipelineBuilder(name="one"), PipelineBuilder(name="two")
    source = first.read_parquet("/input", node_id="same")
    with pytest.raises(ValueError, match="duplicate"):
        first.read_parquet("/other", node_id="same")
    with pytest.raises(ValueError, match="different PipelineBuilder"):
        second._add(
            source=source,
            kind=source.builder.to_spec().nodes[0].kind,
            node_id="foreign",
            resources=None,
            config={"path": "/out"},
        )


def test_importable_callable_contract():
    assert callable_ref(preprocess) == "tests.test_sdk_builder.preprocess"
    assert callable_ref(Predictor) == "tests.test_sdk_builder.Predictor"
    assert callable_ref("dataflow.callables.identity_batch") == "dataflow.callables.identity_batch"
    with pytest.raises(ValueError, match="top-level"):
        callable_ref(lambda value: value)
    with pytest.raises(TypeError):
        callable_ref(Predictor())
    with pytest.raises(ValueError, match="fully-qualified"):
        callable_ref("unqualified")
