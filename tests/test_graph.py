from __future__ import annotations

import pytest

from dataflow.graph import LogicalGraph, PipelineSpec
from tests.test_single_cluster import example


@pytest.mark.parametrize("field", ["cluster_profile", "artifact_base_uri", "runtime"])
def test_removed_spec_fields_are_rejected_by_the_contract(field):
    payload = example.to_dict()
    payload[field] = "removed"
    with pytest.raises(ValueError, match="Extra inputs"):
        PipelineSpec.model_validate(payload)


def test_removed_node_and_edge_options_are_rejected():
    payload = example.to_dict()
    payload["nodes"][0]["runtime"] = {}
    with pytest.raises(ValueError, match="Extra inputs"):
        PipelineSpec.model_validate(payload)
    payload = example.to_dict()
    payload["edges"][0]["boundary"] = "checkpoint"
    with pytest.raises(ValueError, match="Extra inputs"):
        PipelineSpec.model_validate(payload)


@pytest.mark.parametrize(
    "change, message",
    [
        ("duplicate-node", "node ids must be unique"),
        ("missing-node", "unknown node"),
        ("self-edge", "self edges"),
        ("duplicate-edge", "duplicate edge"),
        ("cycle", "cycle"),
    ],
)
def test_invalid_dependencies_are_rejected(change, message):
    payload = example.to_dict()
    if change == "duplicate-node":
        payload["nodes"].append(payload["nodes"][0])
    elif change == "missing-node":
        payload["edges"][0]["from"] = "missing"
    elif change == "self-edge":
        payload["edges"][0]["to"] = payload["edges"][0]["from"]
    elif change == "duplicate-edge":
        payload["edges"].append(payload["edges"][0])
    else:
        payload["edges"].append({"from": "left", "to": "read", "kind": "control"})
    with pytest.raises(ValueError, match=message):
        LogicalGraph(PipelineSpec.model_validate(payload))


def test_topology_is_stable_when_declaration_order_changes():
    payload = example.to_dict(branch=True)
    graph = LogicalGraph(PipelineSpec.model_validate(payload))
    payload["nodes"].reverse()
    payload["edges"].reverse()
    assert (
        LogicalGraph(PipelineSpec.model_validate(payload)).topological_order
        == graph.topological_order
    )
