from __future__ import annotations

import subprocess
import sys
from collections import Counter
from unittest.mock import Mock

import pytest

from dataflow.compiler import PipelineSpec
from dataflow.sdk import Resources, pipeline, run_pipeline
from dataflow.single_cluster import (
    PipelineExecutionError,
    PipelineResult,
    SingleClusterExecutor,
)


@pipeline(name="single-cluster")
def example(flow, *, branch=False):
    data = flow.read_parquet("/input", node_id="read", resources=Resources(cpu=0.25))
    data = data.map_batches(
        "dataflow.callables.identity_batch", node_id="map", resources=Resources(cpu=2)
    )
    data.write_parquet("/left", node_id="left", resources=Resources(cpu=0.5))
    if branch:
        data.filter("dataflow.callables.keep_all", node_id="filter").write_parquet(
            "/right", node_id="right"
        )


class LazyDataset:
    def __init__(self, backend, name, parent=None):
        self.backend, self.name, self.parent = backend, name, parent
        self.cached = False

    def evaluate(self):
        if not self.cached:
            if self.parent:
                self.parent.evaluate()
            self.backend.evaluations[self.name] += 1

    def materialize(self):
        self.backend.calls.append(("materialize", self.name, {}))
        self.evaluate()
        self.cached = True
        return self

    def map_batches(self, fn, **config):
        self.backend.calls.append(("map", fn, config))
        return LazyDataset(self.backend, "map", self)

    def filter(self, fn, **config):
        self.backend.calls.append(("filter", fn, config))
        return LazyDataset(self.backend, "filter", self)

    def write_parquet(self, path, **config):
        self.evaluate()
        self.backend.calls.append(("write", path, config))


class LazyBackend:
    def __init__(self):
        self.calls = []
        self.evaluations = Counter()

    def read_parquet(self, path, **config):
        self.calls.append(("read", path, config))
        return LazyDataset(self, "read")


def test_linear_chain_streams_without_intermediate_materialization():
    backend = LazyBackend()
    result = SingleClusterExecutor(backend).execute(example.spec())
    assert result == PipelineResult("single-cluster", {"left": "/left"})
    assert [c[0] for c in backend.calls] == ["read", "map", "write"]
    assert backend.calls[0][2] == {"ray_remote_args": {"num_cpus": 0.25}}
    assert backend.calls[1][2] == {"num_cpus": 2}
    assert backend.calls[2][2] == {"ray_remote_args": {"num_cpus": 0.5}}


def test_fanout_reuses_object_store_data_without_artifact_io():
    backend = LazyBackend()
    result = SingleClusterExecutor(backend).execute(example.spec(branch=True))
    assert result.output_paths == {"left": "/left", "right": "/right"}
    assert backend.evaluations == {"read": 1, "map": 1, "filter": 1}
    assert [(c[0], c[1]) for c in backend.calls if c[0] in {"read", "write", "materialize"}] == [
        ("read", "/input"),
        ("materialize", "map"),
        ("write", "/left"),
        ("write", "/right"),
    ]


@pytest.mark.parametrize(
    "change, message",
    [
        ({"cluster_profile": "gpu"}, "ClusterProfiles"),
        ({"artifact_base_uri": "s3://legacy"}, "artifact_base_uri"),
        ({"runtime": {"image": "legacy:image"}}, "existing worker environment"),
    ],
)
def test_legacy_settings_fail_before_io(change, message):
    payload = example.to_dict()
    payload.update(change)
    backend = LazyBackend()
    with pytest.raises(ValueError, match=message):
        SingleClusterExecutor(backend).execute(PipelineSpec.model_validate(payload))
    assert backend.calls == []


@pytest.mark.parametrize("boundary", ["hard", "checkpoint"])
def test_legacy_boundaries_do_not_silently_lose_durability(boundary):
    spec = example.spec()
    spec.edges[0].boundary = type(spec.edges[0].boundary)(boundary)
    backend = LazyBackend()
    with pytest.raises(ValueError, match="hard/checkpoint"):
        SingleClusterExecutor(backend).execute(spec)
    assert backend.calls == []


def test_cycle_and_dangling_transform_are_rejected_before_io():
    payload = example.to_dict()
    payload["edges"].append({"from": "left", "to": "read", "kind": "control"})
    with pytest.raises(ValueError, match="cycle"):
        SingleClusterExecutor(LazyBackend()).execute(PipelineSpec.model_validate(payload))
    payload = example.to_dict()
    payload["nodes"] = payload["nodes"][:-1]
    payload["edges"] = payload["edges"][:-1]
    with pytest.raises(ValueError, match="terminal node"):
        SingleClusterExecutor(LazyBackend()).execute(PipelineSpec.model_validate(payload))


def test_control_edge_waits_for_lazy_source_completion():
    payload = example.to_dict()
    payload["edges"].append({"from": "map", "to": "left", "kind": "control"})
    backend = LazyBackend()
    SingleClusterExecutor(backend).execute(PipelineSpec.model_validate(payload))
    assert [c[0] for c in backend.calls] == ["read", "map", "materialize", "write"]
    assert backend.evaluations["map"] == 1


def test_failure_reports_triggering_node_and_preserves_original_error():
    backend = LazyBackend()
    backend.read_parquet = Mock(side_effect=OSError("input unavailable"))
    with pytest.raises(PipelineExecutionError) as caught:
        SingleClusterExecutor(backend).execute(example.spec())
    assert caught.value.node_id == "read"
    assert isinstance(caught.value.__cause__, OSError)


def test_cli_runs_a_whole_json_pipeline_without_legacy_compilation(monkeypatch, tmp_path, capsys):
    from dataflow import cli

    path = tmp_path / "pipeline.json"
    path.write_text(example.spec().model_dump_json(by_alias=True))
    execute = Mock(return_value=PipelineResult("single-cluster", {"left": "/left"}))
    monkeypatch.setattr(cli, "run_pipeline", execute)
    monkeypatch.setattr(sys, "argv", ["dataflow-run", str(path), "--address", "head:6379"])
    cli.run_pipeline_cli()
    assert execute.call_args.args[0] == example.spec()
    assert execute.call_args.kwargs == {"address": "head:6379", "namespace": None}
    assert '"output_paths": {"left": "/left"}' in capsys.readouterr().out


def test_legacy_commands_require_explicit_cluster_image():
    from dataflow.compiler import compile_pipeline
    from dataflow.contracts import ExecutionPlan, OperatorSpec
    from dataflow.kuberay import render_rayjob

    with pytest.raises(ValueError, match="legacy execution-island compiler"):
        compile_pipeline(example.spec(), run_id="run")
    spec = example.spec()
    plan = ExecutionPlan(
        run_id="run",
        unit_id="unit",
        runtime=spec.runtime,
        operators=[
            OperatorSpec(id=node.id, kind=node.kind, config=node.config, resources=node.resources)
            for node in spec.nodes
        ],
    )
    with pytest.raises(ValueError, match="legacy KubeRay"):
        render_rayjob(plan)


@pytest.fixture
def ray_session(monkeypatch):
    ray = pytest.importorskip("ray")
    monkeypatch.setattr(ray, "is_initialized", Mock(return_value=False))
    monkeypatch.setattr(ray, "init", Mock())
    monkeypatch.setattr(ray, "shutdown", Mock())
    monkeypatch.setattr(SingleClusterExecutor, "execute", Mock(return_value="result"))
    monkeypatch.delenv("RAY_ADDRESS", raising=False)
    return ray


def test_connects_once_and_disconnects_without_cluster_creation(ray_session):
    assert run_pipeline(example.spec()) == "result"
    ray_session.init.assert_called_once_with(address="auto", namespace="default")
    ray_session.shutdown.assert_called_once_with()


def test_borrowed_session_is_never_disconnected(ray_session):
    ray_session.is_initialized.return_value = True
    run_pipeline(example.spec())
    ray_session.init.assert_not_called()
    ray_session.shutdown.assert_not_called()
    with pytest.raises(ValueError, match="already initialized"):
        run_pipeline(example.spec(), address="other-head:6379")


@pytest.mark.parametrize("address", ["local", "", " LOCAL "])
def test_cannot_accidentally_start_a_local_cluster(ray_session, address):
    with pytest.raises(ValueError, match="existing Ray cluster"):
        run_pipeline(example.spec(), address=address)
    ray_session.init.assert_not_called()


def test_environment_cannot_request_new_cluster(ray_session, monkeypatch):
    monkeypatch.setenv("RAY_ADDRESS", "local")
    with pytest.raises(ValueError, match="existing Ray cluster"):
        run_pipeline(example.spec())
    ray_session.init.assert_not_called()


def test_disconnects_own_session_on_pipeline_failure(ray_session):
    SingleClusterExecutor.execute.side_effect = RuntimeError("failed")
    with pytest.raises(RuntimeError, match="failed"):
        run_pipeline(example.spec(), address="head:6379")
    ray_session.shutdown.assert_called_once_with()


def test_sdk_import_and_hash_need_no_control_plane_dependencies():
    code = """
import sys
for name in ("psycopg", "httpx", "kubernetes", "boto3", "ray"):
    sys.modules[name] = None
from dataflow.sdk import pipeline, run_pipeline
@pipeline(name="minimal")
def demo(flow):
    flow.read_parquet("/in").write_parquet("/out")
assert len(demo.spec_hash()) == 64
assert demo.spec().runtime.image is None
"""
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True)
