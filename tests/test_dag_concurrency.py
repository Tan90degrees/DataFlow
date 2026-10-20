from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event, Lock
from types import SimpleNamespace

import pytest

from dataflow import PipelineSpec
from dataflow.runner import PipelineBatchError, PipelineRunner
from dataflow.sdk import pipeline
from dataflow.single_cluster import (
    PipelineExecutionError,
    SingleClusterExecutor,
    validate_single_cluster,
)
from tests.test_single_cluster import LazyBackend, LazyDataset, example


class GateDataset(LazyDataset):
    def write_parquet(self, path, **config):
        backend = self.backend
        with backend.lock:
            backend.active += 1
            backend.peak = max(backend.peak, backend.active)
            if backend.active >= backend.target:
                backend.entered.set()
        try:
            assert backend.release.wait(5), "test action did not get released"
            super().write_parquet(path, **config)
        finally:
            with backend.lock:
                backend.active -= 1


class GateBackend(LazyBackend):
    def __init__(self, target=2):
        super().__init__()
        self.target = target
        self.active = self.peak = 0
        self.entered, self.release, self.lock = Event(), Event(), Lock()

    def read_parquet(self, path, **config):
        self.calls.append(("read", path, config))
        return GateDataset(self, "read")


@pipeline(name="minimal")
def minimal(flow, output="/output"):
    flow.read_parquet("/input").write_parquet(output)


def test_independent_sinks_overlap_and_default_fanout_remains_lazy():
    backend = GateBackend(target=2)
    with ThreadPoolExecutor(max_workers=1) as host:
        future = host.submit(
            SingleClusterExecutor(backend, max_concurrency=2).execute, example.spec(branch=True)
        )
        try:
            assert backend.entered.wait(3), "independent sinks were serialized"
            assert backend.peak == 2
        finally:
            backend.release.set()
        result = future.result(3)
    assert result.materialized_nodes == ()
    assert not any(call[0] == "materialize" for call in backend.calls)
    assert backend.evaluations["map"] == 2  # Explicitly permits recomputation, not fake broadcast.


def test_ready_queue_is_bounded_by_node_concurrency():
    @pipeline(name="four-sinks")
    def four(flow):
        source = flow.read_parquet("/input")
        for index in range(4):
            source.write_parquet(f"/output-{index}")

    backend = GateBackend(target=2)
    with ThreadPoolExecutor(max_workers=1) as host:
        future = host.submit(SingleClusterExecutor(backend, max_concurrency=2).execute, four.spec())
        try:
            assert backend.entered.wait(3)
            assert backend.active == 2
        finally:
            backend.release.set()
        assert future.result(3).peak_in_flight_nodes <= 2
    assert backend.peak == 2
    assert len([call for call in backend.calls if call[0] == "write"]) == 4


def test_control_dependency_waits_for_completed_write():
    finished = Event()

    class OrderedDataset(LazyDataset):
        def write_parquet(self, path, **config):
            if path == "/second":
                assert finished.is_set(), "control target ran before its source finished"
            super().write_parquet(path, **config)
            if path == "/first":
                finished.set()

    class Backend(LazyBackend):
        def read_parquet(self, path, **config):
            return OrderedDataset(self, "read")

    @pipeline(name="control")
    def flow(f):
        source = f.read_parquet("/input")
        first = source.write_parquet("/first")
        second = source.write_parquet("/second")
        f.after(first, second)

    SingleClusterExecutor(Backend(), max_concurrency=4).execute(flow.spec())
    assert finished.is_set()


def test_failure_stops_descendants_and_drains_started_actions():
    started, release = Event(), Event()

    class BlockingDataset(LazyDataset):
        def write_parquet(self, path, **config):
            started.set()
            assert release.wait(5)
            super().write_parquet(path, **config)

    class Backend(LazyBackend):
        def read_parquet(self, path, **config):
            if path == "/fail":
                assert started.wait(3)
                raise OSError("broken source")
            return BlockingDataset(self, "read")

    @pipeline(name="failure")
    def flow(f):
        f.read_parquet("/fail", node_id="a-fail").write_parquet("/must-not-write")
        f.read_parquet("/good", node_id="z-good").write_parquet("/started")

    backend = Backend()
    with ThreadPoolExecutor(max_workers=1) as host:
        future = host.submit(SingleClusterExecutor(backend, max_concurrency=2).execute, flow.spec())
        try:
            assert started.wait(3)
            assert not future.done()
        finally:
            release.set()
        with pytest.raises(PipelineExecutionError) as caught:
            future.result(3)
    assert caught.value.node_id == "a-fail"
    assert isinstance(caught.value.__cause__, OSError)
    assert [call[1] for call in backend.calls if call[0] == "write"] == ["/started"]


@pytest.fixture
def runner_backend(monkeypatch):
    backend = GateBackend(target=1)
    disconnected = Event()

    @contextmanager
    def connection(*_args):
        try:
            yield backend
        finally:
            assert backend.active == 0
            disconnected.set()

    monkeypatch.setattr("dataflow.runner.ray_connection", connection)
    return backend, disconnected


def test_queue_backpressure_cancelled_items_remain_physically_bounded(runner_backend):
    backend, disconnected = runner_backend
    with PipelineRunner(max_concurrent_dags=1, max_queued_dags=1, max_concurrent_nodes=1) as runner:
        first = runner.submit(minimal.spec("/first"))
        try:
            assert backend.entered.wait(3)
            queued = runner.submit(minimal.spec("/queued"))
            assert queued.cancel()
            with pytest.raises(TimeoutError):
                runner.submit(minimal.spec("/overflow"), timeout=0)
            assert runner.snapshot().active_dags == 1
            assert runner.snapshot().queued_dags == 1
        finally:
            backend.release.set()
        first.result(3)
    assert disconnected.is_set()
    assert runner.snapshot().cancelled_dags == 1
    with pytest.raises(RuntimeError, match="closed"):
        runner.submit(minimal.spec())


def test_shared_node_pool_bounds_concurrency_across_dags(runner_backend):
    backend, _ = runner_backend
    backend.target = 2
    with PipelineRunner(max_concurrent_dags=3, max_queued_dags=3, max_concurrent_nodes=2) as runner:
        futures = [runner.submit(minimal.spec(f"/out-{index}")) for index in range(3)]
        try:
            assert backend.entered.wait(3)
            assert backend.active == 2
        finally:
            backend.release.set()
        for future in futures:
            future.result(3)
    assert backend.peak == 2
    assert runner.snapshot().completed_dags == 3


def test_iter_completed_bounds_generator_consumption_and_drains_early_close(runner_backend):
    backend, _ = runner_backend
    consumed = []

    def source():
        for index in range(1000):
            consumed.append(index)
            yield minimal.spec(f"/out-{index}")

    with PipelineRunner(max_concurrent_dags=2, max_queued_dags=2, max_concurrent_nodes=2) as runner:
        iterator = runner.iter_completed(source())
        with ThreadPoolExecutor(max_workers=1) as host:
            future = host.submit(next, iterator)
            try:
                assert backend.entered.wait(3)
                # Initial input window is at most active + queued DAG capacity.
                assert len(consumed) <= 4
            finally:
                backend.release.set()
            index, result = future.result(3)
        assert index in range(4)
        assert result.pipeline_name == "minimal"
        iterator.close()
        assert len(consumed) == 4
    assert backend.active == 0


def test_batch_failure_preserves_input_index_and_original_cause(monkeypatch):
    backend = LazyBackend()

    def broken(*_args, **_kwargs):
        raise OSError("bad input")

    backend.read_parquet = broken

    @contextmanager
    def connection(*_args):
        yield backend

    monkeypatch.setattr("dataflow.runner.ray_connection", connection)
    with PipelineRunner(max_concurrent_dags=1, max_queued_dags=1, max_concurrent_nodes=1) as runner:
        with pytest.raises(PipelineBatchError) as caught:
            list(runner.iter_completed([minimal.spec()]))
    assert caught.value.index == 0
    assert isinstance(caught.value.__cause__, PipelineExecutionError)
    assert isinstance(caught.value.__cause__.__cause__, OSError)


def test_submission_snapshots_mutable_specs(runner_backend):
    backend, _ = runner_backend
    with PipelineRunner(max_concurrent_dags=1, max_queued_dags=1, max_concurrent_nodes=1) as runner:
        first = runner.submit(minimal.spec("/first"))
        try:
            assert backend.entered.wait(3)
            spec = minimal.spec("/original")
            second = runner.submit(spec)
            spec.nodes[-1].config["path"] = "/mutated"
        finally:
            backend.release.set()
        first.result(3)
        assert list(second.result(3).output_paths.values()) == ["/original"]


def test_merge_ports_follow_sdk_input_order_not_node_names():
    @pipeline(name="merge")
    def merged(f):
        left = f.read_parquet("/left", node_id="z-left")
        right = f.read_parquet("/right", node_id="a-right")
        left.join(right, on="id", num_partitions=2, node_id="join").write_parquet("/output")

    graph = validate_single_cluster(merged.spec())
    assert [e.from_node for e in graph.data_incoming("join")] == ["z-left", "a-right"]
    assert graph.nodes["join"].config["on"] == ["id"]
    payload = merged.to_dict()
    payload["edges"][1]["input_index"] = 0
    with pytest.raises(ValueError, match="input_index"):
        validate_single_cluster(PipelineSpec.model_validate(payload))


def test_union_rejects_incompatible_schemas_without_writing():
    class SchemaDataset(LazyDataset):
        def schema(self):
            return SimpleNamespace(names=[self.name], types=["int"])

    class Backend(LazyBackend):
        def read_parquet(self, path, **config):
            return SchemaDataset(self, path)

    @pipeline(name="union")
    def flow(f):
        f.read_parquet("left").union(f.read_parquet("right")).write_parquet("/out")

    backend = Backend()
    with pytest.raises(PipelineExecutionError) as caught:
        SingleClusterExecutor(backend).execute(flow.spec())
    assert isinstance(caught.value.__cause__, ValueError)
    assert "identical" in str(caught.value.__cause__)
    assert backend.calls == []


def test_close_unblocks_admission_and_drains_running_work(runner_backend):
    backend, disconnected = runner_backend
    runner = PipelineRunner(max_concurrent_dags=1, max_queued_dags=1, max_concurrent_nodes=1)
    first = runner.submit(minimal.spec("/first"))
    assert backend.entered.wait(3)
    queued = runner.submit(minimal.spec("/queued"))
    with ThreadPoolExecutor(max_workers=2) as host:
        blocked = host.submit(runner.submit, minimal.spec("/blocked"))
        closing = host.submit(runner.close, cancel_pending=True)
        try:
            with pytest.raises(RuntimeError, match="closed"):
                blocked.result(3)
            assert not disconnected.is_set()
        finally:
            backend.release.set()
        closing.result(3)
    first.result(3)
    assert queued.cancelled()
    assert disconnected.is_set()


@pytest.mark.parametrize(
    "config",
    [
        {"on": [], "num_partitions": 2},
        {"on": ["id"], "num_partitions": 0},
        {"on": ["id"], "num_partitions": 2, "join_type": "zip"},
        {"on": ["id"], "right_on": ["x", "y"], "num_partitions": 2},
    ],
)
def test_invalid_join_configuration_fails_before_io(config):
    @pipeline(name="join")
    def flow(f):
        left, right = f.read_parquet("left"), f.read_parquet("right")
        left.join(right, on="id", num_partitions=2, node_id="join").write_parquet("out")

    spec = flow.spec()
    next(n for n in spec.nodes if n.id == "join").config = config
    backend = LazyBackend()
    with pytest.raises(ValueError):
        SingleClusterExecutor(backend).execute(spec)
    assert backend.calls == []
