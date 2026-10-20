"""Long-lived bounded admission and shared node workers for concurrent DAGs."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from math import isfinite
from threading import Condition, Lock, Thread, current_thread
from time import monotonic

from dataflow.graph import LogicalGraph, PipelineSpec
from dataflow.single_cluster import (
    PipelineResult,
    SingleClusterExecutor,
    positive_integer,
    ray_connection,
    validate_single_cluster,
)


class PipelineBatchError(RuntimeError):
    def __init__(self, index: int, pipeline_name: str) -> None:
        self.index, self.pipeline_name = index, pipeline_name
        super().__init__(f"pipeline batch item {index} ({pipeline_name!r}) failed")


@dataclass(frozen=True)
class RunnerSnapshot:
    active_dags: int
    queued_dags: int
    completed_dags: int
    failed_dags: int
    cancelled_dags: int


class PipelineRunner:
    """One Ray connection, fixed DAG coordinators, and one shared node-action pool.

    Use as a context manager. Queue admission blocks when full (or times out).
    Closing drains started work before disconnecting. Running DAGs cannot be killed
    through Future.cancel(); cancellation only applies before a DAG starts.
    """

    def __init__(
        self,
        *,
        address: str | None = None,
        namespace: str | None = None,
        max_concurrent_dags: int = 8,
        max_queued_dags: int = 32,
        max_concurrent_nodes: int = 8,
    ) -> None:
        self._max_dags = positive_integer("max_concurrent_dags", max_concurrent_dags)
        self._max_queued = positive_integer("max_queued_dags", max_queued_dags)
        self._max_nodes = positive_integer("max_concurrent_nodes", max_concurrent_nodes)
        self._condition = Condition()
        self._close_lock = Lock()
        self._queue: deque[tuple[LogicalGraph, Future]] = deque()
        self._closed = False
        self._active = self._completed = self._failed = self._cancelled = 0
        self._workers: list[Thread] = []
        self._connection = ray_connection(address, namespace)
        backend = self._connection.__enter__()
        self._actions = ThreadPoolExecutor(
            max_workers=self._max_nodes, thread_name_prefix="dataflow-node"
        )
        self._executor = SingleClusterExecutor(
            backend,
            max_concurrency=self._max_nodes,
            action_pool=self._actions,
        )
        try:
            for index in range(self._max_dags):
                worker = Thread(target=self._work, name=f"dataflow-dag-{index}", daemon=True)
                worker.start()
                self._workers.append(worker)
        except BaseException:
            self.close(cancel_pending=True)
            raise

    def __enter__(self) -> PipelineRunner:
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def submit(self, spec: PipelineSpec, *, timeout: float | None = None) -> Future[PipelineResult]:
        if timeout is not None and (not isfinite(timeout) or timeout < 0):
            raise ValueError("timeout must be a finite non-negative number")
        graph = validate_single_cluster(spec.model_copy(deep=True))
        deadline = None if timeout is None else monotonic() + timeout
        future: Future[PipelineResult] = Future()
        with self._condition:
            while len(self._queue) >= self._max_queued and not self._closed:
                if current_thread() in self._workers:
                    raise RuntimeError("a worker callback cannot block on a full admission queue")
                remaining = None if deadline is None else deadline - monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("DAG admission queue is full")
                self._condition.wait(remaining)
            if self._closed:
                raise RuntimeError("PipelineRunner is closed")
            self._queue.append((graph, future))
            self._condition.notify()
        return future

    def snapshot(self) -> RunnerSnapshot:
        with self._condition:
            return RunnerSnapshot(
                self._active, len(self._queue), self._completed, self._failed, self._cancelled
            )

    def iter_completed(self, specs: Iterable[PipelineSpec]) -> Iterator[tuple[int, PipelineResult]]:
        """Consume a bounded input window and yield (input_index, result) as DAGs finish.

        On failure or early iterator close, queued work is cancelled and started work
        is drained. Results do not retain an unbounded list of all prior submissions.
        """
        source = iter(enumerate(specs))
        pending: dict[Future, tuple[int, str]] = {}
        window = self._max_dags + self._max_queued
        try:
            for _ in range(window):
                item = next(source, None)
                if item is None:
                    break
                index, spec = item
                pending[self.submit(spec)] = (index, spec.name)
            while pending:
                completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                ordered = sorted(completed, key=lambda f: pending[f][0])
                for future in ordered:
                    if future.cancelled() or future.exception() is not None:
                        index, name = pending[future]
                        try:
                            future.result()
                        except BaseException as error:
                            raise PipelineBatchError(index, name) from error
                for future in ordered:
                    index, _name = pending.pop(future)
                    yield index, future.result()
                    item = next(source, None)
                    if item is not None:
                        next_index, spec = item
                        pending[self.submit(spec)] = (next_index, spec.name)
        finally:
            for future in pending:
                future.cancel()
            wait(pending)

    def close(self, *, cancel_pending: bool = False) -> None:
        if current_thread() in self._workers:
            raise RuntimeError("close the runner from its owning thread, not a worker callback")
        with self._close_lock:
            with self._condition:
                if self._closed:
                    return
                self._closed = True
                if cancel_pending:
                    for _graph, future in self._queue:
                        future.cancel()
                self._condition.notify_all()
            for worker in self._workers:
                worker.join()
            self._actions.shutdown(wait=True, cancel_futures=True)
            self._connection.__exit__(None, None, None)

    def _work(self) -> None:
        while True:
            with self._condition:
                while not self._queue and not self._closed:
                    self._condition.wait()
                if not self._queue:
                    return
                graph, future = self._queue.popleft()
                self._condition.notify_all()
                if not future.set_running_or_notify_cancel():
                    self._cancelled += 1
                    continue
                self._active += 1
            try:
                result = self._executor._execute_graph(graph)
            except BaseException as error:
                with self._condition:
                    self._active -= 1
                    self._failed += 1
                future.set_exception(error)
            else:
                with self._condition:
                    self._active -= 1
                    self._completed += 1
                future.set_result(result)
