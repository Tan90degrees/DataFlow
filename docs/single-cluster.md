# Execution architecture

English | [简体中文](zh-CN/single-cluster.md)

## Responsibilities

DataFlow validates a static data/control DAG and drives Ray Data inside the calling Python
process. Ray owns task/actor placement and block transport; Ray Data owns streaming execution,
fusion and backpressure. A call does not create a driver process or cluster. All work targets
one pre-existing cluster, whose lifecycle/dependencies belong to the platform.

`run_pipeline` executes a single DAG. `PipelineRunner` keeps one connection, a fixed set of DAG
coordinator threads and one shared node-action ThreadPoolExecutor for many DAGs. These threads
construct plans or wait for Ray work; business computation stays in Ray tasks/actors.
There is no database, extra service, per-run cluster or intermediate artifact protocol.

## Dependency scheduling

Every node waits until all predecessor nodes complete. Completing an ordinary transform means
its lazy Dataset plan is built, so downstream Ray Data operations can fuse/stream. An explicit
cache or a control source is materialized before releasing dependents. A writer completes only
after its output write returns. Independent nodes/sinks run concurrently up to the configured
node-action limit. The coordinator uses completion notifications rather than polling.

Each active DAG keeps at most `max_concurrent_nodes` node Futures. A shared runner executes at
most that many actions globally; its finite action backlog is bounded by active DAGs times
that limit. A DAG coordinator never occupies a node-action worker while waiting for dependencies,
avoiding nested-pool starvation. Dataset handles are copied before concurrent consumption;
blocks are not copied by this step. Do not mutate Ray DataContext or caller specs concurrently.

## Sharing and fan-in

Fan-out stays lazy by default. Each consumer may execute the shared prefix again. This favors
streaming and avoids an automatic whole-dataset barrier, but can cost substantial extra work.
UDFs must tolerate recomputation/retries; avoid non-idempotent side effects in transforms.
Use `.cache()` on an expensive shared prefix to materialize it once per run and reuse its blocks.
Cache is a memory/execution tradeoff, not durable state or an adaptive memory budget.

Control edges force completed source work before constructing their targets. A transform used
as a control source is materialized even without `.cache()`. A writer source has already
completed its sink. Control edges transfer no data. `flow.after(source, target)` authors them.

`union` appends rows from two or more same-schema inputs in explicit input-port order, without
alignment or deduplication. Schema names/types are checked; schema discovery can trigger Ray
sampling. `join` combines exactly two inputs by explicit keys and join type; port 0 is left,
port 1 right. It uses Ray's hash-shuffle implementation and can materialize inputs/consume
significant memory. Neither operator is a positional zip or an arbitrary Python fan-in callback.

## Bounds and failures

Runner admission uses a bounded physical queue; a full queue blocks `submit`, or times out.
`iter_completed` limits input consumption to active-plus-queued capacity and yields input index
and result in completion order. Queued cancelled items occupy capacity until workers remove them.
Completed results are not accumulated internally by the iterator.

On an observed node failure, no new descendants are admitted. Queued node actions are cancelled
where possible and started actions are drained before reporting failure. Other DAGs submitted
individually can continue. A batch iterator stops refilling after an observed failure, cancels
queued DAGs and drains started work. Already-started writes are not forcibly killed.

Ray can transfer blocks over the network and spill to configured local disk; DataFlow configures
no remote spilling or S3 handoff. Streaming lowers working sets but caching, join shuffle and
many concurrent pipelines can still exceed available memory. Bounds limit admission/actions,
not OS heap or all Ray task queues. Actor resource reservations must fit actual cluster capacity.

The host owns request persistence, idempotent final writes and driver restart recovery. No
cross-sink atomic commit, exactly-once workflow, global optimal fairness, cross-DAG model actor
pool or automatic memory-aware admission is claimed. See [operations](operations.md) and
[measured benchmark boundaries](performance.md).
