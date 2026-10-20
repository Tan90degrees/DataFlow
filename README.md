# DataFlow

English | [简体中文](README.zh-CN.md) · [Documentation](docs/README.md)

A lightweight Python component for declarative pipelines in **one existing Ray cluster**.
Operators exchange Ray Data Dataset blocks through Ray's object store and node-to-node transport.
DataFlow does not create clusters or write intermediate files to pass data between operators.

Ray Data owns streaming, fusion and backpressure. Ray schedules each operator's tasks/actors.
DataFlow provides the DAG, preflight validation, Python SDK and one JSON execution command.

## Use

Python 3.11/3.12; preinstall the same runtime and business modules on driver and workers:

```bash
pip install -e '.[runtime]'
```

Connect to the shared cluster that is already running:

```python
from dataflow.sdk import Resources, pipeline, run_pipeline


@pipeline(name="preprocess")
def process(flow, source, target):
    dataset = flow.read_parquet(source, resources=Resources(cpu=0.25))
    dataset = dataset.map_batches(
        "dataflow.callables.identity_batch",
        resources=Resources(cpu=1, memory_bytes=256 * 1024**2),
        batch_size=128,
    )
    dataset.write_parquet(target, resources=Resources(cpu=0.25))


result = run_pipeline(process.spec("/shared/input", "/shared/output/run-001"))
print(result.output_paths)
```

Edit the input/final output paths in the JSON example, then:

```bash
dataflow-run examples/single_cluster.json --address auto
```

Use `RAY_ADDRESS`, an explicit `address=`, or an application-owned `ray.init()` session.
`auto` requires an existing cluster; `local` is rejected. Input/final output paths must be
accessible to workers. Intermediate data needs no S3 or shared-filesystem transfer protocol.

The base package requires only Pydantic; `runtime` adds Ray Data. There is no DataFlow server,
database, controller, chart or separate service to deploy. The platform manages the shared cluster.

## Execution boundaries

Linear chains stay lazy. Ready nodes and independent sinks execute with bounded concurrency.
Shared prefixes are recomputed when consumed by different branches unless explicitly marked
`.cache()`; caching materializes them once in the object store. Control dependencies also create
completion barriers. Ray can spill cached blocks to its configured local disks under pressure.

Parquet read/write, map_batches, filter, same-schema `union`, and keyed `join` are supported.
Union appends rows; join requires explicit keys and partition count and uses Ray's shuffle path.

For many DAGs, use one long-lived `PipelineRunner` with bounded active/queued DAGs and a shared
node-action pool. See [concurrent file DAGs](examples/concurrent_dags.py),
[SDK reference](docs/sdk.md) and [performance experiments](docs/performance.md).

The host owns durable request history, restart recovery and idempotent final writes. A failed
pipeline may leave partial outputs. Use a distinct output directory per attempt; Ray's Parquet
writer appends by default. See [architecture](docs/single-cluster.md) and
[operations](docs/operations.md) for precise behavior.

## Develop

```bash
pip install -e '.[dev]'
ruff check .
ruff format --check .
pytest
python -m build
# Optional real head + worker test:
pip install -e '.[runtime]'
DATAFLOW_TEST_RAY=1 pytest tests/test_single_cluster_ray.py -v -s
```

Start with [getting started](docs/getting-started.md), [SDK reference](docs/sdk.md) or
[runnable examples](examples/single_cluster.py). The 0.2 cleanup removes the former execution
path and its interfaces; use the [migration guide](docs/migration.md) when updating old callers.
