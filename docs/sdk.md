# SDK and JSON reference

English | [简体中文](zh-CN/sdk.md)

## Author and run

```python
from dataflow.sdk import Resources, pipeline, run_pipeline


@pipeline(name="processing")
def process(flow, source, target):
    dataset = flow.read_parquet(source, resources=Resources(cpu=0.25))
    dataset = dataset.map_batches(
        "my_package.operators.Normalize",
        resources=Resources(cpu=1),
        batch_size=128,
        concurrency=4,
    )
    dataset.filter("my_package.operators.keep_row").write_parquet(target)


spec = process.spec("/shared/input", "/shared/output/run-001")
result = run_pipeline(spec, address="auto")
print(result.pipeline_name, result.output_paths)
```

`@pipeline(name=...)` wraps a graph-building function. `spec(...)`/calling the wrapper produces
`PipelineSpec`; `to_dict(...)` emits JSON-compatible data; `spec_hash(...)` returns its canonical
SHA-256 identity. None of these methods initialize Ray. `PipelineBuilder(name=...)` is available
for authoring without a decorator. Building a spec checks serialization/references; full execution
validation happens in `run_pipeline`.

`read_parquet(path, *, node_id=None, resources=None, **config)` returns a DatasetNode.
DatasetNode provides `map_batches(fn, ...)`, `filter(fn, ...)` and `write_parquet(path, ...)`.
A writer returns a NodeHandle. Node IDs are generated deterministically unless explicitly supplied.
Use the same DatasetNode more than once to create branches; see
[the branching example](../examples/branching.py).

`fn` is an importable `module.symbol` string, top-level function or callable class. Lambdas,
nested functions, `__main__` definitions and callable instances are rejected by the SDK.
Preinstall business modules on driver and workers. Callable classes are passed to Ray Data,
which owns their actor lifecycle; class constructor settings can use `fn_constructor_kwargs`.

## Resources

| Resources field | Ray scheduling hint |
| --- | --- |
| `cpu` | `num_cpus` per read/map/filter/write worker |
| `gpu` | `num_gpus`, including fractional logical reservations supported by Ray |
| `memory_bytes` | Worker heap-memory scheduling reservation |
| `accelerator_type` | `ray.io/accelerator-type` worker label selector |

These are logical scheduler hints, not CPU throttles, hard memory limits, or physical accelerator
partitioning. Accelerator labels/resources must already exist on workers. CPU/GPU/memory are
per operator worker, not a total pipeline reservation. `concurrency` controls Ray Data execution
concurrency; callable-class defaults follow the pinned Ray version.

Use Resources to declare scheduling needs once. Contradictory resource values in config are
rejected before execution. Other config options, such as `batch_size`, `batch_format`,
`fn_kwargs` and `concurrency`, pass to the corresponding Ray Data API. JSON cannot carry arbitrary
Python objects. Do not mutate shared specs while calls are running.

## Execute

`run_pipeline(spec, *, address=None, namespace=None) -> PipelineResult` is synchronous and
propagates failure. A successful result has `pipeline_name` and `output_paths: dict[str, str]`,
keyed by sink ID. `PipelineExecutionError.node_id` names the node that triggered execution and
`__cause__` retains the underlying exception. For lazy transformations, an upstream UDF error
may surface at the sink or shared materialization that started it.

An initialized application session is reused and left connected; omit `address` and `namespace`
in that case. Otherwise `address` defaults through `RAY_ADDRESS` to `auto`, and namespace defaults
to `default`. The connection is released in `finally`, while the existing cluster keeps running.
Connection failures propagate and never create a cluster.

## JSON

[examples/single_cluster.json](../examples/single_cluster.json) is the complete minimal example.
A spec has optional fixed `api_version="dataflow.io/v1alpha1"`, `kind="Pipeline"`, required `name`
and non-empty `nodes`, and an `edges` list. Each node has `id`, `kind`, `config` and optional
`resources`. Edges have `from`, `to` and `kind` (`data`, the default, or `control`). Unrecognized
fields are rejected. Each transform/sink requires exactly one data input; writers cannot feed
data to another node. Terminal nodes must write outputs.

Run JSON with `dataflow-run PATH [--address ADDRESS] [--namespace NAMESPACE]`, or equivalently
`python -m dataflow.cli PATH ...`. Success prints one JSON result; failure exits nonzero with
its exception chain. There are no other DataFlow console commands.
