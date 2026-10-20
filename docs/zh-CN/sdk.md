# SDK 与 JSON 参考

[English](../sdk.md) | 简体中文

## 定义与运行

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

`@pipeline(name=...)` 包装构图函数。`spec(...)` 或直接调用包装后的函数生成 `PipelineSpec`；
`to_dict(...)` 生成 JSON 兼容字典；`spec_hash(...)` 返回规范化规格的 SHA-256。
这些方法均不初始化 Ray。不使用装饰器时可直接用 `PipelineBuilder(name=...)`。
构图时校验序列化和引用，`run_pipeline` 执行完整的运行前校验。

`read_parquet(path, *, node_id=None, resources=None, **config)` 返回 DatasetNode。
DatasetNode 提供 `map_batches(fn, ...)`、`filter(fn, ...)`、`write_parquet(path, ...)`，
写出方法返回 NodeHandle。未指定的节点 ID 按顺序确定生成；多次使用同一 DatasetNode 即为分支，
见[分支示例](../../examples/branching.py)。

`fn` 接受 `module.symbol` 字符串、可导入顶层函数或 callable class。
SDK 拒绝 lambda、嵌套函数、`__main__` 定义和 callable 实例。业务模块在 driver 与 worker 预装。
callable class 直接传给 Ray Data，由 Ray Data 管理 actor 生命周期，构造参数可用 `fn_constructor_kwargs`。

## 资源

| Resources 字段 | Ray 调度提示 |
| --- | --- |
| `cpu` | 每个读、map、filter、写 worker 的 `num_cpus` |
| `gpu` | `num_gpus`，包括 Ray 支持的小数逻辑预留 |
| `memory_bytes` | worker 堆内存调度预留 |
| `accelerator_type` | worker 的 `ray.io/accelerator-type` 标签选择 |

这些是逻辑调度提示，不是 CPU 限速、硬内存限制或物理加速器切分。worker 需预先注册相应标签和资源。
CPU/GPU/内存是每个算子 worker 的需求，不是整个流水线的总预算。
`concurrency` 控制 Ray Data 执行并发，callable class 的默认行为遵循固定的 Ray 版本。

用 Resources 单处声明资源；config 中矛盾的资源值在执行前拒绝。
其余如 `batch_size`、`batch_format`、`fn_kwargs`、`concurrency` 等配置传给对应 Ray Data API。
JSON 不能携带任意 Python 对象；执行期间不要并发修改共享规格。

## 执行

`run_pipeline(spec, *, address=None, namespace=None, max_concurrent_nodes=4) -> PipelineResult` 同步执行，失败向上抛出。
成功返回 `pipeline_name` 和按 sink ID 索引的 `output_paths: dict[str, str]`。
`PipelineExecutionError.node_id` 标记触发执行的节点，`__cause__` 保留底层异常。
惰性变换的上游 UDF 错误可能在触发它的 sink 或分支物化处暴露。

宿主已初始化 Ray 时，省略 `address`、`namespace`，复用该会话并保持连接。
否则地址依次取参数、`RAY_ADDRESS`、`auto`，namespace 默认 `default`。
在 `finally` 中断开本次建立的连接，常驻集群继续运行；连接失败直接抛出，不会创建集群。

## JSON

[examples/single_cluster.json](../../examples/single_cluster.json) 是完整最小示例。
规格包含可省略的固定标记 `api_version="dataflow.io/v1alpha1"`、`kind="Pipeline"`，
以及必填 `name`、非空 `nodes` 和 `edges` 列表。节点包含 `id`、`kind`、`config`、可选 `resources` 和默认 false 的 `cache`；
边包含 `from`、`to`、`kind`（默认 data，也可为 control）和默认 0 的 `input_index`。未知字段拒绝接受。
一元变换和 sink 各需一个输入，union 至少两个、join 恰好两个；数据端口从 0 连续编号。writer 不能输出数据给其他节点，终端节点必须写出结果。

JSON 入口为 `dataflow-run PATH [--address ADDRESS] [--namespace NAMESPACE] [--max-concurrent-nodes N]`，
或 `python -m dataflow.cli PATH ...`。成功打印一条 JSON，失败保留异常链并以非零状态退出。
DataFlow 不再提供其他控制台命令。


## 公共前缀、汇合与控制依赖

`dataset.cache()` 标记节点在单次运行中物化一次，复用对象存储数据块；未标记时，不同分支可能重算前缀，
不代表实现了流式广播。`flow.after(source_handle, target_handle)` 添加完成控制依赖。

```python
left = flow.read_parquet("/shared/left")
right = flow.read_parquet("/shared/right")
left.union(right).write_parquet("/shared/appended")
left.join(
    right,
    on="id",
    num_partitions=4,
    join_type="left_outer",
    left_suffix="_left",
    right_suffix="_right",
    resources=Resources(cpu=1),
).write_parquet("/shared/joined")
```

union 要求列名和类型相同，保留重复行。join 的 `on` 在 SDK 支持字符串或列表，JSON 中使用列表；
必须显式指定正数 `num_partitions`。还支持 `join_type`、列表 `right_on`、`left_suffix`、`right_suffix`
和默认 true 的 `validate_schemas`。关联类型为 inner、left/right/full_outer、left/right_semi、left/right_anti。
join 的 Resources 作用于 Ray aggregator worker；union 不接受独立资源/config。
join 和 schema 发现可能引入执行屏障，不是无成本的流式汇合。

## 多 DAG

```python
from dataflow.sdk import PipelineRunner

with PipelineRunner(max_concurrent_dags=8, max_queued_dags=32, max_concurrent_nodes=8) as runner:
    for index, result in runner.iter_completed(specs):
        print(index, result.output_paths)
```

构造 runner 时连接一次或借用已初始化会话，务必关闭，推荐使用 with。
`submit(spec, timeout=None)` 快照并校验规格，返回 concurrent.futures.Future。
物理 DAG 队列满时阻塞；`timeout=0` 非阻塞准入，满时抛 TimeoutError。
节点并发上限由该 runner 的所有 DAG 共享，不同 runner/进程各有上限，不是集群全局配额。

`iter_completed` 从可迭代输入中消费有限窗口，按完成顺序返回 `(input_index, result)`，
无需把所有规格和 Future 一次加载进内存。批量失败抛出带输入序号和流水线名的 `PipelineBatchError`，
cause 保留原异常链，尽量取消排队工作并等待已启动工作。提前退出且仍保留迭代器时，需显式关闭迭代器。
Future 只能取消未开始 DAG，不能杀死运行中的 DAG。

`close(cancel_pending=False)` 停止准入并等待活动及排队工作；设为 true 则先取消排队 DAG 再等待，
同时唤醒阻塞的提交方。请在所有者线程关闭，不要在 Future worker 回调中关闭；回调应保持非阻塞。
`snapshot()` 返回活动、排队、完成、失败和取消 DAG 计数，不轮询 Ray。

PipelineResult 新增 `duration_seconds`（DAG 活动时间，含节点池等待）、`peak_in_flight_nodes`
（已提交 Future 数，含节点队列）和 `materialized_nodes`（仅 DataFlow 缓存/控制屏障，不含 Ray 内部 join/shuffle 物化）。
这些字段不代表整集群内存或物理资源使用量。
