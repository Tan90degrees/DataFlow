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

`run_pipeline(spec, *, address=None, namespace=None) -> PipelineResult` 同步执行，失败向上抛出。
成功返回 `pipeline_name` 和按 sink ID 索引的 `output_paths: dict[str, str]`。
`PipelineExecutionError.node_id` 标记触发执行的节点，`__cause__` 保留底层异常。
惰性变换的上游 UDF 错误可能在触发它的 sink 或分支物化处暴露。

宿主已初始化 Ray 时，省略 `address`、`namespace`，复用该会话并保持连接。
否则地址依次取参数、`RAY_ADDRESS`、`auto`，namespace 默认 `default`。
在 `finally` 中断开本次建立的连接，常驻集群继续运行；连接失败直接抛出，不会创建集群。

## JSON

[examples/single_cluster.json](../../examples/single_cluster.json) 是完整最小示例。
规格包含可省略的固定标记 `api_version="dataflow.io/v1alpha1"`、`kind="Pipeline"`，
以及必填 `name`、非空 `nodes` 和 `edges` 列表。节点包含 `id`、`kind`、`config` 和可选 `resources`；
边包含 `from`、`to`、`kind`，默认 `data`，也可为 `control`。未知字段拒绝接受。
变换和 sink 各需一个数据输入，writer 不能输出数据给其他节点，终端节点必须写出结果。

JSON 入口为 `dataflow-run PATH [--address ADDRESS] [--namespace NAMESPACE]`，
或 `python -m dataflow.cli PATH ...`。成功打印一条 JSON，失败保留异常链并以非零状态退出。
DataFlow 不再提供其他控制台命令。
