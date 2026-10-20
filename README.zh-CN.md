# DataFlow

[English](README.md) | 简体中文 · [文档](docs/zh-CN/README.md)

运行于**一个已有 Ray 集群**上的轻量 Python 声明式流水线组件。
算子通过 Ray Data Dataset 衔接，数据块在 Ray 对象存储和节点间流转。
DataFlow 不创建集群，也不写中间文件来传递算子数据。

Ray Data 负责流式执行、融合与背压，Ray 调度每个算子的 task/actor，
DataFlow 负责 DAG、运行前校验、Python SDK 和唯一的 JSON 执行命令。

## 使用

使用 Python 3.11/3.12，driver 与 worker 预装相同运行环境和业务模块：

```bash
pip install -e '.[runtime]'
```

连接已经运行的共享集群：

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

修改 JSON 示例里的输入与最终输出路径后运行：

```bash
dataflow-run examples/single_cluster.json --address auto
```

地址可以来自 `RAY_ADDRESS`、显式 `address=`，也可以复用宿主已建立的 `ray.init()` 会话。
`auto` 要求存在集群，拒绝 `local`。业务输入与最终输出需要 worker 可访问，
中间数据不需要 S3 或共享文件系统中转协议。

基础包只依赖 Pydantic，`runtime` 添加 Ray Data；不需要额外部署 DataFlow 服务、数据库、
控制器或 Chart，由平台管理唯一共享集群。

## 执行边界

线性链惰性流式执行，分支复用对象存储中物化的 Dataset。内存压力下，Ray 可以溢写到其配置的本地磁盘。
支持 Parquet 读写、map_batches、filter、数据分支和控制依赖；每个 driver 内独立 sink 暂时串行，
尚不支持数据 fan-in。

持久请求记录、重启恢复和最终写入幂等由宿主负责。失败可能留下部分输出，
每次尝试应使用独立目录，Ray 的 Parquet writer 默认追加写入。
详细行为见[架构](docs/zh-CN/single-cluster.md)和[运维](docs/zh-CN/operations.md)。

## 开发

```bash
pip install -e '.[dev]'
ruff check .
ruff format --check .
pytest
python -m build
# 可选：真实 head + worker 测试
pip install -e '.[runtime]'
DATAFLOW_TEST_RAY=1 pytest tests/test_single_cluster_ray.py -v -s
```

参见[快速上手](docs/zh-CN/getting-started.md)、[SDK 参考](docs/zh-CN/sdk.md)和
[可执行示例](examples/single_cluster.py)。0.2 清理已移除原执行路径和接口，
旧调用方请按[迁移说明](docs/zh-CN/migration.md)更新。
