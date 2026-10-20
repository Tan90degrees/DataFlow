# DataFlow

[English](README.md) | 简体中文 · [文档](docs/zh-CN/README.md)

运行于**一个已有、常驻的 Ray 集群**上的轻量流水线组件。整个 DAG 由一个 driver
通过 Ray Data 执行，算子之间使用 Dataset 衔接，数据块通过 Ray 对象存储和节点间传输流转。
DataFlow 不写中间 Parquet，也不用 S3 连接算子。

Ray Data 负责流式执行、融合和背压；Ray 负责 task/actor 放置与算子资源调度；DataFlow
负责声明式 DAG、校验和执行入口。Kubernetes/KubeRay 可以在外部部署这个共享集群，
DataFlow 本身不创建集群。

## 执行流水线

需要 Python 3.11+，driver 与 worker 环境均安装运行依赖：

```bash
pip install -e '.[runtime]'
```

连接已经启动的集群（在 head 内运行时，`auto` 发现现有集群）：

```python
from dataflow.sdk import Resources, pipeline, run_pipeline

@pipeline(name="preprocess")
def preprocess(flow, input_path, output_path):
    dataset = flow.read_parquet(input_path, resources=Resources(cpu=0.25))
    dataset = dataset.map_batches(
        "dataflow.callables.identity_batch",
        resources=Resources(cpu=1, memory_bytes=256 * 1024**2),
        batch_size=128,
    )
    dataset.write_parquet(output_path, resources=Resources(cpu=0.25))

result = run_pipeline(preprocess.spec("/shared/input", "/shared/output"))
print(result.output_paths)
```

也可以直接执行 JSON DAG：

```bash
dataflow-run examples/single_cluster.json --address auto
```

该路径不需要 API 服务、PostgreSQL、MinIO、ClusterProfile、运行镜像声明、控制器或
KubeRay Operator。依赖在部署共享集群时预装。可以使用 `RAY_ADDRESS`、`address=`，
或复用宿主已建立的 `ray.init()` 会话。拒绝 `local`；未找到集群就报错，不自动新建集群。

[单集群执行说明](docs/zh-CN/single-cluster.md)介绍分支、资源、部署与迁移；
[examples/single_cluster.py](examples/single_cluster.py)是可执行 SDK 示例。
输入与最终输出路径需要 worker 可访问，使用共享文件系统即可。

## 开发

```bash
pip install -e '.[dev]'
ruff check .
pytest
# 可选：真实 Ray head + worker 验证
pip install -e '.[runtime]'
DATAFLOW_TEST_RAY=1 pytest tests/test_single_cluster_ray.py
```

## 旧控制面

原 PostgreSQL/API/KubeRay 实现暂留用于迁移兼容。旧执行岛编译器、按单元创建的
RayJob、ClusterProfile 与持久制品发布均**不参与 `run_pipeline` 和 `dataflow-run`**。
已有部署仍使用原来的显式命令，见[历史文档](docs/zh-CN/README.md)。
这是第一步迁移，尚未删除这些模块，也不代表新路径已经完成生产验收。
