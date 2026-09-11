# DataFlow

[English](README.md) | 简体中文 · [文档中心](docs/zh-CN/README.md)

基于 Ray、Ray Data、KubeRay 和 Kubernetes 构建的 Ray 原生分布式数据处理编排框架。

## 工程方向

DataFlow 负责工作流状态、DAG 编译、重试、制品、资源策略和生命周期。Ray Data 负责数据集执行规划，Ray 负责分布式任务调度，KubeRay 负责 Kubernetes 上 Ray 集群与作业的生命周期。

当前执行路径如下：

```text
Python SDK / HTTP API
        ↓
PipelineSpec -> LogicalGraph -> ExecutionGraph -> Durable State
             -> ClusterProfile snapshot -> Scheduler/Reconciler
             -> ExecutionPlan -> Ray Data -> KubeRay RayJob
                              -> Durable Artifact -> downstream ExecutionPlan
```

Python SDK 被明确设计为规格生成器和 API 客户端，构建流水线时不会初始化 Ray。Python 可调用对象只会以可复现导入的 `module.symbol` 引用存储，因此持久化版本仍是 JSON，可比较、可检查，并能从不可变运行时镜像执行。

编译器会把兼容的线性数据算子组合为执行岛，而不是为 DAG 中每个节点创建一个 RayJob。硬边界、显式检查点边界、运行时变化、ClusterProfile 变化以及数据扇出都会通过持久化 Parquet 制品完成物化。

ClusterProfile 将基础设施策略从 PipelineSpec 中分离。流水线按名称引用配置；创建 PipelineRun 时，DataFlow 解析当前不可变配置版本，并将完整快照嵌入每个 ExecutionPlan。因此后续配置修改只影响新运行。执行前会根据工作组容量检查 CPU、GPU、内存和加速器请求，固定快照则确定性地驱动 KubeRay 的命名空间、服务账户、Pod 资源、工作组、调度位置和自动扩缩容。

在同一执行岛内，Ray Dataset 数据块保持瞬态，绝不会作为工作流状态持久化。跨执行单元边界时，DataFlow 持久化指向稳定已提交 URI 的 `ArtifactRef`。Ray `ObjectRef` 标识不会进入持久编排契约。

PostgreSQL 是持久编排的事实来源。流水线版本和 ClusterProfile 版本不可变，执行尝试只追加，制品记录完成终态发布后只追加，运行、单元和尝试的状态转换会在同一事务中追加事件。Ray 与 Kubernetes 状态属于外部观测状态，由协调器根据持久期望状态进行收敛。

HTTP API 同样只是持久状态适配层：请求处理器创建流水线元数据、不可变版本、ClusterProfile 版本和已编译运行，但不会直接提交 RayJob。这样，API 可用性不会依赖瞬时 Kubernetes 故障。独立部署的控制器/协调器负责将 `RUNNING` 和 `CANCELLED` 期望状态收敛到 KubeRay。

调度器采用 `all_success` 依赖语义：只有所有上游单元均成功，`PENDING` 执行单元才会变为 `READY`。协调器通过 `Executor` 接口提交 `READY` 单元，跟踪每次尝试的外部作业，将可恢复故障以新尝试和退避方式重试，并传播取消且不创建新的下游工作。KubeRay RayJob 名称由运行、单元和尝试确定，因此重复协调调用与控制器重启都会收敛到同一个 Kubernetes 对象。

持久输出采用两阶段发布。Ray Data 先写入某次尝试专属的 S3 暂存前缀。RayJob 成功后，控制面将暂存数据发布到稳定的 `(run_id, node_id)` 已提交 URI，写入对象存储提交标记，并把 PostgreSQL 制品记录从 `STAGING` 转为 `COMMITTED`。失败或取消的尝试标记为 `ABORTED`，永远不会成为下游可见输出。发布发生瞬时故障时，单元会转为 `UNKNOWN`，并重试已成功 RayJob 的发布过程，而不会重新计算。

## 仓库结构

```text
src/dataflow/sdk/                       Python 编排 DSL 和 HTTP API 客户端
src/dataflow/api*.py                    HTTP 应用、服务层和 API 仓储
src/dataflow/cluster_profiles.py        ClusterProfile 资源与调度位置契约
src/dataflow/cluster_profile_repository.py  PostgreSQL 不可变配置版本
src/dataflow/                           编译器、调度器、协调器、运行时和 KubeRay 适配器
src/dataflow/artifacts.py               制品契约与 S3 兼容发布后端
src/dataflow/artifact_*.py              持久制品管理器与 PostgreSQL 注册表
src/dataflow/metadata/                  PostgreSQL 仓储和随包迁移
examples/                               流水线、配置、执行计划与 SDK 示例
tests/                                  单元测试和 PostgreSQL 集成测试
docs/                                   架构决策和使用文档
```

## 开发

需要 Python 3.11 或更高版本。

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
ruff check .
pytest
```

配置 `DATAFLOW_TEST_DATABASE_URL` 后会运行 PostgreSQL 元数据和控制面集成测试。也可以手工执行随包迁移：

```bash
dataflow-migrate --dsn postgresql://postgres:postgres@localhost:5432/dataflow
```

如需包含 KubeRay、PostgreSQL、MinIO、API、控制器进程的完整本地 Kubernetes 调试环境，请阅读[快速上手](docs/zh-CN/getting-started.md)和[本地 Kubernetes 开发](docs/zh-CN/local-development.md)。

### Python SDK

安装 SDK 客户端依赖：

```bash
pip install -e '.[sdk]'
```

使用与 API 和编译器相同的核心 `PipelineSpec` 编写流水线：

```python
from dataflow.sdk import DataFlowClient, Resources, pipeline


def preprocess(batch):
    return batch


class Predictor:
    def __call__(self, batch):
        return batch


@pipeline(
    name="image-inference",
    runtime_image="registry.example.com/dataflow-runtime:sha-abc123",
    cluster_profile="gpu-medium",
    artifact_base_uri="s3://my-bucket/dataflow",
)
def image_pipeline(flow, input_path: str, output_path: str):
    dataset = flow.read_parquet(input_path)
    dataset = dataset.map_batches(
        preprocess,
        resources=Resources(cpu=2),
        batch_size=128,
    )
    dataset = dataset.checkpoint().map_batches(
        Predictor,
        resources=Resources(cpu=2, gpu=1, accelerator_type="A100"),
    )
    dataset.write_parquet(output_path)


spec = image_pipeline.spec("s3://my-bucket/input", "s3://my-bucket/output")

with DataFlowClient("http://dataflow-api:8080") as client:
    submission = client.submit(spec, parameters={"request_id": "demo"})
    print(submission.run["id"])
```

`@pipeline` 只会在本地执行图构建函数，不会调用 Ray 或读取输入数据集。`map_batches`/`filter` 引用的函数和可调用类必须是运行时镜像中可导入模块的顶层符号。Lambda、嵌套/局部函数、`__main__` 符号和任意可调用实例会被拒绝，而不会被 pickle 到流水线元数据中。

完整示例见 `examples/sdk_pipeline.py`。

### HTTP API

安装 API 服务额外依赖：

```bash
pip install -e '.[api]'
```

配置 PostgreSQL 并启动控制面 API：

```bash
export DATAFLOW_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/dataflow
dataflow-api
```

服务默认监听 `0.0.0.0:8080`，可通过 `DATAFLOW_API_HOST` 和 `DATAFLOW_API_PORT` 覆盖。

当前 API：

```text
POST /v1/cluster-profiles
GET  /v1/cluster-profiles
GET  /v1/cluster-profiles/{name}
PUT  /v1/cluster-profiles/{name}
POST /v1/pipelines
GET  /v1/pipelines
GET  /v1/pipelines/{pipeline_id}
POST /v1/pipelines/{pipeline_id}/versions
POST /v1/pipeline-runs
GET  /v1/pipeline-runs/{run_id}
POST /v1/pipeline-runs/{run_id}/cancel
GET  /v1/pipeline-runs/{run_id}/events
GET  /v1/pipeline-runs/{run_id}/artifacts
GET  /healthz
GET  /readyz
GET  /version
```

创建运行时会持久化已编译执行图，让运行依次经历 `CREATED -> QUEUED -> PLANNING -> RUNNING`，并执行一次就绪度判断。API 不会在 Web 进程中运行长生命周期控制器循环。

### ClusterProfile

PipelineSpec 按名称引用 ClusterProfile，而不是嵌入 Kubernetes 调度策略。内置 `default` 配置为版本 `0`；管理员可以通过 API 创建持久化的 `default` 或其他命名配置。

配置版本不可变。`PUT /v1/cluster-profiles/{name}` 会追加新版本。创建运行时，系统解析每个引用配置并把完整版本快照嵌入持久化 `ExecutionPlan`。运行重试或协调时不会重新解析可能已经变化的当前配置。

配置控制以下内容：

- Ray 版本和 Kubernetes 命名空间；
- 服务账户和可选 Kubernetes PriorityClass；
- 头节点/工作节点 CPU 和内存资源；
- GPU 容量和 Kubernetes GPU 资源名称；
- Ray `label_selector` 调度使用的加速器标签；
- 工作节点副本数、最小/最大值以及 Ray Autoscaler v1/v2 配置；
- 节点选择器和容忍度；
- 可选 Kueue LocalQueue 元数据。

GPU 配置示例见 `examples/gpu_cluster_profile.json`。如果所选配置的所有工作组都无法满足例如 `Resources(gpu=1, accelerator_type="A100")` 的请求，创建流水线版本时就会拒绝该请求。

本地单元测试可以不安装 Ray。要执行真实 Ray Data 计划，安装运行时依赖：

```bash
pip install -e '.[runtime]'
```

运行实时 KubeRay 控制面时安装 Kubernetes 客户端：

```bash
pip install -e '.[control-plane]'
```

在控制面安装 S3 兼容制品发布器：

```bash
pip install -e '.[artifacts]'
```

将流水线编译为物理执行单元：

```bash
dataflow-compile examples/basic_pipeline.json --run-id run-001
```

在现有 Ray 环境中本地执行物理计划：

```bash
dataflow-runtime examples/basic_plan.json
```

渲染对应的 KubeRay `RayJob` 清单：

```bash
dataflow-render-rayjob examples/basic_plan.json
```

## 制品路径约定

对于 `s3://bucket/dataflow` 这样的制品基准 URI，DataFlow 会派生稳定逻辑路径和尝试专属路径：

```text
committed:
s3://bucket/dataflow/runs/<run-id>/artifacts/<node-id>/committed

staging:
s3://bucket/dataflow/runs/<run-id>/artifacts/<node-id>/attempts/<NNN>/data
```

下游执行单元只读取已提交 URI。运行时通过 KubeRay 注入每个 RayJob 的 `DATAFLOW_ATTEMPT_NUMBER` 解析尝试专属暂存 URI。

## 运行时镜像

RayJob 入口会导入 `dataflow` 包，因此生产计划必须引用包含本仓库运行时包的镜像。构建开发镜像：

```bash
docker build -t dataflow-runtime:dev .
```

把镜像推送到 Kubernetes 集群可访问的镜像仓库，并将执行计划或流水线中的 `runtime.image` 设置为已推送的不可变标签。生产执行计划不要直接使用原生 Ray 镜像，除非通过显式 Ray runtime environment 提供 DataFlow。

## v0.1.0 候选版本

初始实现里程碑已经完成。Python 包和 Helm Chart 已统一版本为 `0.1.0`，但只有匹配的 Git 标签通过发布工作流并发布 GitHub Release 后，才视为正式发布。

- [x] 版本化执行计划契约
- [x] Ray Data 运行时驱动
- [x] KubeRay RayJob 渲染器
- [x] 运行时容器定义
- [x] PipelineSpec 与确定性 LogicalGraph
- [x] 执行岛编译器
- [x] 硬边界物化约定
- [x] 持久编排状态机
- [x] PostgreSQL 元数据存储与迁移
- [x] 调度器
- [x] 幂等 Kubernetes/KubeRay 协调器
- [x] 持久制品/检查点
- [x] HTTP 控制面 API
- [x] Python SDK
- [x] ClusterProfile/资源策略
- [x] 控制面可观测性和运行诊断
- [x] 真实 KubeRay 端到端套件
- [x] PostgreSQL 防护的控制器高可用
- [x] API 认证与基于角色的授权
- [x] 持久准入控制与调度公平性
- [x] 制品保留和垃圾回收
- [x] 面向生产的 Helm 部署与 HA 端到端覆盖
- [x] 可复现的标签驱动发布流水线
- [x] 控制面构建/版本端点与镜像标识

下一里程碑重点是在类似生产的运行条件下验证发布：升级与回滚安全、备份与恢复、安全加固以及规模/浸泡测试。顺序验收门槛见[项目路线图](docs/zh-CN/roadmap.md)。
