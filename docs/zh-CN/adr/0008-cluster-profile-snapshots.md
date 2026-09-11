# ADR 0008：版本化 ClusterProfile 并固定执行计划快照

[English](../../adr/0008-cluster-profile-snapshots.md) | 简体中文

## 状态

已接受

## 背景

`PipelineSpec` 必须表达逻辑资源意图，而不能在每条流水线中嵌入 Kubernetes Pod 模板。控制面还需要在提交 RayJob 前验证资源请求，并确保管理员后来修改集群大小、调度位置或自动扩缩容策略时，已经创建的运行仍可复现。

Ray Data 和 Ray Core 负责 Ray 集群内 task/actor 调度，KubeRay 负责该集群的 Kubernetes 表示。因此 DataFlow 需要位于逻辑流水线资源与 KubeRay 清单之间的资源策略层，但不能让可变管理员配置变成可变工作流历史。

## 决策

DataFlow 引入管理员管理的 `ClusterProfile` 契约。

配置定义：

- Ray 版本和 Kubernetes 命名空间；
- 服务账户和可选 Kubernetes 优先级类；
- 头节点 Pod CPU/内存和调度位置；
- 一个或多个工作组，包括 CPU、内存、GPU 容量、副本数和自动扩缩边界；
- 可选加速器类型和 GPU Kubernetes 资源名；
- 节点选择器与容忍度；
- Ray Autoscaler 设置；
- 可选 Kueue LocalQueue 名称。

`PipelineSpec` 和节点覆盖继续仅按名称引用配置，不持久化 Kubernetes Pod 模板。

ClusterProfile 版本在 PostgreSQL 中只追加。更新配置会创建新的不可变版本，而不修改已有版本。

创建 `PipelineRun` 时，控制面把每个配置名称解析为当前版本并将不可变快照传给编译器。每个物理 `ExecutionPlan` 嵌入完整 `ClusterProfileSnapshot`，包括名称、版本、哈希和规格。因此已持久化执行单元计划是自包含的，协调或重试时不重新解析可变配置。

编译器验证每个节点 `ResourceSpec` 至少能由所选配置中的一个工作组满足。CPU、GPU 和内存必须装入单个工作 Pod。请求 `accelerator_type` 时必须存在同类型兼容工作组。无效资源请求会在 API 持久化 PipelineVersion 前失败。

对于加速器特定 Ray Data 算子，DataFlow 使用保留节点标签 `ray.io/accelerator-type` 把 `ResourceSpec.accelerator_type` 映射为 Ray `label_selector`。相应 KubeRay 工作组发布该 Ray 节点标签。Kubernetes 节点选择仍通过配置中的 `nodeSelector` 和容忍度显式定义。

KubeRay 渲染器使用 `ExecutionPlan` 内的快照，而不是 PostgreSQL 当前配置。命名空间、服务账户、Pod 资源、工作组、调度位置、自动扩缩容和可选 Kueue 元数据都从快照确定性渲染。

保留 `default` 配置作为进程内版本 `0` 回退，让管理员尚未创建持久默认配置时已有 PipelineSpec 仍可工作。持久化的 `default` 会在未来运行中取代内置回退。

## 影响

- 更新 ClusterProfile 只影响更新后创建的运行。
- 已有运行的重试与控制器重启会渲染相同 RayJob 资源策略。
- 无需在线 Kubernetes 集群即可测试资源策略验证。
- 流水线元数据保持可移植，不成为 Kubernetes 清单存储。
- 配置版本可独立于 PipelineVersion 历史审计。
- 运行时镜像仍属于 Pipeline/Runtime；ClusterProfile 描述计算和调度策略，而非代码打包。
- 跨集群准入、配额和 Kueue 策略可以演进而不改变逻辑 DAG 契约。

## 延期事项

- 共享/每运行预创建 RayCluster 池；
- Kueue ClusterQueue/ResourceFlavor 生命周期管理；
- 成本感知配置选择；
- 配置管理的租户级授权；
- 配置删除和保留策略；
- 实时 Kubernetes 能力发现。
