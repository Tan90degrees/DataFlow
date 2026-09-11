# ADR 0002：将逻辑 DAG 编译为执行岛

[English](../../adr/0002-execution-islands.md) | 简体中文

## 状态

已接受

## 背景

DataFlow 需要一个可持久化、可调度的 DAG 模型，同时不能取代 Ray Data 内部执行规划器。将每个 DAG 节点映射为一个 RayJob 会强制进行不必要的物化，并丢弃 Ray Data 的流处理、融合、背压和 actor/task 调度能力。

## 决策

DataFlow 引入三种表示：

1. `PipelineSpec`：面向用户的静态 DAG，包含显式数据边和控制边。
2. `LogicalGraph`：用于依赖和拓扑分析的已验证确定性图。
3. `ExecutionGraph`：物理执行岛，每个岛携带现有 Ray Data 运行时使用的一个 `ExecutionPlan`。

在 `v1alpha1` 中，如果数据边两端都属于线性数据链、该边不是硬边界，且运行时镜像/集群配置兼容，则进行融合。数据扇出会创建持久暂存边界，让每个下游岛都能读取相同上游结果。逻辑 DAG 模型保留数据扇入，但在 join 等多输入 Ray Data 算子进入运行时契约前，当前物理编译器会拒绝它。

硬数据边界会使用 `artifact_base_uri` 插入内部 Parquet 写/读算子。这是有意保持最小化的物化协议。事务性制品元数据以及提交/终止语义是独立关注点，将在制品里程碑中取代暂存约定。

控制边永不融合执行单元，而会成为调度器使用的单元依赖。

## 影响

- 调度器面向 `ExecutionUnit`，而不是单个 Ray Data 算子。
- 线性 Ray Data 流水线保持在一个 RayJob 中，保留 Ray 原生执行行为。
- 运行时或集群变化可以强制确定性执行边界。
- 编译器独立于 Kubernetes API 和 Ray 导入。
- 当前暂存 URI 还不是持久制品契约，外部消费者不得将其视为此类契约。
