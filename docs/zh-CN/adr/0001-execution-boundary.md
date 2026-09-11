# ADR-0001：分离编排与 Ray Data 执行

[English](../../adr/0001-execution-boundary.md) | 简体中文

状态：已接受

## 背景

DataFlow 需要 DAG 编排、持久工作流状态、重试、取消、制品和 Kubernetes 生命周期管理。Ray Data 已经负责数据集执行规划，Ray Core 负责分布式任务调度。

如果把每个逻辑 DAG 节点映射为独立 RayJob，会迫使数据算子之间进行不必要的物化，还会丢弃 Ray Data 在单次执行中优化并流式处理数据块的能力。

## 决策

DataFlow 引入三种不同表示：

1. `PipelineSpec`：面向用户的逻辑 DAG。
2. `ExecutionGraph`：包含一个或多个执行单元的编译器输出。
3. `ExecutionPlan`：某个 Ray 原生执行单元的版本化物理计划。

连续且兼容的 Ray Data 算子会被编译为一个执行单元，由运行时在一个 RayJob 中执行。持久边界、运行时不兼容、跨集群转换或显式检查点会拆分执行单元。

初始 `v1alpha1` 执行计划有意限制为线性，并支持 `read_parquet`、`map_batches`、`filter` 和 `write_parquet`。DAG 分支属于编译器层，将在运行时契约稳定后加入。

## 影响

- 工作流状态持久化且独立于 Ray 集群生命周期。
- Ray 对象引用绝不作为工作流恢复状态持久化。
- KubeRay 保持为执行适配器，而不是 DataFlow 领域模型。
- Ray Data 继续负责数据集级优化和调度。
- 执行计划模式必须版本化并保持向后兼容。
