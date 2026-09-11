# ADR 0005：在执行单元边界发布持久制品

[English](../../adr/0005-durable-artifact-publication.md) | 简体中文

## 状态

已接受。

## 背景

DataFlow 把兼容 Ray Data 算子保留在一个执行岛内，使数据块通过 Ray 内存/对象存储路径流动。边跨越执行单元边界时，瞬态表示不能作为持久工作流状态：Ray `ObjectRef` 受 Ray 集群/作业生命周期和所有权语义约束，集群丢失后不是恢复契约。

边界需要在控制器重启、RayJob 重试和 RayCluster 替换后仍存在的持久数据集引用。发布还必须区分计算成功与逻辑输出成功发布；失败尝试绝不能让部分写入数据对下游可见。

## 决策

DataFlow 使用 `ArtifactRef` 和 `ArtifactOutputSpec` 表示跨单元数据。首个持久格式是 S3 兼容对象存储上的 Parquet。

执行岛内 Dataset/数据块流保持瞬态，不进入工作流元数据。跨执行单元边界时，编译器发出：

- 由 `(run_id, node_id)` 确定的稳定已提交 URI；
- 尝试专属暂存 URI 模板；
- 生产方 `ExecutionPlan` 上的 `ArtifactOutputSpec`；
- 消费方 `ExecutionPlan` 上的 `ArtifactRef` 和已提交读取 URI。

运行时从尝试专属 KubeRay RayJob 接收 `DATAFLOW_ATTEMPT_NUMBER`。内部持久写算子将路径解析到对应暂存前缀，绝不直接写入稳定已提交 URI。

发布分两阶段：

1. Ray Data 将 Parquet 文件写入尝试专属暂存前缀。
2. RayJob 报告成功后，控制面 `ArtifactManager` 把对象发布到稳定已提交前缀，再将 PostgreSQL 制品记录从 `STAGING` 转为 `COMMITTED`。

S3 兼容发布器先复制数据对象，最后写 `_dataflow_commit.json`。PostgreSQL 仍是逻辑可见性事实来源；标记只是对象存储完整性信号，不替代持久元数据。

制品为只追加记录，状态转换仅限：

```text
STAGING -> COMMITTED
STAGING -> ABORTED
```

终态制品记录不可变。数据库限制每个 `(pipeline_run_id, node_id)` 逻辑输出最多一个 `COMMITTED` 制品，且每个制品必须关联真实 `(execution_unit_id, attempt_number)` 执行尝试。

## 故障与恢复语义

计算失败或取消时，DataFlow 终止暂存制品，并尽力删除尝试前缀，绝不将其发布为已提交。

计算成功但对象存储发布瞬时失败时，尝试和单元转为 `UNKNOWN`。协调器观察已成功外部 RayJob，只重试制品发布，不重新提交计算。

发布永久失败时，暂存制品终止，尝试/单元收敛为 `FAILED`。

控制器在对象发布后、元数据收敛前崩溃时，会幂等重试发布。PostgreSQL 制品已经 `COMMITTED` 时，管理器返回已有逻辑输出，不重新写入。

## 检查点

`ExecutionBoundary.CHECKPOINT` 是显式数据边界，强制创建新执行岛，并把产出制品标为检查点。数据路径与其他持久跨单元制品相同，因此即使原 Ray 集群已不存在，下游也能从已提交 Parquet URI 重启。

## 影响

- 持久工作流状态永不依赖 Ray `ObjectRef` 标识。
- 跨单元恢复由存储支撑，与 Ray 集群生命周期无关。
- 重试期间逻辑输出标识稳定，暂存标识保持尝试专属。
- 调度器/UI 无需扫描数据集，即可从 PostgreSQL 读取格式、模式、字节数、行数、内容哈希、检查点标志和 URI。
- S3 写时复制会在执行边界增加 I/O，因此编译器应继续积极融合算子，只在编排边界需要持久性时物化。
- 首个实现支持 Parquet 和 S3 兼容存储；后续格式/后端可实现相同 `ArtifactStorage` 契约。
