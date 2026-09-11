# 持久制品与检查点

[English](../ARTIFACTS.md) | 简体中文

DataFlow 让 Ray Data 数据流在执行岛内部保持瞬态，只在编译后的 DAG 跨越执行单元边界时才进行物化。

## 逻辑输出标识

持久逻辑输出由 `(run_id, node_id)` 标识，重试不会改变该标识。

对于 `artifact_base_uri: s3://bucket/dataflow`，编译器生成：

```text
s3://bucket/dataflow/runs/<run-id>/artifacts/<node-id>/committed
```

每次工作流尝试写入独立的暂存前缀：

```text
s3://bucket/dataflow/runs/<run-id>/artifacts/<node-id>/attempts/<NNN>/data
```

运行时从该次尝试对应的 KubeRay RayJob 注入的 `DATAFLOW_ATTEMPT_NUMBER` 解析 `<NNN>`。

## 发布顺序

```text
Ray Data write_parquet
        |
        v
attempt staging prefix
        |
RayJob SUCCEEDED
        |
        v
ArtifactManager.commit_outputs
        |
        +--> copy data to stable committed prefix
        +--> write _dataflow_commit.json last
        +--> PostgreSQL STAGING -> COMMITTED
        |
        v
downstream unit becomes eligible after producer unit succeeds
```

PostgreSQL 是逻辑可见性的事实来源。下游执行计划只使用稳定的已提交 URI，不会持久化 Ray `ObjectRef` 值，也不会通过它恢复。

## 故障行为

- RayJob 失败或取消：制品记录变为 `ABORTED`，以尽力而为方式删除暂存数据。
- RayJob 成功后对象存储发布发生瞬时故障：单元/尝试变为 `UNKNOWN`；协调器重试发布而不重新提交 Ray 计算。
- 发布永久失败：暂存输出终止，单元/尝试失败。
- 重复执行成功协调：幂等返回已有 `COMMITTED` 制品。

## 显式检查点

将数据边界设为 `checkpoint`，可以在相邻算子本来可以融合时强制物化：

```json
{
  "from": "expensive_inference",
  "to": "enrich",
  "kind": "data",
  "boundary": "checkpoint"
}
```

生成的制品在 PostgreSQL 中具有 `checkpoint=true`，Ray 集群丢失后可作为持久重启点。

## S3 兼容控制面依赖

在控制面安装发布器依赖：

```bash
pip install -e '.[artifacts]'
```

`Boto3S3ObjectClient` 接受普通 boto3 客户端配置，因此可以通过 boto3 相同的客户端参数和凭据机制提供 S3 兼容端点。
