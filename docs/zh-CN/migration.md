# 迁移到 0.2

[English](../migration.md) | 简体中文

单集群清理已删除旧实现，包内不再包含兼容运行路径、历史部署 Chart 或第二套执行器。
如需查看旧系统，请使用 Git 历史。

| 已删除能力 | 替代方式或归属 |
| --- | --- |
| HTTP 客户端/API、PostgreSQL 表与迁移 | 宿主管理请求状态，直接调用 `run_pipeline(spec)` |
| ExecutionPlan、执行岛编译器、按单元创建任务 | 一个 PipelineSpec、一个 driver，在共享 Ray 集群执行 |
| ClusterProfile、镜像/service-account/runtime 覆盖 | 平台部署唯一共享 worker 环境 |
| S3 制品注册、发布、保留和检查点 | 中间结果使用 Ray Data 块，最终结果显式业务写出 |
| 控制器、领导权/HA、准入与鉴权服务 | 由宿主与平台制定策略，DataFlow 不提供替代服务 |
| Helm、本地多服务脚本、离线厂商镜像包 | 连接已有集群，可使用小型运行镜像 Dockerfile |
| 其他 DataFlow 控制台命令 | `dataflow-run` 或 `python -m dataflow.cli` |

使用 `@pipeline(name="...")` 定义流水线，删除 `runtime`、`runtime_image`、`cluster_profile`、
`artifact_base_uri`、节点运行环境/集群选择参数，以及边上的 `boundary`。
`.checkpoint()`、`.hard_boundary()` 已移除，旧 JSON 字段明确拒绝接受。
不再导出 `Runtime`、`DataFlowClient`、编译器及持久制品/状态 API。

地址和 namespace 属于执行参数或宿主已建立的 Ray 会话，不放在流水线规格里。
契约和拓扑从 `dataflow` 或 `dataflow.graph` 导入，构图与执行从 `dataflow.sdk` 导入。

在干净环境安装新包或重建集群镜像，通过平台正常维护流程下线原服务。
源码清理不会停止已部署服务、迁移运行中的数据库或删除外部业务数据。
生产切换前应确认输出幂等以及业务需要的故障与恢复行为。


并发 DAG 更新：分支默认不再自动物化。昂贵公共前缀在值得付出内存/屏障成本时加 `.cache()`，
控制源仍物化。一元边的 input_index 默认 0，union/join 需要连续显式端口。
多 DAG 使用 PipelineRunner；单次 run_pipeline 默认也会重叠执行就绪节点/sink，
设 max_concurrent_nodes=1 可作串行对照。并发 sink 使用独立输出路径，未恢复持久检查点或回滚机制。
