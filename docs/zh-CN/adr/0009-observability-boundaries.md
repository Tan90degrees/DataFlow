# ADR 0009：不在指标标签中使用关联标识符

[English](../../adr/0009-observability-boundaries.md) | 简体中文

## 状态

已接受

## 背景

DataFlow 跨越多个独立故障层：HTTP API、PostgreSQL 持久状态、调度器/协调器、KubeRay RayJob、Ray 执行和持久制品。运维人员需要从 PipelineRun 导航到 ExecutionUnit、尝试、外部作业、事件和制品，而无需查询原始表或搜索无关日志。

同时，运行 UUID、单元 UUID、尝试 ID、RayJob 名称和制品 ID 等标识符基数无界。把它们作为 Prometheus 标签会让指标后端本身成为扩展风险。

## 决策

DataFlow 向控制面组件提供一个可选 `Observability` 门面。

### 结构化日志

控制器和协调器日志记录在结构化元数据中携带关联字段：

- `run_id`；
- `unit_id` 和 `unit_key`；
- 可用时的 `attempt_id` 和 `attempt_number`；
- 观察到外部作业时的 `external_job_id`；
- 有界状态/错误字段。

提供 JSON 格式器但不默认启用，让嵌入应用选择自己的日志配置。

### 指标

Prometheus 指标只使用低基数标签，并有意排除 ID。初始指标覆盖：

- 按方法/路由/状态的 API 请求数和延迟；
- 运行排队时长；
- 按结果的协调次数/延迟；
- 按错误码的协调错误；
- 按错误码的工作流重试数；
- 按状态的终态执行单元时长；
- 制品发布结果。

每个 DataFlow 进程拥有显式 Prometheus 注册表，而不是修改全局注册表。这使测试相互隔离，也避免单进程创建多个应用实例时重复注册收集器。

### 追踪

OpenTelemetry 是可选钩子。API 运行创建和执行单元协调会创建带关联属性的 span。核心包不配置导出器或采样策略；部署负责 SDK/provider/exporter 配置。

### 诊断读取模型

`GET /v1/pipeline-runs/{run_id}/diagnostics` 返回控制面诊断视图，包含：

- 流水线和不可变版本标识；
- 运行状态；
- 每个 ExecutionUnit 及其全部持久尝试；
- 每个单元最新已知外部作业 ID；
- 按生产单元分组的制品；
- 最近持久编排事件。

该端点是 PostgreSQL 上的读取模型，不同步查询 Kubernetes，因此诊断 API 可用性不会耦合到 Kubernetes 可用性。

## 影响

- 高基数 ID 可在日志、追踪和诊断中使用，而不会污染 Prometheus 标签。
- 本地单元测试可以不启用指标和追踪。
- Kubernetes/Ray 状态保持为协调器观测状态，而不是 API 请求时依赖。
- 运维人员可通过一个 API 响应从运行导航到外部作业和制品状态。
- 导出器配置、仪表盘和告警策略仍是部署关注点。

## 延期事项

- 直接日志聚合集成；
- OTLP 导出器配置助手；
- 把 Prometheus 样本链接到追踪的 exemplar；
- 实时 Kubernetes/Ray 诊断扇出；
- 文档指标名之外的预制 Grafana 仪表盘。
