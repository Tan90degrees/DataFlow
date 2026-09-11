# DataFlow 可观测性

[English](../observability.md) | 简体中文

DataFlow 将高基数关联信息与低基数指标分离。

## 启用指标

在 API/控制面依赖之外安装可观测性额外依赖：

```bash
pip install -e '.[api,control-plane,artifacts,observability]'
```

安装可选 Prometheus 客户端且 `DATAFLOW_METRICS_ENABLED` 未设为 `false` 时，`dataflow-api` 会启用 Prometheus 注册表。抓取端点：

```text
GET /metrics
```

未配置可观测性时，核心编排包使用空操作指标后端，因此本地测试和嵌入使用无需遥测服务。

## 指标

初始控制面指标如下：

| 指标 | 用途 | 标签 |
| --- | --- | --- |
| `dataflow_api_requests_total` | HTTP 请求数 | method, route, status |
| `dataflow_api_request_duration_seconds` | HTTP 延迟 | method, route |
| `dataflow_run_queue_duration_seconds` | 排队到运行的延迟 | 无 |
| `dataflow_reconciliations_total` | 执行单元协调轮次 | outcome |
| `dataflow_reconciliation_duration_seconds` | 协调延迟 | outcome |
| `dataflow_reconciliation_errors_total` | 执行器/制品协调错误 | error_code |
| `dataflow_execution_unit_retries_total` | 工作流级重试 | error_code |
| `dataflow_execution_unit_duration_seconds` | 终态单元时长 | status |
| `dataflow_artifact_publications_total` | 提交/终止/错误结果 | outcome |

运行 ID、单元 ID、尝试 ID、RayJob 名称和制品 ID 有意不作为指标标签。

可用的 PromQL 起点：

```promql
sum(rate(dataflow_api_requests_total{status=~"5.."}[5m]))
/
sum(rate(dataflow_api_requests_total[5m]))
```

```promql
histogram_quantile(
  0.95,
  sum by (le, route) (rate(dataflow_api_request_duration_seconds_bucket[5m]))
)
```

```promql
sum by (error_code) (rate(dataflow_reconciliation_errors_total[10m]))
```

```promql
sum by (outcome) (rate(dataflow_artifact_publications_total[10m]))
```

## 结构化日志

设置：

```bash
export DATAFLOW_JSON_LOGS=true
```

内置 JSON 格式器会保留 API/控制器/协调器发出的关联字段。根据生命周期节点，记录会包含 `run_id`、`unit_id`、`unit_key`、`attempt_id`、`attempt_number`、`external_job_id` 以及有界状态/错误字段。

使用这些 ID 搜索日志，不要将它们转为 Prometheus 标签。

## OpenTelemetry

DataFlow 通过 `opentelemetry-api` 创建 OpenTelemetry span，但不会安装或配置导出器。部署负责 provider、sampler、processor 和 OTLP 导出器配置。未配置 SDK/provider 时，钩子表现为空操作 span，不改变编排语义。

初始 span 包括 HTTP 请求、PipelineRun 创建和 ExecutionUnit 协调。

## 构建标识

使用公开端点：

```text
GET /version
```

响应报告已安装包版本，并在可用时报告不可变 Git 提交与容器镜像坐标。发布构建会把 `DATAFLOW_BUILD_COMMIT` 注入控制面和运行时镜像。Helm Chart 会根据准确渲染的镜像引用（包括运维人员提供的标签或摘要）在 API 和控制器 Pod 上设置 `DATAFLOW_BUILD_IMAGE`。

```json
{
  "version": "0.1.0",
  "commit": "0123456789abcdef",
  "image": "ghcr.io/tan90degrees/dataflow-control-plane:0.1.0"
}
```

未作为发行包安装的源码树进程报告 `0+unknown`；没有构建参数的本地镜像为 `commit` 返回 `null`；非 Helm 进程为 `image` 返回 `null`。

## 运行诊断

使用：

```text
GET /v1/pipeline-runs/{run_id}/diagnostics
```

或：

```python
client.get_run_diagnostics(run_id)
```

响应将流水线/版本/运行标识与每个 ExecutionUnit、所有持久尝试、最新已知外部 RayJob ID、该单元产生的制品和最近持久事件关联起来。它只读取 PostgreSQL，不同步调用 Kubernetes，因此 Kubernetes API 中断不会让诊断端点不可用。
