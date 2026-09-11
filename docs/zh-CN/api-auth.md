# API 认证与 RBAC

[English](../api-auth.md) | 简体中文

DataFlow 将 API 身份与 Ray、Kubernetes 身份分离。生产 `dataflow-api` 入口会构建常规控制面应用，再安装可插拔 Bearer 认证边界。

## 模式

`DATAFLOW_API_AUTH_MODE=disabled` 保留现有开发行为。为了向后兼容，这是默认值；只有在另一个可信网络边界已经保护 API 时才应使用。

`DATAFLOW_API_AUTH_MODE=static` 使用 `DATAFLOW_API_AUTH_STATIC_TOKENS` 中的 JSON 映射启用 Bearer 认证。静态模式适用于确定性测试、开发和小型受控部署；令牌属于机密，应通过部署密钥机制提供，不应提交到源代码。

配置示例：

```bash
export DATAFLOW_API_AUTH_MODE=static
export DATAFLOW_API_AUTH_STATIC_TOKENS='{
  "submit-secret": {
    "sub": "pipeline-service",
    "roles": ["pipeline.read", "pipeline.submit", "run.cancel"]
  },
  "admin-secret": {
    "sub": "platform-admin",
    "roles": ["pipeline.read", "cluster_profile.admin"]
  }
}'
```

声明适配器默认使用 OIDC 风格的 `sub` 主体声明和 `roles` 声明。`DATAFLOW_API_AUTH_SUBJECT_CLAIM` 与 `DATAFLOW_API_AUTH_ROLES_CLAIM` 可选择其他声明名称。角色声明可以是字符串数组或空格分隔字符串。

静态提供器特意放在声明提供器协议之后，因此 JWT/OIDC 验证器可以替换令牌查找，同时保持身份与授权层不变。

## 角色

| 角色 | 操作 |
| --- | --- |
| `pipeline.read` | 读取流水线、运行、诊断、事件、制品和 ClusterProfile |
| `pipeline.submit` | 创建流水线、不可变版本和 PipelineRun |
| `run.cancel` | 取消 PipelineRun |
| `cluster_profile.admin` | 创建或更新 ClusterProfile |

健康、就绪和指标端点保持无需认证。未知 `/v1/*` 操作至少受 `pipeline.read` 保护，避免新增 API 意外变成匿名接口。

凭据缺失或无效时返回带 `WWW-Authenticate: Bearer` 的 `401 UNAUTHENTICATED`。已认证但缺少所需角色的调用方收到 `403 FORBIDDEN`。两者都使用标准 DataFlow 错误信封。

## 审计关联

认证边界接受传入的 `X-Request-ID`，否则会创建一个。响应返回相同 ID，结构化授权日志中也会记录它，并包含主体、角色决策、HTTP 方法、路径和所需角色。Bearer 令牌和原始声明绝不会写入日志。

## Python SDK

把 Bearer 令牌直接传给 SDK：

```python
from dataflow.sdk import DataFlowClient

with DataFlowClient("https://dataflow.example", token="...") as client:
    pipelines = client.list_pipelines()
```

高级调用方也可通过已有 `headers=` 参数显式设置 `Authorization` 请求头。`token=` 与显式 Authorization 头互斥，以避免凭据含义不清。
