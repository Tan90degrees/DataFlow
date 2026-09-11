# ADR 0011：让 API 认证独立于编排身份

[English](../../adr/0011-api-authentication-boundary.md) | 简体中文

- 状态：已接受
- 日期：2026-09-10

## 背景

DataFlow 正从可信单团队控制面走向共享部署。因此流水线提交、取消和 ClusterProfile 管理需要显式授权边界。复用 Kubernetes 服务账户或 Ray 身份会把公开 API 策略耦合到执行基础设施，还会让本地/集成测试依赖集群或身份提供器。

首个实现还需要避免让 DataFlow 绑定单一 OIDC 厂商。

## 决策

生产 HTTP 入口使用可插拔 Bearer 认证与角色授权中间件包装现有 FastAPI 控制面应用。

认证拆分为三个契约：

1. Bearer 声明提供器把不透明令牌转换为提供器声明；
2. 声明适配器把 OIDC 风格声明（`sub` 加可配置角色声明）映射为精简 DataFlow 身份；
3. HTTP 授权策略把 DataFlow API 操作映射到稳定 DataFlow 角色。

初始提供器是静态令牌到声明的映射，面向测试、开发和小型受控部署。未来已验证 JWT/OIDC 提供器可以实现相同声明提供器协议，无需修改 API 处理器或调度器/控制器代码。

授权使用四个粗粒度角色：`pipeline.read`、`pipeline.submit`、`run.cancel` 和 `cluster_profile.admin`。Kubernetes 与 Ray 凭据仍是执行关注点，不参与 API 授权。

为兼容现有开发和 E2E 部署，默认禁用认证；共享生产部署必须显式配置认证模式。后续生产 Helm 打包会通过部署 values/Secret 公开该配置。

## 影响

- API 授权可完全在 FastAPI/PostgreSQL 上测试，无需外部 IdP。
- ClusterProfile 管理可以与工作流提交和取消分离。
- 未来 OIDC 验证器只是替换适配器，而不是重写 API。
- 静态 Bearer 令牌只适用于由部署环境负责密钥分发和轮换的场景。
- 请求关联与允许/拒绝决策以结构化日志发出，不记录 Bearer 令牌或原始声明。
- 有意在日志中明确显示认证禁用模式，便于运维人员发现只依赖网络边界的部署。
