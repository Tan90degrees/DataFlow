# DataFlow 文档

[English](../README.md) | 简体中文

本页是文档入口。英文文档保留原路径，每份文档在 `docs/zh-CN/` 下都有对应的简体中文版本。

## 从这里开始

- [快速上手](getting-started.md)：安装依赖、启动隔离的本地 Kubernetes 环境、提交和检查流水线，并选择后续部署路径。
- [本地 Kubernetes 开发](local-development.md)：本地集群生命周期、端点和调试命令。
- [Kubernetes 部署与控制器高可用](kubernetes-deployment.md)：面向生产的 Helm 安装和升级。
- [API 认证与 RBAC](api-auth.md)：认证模式、角色和 SDK 凭据。

## 使用和运维 DataFlow

- [持久制品与检查点](ARTIFACTS.md)
- [调度准入控制](admission-control.md)
- [控制器高可用](controller-ha.md)
- [可观测性与诊断](observability.md)
- [保留策略与制品垃圾回收](retention-gc.md)
- [发布 DataFlow](releases.md)
- [项目路线图](roadmap.md)
- [真实 KubeRay 端到端套件](../../e2e/kuberay/README.zh-CN.md)

## 架构决策

- [ADR-0001：分离编排与 Ray Data 执行](adr/0001-execution-boundary.md)
- [ADR-0002：将逻辑 DAG 编译为执行岛](adr/0002-execution-islands.md)
- [ADR-0003：PostgreSQL 是持久编排的事实来源](adr/0003-durable-metadata.md)
- [ADR-0004：持久期望状态与幂等外部协调](adr/0004-idempotent-reconciliation.md)
- [ADR-0005：在执行单元边界发布持久制品](adr/0005-durable-artifact-publication.md)
- [ADR-0006：HTTP API 写入持久期望状态](adr/0006-api-desired-state-boundary.md)
- [ADR-0007：Python SDK 生成规格而非分布式工作](adr/0007-sdk-spec-generation.md)
- [ADR-0008：版本化 ClusterProfile 并固定执行计划快照](adr/0008-cluster-profile-snapshots.md)
- [ADR-0009：不在指标标签中使用关联标识符](adr/0009-observability-boundaries.md)
- [ADR-0010：使用 PostgreSQL 领导权约束控制器协调](adr/0010-controller-leader-election.md)
- [ADR-0011：让 API 认证独立于编排身份](adr/0011-api-authentication-boundary.md)
- [ADR-0012：持久准入独立于 DAG 就绪](adr/0012-durable-admission-and-fairness.md)
- [ADR-0013：保留策略与可恢复制品垃圾回收](adr/0013-retention-gc.md)

## 文档约定

在 `docs/` 下新增或重命名英文文件时，请在 `docs/zh-CN/` 的同一相对路径新增或重命名中文文件。两种语言中的命令、配置键、API 路径、代码和版本号应保持一致。测试套件会检查文件配对和语言切换链接。
