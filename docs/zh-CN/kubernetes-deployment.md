# Kubernetes 部署与控制器高可用

[English](../kubernetes-deployment.md) | 简体中文

`charts/dataflow` Helm Chart 为面向生产的 Kubernetes 部署打包 DataFlow API 和持久控制器。PostgreSQL 与 S3 兼容存储是外部依赖，Chart 有意不安装它们。

## 前置条件

- Kubernetes 1.28+
- Helm 3
- 与配置的 `ClusterProfile` 资源兼容的 KubeRay operator
- API/控制器 Pod 可访问的 PostgreSQL
- 控制器和 Ray Pod 可访问的 S3 兼容对象存储
- 每个 `ClusterProfile` 引用的运行时服务账户；它独立于 Chart 管理的控制面服务账户

首次安装前，以及升级到包含新迁移的应用版本前，执行数据库迁移：

```bash
dataflow-migrate --dsn "$DATAFLOW_DATABASE_URL"
```

把数据库 URL 存入 Secret，而不是 values 文件：

```bash
kubectl create secret generic dataflow-database \
  --namespace dataflow \
  --from-literal=database-url="$DATAFLOW_DATABASE_URL"
```

使用标准 AWS 凭据环境变量名，把对象存储凭据放入独立 Secret：

```bash
kubectl create secret generic dataflow-s3 \
  --namespace dataflow \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"
```

## 安装

最小生产安装将状态放在 Chart 外部，并运行两个控制器副本：

```bash
helm upgrade --install dataflow charts/dataflow \
  --namespace dataflow \
  --create-namespace \
  --set image.repository=registry.example.com/dataflow-control-plane \
  --set image.tag=0.1.0 \
  --set database.existingSecret=dataflow-database \
  --set s3.existingSecret=dataflow-s3 \
  --set s3.endpointUrl=https://s3.example.com \
  --set controller.replicaCount=2
```

默认控制器 PodDisruptionBudget 会在自愿中断期间确保两个副本中至少一个可用。两个副本运行相同持久协调循环，但 PostgreSQL 咨询锁选主会把变更操作限制在单个控制器。备用副本持续重试同一把锁，领导进程/会话消失后即可接管。

API 默认两个副本。API 认证通过 `api.auth` 配置；`static` 模式要求 `api.auth.existingSecret` 包含配置的 `staticTokensKey`。对互联网公开的部署不要禁用 API 认证。

## 控制器权限

Chart 默认分别创建 API 与控制器服务账户。控制器角色限制在命名空间内，只授予控制面所需的 RayJob 操作。Ray 头节点/工作节点 Pod 使用所选 `ClusterProfile` 配置的服务账户；Chart 不会向运行时 Pod 授予控制面权限。

平台管理身份时，设置 `serviceAccount.create=false` 并提供 `serviceAccount.apiName` / `serviceAccount.controllerName`。

## 升级顺序

1. 阅读发布说明，并用现有生产 values 渲染新 Chart。
2. 滚动 API/控制器 Pod 前，应用所有新 PostgreSQL 迁移。
3. 使用相同外部 Secret 引用运行 `helm upgrade`。
4. 等待 API 和控制器 Deployment 可用。
5. 确认一个控制器报告领导状态，另一个报告备用状态。
6. 验证滚动期间活跃运行保持尝试和 RayJob 标识不变。

Chart 使用滚动 Deployment，不会重建 PostgreSQL、对象存储、流水线元数据、尝试或 RayJob。持久协调与领导权约束保证控制器替换安全，PDB 则防止自愿操作导致所有控制器副本同时丢失。

## HA 验证

仓库的 `e2e-kuberay-ha` 工作流会在 Kind 中安装两个控制器的生产 Chart，创建真实慢速 Ray Data 流水线，通过 PostgreSQL `pg_stat_activity` 找到当选控制器，并在 RayJob 活跃时终止该领导者。测试要求预先存在的备用 Pod 获取领导权，再断言接管期间上游单元只保留一次尝试和一个 RayJob 标识，最终流水线成功完成。

该测试特意通过 PostgreSQL 持锁会话识别领导权，而不是根据 Pod 就绪状态推断。失败时诊断会收集控制器日志、PostgreSQL 领导会话、RayJob/RayCluster 和命名空间事件。

## 运维信号

控制器日志发出 `controller_standby`、`controller_leadership_acquired`、`controller_leadership_lost`、`controller_leadership_released` 和 `controller_leadership_unavailable`。健康的双副本部署在 Prometheus 领导权指标中应恰好有一个活跃领导者。

终止控制器 Pod 不会取消已经提交的 RayJob。替代领导者从 PostgreSQL 重建持久状态并观察已有 RayJob，不会创建副本。PostgreSQL 不可用时，控制器无法获取领导权，变更工作会停止，不会进行无约束协调。
