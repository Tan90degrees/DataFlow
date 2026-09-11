# 本地 Kubernetes 开发环境

[English](../local-development.md) | 简体中文

本地调试环境在隔离 Kind 集群中运行完整 DataFlow 执行路径：KubeRay、PostgreSQL、MinIO、DataFlow API 与控制器以及 RayJob/RayCluster 工作负载。它使用 `.dataflow-dev/` 下的专用 kubeconfig，不会替换用户默认 Kubernetes 上下文。

## 前置依赖

- 已启动守护进程的 Docker
- `kubectl`
- Helm 3
- kind v0.33.0
- `curl`

仓库默认期望 kind 二进制位于 `.dataflow-dev/bin/kind`，可通过 `DATAFLOW_DEV_KIND_BIN=/path/to/kind` 覆盖。

## 启动或重建

```bash
bash scripts/local-dev-up.sh
```

脚本会按需创建 `dataflow-dev` 集群，安装 KubeRay 1.6.2，基于当前工作树重建两个 DataFlow 镜像并加载到 Kind，再部署现有真实 KubeRay 装置。每次运行只重置专用 `dataflow-e2e` 命名空间，因此上一轮本地调试部署中的 PostgreSQL 和 MinIO 数据会有意丢弃。

本地端点通过 Kind 的仅 localhost 端口映射保持可用，无需后台端口转发进程：

| 组件 | 端点 | 凭据 |
| --- | --- | --- |
| DataFlow API | `http://127.0.0.1:18080` | 本地装置禁用认证 |
| OpenAPI UI | `http://127.0.0.1:18080/docs` | 无 |
| MinIO 控制台 | `http://127.0.0.1:19001` | `dataflow` / `dataflow-secret` |
| PostgreSQL | `127.0.0.1:15432` | `dataflow` / `dataflow`，数据库 `dataflow` |

这些是有意设置的纯本地测试凭据，绝不能在专用调试集群之外复用。

## 检查与调试

```bash
bash scripts/local-dev-status.sh

export KUBECONFIG="$PWD/.dataflow-dev/kubeconfig"
kubectl get pods,jobs,rayjobs,rayclusters -n dataflow-e2e
kubectl logs -n dataflow-e2e deployment/dataflow-controller -f
kubectl logs -n dataflow-e2e deployment/dataflow-api -f
```

可以通过 API 提交 `e2e/kuberay/` 下的流水线和 ClusterProfile 示例。`bash e2e/kuberay/run.sh` 仍是破坏性的完整生命周期测试；需要保留交互式调试会话时不要运行它。完整提交示例见[快速上手](getting-started.md)。

## 停止并删除

```bash
bash scripts/local-dev-down.sh
```

该命令只删除 `dataflow-dev` Kind 集群并释放其 localhost 端口。
