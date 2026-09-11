# 真实 KubeRay 端到端套件

[English](README.md) | 简体中文

本套件在真实 Kubernetes 控制面、KubeRay operator、RayCluster/RayJob 生命周期、Ray Data 执行、PostgreSQL 状态和 S3 兼容持久制品上验证 DataFlow。

## 固定测试栈

- kind：`v0.33.0`
- Kubernetes 节点镜像：`kindest/node:v1.36.1`
- KubeRay Helm Chart/operator：`1.6.2`
- Ray 运行时：`2.58.0`
- PostgreSQL：`16-alpine`
- MinIO：`RELEASE.2025-09-07T16-13-09Z`

GitHub Actions 工作流固定 kind 二进制校验和与 kind 节点镜像摘要。这些固定值属于测试基础设施，不是 DataFlow 运行时兼容性承诺。

## 场景验证内容

`run.sh` 通过公开 HTTP API 操作，并在独立控制器协调真实 KubeRay 资源时观察持久诊断读取模型。

主运行依次验证：

1. 通过公开 API 创建 ClusterProfile 并持久化不可变 PipelineVersion。
2. 调度器/协调器提交真实 KubeRay RayJob。
3. 第一次尝试活跃时重启控制器，且不创建重复 RayJob。
4. 删除 RayJob 注入故障，产生失败的第一次工作流尝试。
5. 失败尝试不能公开已提交持久制品，其制品记录保持 `ABORTED`。
6. 第二次尝试成功并通过 MinIO 发布检查点。
7. 下游提交前停止控制器，再删除上游 RayJob/RayCluster。
8. 控制器重启后，下游执行单元读取已提交检查点，PipelineRun 达到 `SUCCEEDED`。
9. 检查点包含 Parquet 数据和 DataFlow 提交标记，最终输出包含 Parquet 数据。
10. 第二个真实运行在活跃时取消；取消传递到 KubeRay，且不发布已提交制品。
11. 场景期间 API Prometheus 接口保持可用。

设置 `DATAFLOW_E2E_WORKER_RESTART=1` 可启用可选工作器 Pod 重启压力步骤。它有意不属于阻断 CI 路径，因为 Ray 可以合理地在不触发工作流级重试的情况下恢复工作器；阻断套件重点验证确定性编排语义。

## 本地复现

前置依赖：

- Docker
- `kubectl`
- Helm 3
- kind `v0.33.0`
- `curl` 和 `jq`

创建集群并安装 KubeRay：

```bash
kind create cluster \
  --name dataflow-e2e \
  --image kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5

helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator \
  --version 1.6.2 \
  --namespace kuberay-system \
  --create-namespace \
  --wait
```

构建并加载 DataFlow 镜像：

```bash
docker build -t dataflow-runtime:e2e -f Dockerfile .
docker build -t dataflow-control-plane:e2e -f Dockerfile.control-plane .
kind load docker-image --name dataflow-e2e dataflow-runtime:e2e
kind load docker-image --name dataflow-e2e dataflow-control-plane:e2e
```

运行套件：

```bash
bash e2e/kuberay/run.sh
```

可选工作器重启压力路径：

```bash
DATAFLOW_E2E_WORKER_RESTART=1 bash e2e/kuberay/run.sh
```

失败时，脚本会在非零退出前输出 Pod、Job、RayJob、RayCluster 状态，以及 DataFlow 控制器、API 和 KubeRay operator 日志。

## 存储行为

Ray Pod 从固定 ClusterProfile 接收 MinIO 端点配置，并通过 Kubernetes Secret 引用获取凭据。DataFlow 不会把凭据值存入 PipelineSpec 或 ExecutionPlan 元数据。

配置 `DATAFLOW_S3_ENDPOINT_URL` 时，运行时构建 PyArrow `S3FileSystem`，在调用 Ray Data 前把 `s3://bucket/key` 转换为文件系统限定路径 `bucket/key`。控制器则通过 boto3 使用同一个 S3 兼容端点完成两阶段制品发布。
