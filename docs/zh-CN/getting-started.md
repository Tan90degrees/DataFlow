# DataFlow 快速上手

[English](../getting-started.md) | 简体中文

本指南帮助新贡献者从源代码检出开始，直至成功运行一条流水线。最短路径是使用隔离的 Kind 环境：它会运行 PostgreSQL、MinIO、KubeRay、DataFlow API 与控制器以及真实 Ray 工作负载，同时不会修改你的默认 Kubernetes 上下文。

## 1. 选择工作方式

| 目标 | 推荐路径 |
| --- | --- |
| 探索 API 并运行真实流水线 | [本地 Kubernetes 环境](#3-启动本地-kubernetes-环境) |
| 修改 Python 代码并快速验证 | [Python 开发](#2-配置-python-开发环境) |
| 端到端验证故障恢复 | [KubeRay E2E 套件](../../e2e/kuberay/README.zh-CN.md) |
| 部署共享环境 | [生产 Helm 部署](kubernetes-deployment.md) |

## 2. 配置 Python 开发环境

需要 Python 3.11 或更高版本。

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
ruff check .
pytest
```

设置 `DATAFLOW_TEST_DATABASE_URL` 后会启用 PostgreSQL 集成测试。本地 Kind 环境会在 `15432` 端口公开数据库，因此启动后可以运行：

```bash
export DATAFLOW_TEST_DATABASE_URL=postgresql://dataflow:dataflow@127.0.0.1:15432/dataflow
pytest
```

## 3. 启动本地 Kubernetes 环境

前置依赖为 Docker、`kubectl`、Helm 3、kind v0.33.0、`curl` 和 `jq`。在仓库根目录启动或重建环境：

```bash
bash scripts/local-dev-up.sh
```

该命令创建专用 `dataflow-dev` Kind 集群，基于当前工作树重建运行时和控制面镜像，并部署完整技术栈。隔离的 kubeconfig 写入 `.dataflow-dev/kubeconfig`，不会替换你的常用 Kubernetes 上下文。

验证部署：

```bash
bash scripts/local-dev-status.sh
curl -fsS http://127.0.0.1:18080/readyz
curl -fsS http://127.0.0.1:18080/version | jq
```

常用端点：

| 服务 | 地址 | 本地凭据 |
| --- | --- | --- |
| DataFlow API | `http://127.0.0.1:18080` | 认证已禁用 |
| OpenAPI UI | `http://127.0.0.1:18080/docs` | 无 |
| MinIO 控制台 | `http://127.0.0.1:19001` | `dataflow` / `dataflow-secret` |
| PostgreSQL | `127.0.0.1:15432` | `dataflow` / `dataflow`，数据库 `dataflow` |

这些值仅是隔离集群中的测试凭据。

## 4. 通过 API 提交流水线

本地装置已经包含 KubeRay 服务账户、S3 凭据、输入 Parquet 数据以及匹配的运行时镜像。依次创建 ClusterProfile、流水线、不可变版本和运行：

```bash
export DATAFLOW_API_URL=http://127.0.0.1:18080

curl -fsS -X POST "$DATAFLOW_API_URL/v1/cluster-profiles" \
  -H 'content-type: application/json' \
  --data-binary @e2e/kuberay/cluster-profile.json | jq

PIPELINE_ID=$(curl -fsS -X POST "$DATAFLOW_API_URL/v1/pipelines" \
  -H 'content-type: application/json' \
  -d '{"name":"kuberay-e2e","tenant_id":"local"}' | jq -r '.id')

VERSION_ID=$(curl -fsS -X POST \
  "$DATAFLOW_API_URL/v1/pipelines/$PIPELINE_ID/versions" \
  -H 'content-type: application/json' \
  --data-binary @e2e/kuberay/pipeline.json | jq -r '.id')

RUN_ID=$(curl -fsS -X POST "$DATAFLOW_API_URL/v1/pipeline-runs" \
  -H 'content-type: application/json' \
  -d "{\"pipeline_version_id\":\"$VERSION_ID\",\"created_by\":\"local-debug\"}" \
  | jq -r '.id')

echo "$RUN_ID"
```

该示例特意包含一个 90 秒算子，便于在运行期间检查 RayJob 和控制器行为。

## 5. 观察、取消和调试运行

读取持久诊断视图：

```bash
curl -fsS "$DATAFLOW_API_URL/v1/pipeline-runs/$RUN_ID/diagnostics" | jq
curl -fsS "$DATAFLOW_API_URL/v1/pipeline-runs/$RUN_ID/events" | jq
curl -fsS "$DATAFLOW_API_URL/v1/pipeline-runs/$RUN_ID/artifacts" | jq
```

使用隔离 kubeconfig 检查实时 Kubernetes 资源和日志：

```bash
export KUBECONFIG="$PWD/.dataflow-dev/kubeconfig"
kubectl get pods,jobs,rayjobs,rayclusters -n dataflow-e2e
kubectl logs -n dataflow-e2e deployment/dataflow-controller -f
kubectl logs -n dataflow-e2e deployment/dataflow-api -f
```

取消操作会先持久化，随后异步传递到外部 RayJob：

```bash
curl -fsS -X POST "$DATAFLOW_API_URL/v1/pipeline-runs/$RUN_ID/cancel" | jq
```

常见检查：

- `readyz` 失败：检查 API 日志和 PostgreSQL Pod；就绪状态依赖 PostgreSQL。
- 没有出现 RayJob：检查控制器领导权和准入日志，再读取运行诊断。
- RayJob 无法启动：检查 RayJob、RayCluster、Pod 事件、运行时镜像、服务账户和所选 ClusterProfile 资源。
- 制品发布失败：核对控制器与 Ray Pod 中的 MinIO/S3 端点和凭据；计算成功与制品提交是两个独立状态。
- 修改源代码后：重新执行 `bash scripts/local-dev-up.sh`，以重建并加载两个镜像。脚本会重置本地装置命名空间，因此会丢弃其中的 PostgreSQL 和 MinIO 数据。

## 6. 使用 Python SDK

安装客户端额外依赖并连接 API：

```bash
pip install -e '.[sdk]'
```

```python
from dataflow.callables import identity_batch
from dataflow.sdk import DataFlowClient, Resources, pipeline


@pipeline(
    name="example",
    runtime_image="dataflow-runtime:e2e",
    cluster_profile="e2e-cpu",
    artifact_base_uri="s3://dataflow-e2e/checkpoints",
)
def example(flow, input_path: str, output_path: str):
    data = flow.read_parquet(input_path)
    data.map_batches(identity_batch, resources=Resources(cpu=1)).write_parquet(output_path)


spec = example.spec(
    "s3://dataflow-e2e/input/data.parquet",
    "s3://dataflow-e2e/sdk-output",
)
with DataFlowClient("http://127.0.0.1:18080") as client:
    result = client.submit(spec, created_by="sdk-local")
    print(result.run["id"])
```

构建规格不会初始化 Ray 或读取数据。生产可调用对象必须是不可变运行时镜像中可导入模块的顶层符号。更完整的示例见 `examples/sdk_pipeline.py`。

## 7. 关闭环境或走向生产

仅删除专用本地集群并释放端口：

```bash
bash scripts/local-dev-down.sh
```

共享部署请继续阅读 [Kubernetes 部署与控制器高可用](kubernetes-deployment.md)，然后配置 [API 认证](api-auth.md)、[可观测性](observability.md)、准入限制以及[保留/垃圾回收](retention-gc.md)。不要在生产环境复用本地数据库、S3 凭据、运行时镜像标签或禁用认证的配置。
