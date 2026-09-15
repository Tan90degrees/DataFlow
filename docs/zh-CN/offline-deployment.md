# 离线与多架构部署

[English](../offline-deployment.md) | 简体中文

DataFlow 发布包支持 `linux/amd64` 和 `linux/arm64` 的隔离 Kubernetes 环境。
构建流水线会为每个架构生成一个独立 bundle，运维人员只需把所需镜像和 Chart
跨隔离边界传输一次，完成校验后即可安装，部署阶段无需访问公网容器仓库或
Helm 仓库。

## Bundle 内容

例如 `v0.1.0` 会生成：

```text
dataflow-offline-0.1.0-amd64.tar.gz
dataflow-offline-0.1.0-amd64.sha256
dataflow-offline-0.1.0-arm64.tar.gz
dataflow-offline-0.1.0-arm64.sha256
```

每个归档包含：

- 所选架构的 DataFlow Ray 运行时镜像；
- 所选架构的 DataFlow API/Controller 镜像；
- 固定版本 KubeRay operator `v1.6.2` 镜像；
- 已打包的 DataFlow Helm Chart；
- 已打包的 KubeRay operator Helm Chart；
- 仅用于独立离线启动和验收的 PostgreSQL 16 与 MinIO 参考镜像；
- 描述精确版本与源镜像名称的 `bundle.env` 和 `manifest.json`；
- 覆盖 bundle 内全部载荷的 `SHA256SUMS`；
- 镜像导入、Kind 导入、安装以及参考依赖启动脚本。

生产 DataFlow Chart 仍把 PostgreSQL 和 S3 兼容对象存储视为外部服务。
Bundle 中包含 PostgreSQL 和 MinIO 参考镜像，是为了让隔离环境无需公网即可
完成全量安装验证，并不代表生产持久化方案应使用这些参考部署。

## 跨越隔离区后先校验

解压前先验证外层归档：

```bash
sha256sum -c dataflow-offline-0.1.0-amd64.sha256
tar -xzf dataflow-offline-0.1.0-amd64.tar.gz
cd dataflow-offline-0.1.0-amd64
sha256sum -c SHA256SUMS
```

外层或内部任何校验失败时都不要继续安装。

## 使用内网镜像仓库的生产流程

目标环境需要在传输主机上提供可访问内网仓库的 Docker，并预先安装配置好
`kubectl` 与 Helm 3。Bundle 脚本不会在线下载这些工具。

导入所有镜像并推送到内网仓库前缀：

```bash
./scripts/load-images.sh \
  --registry registry.internal.example/dataflow
```

命令会在隔离环境发布以下镜像名：

```text
registry.internal.example/dataflow/dataflow-runtime:0.1.0
registry.internal.example/dataflow/dataflow-control-plane:0.1.0
registry.internal.example/dataflow/kuberay-operator:v1.6.2
registry.internal.example/dataflow/postgres:16-alpine
registry.internal.example/dataflow/minio:RELEASE.2025-09-07T16-13-09Z
```

生产环境请使用隔离网络内部可访问的平台化 PostgreSQL 与 S3 服务，并创建对应
Secret：

```bash
kubectl create namespace dataflow

kubectl -n dataflow create secret generic dataflow-database \
  --from-literal=database-url='postgresql://USER:PASSWORD@postgres.internal:5432/dataflow'

kubectl -n dataflow create secret generic dataflow-s3 \
  --from-literal=AWS_ACCESS_KEY_ID='...' \
  --from-literal=AWS_SECRET_ACCESS_KEY='...'
```

只使用 bundle 内本地 Chart 安装 KubeRay 和 DataFlow：

```bash
./scripts/install.sh \
  --registry registry.internal.example/dataflow \
  --namespace dataflow \
  --database-secret dataflow-database \
  --s3-secret dataflow-s3 \
  --s3-endpoint http://s3.internal:9000
```

安装器会先使用 bundle 内控制面镜像运行 `dataflow-migrate`，然后再滚动安装
API/Controller Chart，同时创建参考 ClusterProfile 路径使用的 `dataflow-ray`
ServiceAccount。

如果平台已经运行兼容的 KubeRay，可增加 `--skip-kuberay`。

隔离环境中的 PipelineSpec 必须使用已镜像到内网的 runtime 镜像：

```text
registry.internal.example/dataflow/dataflow-runtime:0.1.0
```

## 完全独立的参考启动

用于演示和验收时，bundle 可以只使用自身镜像创建非生产 PostgreSQL 和 MinIO：

```bash
./scripts/bootstrap-reference-deps.sh \
  --registry registry.internal.example/dataflow \
  --namespace dataflow \
  --pull-policy IfNotPresent

./scripts/install.sh \
  --registry registry.internal.example/dataflow \
  --namespace dataflow \
  --database-secret dataflow-database \
  --s3-secret dataflow-s3 \
  --s3-endpoint http://minio.dataflow.svc.cluster.local:9000
```

参考启动脚本使用固定演示凭据和临时存储，不要用于生产数据。

## 无镜像仓库的 Kind 验收路径

本地 Kind 集群可以把 bundle 镜像直接注入节点，并使用一个虚拟仓库前缀，不需要
真的启动 registry：

```bash
./scripts/kind-load-images.sh \
  --cluster dataflow-offline \
  --registry offline.invalid/dataflow
```

随后使用 `--pull-policy Never` 运行参考依赖和安装脚本。任何镜像没有预装时，
Kubernetes 会立即失败，而不会尝试访问仓库。发布流水线的离线 smoke test
正是使用这一模式，并会检查 DataFlow 与 KubeRay namespace 在安装期间没有
出现 `Pulling` 事件。

## 架构选择

根据 Kubernetes 节点架构选择对应 bundle：

- `amd64` 对应 `linux/amd64`；
- `arm64` 对应 `linux/arm64`。

发布流水线会为两个平台构建两类应用镜像。常规 GHCR 版本标签和
`sha-<commit>` 标签都是多平台 manifest list，因此联网环境在两种架构上可以
使用同一个标签。

不要把 `amd64` bundle 用到 `arm64` 节点，反之亦然。Bundle manifest 同时记录
`architecture` 和 `platform`，部署自动化可以在载入镜像前拒绝架构不匹配。

## 升级与回滚

离线升级流程：

1. 传输并验证新的架构专属 bundle；
2. 把新镜像标签导入/推送到内网仓库；
3. 使用原 namespace、Secret 名称和 S3 endpoint 再次运行 `scripts/install.sh`；
4. 验证两个 Controller 副本、API readiness 和 KubeRay 健康状态；
5. 需要时把 ClusterProfile 更新到匹配的新 runtime 镜像。

发布版本和 SHA 标签不可变。在新版本通过业务负载验证前，保留上一版本 bundle
及其内网镜像标签。
