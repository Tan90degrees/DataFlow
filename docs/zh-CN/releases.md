# 发布 DataFlow

[English](../releases.md) | 简体中文

DataFlow 发布由不可变 Git 标签驱动。一个发布版本在 Python 包和 Helm Chart 之间遵循统一事实来源契约，发布由 GitHub Actions 使用仓库 `GITHUB_TOKEN` 完成，不使用长期镜像仓库凭据。

## 版本契约

发布前，在以下两处设置相同的稳定语义化版本：

- `pyproject.toml`：`project.version`
- `charts/dataflow/Chart.yaml`：`version` 和 `appVersion`

打标签前验证仓库：

```bash
dataflow-check-release
# 或显式验证目标标签：
dataflow-check-release v0.1.0
```

只接受规范 `vX.Y.Z` 标签。三个内置版本中任意一个不同都会拒绝标签。

Helm Chart 默认特意把 `image.tag` 留空。模板会把空标签解析为 `Chart.appVersion`，因此升级 Chart 版本契约也会升级默认控制面镜像，无需在 `values.yaml` 中维护第二个版本值。

## 拉取请求验证

修改发布敏感文件会以仅验证模式运行 `release` 工作流。它会构建但不发布：

- Python wheel 和源码发行包；
- 打包后的 Helm Chart；
- Ray 运行时镜像；
- 控制面镜像；
- 可分发归档的 SHA256 校验和。

这组验证附加在常规单元测试、Helm 渲染、KubeRay 和 HA 门禁之上。PR 验证只有仓库读取权限。

## 创建发布

发布提交进入 `main` 且所有必需检查通过后，创建并推送匹配的附注或轻量 Git 标签：

```bash
git switch main
git pull --ff-only
dataflow-check-release v0.1.0
git tag v0.1.0
git push origin v0.1.0
```

标签触发的工作流会在发布前再次验证标签。如果验证或任何构建失败，发布作业不会发布任何内容。

`v0.1.0` 的发布坐标为：

```text
ghcr.io/tan90degrees/dataflow-runtime:0.1.0
ghcr.io/tan90degrees/dataflow-runtime:sha-<git-sha>
ghcr.io/tan90degrees/dataflow-control-plane:0.1.0
ghcr.io/tan90degrees/dataflow-control-plane:sha-<git-sha>
oci://ghcr.io/tan90degrees/charts/dataflow:0.1.0
```

工作流有意不发布可变 `latest` 标签。部署应固定发布版本或镜像摘要。

该标签的 GitHub Release 包含 Python wheel、Python 源码发行包、打包 Helm Chart 以及由准确已验证归档生成的 `SHA256SUMS`。

## 安装已发布 Chart

创建 `kubernetes-deployment.md` 所述的外部数据库和 S3 凭据 Secret，然后安装 OCI Chart：

```bash
helm upgrade --install dataflow \
  oci://ghcr.io/tan90degrees/charts/dataflow \
  --version 0.1.0 \
  --namespace dataflow \
  --create-namespace \
  --set database.existingSecret=dataflow-db \
  --set s3.existingSecret=dataflow-s3
```

Chart 默认为 `ghcr.io/tan90degrees/dataflow-control-plane:<appVersion>`。只有明确使用镜像或自定义构建时才覆盖 `image.repository` 或 `image.tag`。

Ray 运行时镜像独立于控制面。ClusterProfile 应引用匹配的已发布运行时镜像，例如 `ghcr.io/tan90degrees/dataflow-runtime:0.1.0`。

## 失败与重试语义

标签工作流失败后应在新提交中诊断并修复，不要移动已经发布的标签。发布任何制品前，重试失败工作流是安全的。发布开始后，将版本标签视为不可变：为修正制品提升补丁版本，不要覆盖已发布镜像或 Chart。
