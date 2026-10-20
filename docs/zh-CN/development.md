# 开发与验证

[English](../development.md) | 简体中文

## 仓库

| 路径 | 用途 |
| --- | --- |
| `src/dataflow/contracts.py` | 算子类型与每 worker 资源 |
| `src/dataflow/graph.py` | 流水线、边、引用校验与拓扑 |
| `src/dataflow/sdk/` | Python 构图 DSL |
| `src/dataflow/runtime.py` | Ray Data 算子与资源适配 |
| `src/dataflow/single_cluster.py` | 预检、DAG 执行与连接归属 |
| `src/dataflow/cli.py` | 唯一命令 `dataflow-run` |
| `examples/` | SDK、JSON 与分支示例 |
| `tests/` | 单元、文档、打包和可选真实 Ray 验证 |
| `docs/` | 当前中英文文档 |

## 本地检查

```bash
pip install -e '.[dev]'
ruff check .
ruff format --check .
pytest
python -m build
```

单元测试不需要运行 Ray 集群，使用惰性后端验证分支计算次数、资源透传、校验、异常和连接归属。
SDK 导入与规格哈希还在主动屏蔽运行和服务依赖的环境里验证。
真实 Ray 测试默认跳过，跳过不等于真实执行已经通过。

```bash
pip install -e '.[dev,runtime]'
DATAFLOW_TEST_RAY=1 pytest tests/test_single_cluster_ray.py -v -s -o faulthandler_timeout=90
```

集成测试只为测试环境创建一个 head 和一个 worker，在同一个集群连续执行流水线，
验证最终记录和宿主会话复用。它需要进程信息读取、本地 socket 和足够的进程、共享内存资源，
受限环境可能不满足条件。CI 限制执行时间，失败时打印并保存 Ray 日志，避免无输出地长期卡住。

CI 覆盖 Python 3.11/3.12 单元测试、lint/格式、源码包与 wheel 构建、无 Ray 的 wheel 安装冒烟测试，
以及独立 Python 3.11 head/worker 验证。构建结果保存为工作流产物，不再构建集群部署和厂商镜像矩阵。

## 打包与运行镜像

```bash
python -m build
# 基础镜像已经安装 Ray：
docker build -t dataflow-runtime:0.2.0 .
```

基础包只依赖 Pydantic，`runtime` 添加 Ray Data，`dev` 添加构建测试工具。
Dockerfile 不启动管控服务，也不自动创建集群，由平台决定 head/worker 启动命令并加入业务模块。

该分支进行 0.2 破坏性 API 清理，生成产物不等于已发布版本；审阅、打标签和发布是独立仓库操作。
没有相应工作负载结果时，不应宣称已验证压力、故障切换、内存或公平性保证。

`docs/` 下每份 Markdown 均需有 `docs/zh-CN/` 对应版本和语言切换链接，文档测试会检查配对和本地链接。
两种语言的可执行命令和 schema 名称应保持一致。
