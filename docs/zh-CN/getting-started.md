# 快速上手

[English](../getting-started.md) | 简体中文

## 安装

使用 Python 3.11 或 3.12，driver 和 worker 安装同版本 Ray。在仓库目录运行：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[runtime]'
```

基础包只依赖 Pydantic，`runtime` 扩展固定为 `ray[data]==2.58.0`。
不需要数据库、HTTP API、对象存储服务或控制器。
其他 Python 版本取决于 Ray 是否提供对应 wheel，目前不在 CI 覆盖范围内。

## 连接一个集群

单机开发演示中，在该环境预先启动一次共享集群：

```bash
ray start --head --port=6379 --num-cpus=2 --include-dashboard=false
```

DataFlow 本身不启动 Ray，生产使用平台已部署的常驻集群。额外 worker 节点加入同一个 head，
后续处理阶段不另起集群。

在同一台机器上准备小份输入：

```bash
mkdir -p /tmp/dataflow-demo
python - <<'CODE'
import pyarrow as pa
import pyarrow.parquet as pq
pq.write_table(pa.table({"value": [1, 2, 3, 4]}), "/tmp/dataflow-demo/input.parquet")
CODE
python examples/single_cluster.py /tmp/dataflow-demo/input.parquet /tmp/dataflow-demo/output --address auto
```

命令输出 sink 节点 ID 到最终路径的字典。读取结果：

```bash
python -c 'import pyarrow.parquet as pq; print(pq.read_table("/tmp/dataflow-demo/output"))'
```

再次运行应换一个输出目录，Ray 的 Parquet writer 默认追加写入。
多物理节点场景下，输入与最终输出必须以相同路径挂载到 worker，例如使用 NFS。
中间数据由 Ray 传输，不依赖这些共享文件。

## JSON 入口

先修改 `examples/single_cluster.json` 中的业务输入和最终输出路径，然后运行：

```bash
dataflow-run examples/single_cluster.json --address auto
# 等效模块入口：
python -m dataflow.cli examples/single_cluster.json --address auto
```

`--address` 指向已有 Ray 集群。不传时使用 `RAY_ADDRESS`，未设置则使用 `auto`。
拒绝 `local`，找不到集群也不会自动创建。嵌入宿主应用时参见[连接归属](operations.md)。

开发集群不再使用时，由其所有者在相应环境执行 `ray stop`。DataFlow 运行结束断开连接不会停止集群。
