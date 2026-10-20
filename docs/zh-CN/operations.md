# 部署与运维

[English](../operations.md) | 简体中文

## 共享环境

由宿主平台通过 Ray 原生命令或已有集群 Operator 部署一个 Ray 集群。
driver 和 worker 预装同版本 Ray/DataFlow 与业务模块。
[Dockerfile](../../Dockerfile) 在 Ray 2.58.0 Python 3.11 镜像上安装 DataFlow，
可以继续加入业务依赖后部署到共享集群。

集群内的 driver 使用 `auto`；可互通的其他节点可以使用 GCS 地址，如 `ray-head:6379`，
同时需要 driver 和 worker 双向互通。仅提供地址不会代理全部网络流量。
Kubernetes 中把 driver 放在集群网络或已有 head Pod 中，并挂载业务输入输出目录。
无需 DataFlow 服务、Chart 或额外数据库。

同一应用进程应先初始化一个 Ray 会话，再供并发调用共享：

```python
import ray
from dataflow.sdk import run_pipeline

ray.init(address="auto", namespace="processing")
try:
    # 应用调用复用该会话，不传连接覆盖参数。
    result = run_pipeline(spec)
finally:
    ray.shutdown()
```

其中 `spec` 由宿主构造。未预先初始化会话时，单次调用自行连接和断开，
不要让独立线程竞争初始化或关闭 Ray。多个独立 driver 进程可以共享同一个外部管理的集群。

## 数据与容量

只有业务输入和最终输出需要文件系统访问；路径以相同位置挂载到 worker。
分布式输入避免使用 `local://`，它明确指向 driver 本地文件。
中间 Dataset 块不需要共享文件系统或 S3。

根据 UDF 计算设置 CPU，根据每个 worker 的堆内存需求设置 `memory_bytes`，按有效并行度设置 `concurrency`。
单个请求必须能被某个符合条件的 worker 满足。Ray 内存预留不会限制 UDF 的实际分配，
必要时由平台配置进程或容器限制。小数 GPU 是逻辑资源预留，不隔离物理加速器显存。

流式链通常比完整物化使用更少对象存储，但分支与控制屏障可能保留整份数据。
监测 worker 堆内存/RSS、对象存储用量、本地 spill 磁盘、排队任务和上下游吞吐。
慢 sink 会对上游施加背压，应测量瓶颈后再调整并发。

## 故障与定位

最终输出使用每次运行独立目录。失败可能留下已完成的较早 sink 或部分文件，
同路径重提默认追加写入。宿主重试和外部数据库、向量库等副作用必须保证幂等。
Ray 内部重试按 Ray 环境与算子配置执行，DataFlow 没有重试注册表。

捕获 `PipelineExecutionError`，记录 `node_id` 和完整 cause 异常链；把宿主请求 ID 与 driver 日志、
Ray job/task ID 关联。平台启用时使用 Ray dashboard、任务日志和 Dataset 统计。
组件不独立启动指标服务或保存执行历史。终止 driver 不等于持久工作流取消协议，
宿主需负责生命周期管理和业务清理。

找不到集群时连接失败；缺少业务模块会在 driver 预检或 worker 执行时报错。
资源不足可能导致 Ray 任务持续 pending，应检查 worker 容量、标签和 actor 并发。
输入路径错误通常与共享挂载缺失有关。

集群或 driver 重启恢复、最终结果保留、准入公平性和可用性由宿主承担，需按业务工作负载进行验收。
