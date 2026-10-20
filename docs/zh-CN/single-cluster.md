# 单集群执行

[English](../single-cluster.md) | 简体中文

## 执行约定

只部署一个常驻 Ray 集群。driver 调用 `run_pipeline(spec)`，把整个 DAG 构建为该集群中
Ray Data Dataset 的变换。多个 driver、多个流水线共享 worker 资源池，不按任务或算子创建集群。

同步 SDK 与 `dataflow-run` 不访问 PostgreSQL、Kubernetes、HTTP 控制面或制品注册表，
也不创建、发布中间存储路径。DAG 只显式读业务输入、写最终输出，中间传递全部使用 Ray 数据块。

线性链直到 sink 消费时才触发执行，Ray Data 可以进行流式处理、兼容算子融合与背压，
普通算子之间不等待整份数据完成，也不把 Dataset 的实际数据收集到 driver。

同一 Dataset 存在多个消费者时，DataFlow 调用一次 `materialize()`，复用返回的
对象存储 Dataset，避免每个 sink 重算公共前缀。这是执行和内存屏障，不是持久检查点。
控制依赖同样先完成源节点再继续。最后一个消费者构建后释放相应 Dataset 句柄，
下游仍引用的数据块继续由 Ray 保留。

分支缓存可能超过对象存储内存，触发 Ray 配置的本地磁盘 spilling，本路径不配置 S3 spilling。
应按分支共享工作集规划容量。同节点 Arrow/NumPy 数据可支持零拷贝访问，跨节点仍有网络传输，
UDF 也可能分配自己的堆内存。

## 连接与部署

```bash
pip install -e '.[runtime]'
dataflow-run examples/single_cluster.json --address auto
# 同网络内显式指定地址：
dataflow-run examples/single_cluster.json --address ray-head:6379
```

driver 和 worker 预装相同版本的 DataFlow/Ray 与业务模块。driver 在共享集群或可互通网络内运行，
也可使用 Ray 支持的连接地址。普通 GCS 地址要求 worker 与 driver 之间也能互通，
不只是开放 head 端口。算子使用可导入的顶层函数或 callable class，不按流水线动态安装镜像或依赖。

在集群节点上使用 `auto`，也可设置 `RAY_ADDRESS` 或传入 `address=`。未找到集群就失败，
拒绝 `local`，包括通过 `RAY_ADDRESS` 设置的情况。宿主已经初始化 Ray 时，省略 `address`
和 `namespace`，复用已有会话并保持连接；否则由 DataFlow 连接一次，在 `finally` 中断开，
外部常驻集群继续运行。Ray 连接生命周期是进程级状态，同进程并发调用应共享宿主预先建立的会话。

示例文件路径必须对 worker 可见。多节点使用共享文件系统或挂载存放输入和最终输出，
Ray 中间对象传输不需要这个挂载。KubeRay 可以独立管理唯一的共享 RayCluster，无需 DataFlow 控制器。

## 资源与行为

按算子声明 `Resources(cpu=..., gpu=..., memory_bytes=..., accelerator_type=...)`。
读写算子通过 `ray_remote_args` 传递资源；map/filter 直接传给 Ray Data。
加速器类型使用现有 worker 的 `ray.io/accelerator-type` 标签。`batch_size`、`concurrency`、
actor 类构造参数等 Ray Data 设置保留在算子 config 内。CPU/内存资源是调度提示，不是操作系统硬限制，
请求的资源必须能由同一集群中的某个 worker 满足。

首版支持 Parquet 输入输出、map_batches、filter、数据分支与控制依赖。
变换和 sink 各有一个数据输入，fan-in 在任何 IO 前拒绝。driver 按确定的拓扑顺序执行独立 sink，
每个 sink 内由 Ray 分布式并行；多个流水线 driver 仍共享该集群。
DataFlow 不额外实现全局公平性或准入调度器。

返回值包含流水线名和最终输出路径。失败保留 Ray 原异常为 cause，并报告触发执行的节点；
惰性执行时可能在 sink 处暴露上游 UDF 异常。任务和 actor 按 Ray 自身配置重试。
本路径不提供持久工作流恢复，也不提供多个 sink 的 exactly-once 原子提交。
失败可能留下较早完成的 sink 或部分输出，重试时使用独立运行目录或业务幂等写入；
Parquet 写入沿用 Ray 默认 append 语义。

## 从旧流水线迁移

1. 删除 `runtime_image`、Kubernetes service account 与节点级运行环境覆盖，改为共享 worker 预装依赖。
2. 删除具名 `cluster_profile` 选择，用算子资源在同一集群内选择容量。
3. 删除 `artifact_base_uri`、`.checkpoint()` 和 `.hard_boundary()`，普通边和分支直接传 Ray 数据。
   旧持久边界会明确校验失败，避免静默改变恢复语义。
4. 把 HTTP 客户端 `submit` 替换为 `run_pipeline(spec)`，或使用 `dataflow-run`。
5. 只保留业务输入输出路径，算子之间不需要 MinIO/S3 服务。

旧命令和控制面模块暂留给已有部署，不参与新入口，也不符合这份新的执行约定。
旧文档和早期 ADR 描述的是历史架构。完整移除控制面以及生产压力、故障验收属于后续工作，
本路径目前不宣称具备这些保证。

## 验证

```bash
pip install -e '.[dev,runtime]'
ruff check .
pytest
DATAFLOW_TEST_RAY=1 pytest tests/test_single_cluster_ray.py
```

可选集成测试只为测试基础设施创建一个 head 和一个 worker，然后对同一个集群连续运行流水线，
验证最终数据、连接归属和节点身份不变，不使用对象存储服务或 Kubernetes。
