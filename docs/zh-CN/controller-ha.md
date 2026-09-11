# 控制器高可用

[English](../controller-ha.md) | 简体中文

DataFlow 控制器使用 PostgreSQL 咨询锁选主。可以让多个长期运行的 `dataflow-controller` 副本连接同一 DataFlow 数据库；其中恰好一个副本持有配置的领导锁并执行调度/协调，其他副本保持备用。

## 推荐部署

至少运行两个控制器副本以支持故障转移。每个副本必须使用相同的：

- `DATAFLOW_DATABASE_URL`
- `DATAFLOW_CONTROLLER_LOCK_NAMESPACE`
- `DATAFLOW_CONTROLLER_LOCK_KEY`
- 重试策略和外部基础设施配置

默认锁标识适用于每个数据库一个 DataFlow 控制器组：

```text
DATAFLOW_CONTROLLER_LOCK_NAMESPACE=441001
DATAFLOW_CONTROLLER_LOCK_KEY=1
```

如果多个有意独立的 DataFlow 控制器组共享一个 PostgreSQL 数据库，请为每组分配不同锁对。不要只修改同一 HA 组中的部分副本，否则会产生多个领导者。

## 轮询

`DATAFLOW_CONTROLLER_POLL_SECONDS` 控制当选领导者的协调频率，默认为 `2` 秒。

`DATAFLOW_CONTROLLER_STANDBY_POLL_SECONDS` 控制备用副本重试获取领导权的频率，默认与领导者轮询间隔相同。两个值都必须为正数。

较短的备用间隔可减少故障接管延迟，但会增加领导连接尝试。PostgreSQL 观测到领导会话关闭后立即释放咨询锁，因此不存在固定租约到期延迟。

## 故障行为

领导进程退出或其专用 PostgreSQL 领导会话丢失时，PostgreSQL 会释放咨询锁，备用副本随后可获取锁并从持久工作流状态恢复协调。

领导会话约束每次协调轮次的入口。连接故障无法中断恰好在该时刻已经执行的外部调用，因此在这个很窄的崩溃窗口中，DataFlow 继续依赖两项已有幂等保证：带期望状态的 PostgreSQL 转换，以及按运行/单元/尝试确定的 RayJob 名称。

获取领导权期间 PostgreSQL 暂时中断会让控制器保持备用，不会退回到无约束协调。

## 优雅关闭

正常关闭控制器会显式释放咨询锁。进程异常终止也会关闭 PostgreSQL 会话，由服务端释放锁。

`dataflow-controller --once` 有意绕过长期选主，用于调试和管理恢复。除非正在执行明确的运维干预并理解其中的协调竞争，否则不要针对在线 HA 控制器组并发运行 `--once`。

## 可观测性

结构化日志通过以下事件标识领导权转换：

```text
controller_standby
controller_leadership_acquired
controller_leadership_lost
controller_leadership_released
controller_leadership_unavailable
```

启用 Prometheus 指标后，每个长期控制器都会启动进程本地指标监听器。配置方式：

```text
DATAFLOW_CONTROLLER_METRICS_HOST=0.0.0.0
DATAFLOW_CONTROLLER_METRICS_LISTEN_PORT=9091
```

监听器使用与控制器协调和领导权检测相同的注册表提供 `/metrics`，其中包括：

```text
dataflow_controller_leader
```

当选进程报告 `1`，备用进程报告 `0`。领导权指标不包含运行、单元、尝试或作业标识。设置 `DATAFLOW_METRICS_ENABLED=false` 时，控制器不会启动指标监听器。
