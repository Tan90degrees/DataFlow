# ADR 0010：使用 PostgreSQL 领导权约束控制器协调

[English](../../adr/0010-controller-leader-election.md) | 简体中文

## 状态

已接受。

## 背景

DataFlow 将期望编排状态保存在 PostgreSQL 中，并通过长期运行控制器让该状态与 KubeRay 收敛。协调器具有幂等性：状态转换使用期望状态检查，RayJob 名称按运行、单元和尝试确定。这些属性使崩溃恢复安全，但不意味着应该执行主动/主动控制器。

运行多个无约束控制器会让所有副本扫描相同活跃运行和可恢复单元，竞争状态转换并对 Kubernetes 发出冗余读写。即使竞争最终正确收敛，也会放大负载并扩大运维故障面。

每个控制器部署已经需要 PostgreSQL。仅为控制器选主增加第二套共识或租约依赖，会使控制面复杂化，也更难在本地测试故障转移。

## 决策

长期运行控制器使用专用 PostgreSQL 会话咨询锁作为排他领导权原语。

该锁使用双整数 PostgreSQL 咨询锁命名空间。默认值为 `441001` 和 `1`，可通过 `DATAFLOW_CONTROLLER_LOCK_NAMESPACE` 与 `DATAFLOW_CONTROLLER_LOCK_KEY` 配置，以支持有意针对同一数据库运行独立控制器组的部署。

控制器副本行为如下：

1. 打开只用于领导权的专用自动提交 PostgreSQL 会话。
2. 尝试 `pg_try_advisory_lock(namespace, key)`。
3. 锁不可用时保持备用，不调用调度器或协调器。
4. 获取锁后将进程标记为领导者并运行正常持久协调轮次。
5. 每次后续轮次前验证领导会话仍存活。
6. 会话丢失时停止进入新轮次、释放本地领导状态并返回获取模式。
7. 优雅关闭时显式解锁并关闭领导会话；进程死亡或连接丢失也会自动释放 PostgreSQL 咨询锁。

`dataflow-controller --once` 是显式管理/调试操作，会绕过长期选主。运维人员不得把并发 `--once` 用作生产控制器组的替代循环。

## 约束边界

咨询锁会话约束进入协调轮次，但无法取消网络故障破坏领导会话时恰好已经执行的 Python 代码。因此在这个窄崩溃窗口中，DataFlow 保留两层已有保护：

- 持久状态转换使用期望状态检查，因此陈旧写入者会在竞争中失败，而不是静默覆盖新状态；
- 外部 RayJob 标识按运行/单元/尝试确定，因此重复提交收敛到同一 Kubernetes 对象。

检测到领导会话死亡的副本在重新获取锁前不会再执行协调轮次。

## 可观测性

领导权转换以结构化日志事件发出：

- `controller_standby`
- `controller_leadership_acquired`
- `controller_leadership_lost`
- `controller_leadership_released`
- `controller_leadership_unavailable`

Prometheus 注册表还提供进程本地 gauge `dataflow_controller_leader`：当选副本为 `1`，其他副本为 `0`。指标标签不加入控制器、运行、单元或尝试标识。

## 影响

- 可部署多个控制器副本进行故障转移，同时只有一个执行编排工作。
- PostgreSQL 观测旧会话关闭后，故障转移无需等待租约过期。
- 仅用 PostgreSQL 即可测试领导权，无需在线 Kubernetes 集群。
- PostgreSQL 同时明确成为工作流状态和控制器领导权的必需依赖，与已有控制面依赖模型一致。
- 这是单领导者 HA，不是分片主动/主动协调。按运行分区或多区域共识属于后续独立设计。
