# 调度准入控制

[English](../admission-control.md) | 简体中文

DataFlow 将 DAG 就绪与执行准入分离。调度器判断 ExecutionUnit 何时进入 `READY`；准入控制器判断该就绪单元能否占有执行槽并交给 Ray/KubeRay 协调器。

准入状态存储在 PostgreSQL 的 `execution_admissions` 中，因此由控制器副本共享并能跨控制器重启保留。除了控制器长期 HA 领导锁之外，准入决策还通过 PostgreSQL 事务级咨询锁串行化。第二把锁可防止故障转移的短暂重叠窗口过量准入工作。

## 配置

控制器读取四个与准入和调度器扇出相关的环境变量：

- `DATAFLOW_ADMISSION_GLOBAL_LIMIT`：所有 ClusterProfile 上准入单元的最大总数。省略表示无全局容量上限。
- `DATAFLOW_ADMISSION_PROFILE_LIMITS`：ClusterProfile 名称到最大准入单元数的 JSON 对象，例如 `{"cpu": 20, "gpu": 4}`。对象中未出现的配置没有单独上限。
- `DATAFLOW_ADMISSION_MAX_NEW_PER_PASS`：一次控制器协调轮次允许的新准入数。即使总容量不受限，默认仍为 `32`。
- `DATAFLOW_CONTROLLER_MAX_RUNS_PER_PASS`：一次控制器轮次交给调度器协调的最大活跃 PipelineRun 数，默认为 `128`。

准入限制为 `0` 会有意阻止该作用域的新准入，但保留已经运行或恢复的工作。`DATAFLOW_CONTROLLER_MAX_RUNS_PER_PASS` 必须为正数。

## 有界调度工作

控制器把运行数量限制应用为稳定活跃运行顺序上的轮转窗口。活跃运行多于单轮容量时，下一轮会从下一个运行继续，而不是重复选择前 N 个。这样既限制调度器 CPU/数据库工作，又避免顺序靠前的长生命周期运行使后续运行饥饿。

协调后的调度器轮次使用同一选择窗口，因此每一控制器轮次对每个选中运行最多执行两次调度器协调。准入公平性独立存放并持久化在 PostgreSQL；调度器游标只决定本轮哪些运行推进 DAG 状态。

## 槽位生命周期

准入属于逻辑 ExecutionUnit，而不是单次 RayJob 尝试。当单元为 `READY`、`SUBMITTING`、`RUNNING`、`UNKNOWN` 或 `RETRY_WAIT` 时保留槽位；进入终态或离开保留状态集合后释放。

重试退避期间保留槽位是有意设计：一次重试不会获得第二个槽位，控制器重启也不会重复计算同一逻辑单元。该选择优先保证控制面稳定，而不是最大化重试退避期间的利用率。未来如工作负载需要，可把重试让出槽位设为可配置策略。

启动或协调时，如果 `SUBMITTING`、`RUNNING`、`UNKNOWN` 或 `RETRY_WAIT` 单元缺少准入记录，会将其恢复为已准入。恢复工作可能导致观测到的活跃数暂时高于新降低的限制；DataFlow 不会终止已有工作，但在容量回落至上限以下之前不会做出新准入决策。

## 公平性

每次准入都会获得单调递增的持久序号。多个运行均有合格工作时，DataFlow 选择最近一次准入序号最旧的运行，并以稳定创建时间和标识符打破平局。运行内部优先选择最早排队的单元。

这种“最近最少准入”策略无需进程本地游标即可产生确定的类轮询进展。控制器重启后读取相同序号历史，继续相同公平顺序。

ClusterProfile 已满的候选项会被跳过，因此一个饱和配置不会队首阻塞其他配置中的合格工作。

## 可观测性

准入转换向现有运行事件流追加持久事件：

- `ADMISSION_QUEUED`
- `ADMISSION_ADMITTED`
- `ADMISSION_RECOVERED`
- `ADMISSION_RELEASED`

每轮控制器还会发出 `admission_reconciled` 结构化日志，其中包含活跃槽位数、每配置计数、新准入、排队/节流工作、恢复和释放槽位以及全局/单轮限制。控制器协调日志同时包含活跃运行总数和调度窗口选中的运行数。
