# ADR 0006：HTTP API 写入持久期望状态

[English](../../adr/0006-api-desired-state-boundary.md) | 简体中文

## 状态

已接受。

## 背景

DataFlow 已具备持久 PostgreSQL 状态机、依赖调度器、幂等协调器、KubeRay 执行和持久制品发布。下一个产品接口需要提供流水线、版本和运行生命周期的 HTTP API。

一种看似直接的实现是让 HTTP 处理器直接提交或取消 RayJob。但这会让 API 可用性与请求延迟耦合到 Kubernetes/KubeRay 可用性，重复协调器行为，并在外部副作用和持久工作流状态之间增加崩溃窗口。

## 决策

HTTP API 是持久编排状态之上的轻量控制面适配器。

- 创建流水线只持久化元数据。
- 创建流水线版本时先验证 `PipelineSpec`，执行编译器预检，再持久化。
- 创建流水线运行时，加载不可变版本、编译 `ExecutionGraph`、持久化图，让运行经过 `CREATED -> QUEUED -> PLANNING -> RUNNING`，然后执行一次调度器就绪判断。
- API 不创建 RayJob。
- 取消通过已有调度器状态机记录持久取消意图；独立运行的控制器/协调器观察已取消运行并停止活跃外部作业。
- 运行读取公开持久单元、尝试、事件和制品，绝不公开 Ray `ObjectRef` 或进程本地数据集标识。
- `/readyz` 检查 PostgreSQL，因为它是 API 必需的事实来源；Kubernetes 健康不属于 API 就绪条件。

服务层位于传输与仓储之间，未来 Python SDK、CLI 或其他传输可以复用相同用例而不重复状态转换。

## 影响

### 正面影响

- Kubernetes 中断不会阻止流水线/版本元数据操作或持久取消请求。
- HTTP 重试不会引入第二条 RayJob 提交路径。
- 控制器仍是唯一负责收敛外部执行状态的组件。
- API 测试可使用真实 PostgreSQL 状态，而无需 Kubernetes 集群。
- SDK 可以面向稳定 HTTP/用例边界，而不是内部仓储。

### 权衡

- 对已运行 RayJob 的取消相对于 HTTP 响应是异步的。运行先持久变为 `CANCELLED`，控制器在下次协调时停止外部作业。
- 创建运行目前在持久化前执行编译器验证，并在创建运行后立即持久化执行图。数据库操作失败仍可能留下需要恢复或诊断的非终态运行；以后可以用事务性 `create_compiled_run` 收紧边界。
- 认证、RBAC、租户授权和限流有意延期。

## 被拒绝方案

### 在 API 处理器中提交/取消 KubeRay 资源

拒绝原因：重复协调器职责，并使 API 成功依赖外部控制面可用性。

### 在 API 进程中运行控制器循环

V1 拒绝。API 服务与协调具有不同扩缩容和故障特征，应保持独立部署。

### 持久化序列化 Python DAG 对象

拒绝。API 版本持久化规范 `PipelineSpec` JSON，让版本保持可检查、可比较且与编译器兼容。
