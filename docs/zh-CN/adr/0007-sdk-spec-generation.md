# ADR 0007：Python SDK 生成规格而非分布式工作

[English](../../adr/0007-sdk-spec-generation.md) | 简体中文

## 状态

已接受。

## 背景

DataFlow 已公开持久 HTTP 控制面，需要 Python 优先的编写体验。Python SDK 可以在建图时执行用户代码并持久化任意 Python 对象，也可以生成编译器和 API 已消费的同一个显式 `PipelineSpec`。

持久化闭包、lambda、pickle 函数或进程本地对象会让版本难以检查、比较、迁移、保护和复现，也会在控制面之外引入第二条执行路径。

## 决策

SDK 是确定性 `PipelineSpec` 生成器加 HTTP API 客户端。

- `@pipeline` 创建可复用流水线定义。
- 每次调用 `spec()` 都会创建新的 `PipelineBuilder`，并在本地调用用户建图函数。
- Builder 操作只追加类型化节点和边，绝不初始化 Ray 或执行数据集。
- 用户函数和可调用类只以完全限定导入路径持久化。
- Lambda、局部/嵌套函数、`__main__` 符号和任意可调用实例无法由当前运行时解析器可复现导入，因此会被拒绝。
- 支持显式节点 ID；省略时按算子类型和插入顺序确定性生成。
- `checkpoint()` 和 `hard_boundary()` 标注下一条数据边，而不是在编写时物化数据。
- 生成规格使用核心 `PipelineSpec`、`ResourceSpec` 和 `RuntimeSpec` 模型，以及与控制面相同的规范哈希逻辑。
- `DataFlowClient.submit()` 通过公开 HTTP API 持久化版本并创建运行，不会绕过 API 直接访问 PostgreSQL、Ray 或 Kubernetes。

## 影响

### 正面影响

- Python 和 JSON 编写的流水线具有相同编译器语义。
- 流水线版本保持人类可读且可比较。
- 相同输入重复建图产生稳定序列化和哈希。
- 用户代码依赖明确：被引用符号必须存在于不可变运行时镜像。
- SDK 测试无需启动 Ray 即可验证编译。

### 权衡

- 任意 Notebook 闭包和 lambda 不能直接提交。
- 生产执行要求用户把顶层函数/类打包到可导入模块。
- 当前运行时解析器在模块导入后只支持一个顶层符号，因此有意拒绝嵌套类方法或嵌套限定符号。
- SDK 尚不上传源代码或构建运行时镜像。

## 被拒绝方案

### 用 pickle 或 cloudpickle 保存 Python DAG

拒绝原因：将持久元数据耦合到 Python 解释器状态，削弱可复现性、可检查性、可迁移性和安全性。

### 编写期间执行 Ray Data 调用

拒绝原因：混淆控制面建图与执行，并创建第二条调度路径。

### SDK 直接写 PostgreSQL

拒绝原因：HTTP API 拥有公开生命周期边界以及未来认证/授权策略。
