# DataFlow 文档

[English](../README.md) | 简体中文

DataFlow 是在一个已有 Ray 集群中执行声明式数据流水线的小型 Python 组件，无需单独部署服务或数据库。

- [快速上手](getting-started.md)：安装、准备输入、连接集群并运行。
- [执行架构](single-cluster.md)：流式执行、分支、控制依赖和限制。
- [SDK 与 JSON 参考](sdk.md)：定义流水线、算子资源、配置与返回值。
- [部署与运维](operations.md)：共享 worker、文件系统、内存、重试和定位。
- [开发与验证](development.md)：测试、打包、容器与 CI。
- [迁移到 0.2](migration.md)：已删除的接口及替代命令。
- [路线图](roadmap.md)：当前能力与待完成的验收。
- [性能实验](performance.md)：可复现并发 DAG 基准与测量边界。
