# 性能实验

[English](../performance.md) | 简体中文

## 改动

就绪节点与独立 sink 现在并发执行，PipelineRunner 使用固定全局动作池，DAG 准入与输入消费均有界。
线程等待完成通知，不增加空闲轮询，也不在 runner 中为每个文件创建线程池。

共享数据的默认策略已变化：分支保留惰性，允许重算；`.cache()` 用整份数据屏障和对象存储占用换取前缀复用。
这是显式策略，不是自适应算法或流式广播。union 可能探测 schema，按键 join 走 Ray shuffle/物化路径，
测量时需把这些成本纳入。

## 在已有集群复现

使用 Linux，driver/worker 统一 Ray 2.58.0，driver 中有仓库检出，worker 预装更新后的 DataFlow。
基准 UDF 位于 dataflow.callables，对照期间不要混入无关负载。

```bash
pip install -e '.[runtime]'
python -m benchmarks.dag_throughput --address auto --dags 8 --rows 2048 \
  --batch-size 128 --payload-bytes 512 --delay-seconds 0.01 \
  --output-json test-artifacts/dag-benchmark.json
```

默认临时工作目录只适合同机 head/worker 测试。物理多节点使用共享挂载，并传入
`--work-dir /shared/unique-benchmark-run`。每次使用新目录，最终 Parquet 默认追加写入。
DataFlow 不增加中间存储文件或 S3，CLI 要求集群已经启动。

正式测量前先预热，四组使用同一负载与 worker 池：

| 场景 | 活动 DAG 上限 | 节点动作上限 | 公共前缀 |
| --- | --- | --- | --- |
| serial-cached | 1 | 1 | 缓存 |
| parallel-sinks-cached | 1 | 4 | 缓存 |
| parallel-dags-cached | 4 | 4 | 缓存 |
| parallel-dags-recompute | 4 | 4 | 消费时允许重算 |

每个 DAG 有两个输出分支，UDF 短暂 sleep 模拟 IO，不代表 OCR、模型或 CPU 负载。
分别比较 sink 并发与多 DAG 并发；缓存/重算对照展示策略取舍，不证明某个默认值总是更快。

报告记录墙钟耗时、DAG/s、输入行/s、观测到的单 DAG P50/P95、driver RSS 快照、
worker 进程 RSS 高水位和验证后的输出行数。延迟从有界输入提供规格时开始，含准入等待，
不含 runner/连接构造；输入行吞吐不把两个分支输出重复计数。

内存指标有限制：worker RSS 可能包含该进程此前任务，不是整集群 RSS；driver 快照不是内存峰值。
这里没有测量对象存储/spill 峰值，小型 CI 样本也不能证明生产 P95 或持续吞吐。
真实 OCR/页/BOX 负载需单独测量 worker 堆、对象存储与 spill 峰值。
基准只断言正确性，不预设加速比例。

## CI 与验收

真实 head/worker 测试使用四个 DAG 和较小数据运行同一基准，上传 `dag-benchmark` 工作流产物中的
`dag-benchmark.json`。单元测试使用 gate/event 验证 sink 重叠、共享全局上限、物理队列背压、
取消、失败等待清理和输入消费上限，不用紧凑计时阈值伪造加速结论。

生产验收还需长时间混合负载、真实模型初始化、actor 资源预留、内存/spill 压力、文件倾斜、
worker 丢失与宿主恢复。上限只作用于当前 runner，不是所有集群用户共享的资源或公平性策略。
