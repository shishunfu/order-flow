# EXP-003：线程池运行指标监控实验

## 一、实验目标

在 OrderFlow 项目中使用 JDK `ThreadPoolExecutor` 原生 API 采集线程池运行指标，理解线程池运行状态、任务堆积、任务完成情况以及任务等待时间。

本实验不引入 Micrometer、Actuator 等监控组件，重点掌握线程池底层指标的含义。

## 二、实验环境

- JDK：OpenJDK 25.0.2
- Maven：3.9.12
- 测试框架：JUnit 5
- 线程池：ThreadPoolExecutor

实验代码：

`src/test/java/com/xst/orderflow/concurrency/ThreadPoolMetricsExperimentTest.java`

## 三、实验参数

| 参数 | 配置 |
|---|---:|
| corePoolSize | 2 |
| maximumPoolSize | 2 |
| 任务数量 | 10 |
| 队列类型 | ArrayBlockingQueue |
| 队列容量 | 8 |
| 拒绝策略 | AbortPolicy |
| 模拟任务执行时间 | 500ms |

实验通过 CountDownLatch 控制前两个工作线程，使线程池先进入稳定的任务堆积状态，再释放工作线程执行任务。

## 四、监控指标

| 指标 | 获取方式 | 说明 |
|---|---|---|
| PoolSize | getPoolSize() | 当前线程池线程数 |
| ActiveCount | getActiveCount() | 当前活跃线程数，近似值 |
| CorePoolSize | getCorePoolSize() | 核心线程数配置 |
| MaximumPoolSize | getMaximumPoolSize() | 最大线程数配置 |
| LargestPoolSize | getLargestPoolSize() | 历史最大线程数 |
| QueueSize | getQueue().size() | 当前队列任务数量 |
| QueueRemainingCapacity | getQueue().remainingCapacity() | 队列剩余容量 |
| TaskCount | getTaskCount() | 生命周期内任务数量的估算值 |
| CompletedTaskCount | getCompletedTaskCount() | 生命周期内已完成任务数量的估算值 |
| RejectedCount | 自定义 AtomicInteger | 本实验统计的拒绝任务数 |

## 五、实验结果

### 5.1 任务堆积时

```text
PoolSize             : 2
ActiveCount          : 2
CorePoolSize         : 2
MaximumPoolSize      : 2
LargestPoolSize      : 2
QueueSize            : 8
QueueRemainingCapacity: 0
TaskCount            : 10
CompletedTaskCount   : 0
RejectedCount        : 0
```

分析：

- 两个工作线程均已启动。
- 两个工作线程正在执行被闸门阻塞的任务。
- 另外八个任务进入等待队列。
- 队列剩余容量为 0，说明队列已经满。
- 尚未有任务完成。
- 没有发生任务拒绝。

此时线程池承载能力为：

`2 个执行中任务 + 8 个排队任务 = 10 个任务`

如果继续提交任务，将触发 AbortPolicy。

### 5.2 任务完成后

```text
PoolSize             : 0
ActiveCount          : 0
CorePoolSize         : 2
MaximumPoolSize      : 2
LargestPoolSize      : 2
QueueSize            : 0
QueueRemainingCapacity: 8
TaskCount            : 10
CompletedTaskCount   : 10
RejectedCount        : 0
```

分析：

- 所有任务执行完成。
- 任务队列已经清空。
- 队列恢复全部容量。
- 已完成任务数为 10。
- 当前线程数为 0，因为采样发生在关闭线程池并等待终止之后。
- 历史最大线程数仍然为 2。

`PoolSize=0` 与 `LargestPoolSize=2` 并不矛盾：

- PoolSize 反映当前线程数。
- LargestPoolSize 记录运行期间的历史最大线程数。

### 5.3 等待时间与执行时间

```text
任务等待时间：样本数=10，平均值=1002.30ms，最大值=2004ms
任务执行时间：样本数=10，平均值=500.00ms，最大值=500ms
```

等待时间是从任务提交到任务开始执行之间的时间。

执行时间是模拟业务代码实际运行的时间，不包括前两个任务等待实验闸门释放的时间。

两个工作线程并行处理任务，每个任务约耗时 500ms，因此后续任务的等待时间大致每两个任务增加 500ms。

理论平均等待时间约为 1000ms，与实际测量的 1002.30ms 接近。

## 六、核心结论

### 6.1 线程池运行状态不能只看线程数

PoolSize 和 ActiveCount 是不同的指标：

- PoolSize：当前线程池中存在多少工作线程。
- ActiveCount：当前有多少工作线程正在执行任务。

### 6.2 队列容量和剩余容量可以判断积压情况

QueueSize 与 QueueRemainingCapacity 应结合观察。

当 QueueRemainingCapacity 持续下降至 0 时，说明队列已饱和，需要关注任务拒绝风险。

### 6.3 当前状态和历史峰值必须区分

PoolSize 是当前值，LargestPoolSize 是历史峰值。

线程池关闭后，PoolSize 可以归零，但历史峰值仍然保留。

### 6.4 线程池需要关注任务完成情况

CompletedTaskCount 可以帮助判断任务处理进度。

TaskCount 和 CompletedTaskCount 都是线程池层面的估算统计，不应作为精确业务账本。

### 6.5 拒绝任务需要额外统计

ThreadPoolExecutor 没有提供直接的累计拒绝任务数。

本实验通过 AtomicInteger 记录拒绝次数。

实际工程中可以通过自定义 RejectedExecutionHandler 统一记录拒绝事件。

### 6.6 等待时间与执行时间需要分开监控

- 等待时间增长：可能意味着任务堆积或处理能力不足。
- 执行时间增长：可能意味着业务处理变慢。
- 两者同时增长：可能存在处理能力下降和任务堆积叠加的问题。

## 七、实验局限

1. 本实验是单机、单线程池的受控实验，不代表真实生产流量。
2. ActiveCount、TaskCount、CompletedTaskCount 等指标具有近似或瞬时统计特征。
3. 等待时间和执行时间使用简单模拟任务测量。
4. RejectedCount 只覆盖本实验自定义的拒绝统计逻辑。
5. 本实验没有接入指标采集平台、监控面板或告警系统。

## 八、后续工程思考

真实业务中可以进一步关注：

- 线程池活跃度及使用率。
- 队列长度及队列使用率。
- 任务拒绝次数。
- 任务等待时间 P95、P99。
- 任务执行时间 P95、P99。
- 线程池任务吞吐量。
- 长时间运行任务及异常任务。

注意：这些是后续工程监控方向，不是本实验已经实现的能力。

## 九、实验结论

本次实验成功验证了 ThreadPoolExecutor 的核心运行指标。

通过对任务堆积前后两个时点的采样，掌握了线程数、活跃线程数、队列容量、任务完成数、历史最大线程数等指标的含义。

核心认识：

**线程池监控不仅要关注线程是否繁忙，更要关注任务是否持续堆积、等待时间是否增长，以及任务是否被拒绝。**
