# EXP-005：线程池背压与系统过载实验

## 1. 实验目标

通过模拟生产者持续提交任务、线程池消费任务的过程，观察生产速率超过线程池处理能力时的系统行为。

重点比较：

- `AbortPolicy`
- `CallerRunsPolicy`

理解线程池队列饱和、任务拒绝、生产者阻塞和背压机制。

## 2. 实验环境与参数

| 参数 | 配置 |
|---|---:|
| CorePoolSize | 2 |
| MaximumPoolSize | 2 |
| QueueCapacity | 8 |
| TaskCount | 40 |
| TaskExecutionTime | 500ms |
| SubmitInterval | 50ms |
| 理论线程池处理能力 | 4 tasks/s |
| 生产者目标提交速率 | 约 20 tasks/s |

理论处理能力：

```text
2 / 0.5 = 4 tasks/s
```

生产者目标提交速率约为线程池处理能力的 5 倍，因此预期会出现队列饱和。

## 3. 实验结果

### 3.1 AbortPolicy

```text
Submitted          : 40
Accepted           : 16
Rejected           : 24
Executed           : 16
CallerRuns         : 0
MaxQueueSize       : 8
ExecutedTaskIds    : 16
PoolCompletedCount : 16
PoolTaskCount      : 16
ProducerDuration   : 1981.50 ms
TotalDuration      : 4062.45 ms
ActualThroughput   : 3.94 tasks/s
```

分析：

1. 线程池最多同时运行 2 个任务。
2. 队列最大容量为 8，队列达到上限后，新任务提交会触发拒绝策略。
3. AbortPolicy 抛出 `RejectedExecutionException`。
4. 本次 40 个任务中，16 个任务被接收并执行，24 个任务被拒绝。
5. 生产者不需要执行任务，因此提交过程较快。
6. 实际吞吐量约为 3.94 tasks/s，与理论处理能力 4 tasks/s 接近。

结论：

AbortPolicy 能够快速暴露系统过载，但如果上层没有重试、降级或其他补偿机制，任务可能丢失。

### 3.2 CallerRunsPolicy

```text
Submitted          : 40
Accepted           : 40
Rejected           : 0
Executed           : 40
CallerRuns         : 8
MaxQueueSize       : 8
ExecutedTaskIds    : 40
PoolCompletedCount : 32
PoolTaskCount      : 32
ProducerDuration   : 5982.49 ms
TotalDuration      : 8065.30 ms
ActualThroughput   : 4.96 tasks/s
```

分析：

1. 队列满时，CallerRunsPolicy 让提交任务的调用线程直接执行任务。
2. 本次有 8 个任务由生产者线程执行。
3. 生产者执行任务期间无法继续提交新任务。
4. 生产者提交耗时由 AbortPolicy 下的约 1.98 秒增加到约 5.98 秒。
5. 最终 40 个任务全部执行，没有任务被拒绝。
6. `PoolCompletedCount=32` 只统计线程池 worker 完成的任务，不包含调用者线程执行的 8 个任务。

因此：

```text
线程池 worker 执行：32
生产者线程执行：    8
实际完成任务：      40
```

结论：

CallerRunsPolicy 通过让生产者承担部分执行工作，降低上游提交速度，从而形成背压。

## 4. 两种策略对比

| 指标 | AbortPolicy | CallerRunsPolicy |
|---|---:|---:|
| 提交任务数 | 40 | 40 |
| 接收任务数 | 16 | 40 |
| 拒绝任务数 | 24 | 0 |
| 实际执行任务数 | 16 | 40 |
| 调用者执行数 | 0 | 8 |
| 最大队列长度 | 8 | 8 |
| 线程池完成数 | 16 | 32 |
| 生产者耗时 | 1981.50ms | 5982.49ms |
| 总耗时 | 4062.45ms | 8065.30ms |
| 实际执行吞吐量 | 3.94 tasks/s | 4.96 tasks/s |

## 5. 核心知识点

### 5.1 什么是背压（Backpressure）

背压是指当下游处理能力不足时，通过某种机制限制上游继续提交任务的速度。

本次实验中：

```text
AbortPolicy：

生产者快速提交
      |
      v
线程池队列满
      |
      v
拒绝新任务
      |
      v
生产者继续提交
```

```text
CallerRunsPolicy：

生产者提交任务
      |
      v
线程池队列满
      |
      v
生产者自己执行任务
      |
      v
生产者提交速度下降
      |
      v
线程池有机会继续消费队列
```

### 5.2 CallerRunsPolicy 不会扩展线程池

本实验配置：

```java
corePoolSize = 2;
maximumPoolSize = 2;
```

线程池 worker 数量仍然最多为 2。

CallerRunsPolicy 并没有增加线程池 worker，而是利用提交任务的调用线程执行任务。

因此，系统执行任务的线程可以包括：

- 线程池 worker 1
- 线程池 worker 2
- 生产者线程

### 5.3 为什么 PoolCompletedCount 不等于 Executed

`ThreadPoolExecutor.getCompletedTaskCount()` 统计的是线程池内部 worker 完成的任务。

CallerRunsPolicy 执行的任务不属于线程池 worker 的执行统计。

因此：

```text
PoolCompletedCount = 32
CallerRuns          = 8
Executed            = 40
```

不能只依赖线程池自身的完成任务数来衡量整个系统的任务执行量。

### 5.4 为什么队列容量不是越大越好

队列只能提供缓冲，不能提高线程池实际处理能力。

队列过大可能导致：

- 任务等待时间增加；
- 内存占用增加；
- 过载持续时间变长；
- 用户请求延迟升高。

因此，队列容量需要结合任务耗时、系统吞吐量、允许等待时间和内存预算进行设计。

## 6. 工程实践结论

| 业务场景 | 策略选择考虑 |
|---|---|
| 允许明确拒绝，上层可以重试或降级 | AbortPolicy |
| 可以让调用线程承担任务执行成本 | CallerRunsPolicy |
| 任务不能丢失 | 需要可靠任务存储、重试和补偿机制 |
| 对请求响应时间敏感 | 谨慎使用 CallerRunsPolicy |
| 消息消费需要控制消费速度 | 可以评估背压机制与消费端限流 |

注意：CallerRunsPolicy 在执行器关闭后不会执行被拒绝的任务，因此不能将其当作任务可靠性保证。

## 7. 实验结论

本次实验验证了：

1. 当生产速率明显超过消费能力时，线程池队列会饱和。
2. AbortPolicy 会通过拒绝任务快速暴露过载。
3. CallerRunsPolicy 会让生产者参与任务执行，从而降低生产速度。
4. CallerRunsPolicy 可以在本实验中避免任务拒绝，但会增加生产者耗时。
5. 线程池自身的完成任务指标不能覆盖 CallerRuns 执行的任务。
6. 背压是一种过载控制机制，不等于提高线程池 worker 的处理能力。

**最终认知：线程池设计不能只关注线程数量和队列容量，还必须考虑任务拒绝策略、上游生产速度、任务可靠性以及系统过载时的行为。**

## 8. 后续可观测性改进

后续可以考虑增加：

- 任务提交总数；
- 任务执行成功数；
- 任务执行失败数；
- 任务拒绝数；
- 调用者执行数；
- 队列当前长度及峰值；
- 任务平均等待时间；
- 任务最大等待时间；
- 任务执行耗时分布。

需要注意区分线程池内部指标与业务层指标，避免漏算调用者线程执行的任务。

## 9. 实验状态

- [x] AbortPolicy 过载实验
- [x] CallerRunsPolicy 背压实验
- [x] 对比生产者耗时
- [x] 对比任务执行数量
- [x] 分析线程池指标差异
- [x] 总结工程应用场景
