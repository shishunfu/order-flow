# EXP-004：线程池拒绝策略实验

## 一、实验目标

通过 ThreadPoolExecutor 对比 JDK 提供的四种拒绝策略，理解线程池达到承载上限后的行为。

重点观察：

1. 不同拒绝策略下，哪些任务会执行。
2. 哪些任务会被拒绝或丢弃。
3. CallerRunsPolicy 如何将执行压力传递给提交线程。
4. 线程池任务统计与实际业务执行数量之间的区别。

## 二、实验环境

- JDK：OpenJDK 25.0.2
- Maven：3.9.12
- 测试框架：JUnit 5
- 线程池：ThreadPoolExecutor

实验代码：

`src/test/java/com/xst/orderflow/concurrency/ThreadPoolRejectPolicyExperimentTest.java`

## 三、实验参数

| 参数 | 设置 |
|---|---:|
| corePoolSize | 2 |
| maximumPoolSize | 2 |
| 队列类型 | ArrayBlockingQueue |
| 队列容量 | 2 |
| 任务总数 | 8 |
| 模拟任务执行时间 | 500ms |
| 拒绝策略 | 四种策略分别测试 |

线程池最大承载任务数：

`2 个执行中任务 + 2 个排队任务 = 4 个任务`

因此，提交 8 个任务时必然触发拒绝策略。

## 四、实验结果

### 4.1 AbortPolicy

```text
ExecutedTasks      : [1, 2, 3, 4]
RejectedTasks      : [5, 6, 7, 8]
NotExecutedTasks   : [5, 6, 7, 8]
CallerRunsCount    : 0
PoolCompletedCount : 4
PoolTaskCount      : 4
```

结论：

- 前四个任务被线程池接受。
- 后四个任务提交时抛出 RejectedExecutionException。
- 调用方可以明确感知任务提交失败。

适合不能静默丢失任务、要求调用方明确处理失败的场景。

### 4.2 CallerRunsPolicy

```text
ExecutedTasks      : [1, 2, 3, 4, 5, 6, 7, 8]
NotExecutedTasks   : []
CallerRunsCount    : 4
PoolCompletedCount : 4
PoolTaskCount      : 4
```

执行线程：

- 任务 1、2、3、4：线程池工作线程。
- 任务 5、6、7、8：main 提交线程。

结论：

- 八个任务全部执行。
- 后四个任务由提交线程直接执行。
- 提交线程执行任务期间无法继续提交后续任务。
- 线程池自身的 CompletedTaskCount 只有 4，因为另外四个任务不由线程池工作线程执行。

CallerRunsPolicy 可以形成简单的背压机制，但会占用提交线程，可能增加上游请求延迟。

### 4.3 DiscardPolicy

```text
ExecutedTasks      : [1, 2, 3, 4]
NotExecutedTasks   : [5, 6, 7, 8]
CallerRunsCount    : 0
PoolCompletedCount : 4
PoolTaskCount      : 4
```

结论：

- 前四个任务正常执行。
- 后四个任务被直接丢弃。
- 不抛出异常。
- 调用方无法通过异常感知任务丢失。

适用于允许丢弃的非关键任务，但不适合要求任务可靠执行的核心业务。

### 4.4 DiscardOldestPolicy

```text
ExecutedTasks      : [1, 2, 7, 8]
NotExecutedTasks   : [3, 4, 5, 6]
CallerRunsCount    : 0
PoolCompletedCount : 4
PoolTaskCount      : 4
```

结论：

- 任务 1、2 在工作线程执行。
- 任务 3、4 首先进入队列。
- 提交任务 5 时，任务 3 被移除。
- 提交任务 6 时，任务 4 被移除。
- 提交任务 7 时，任务 5 被移除。
- 提交任务 8 时，任务 6 被移除。
- 最终任务 7、8 被执行。

该策略倾向保留较新的排队任务，适用于新任务比旧任务更有价值的特定场景。

## 五、四种拒绝策略对比

| 策略 | 异常 | 执行任务 | 未执行任务 | 主要行为 |
|---|---|---|---|---|
| AbortPolicy | 抛异常 | 1～4 | 5～8 | 快速失败 |
| CallerRunsPolicy | 不抛异常 | 1～8 | 无 | 调用线程执行 |
| DiscardPolicy | 不抛异常 | 1～4 | 5～8 | 丢弃新任务 |
| DiscardOldestPolicy | 不抛异常 | 1、2、7、8 | 3～6 | 丢弃最旧排队任务 |

## 六、重要发现

### 6.1 拒绝不一定意味着抛出异常

AbortPolicy 会抛出 RejectedExecutionException。

CallerRunsPolicy、DiscardPolicy、DiscardOldestPolicy 在本实验场景下均不会抛出异常。

因此，仅通过捕获 RejectedExecutionException 统计拒绝任务是不完整的。

### 6.2 CallerRunsPolicy 会影响线程池监控指标

CallerRunsPolicy 下：

- 实际执行任务数：8。
- 线程池 CompletedTaskCount：4。
- 提交线程执行任务数：4。

线程池自身的完成任务统计不包含调用线程执行的任务。

因此，业务系统需要独立统计实际任务执行数量，不能直接将线程池的完成任务数作为业务吞吐量。

### 6.3 DiscardOldestPolicy 会丢弃已经排队的旧任务

它并不是简单丢弃刚提交的新任务。

在本实验中，最终任务 3、4、5、6 被移除，任务 7、8 得以执行。

如果任务具有严格时序、不能丢失或需要完整审计，该策略可能造成业务问题。

### 6.4 getTaskCount 不是提交 API 调用次数

本实验中四种策略的 PoolTaskCount 都是 4。

不能把 getTaskCount 直接解释为所有尝试提交的任务数量。

需要分别统计：

- 提交尝试数
- 线程池接受数
- 拒绝数
- 实际执行数
- 丢弃数
- 完成数

## 七、工程应用思考

| 业务场景 | 可考虑的策略 | 注意事项 |
|---|---|---|
| 核心订单处理 | AbortPolicy | 需要失败反馈、重试或持久化补偿 |
| 可降速的后台任务 | CallerRunsPolicy | 评估提交线程是否允许执行耗时任务 |
| 非关键实时刷新 | DiscardPolicy | 必须明确允许丢弃 |
| 只关心最新状态的任务 | DiscardOldestPolicy | 必须确认旧任务可以被安全淘汰 |

拒绝策略不是单纯的线程池配置问题，而是系统过载时的业务取舍。

## 八、实验局限

1. 本实验采用固定任务数和人工阻塞，不代表真实线上流量。
2. NotExecutedTasks 是通过实际执行集合反推的，不是 JDK 提供的丢弃事件统计。
3. RejectedTasks 仅统计捕获到 RejectedExecutionException 的任务。
4. 本实验没有实现持久化任务补偿、重试或死信队列。
5. 本实验未涉及线程池关闭期间的拒绝行为。

## 九、实验结论

四种拒绝策略代表四种不同的过载处理方式：

- AbortPolicy：拒绝并通知调用方。
- CallerRunsPolicy：让调用方承担执行压力。
- DiscardPolicy：丢弃新任务。
- DiscardOldestPolicy：淘汰旧排队任务。

核心认识：

**线程池拒绝策略本质上是系统过载时的流量治理和业务取舍机制。**

选择拒绝策略之前，必须先明确任务是否允许丢失、调用方是否能感知失败，以及上游是否能够承受反向压力。
