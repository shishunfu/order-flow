# EXP-017：CompletableFuture 异步任务创建与执行基础

## 1. 实验目标

掌握 CompletableFuture 的基础异步任务创建方式，理解异步任务的执行线程、返回值以及自定义线程池的使用。

本次实验验证：

1. `supplyAsync()`：创建有返回值的异步任务。
2. `runAsync()`：创建无返回值的异步任务。
3. 自定义 Executor：指定异步任务执行线程池。
4. 异步任务状态：理解任务未完成和完成状态的变化。

## 2. 实验环境

- JDK：25.0.2
- Maven：3.9.12
- 项目：OrderFlow
- 测试框架：JUnit
- 实验类：`CompletableFutureBasicExperimentTest`

## 3. 实验结果

### 场景 A：supplyAsync()

实测结果：

```text
supplyAsync thread = ForkJoinPool.commonPool-worker-1
Caller thread = main
Result = 300
```

结论：

- `supplyAsync()` 创建有返回值的异步任务。
- 未指定 Executor 时，通常使用 `ForkJoinPool.commonPool()`。
- 异步任务可以在不同于调用线程的工作线程中执行。
- 任务结果通过 CompletableFuture 获取。

### 场景 B：runAsync()

实测结果：

```text
runAsync thread = ForkJoinPool.commonPool-worker-1
runAsync result = null
executed = true
```

结论：

- `runAsync()` 创建无返回值的异步任务。
- 适合只执行动作、不需要返回业务结果的场景。
- `CompletableFuture<Void>` 的正常完成结果为 `null`。

### 场景 C：自定义 Executor

实测结果：

```text
Custom executor thread = order-flow-worker
```

结论：

- 可以为 CompletableFuture 指定自定义 Executor。
- 自定义线程池可以控制线程命名、线程数量、队列容量和拒绝策略。
- 业务系统中应根据任务类型合理隔离线程池，避免公共线程池被阻塞型业务任务占用。

### 场景 D：异步任务状态

实测结果：

```text
Future isDone before release = false
Future result = Order-1001
Future isDone after release = true
```

结论：

- 异步任务尚未完成时，`isDone()` 返回 `false`。
- 任务正常完成后，`isDone()` 返回 `true`。
- `get()` 可以获取任务结果，但如果任务尚未完成，调用线程可能阻塞等待。
- 异步执行不等于调用线程永远不会阻塞。

## 4. 核心知识总结

| 方法 | 输入 | 返回结果 | 主要用途 |
|---|---|---|---|
| `supplyAsync()` | Supplier | 有业务结果 | 异步查询、计算 |
| `runAsync()` | Runnable | Void | 异步执行动作 |

常见调用形式：

```java
CompletableFuture.supplyAsync(() -> {
    return "Order-1001";
});

CompletableFuture.runAsync(() -> {
    System.out.println("异步执行");
});

CompletableFuture.supplyAsync(
    () -> "Order-1001",
    customExecutor
);
```

## 5. 工程实践结论

在实际订单系统中，可以将以下任务设计为异步任务：

- 订单创建后的通知发送。
- 订单事件发布。
- 非关键链路的数据统计。
- 订单操作审计日志。
- 订单状态变更后的后续处理。

但需要注意：

1. 不能仅因为使用 CompletableFuture 就认为任务可靠。
2. 不能默认所有任务都适合使用 `ForkJoinPool.commonPool()`。
3. 对数据库访问、远程调用等阻塞型任务，应评估并配置合适的独立线程池。
4. 订单核心事务不能依赖一个没有可靠投递保障的异步任务来完成关键业务。

## 6. 实验结论

本次实验全部通过：

```text
Tests run: 4
Failures: 0
Errors: 0
Skipped: 0
```

已经掌握 CompletableFuture 的异步任务创建、执行线程识别、自定义 Executor 和任务完成状态判断。

**下一阶段：学习 CompletableFuture 的任务编排与阶段链式调用。**
