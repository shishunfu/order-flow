# EXP-023：CompletableFuture 异步编排中的线程池拒绝处理

## 1. 实验目标

验证 CompletableFuture 异步任务提交时发生线程池拒绝的行为，以及拒绝对多任务组合的影响。

重点验证：

1. 线程池容量耗尽时，supplyAsync 的提交行为。
2. 异步任务提交被拒绝后，组合 Future 的异常传播。
3. 将提交拒绝异常转换为异常完成的 Future。
4. 使用 exceptionally 对允许降级的业务任务提供备用结果。

## 2. 实验环境

- JDK：OpenJDK 25.0.2
- Maven：3.9.12
- 测试框架：JUnit 5
- 线程池：ThreadPoolExecutor
- CorePoolSize：1
- MaximumPoolSize：1
- QueueCapacity：1
- 拒绝策略：AbortPolicy

## 3. 实验结果

### EXP-023-A：线程池任务提交拒绝

实际输出：

```text
A active = 1
A queue size = 1
A rejected count = 1
```

结论：

- Worker 已经执行一个任务。
- 队列已经存放一个任务。
- 第三个任务提交时触发 RejectedExecutionException。
- AbortPolicy 将拒绝异常直接抛给提交方。

重要认知：

CompletableFuture.supplyAsync(supplier, executor) 在提交阶段发生拒绝时，异常可能直接从调用处抛出，调用方不一定能拿到 Future。

### EXP-023-B：多任务组合中的拒绝异常

实际输出：

```text
B order done = false
B product done = false
B inventory exceptional = true
B detail cause = java.util.concurrent.RejectedExecutionException
```

结论：

- 订单和商品任务已成功提交，但尚未完成。
- 库存任务提交被拒绝。
- 使用 failedFuture 将拒绝异常转换成异常完成的 Future。
- 该 Future 可以参与 thenCombine 组合。
- 库存 Future 异常最终传播到订单详情组合 Future。

### EXP-023-C：拒绝异常降级

实际输出：

```text
C fallback cause = java.util.concurrent.RejectedExecutionException
C detail = ORDER-1002 + PRODUCT-2002 + INVENTORY-DEFAULT
C fallback count = 1
```

结论：

- 库存任务提交失败后，进入 exceptionally。
- 降级逻辑返回 INVENTORY-DEFAULT。
- 订单与商品任务正常完成。
- 最终订单详情成功组装。
- 降级回调只执行一次。

## 4. 核心技术结论

### 4.1 任务提交失败与任务执行失败不同

任务提交失败：

```java
CompletableFuture.supplyAsync(supplier, executor);
```

如果 executor 拒绝任务，RejectedExecutionException 可能直接从该调用抛出。

任务执行失败：

```java
CompletableFuture.supplyAsync(() -> {
    throw new IllegalStateException("service failure");
}, executor);
```

异常会反映在返回的 Future 中，可以使用 exceptionally、handle 等阶段处理。

### 4.2 统一提交异常与执行异常的表达方式

可以将提交拒绝转换为异常完成的 Future：

```java
try {
    return CompletableFuture.supplyAsync(supplier, executor);
} catch (RejectedExecutionException e) {
    return CompletableFuture.failedFuture(e);
}
```

这样调用方可以统一通过 Future 组合和异常处理机制管理失败。

### 4.3 业务降级必须符合业务语义

并非所有异常都适合返回默认值。

例如：

- 商品展示信息缺失：可能允许展示降级信息。
- 库存真实数量缺失：不能直接使用默认库存继续下单。
- 支付结果未知：不能简单降级为支付成功或失败。
- 订单核心数据查询失败：通常应让业务请求明确失败。

降级必须建立在业务允许的前提下。

## 5. 生产环境设计启示

异步业务编排需要考虑：

1. 线程池容量与拒绝策略。
2. 提交阶段异常捕获。
3. 执行阶段异常传播。
4. 关键依赖与可降级依赖的区分。
5. 线程池拒绝次数和业务失败次数的监控。
6. 是否允许重试以及重试后的资源压力。

拒绝任务不能简单地无限重试，否则可能进一步加重系统过载。

## 6. 实验验收

- [x] A：线程池容量耗尽与拒绝验证
- [x] B：组合 Future 异常传播验证
- [x] C：拒绝异常降级验证
- [x] 3 个测试全部通过
- [x] 自定义线程池正常关闭

测试结果：

```text
Tests run: 3
Failures: 0
Errors: 0
Skipped: 0
BUILD SUCCESS
```

## 7. 最终结论

CompletableFuture 的异常处理不仅要考虑任务执行期间发生的异常，还必须考虑任务提交阶段的拒绝异常。

对于多任务业务编排，需要明确区分核心依赖和可降级依赖，避免线程池过载时将局部故障扩大为整个业务系统的故障。
