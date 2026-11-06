# EXP-022：CompletableFuture 与自定义线程池隔离

## 1. 实验目标

验证 CompletableFuture 异步任务的执行器选择机制，以及自定义线程池在业务隔离中的作用。

重点验证：

1. 不指定 Executor 时默认使用哪个线程池。
2. thenApply 与 thenApplyAsync 的执行线程差异。
3. thenApplyAsync 如何指定自定义 Executor。
4. 不同业务使用独立线程池时是否能够隔离任务阻塞。

## 2. 实验环境

- JDK：OpenJDK 25.0.2
- Maven：3.9.12
- 测试框架：JUnit 5
- 默认异步执行器：ForkJoinPool.commonPool()
- 自定义执行器：Executors.newSingleThreadExecutor / newFixedThreadPool
- 实验方式：线程名记录、CountDownLatch 控制任务执行时机

## 3. 实验结果

### EXP-022-A：默认异步执行器

实际输出：

```text
A caller thread = main
A worker thread = ForkJoinPool.commonPool-worker-1
A result = ForkJoinPool.commonPool-worker-1
```

结论：

- 不指定 Executor 的 supplyAsync() 使用默认异步执行器。
- 本次运行使用 ForkJoinPool.commonPool()。
- 实际线程选择还可能受到 commonPool 配置和运行环境影响，不应将线程名称写死在业务逻辑中。

### EXP-022-B：thenApply 与 thenApplyAsync

实际输出：

```text
B supplyAsync thread = order-flow-worker
B thenApply thread = order-flow-worker
B thenApplyAsync thread = ForkJoinPool.commonPool-worker-1
```

结论：

- thenApply() 不主动切换到另一个异步执行器。
- thenApplyAsync() 未指定 Executor 时使用默认异步执行器。
- thenApplyAsync(..., executor) 可以指定执行器。
- 非 Async 阶段可能由前一阶段完成线程执行，也可能由注册后续阶段的调用线程执行，不能依赖固定线程归属。

### EXP-022-C：自定义 Executor

实际输出：

```text
C processing thread = order-detail-worker
C result = Order-1002-DETAIL
```

结论：

通过 thenApplyAsync(..., executor) 可以将异步阶段交给指定线程池执行。

### EXP-022-D：业务线程池隔离

实际输出：

```text
D product thread = product-query-worker
D product result = Product-2003
D order future done before release = false
D order result = Order-1003
```

结论：

- 订单查询任务被阻塞时，商品查询任务仍能独立完成。
- 两个任务使用不同的线程池，线程资源相互独立。
- 该实验验证了线程池层面的隔离能力。

## 4. 核心 API 对比

| API                             | 默认执行方式          | 是否能指定 Executor |
|---------------------------------|-----------------------|---------------------|
| supplyAsync(supplier)           | 默认异步执行器        | 否                  |
| supplyAsync(supplier, executor) | 指定执行器            | 是                  |
| thenApply(fn)                   | 非 Async 阶段执行规则 | 否                  |
| thenApplyAsync(fn)              | 默认异步执行器        | 否                  |
| thenApplyAsync(fn, executor)    | 指定执行器            | 是                  |

## 5. 生产环境设计启示

在 OrderFlow 中，可以按业务资源特征考虑执行器隔离：

| 业务类型     | 资源特征      | 设计考虑                           |
|--------------|---------------|------------------------------------|
| 订单查询     | 数据库访问    | 独立执行器、数据库连接池容量       |
| 商品查询     | HTTP 外部调用 | 独立执行器、连接超时、并发限制     |
| 库存查询     | 数据库访问    | 独立执行器、连接池和事务边界       |
| 订单详情组装 | CPU 计算      | 控制 CPU 并发，避免阻塞 I/O 执行器 |

线程池隔离不是越多越好。还需考虑：

- 线程总量和上下文切换成本。
- 下游数据库连接池容量。
- HTTP 客户端连接池容量。
- 队列容量与拒绝策略。
- 超时和取消机制。
- 线程池关闭与应用生命周期。

## 6. 实验验收

- [x] A：默认异步线程识别
- [x] B：thenApply 与 thenApplyAsync 区别
- [x] C：自定义 Executor 生效
- [x] D：业务线程池隔离验证
- [x] 4 个测试全部通过
- [x] 自定义线程池正常关闭

测试结果：

```text
Tests run: 4
Failures: 0
Errors: 0
Skipped: 0
BUILD SUCCESS
```

## 7. 最终结论

CompletableFuture 的异步执行器选择是系统资源隔离的重要组成部分。

生产代码不应无差别地将所有异步任务提交到 commonPool。

应根据任务的 CPU、I/O、下游依赖和资源限制设计执行器，并通过容量、拒绝策略、超时及监控形成完整的资源治理方案。
