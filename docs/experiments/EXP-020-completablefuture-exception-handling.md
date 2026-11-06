# EXP-020：CompletableFuture 异常处理与异常恢复

## 1. 实验目标

掌握 CompletableFuture 异常处理机制，理解异常传播、异常恢复和异常观察之间的区别。

实验内容：

- `exceptionally()`：异常发生时返回备用结果。
- `handle()`：统一处理正常结果和异常。
- `whenComplete()`：观察任务完成状态，记录结果和异常。
- `get()` 与 `join()`：理解异步异常的包装方式。

## 2. 实验环境

- JDK：25.0.2
- Maven：3.9.12
- 项目：OrderFlow
- 实验类：`CompletableFutureExceptionExperimentTest`

## 3. 实验结果

测试结果：

```text
Tests run: 5
Failures: 0
Errors: 0
Skipped: 0
```

### 场景 A：exceptionally 异常恢复

实测：

```text
exceptionally exception = java.util.concurrent.CompletionException:
java.lang.IllegalStateException: Order service unavailable

Recovered result = DEFAULT-ORDER
```

结论：

1. 上游任务发生异常。
2. exceptionally 接收到异常。
3. 返回备用结果 DEFAULT-ORDER。
4. 后续 Future 正常完成。
5. `isCompletedExceptionally()` 返回 false。

适用场景：

- 非核心数据查询失败后返回默认值。
- 可降级的数据查询。
- 非关键业务信息缺失时提供备用结果。

注意：不能通过默认值掩盖订单创建、库存扣减等核心业务失败。

### 场景 B：handle 成功和失败统一处理

实测：

```text
handle exception = java.util.concurrent.CompletionException:
java.lang.IllegalArgumentException: Invalid order

Success handle result = Order-1001-PROCESSED
Failure handle result = FALLBACK
```

结论：

- handle 无论上游成功还是失败都会执行。
- 成功时可以转换正常结果。
- 失败时可以根据异常返回备用结果。
- 适合统一结果转换和异常恢复。

### 场景 C：whenComplete 观察异常

实测：

```text
whenComplete result = null
whenComplete exception = java.util.concurrent.CompletionException:
java.lang.IllegalStateException: Database timeout

Final exception cause = java.lang.IllegalStateException:
Database timeout
```

结论：

- 上游成功时，result 有值，exception 为 null。
- 上游失败时，result 为 null，exception 有值。
- whenComplete 正常执行后，上游异常仍然传播。
- whenComplete 不负责异常恢复。

适用场景：

- 记录异步任务执行日志。
- 记录执行成功或失败状态。
- 记录异常信息。
- 统计任务执行结果。

### 场景 D：whenComplete 后异常继续传播

实测：

```text
Observed payment exception = java.util.concurrent.CompletionException:
java.lang.IllegalStateException: Payment failed
```

结论：

whenComplete 正常执行并不代表异常被处理。

如果上游任务异常，后续 Future 仍然异常完成。

因此不能将 whenComplete 当作异常恢复方法。

### 场景 E：get 与 join 异常包装

实测：

```text
join exception = java.util.concurrent.CompletionException:
java.lang.IllegalArgumentException: Original business error

join root cause = java.lang.IllegalArgumentException:
Original business error

get exception = java.util.concurrent.ExecutionException:
java.lang.IllegalArgumentException: Original business error

get root cause = java.lang.IllegalArgumentException:
Original business error
```

结论：

| 方法   | 外层异常            | 根因                     |
|--------|---------------------|--------------------------|
| get()  | ExecutionException  | IllegalArgumentException |
| join() | CompletionException | IllegalArgumentException |

两种方法都会等待异步任务完成。

区别：

- get() 抛出受检异常，需要处理 ExecutionException 和 InterruptedException。
- join() 抛出运行时异常 CompletionException，不需要声明受检异常。
- 需要分析真实业务错误时，应继续查看根因。

## 4. 三个异常处理方法对比

| 方法          | 成功时执行 | 失败时执行 | 恢复异常 |
|---------------|------------|------------|----------|
| exceptionally | 否         | 是         | 可以     |
| handle        | 是         | 是         | 可以     |
| whenComplete  | 是         | 是         | 不负责   |

## 5. 工程实践思考

以 OrderFlow 订单详情查询为例：

```java
CompletableFuture<Product> productFuture =
        CompletableFuture.supplyAsync(
                () -> productService.getProduct(productId),
                productExecutor
        ).exceptionally(ex -> {
            log.error("Product query failed", ex);
            return Product.defaultProduct();
        });
```

这里假设商品信息允许降级，才适合返回默认商品信息。

如果是库存扣减、订单创建等核心业务操作，不应该简单地使用 exceptionally 返回成功结果。

实际工程中需要区分：

1. 可降级异常。
2. 可重试异常。
3. 参数或业务规则异常。
4. 数据库连接和事务异常。
5. 不可恢复的系统异常。

异常处理策略应该由业务语义决定，而不是所有异常都返回默认值。

## 6. 核心知识总结

- exceptionally：失败时恢复。
- handle：成功和失败统一处理。
- whenComplete：观察和记录完成状态。
- get：通过 ExecutionException 包装执行异常。
- join：通过 CompletionException 包装执行异常。
- 异常恢复必须明确业务边界，不能掩盖核心业务失败。

## 7. 最终结论

EXP-020 全部通过。

已经掌握 CompletableFuture 的基础异常传播、异常观察和异常恢复机制。

下一阶段：EXP-021，CompletableFuture 超时控制与任务取消，重点掌握 orTimeout、completeOnTimeout 和取消机制。
