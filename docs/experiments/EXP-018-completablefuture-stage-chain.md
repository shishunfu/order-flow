# EXP-018：CompletableFuture 阶段链式调用

## 1. 实验目标

掌握 CompletableFuture 的三个基础阶段方法，理解多个异步处理阶段之间的数据传递关系。

实验内容：

- `thenApply()`：接收上一步结果并转换。
- `thenAccept()`：接收上一步结果并消费。
- `thenRun()`：不接收上一步结果，只执行后续动作。
- 多阶段链式组合：将多个处理步骤组织成完整的数据处理链。

## 2. 实验环境

- JDK：25.0.2
- Maven：3.9.12
- 项目：OrderFlow
- 实验类：`CompletableFutureStageExperimentTest`

## 3. 实验结果

测试结果：

```text
Tests run: 4
Failures: 0
Errors: 0
Skipped: 0
```

### 场景 A：thenApply 结果转换

实测：

```text
thenApply input = Order-1001
thenApply result = 1001
```

结论：

- thenApply 能获取上一个阶段的结果。
- 可以将原始结果转换为新的业务结果。
- 返回新的 CompletableFuture，并保留转换后的结果。

### 场景 B：thenAccept 消费结果

实测：

```text
thenAccept input = Order-1002
consumed = Order-1002
thenAccept result = null
```

结论：

- thenAccept 能获取上一个阶段的结果。
- 适合日志记录、通知发送等消费型操作。
- 不产生新的业务返回值，返回类型为 `CompletableFuture<Void>`。

### 场景 C：thenRun 执行后续动作

实测：

```text
thenRun executed
executed = true
```

结论：

- thenRun 不接收上一个阶段的业务结果。
- 只在前一阶段正常完成后执行后续动作。
- 返回类型为 `CompletableFuture<Void>`。

### 场景 D：多个阶段组合

实测：

```text
Stage 1: create order
Stage 2: convert order
Stage 3: build message
Stage 4: consume message
Final notification = Order created, id = 1004
```

数据处理链：

```text
supplyAsync
    |
    | Order-1004
    v
thenApply
    |
    | 1004
    v
thenApply
    |
    | Order created, id = 1004
    v
thenAccept
    |
    v
消费通知消息
```

结论：

- 每个阶段可以依赖前一个阶段的结果。
- thenApply 可以连续转换业务数据。
- thenAccept 可以作为数据处理链的消费终点。
- 多阶段组合可以将业务处理拆分为多个职责清晰的步骤。

## 4. 三个方法对比

| 方法 | 是否接收上一步结果 | 是否产生新业务结果 | 返回类型 |
|---|---|---|---|
| thenApply | 是 | 是 | CompletableFuture<U> |
| thenAccept | 是 | 否 | CompletableFuture<Void> |
| thenRun | 否 | 否 | CompletableFuture<Void> |

## 5. 工程实践思考

在 OrderFlow 订单场景中，可以构建类似的处理链：

```java
CompletableFuture.supplyAsync(
        () -> loadOrder(orderId),
        orderExecutor
)
.thenApply(this::convertOrderDTO)
.thenApply(this::buildNotificationMessage)
.thenAccept(notificationService::send);
```

这里仅用于说明数据流，不代表通知发送已经具备可靠投递能力。

实际工程还需要考虑：

1. 异步阶段的异常传播和处理。
2. 是否为阻塞型任务指定独立 Executor。
3. 通知发送失败后的重试与补偿。
4. 订单事务提交与异步任务启动之间的一致性。
5. 关键事件是否需要消息队列或 Outbox 等可靠投递机制。

## 6. 实验注意事项

`thenApply()`、`thenAccept()`、`thenRun()` 是非 Async 版本。

后续阶段可能由完成前一阶段的线程执行，也可能在前一阶段已经完成时由调用者线程执行。

因此：

- 阶段的依赖顺序是确定的。
- 阶段的执行线程不应被假设为固定线程。
- 如果需要明确指定异步执行线程，可以使用对应的 `thenApplyAsync()`、`thenAcceptAsync()`、`thenRunAsync()`，并传入自定义 Executor。

另外，JUnit 测试方法之间的控制台输出顺序不代表测试方法的执行顺序。

## 7. 最终结论

EXP-018 全部通过。

已经掌握 CompletableFuture 基础阶段链的职责划分和结果传递方式，可以使用多个阶段组织简单的业务处理流程。

下一阶段建议学习异步任务之间的组合：`thenCombine()`、`allOf()` 和 `anyOf()`。
