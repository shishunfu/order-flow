# EXP-019：CompletableFuture 多任务组合与结果汇总

## 1. 实验目标

掌握多个 CompletableFuture 之间的组合方式，理解并行执行、结果汇总和任意任务完成机制。

实验内容：

- `thenCombine()`：组合两个独立异步任务的结果。
- `allOf()`：等待多个异步任务全部完成。
- `anyOf()`：任意一个异步任务完成时触发后续处理。
- 使用 `allOf()` 聚合订单详情。

## 2. 实验环境

- JDK：25.0.2
- Maven：3.9.12
- 项目：OrderFlow
- 实验类：`CompletableFutureCombineExperimentTest`

## 3. 实验结果

测试结果：

```text
Tests run: 4
Failures: 0
Errors: 0
Skipped: 0
Time elapsed: 0.863 s
```

### 场景 A：thenCombine 并行执行与结果合并

实测：

```text
Order query finished
Product query finished
Combining results
Combined result = Order-1001 + Product-2001
Parallel duration = 302ms
```

实验参数：

- 订单查询模拟耗时：300ms。
- 商品查询模拟耗时：300ms。

实验结论：

- 两个独立任务并行执行。
- 两个任务完成后，thenCombine 执行结果合并。
- 总耗时约302ms，明显低于串行执行预期的约600ms。
- thenCombine 适合两个独立任务完成后需要同时使用两个结果的场景。

### 场景 B：allOf 等待全部任务

实测：

```text
Order task completed
Product task completed
Customer task completed
All tasks completed
Completed count = 3
Order = Order-1002
Product = Product-2002
Customer = Customer-3002
```

实验参数：

| 任务 | 模拟耗时 |
|---|---:|
| 订单查询 | 200ms |
| 商品查询 | 300ms |
| 客户查询 | 400ms |

实验结论：

- allOf 等待所有输入任务完成。
- 所有任务完成后，后续阶段才执行。
- allOf 返回 `CompletableFuture<Void>`，不直接提供结果集合。
- 需要保留各个原始 Future，再分别获取结果。
- 如果 allOf 中有任务异常完成，组合 Future 也会异常完成；但其他任务不会因此自动取消。

### 场景 C：anyOf 任意任务完成

实测：

```text
Fast task completed
Any result = Fast-Result
Slow task completed
```

实验参数：

- 快速任务：100ms。
- 慢速任务：500ms。

实验结论：

- anyOf 在任意一个输入 Future 完成时触发。
- 返回第一个完成任务的结果，类型为 Object。
- 第一个完成也可能是异常完成，而不一定是正常结果。
- anyOf 不会自动取消其他未完成任务。
- 本次实验中，快速任务返回结果后，慢速任务仍然继续执行。

### 场景 D：订单详情聚合

实测：

```text
Order detail = OrderDetail{Order-1004, Product-2004, Customer-3004}
```

实验结论：

通过 allOf 等待订单、商品、客户三个异步任务全部完成，再分别获取结果并组装订单详情。

这是典型的多数据源聚合场景。

## 4. 三种组合方法对比

| 方法        | 完成条件                     | 结果特征                   | 典型用途             |
|-------------|------------------------------|----------------------------|----------------------|
| thenCombine | 两个输入都正常完成后执行组合 | 返回组合后的业务结果       | 两个查询结果合并     |
| allOf       | 所有输入完成后完成           | Void，需要分别读取结果     | 多任务全部完成后汇总 |
| anyOf       | 任意一个输入完成             | Object，可能正常也可能异常 | 首个结果、竞速查询   |

## 5. 工程实践思考

在 OrderFlow 中，可以将订单详情聚合设计为：

```java
CompletableFuture<Order> orderFuture =
        CompletableFuture.supplyAsync(
                () -> orderService.getOrder(orderId),
                orderExecutor
        );

CompletableFuture<Product> productFuture =
        CompletableFuture.supplyAsync(
                () -> productService.getProduct(productId),
                productExecutor
        );

CompletableFuture<Customer> customerFuture =
        CompletableFuture.supplyAsync(
                () -> customerService.getCustomer(customerId),
                customerExecutor
        );

CompletableFuture<OrderDetail> detailFuture =
        CompletableFuture.allOf(
                orderFuture,
                productFuture,
                customerFuture
        ).thenApply(ignored ->
                new OrderDetail(
                        orderFuture.join(),
                        productFuture.join(),
                        customerFuture.join()
                )
        );
```

以上是说明数据流的示意代码，具体类型和服务接口需要根据实际业务实现。

工程上还需要考虑：

1. 各数据源是否真正独立。
2. 是否使用独立 Executor 隔离阻塞型任务。
3. 单个任务失败后，整体聚合如何处理。
4. 如何设置整体超时和单任务超时。
5. anyOf 返回后，其他任务是否需要继续执行或取消。
6. 是否需要部分结果降级，而不是要求所有数据源都成功。

## 6. 实验注意事项

- thenCombine 适合两个结果都必须具备的场景。
- allOf 不会自动将多个 Future 的结果转换为 List。
- allOf 异常完成不意味着其他任务自动停止。
- anyOf 不等同于最快的成功结果。
- anyOf 完成后，其他任务仍可能继续运行。
- Future 的取消状态不等同于业务任务已经停止。

## 7. 最终结论

EXP-019 全部通过。

已经掌握多个异步任务的并行执行、结果组合、全部完成等待以及任意任务完成机制。

下一阶段：EXP-020，CompletableFuture 异常处理与异常恢复，重点掌握 exceptionally、handle、whenComplete 的区别。
