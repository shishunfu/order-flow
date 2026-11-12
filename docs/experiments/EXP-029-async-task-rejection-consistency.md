
# EXP-029：线程池拒绝与异步任务登记一致性实验

## 1. 实验目标

研究异步任务登记与线程池提交之间的一致性，验证：

1. 线程池拒绝后是否产生残留记录。
2. 拒绝后清理是否支持任务重试。
3. 并发提交相同 taskId 是否产生悬空 Future。
4. 并发提交成功时是否复用同一个 Future。
5. 相同 taskId 是否只执行一次业务任务。

## 2. 实验环境

- JDK：25
- Maven：3.9.12
- 测试框架：JUnit 5
- 线程池：ThreadPoolExecutor
- 拒绝策略：AbortPolicy
- 核心线程数：1
- 最大线程数：1
- 队列容量：1

## 3. 实验结果

### EXP-029-A：旧实现拒绝后残留记录

实现方式：

先使用 putIfAbsent 登记任务，再执行 executor.execute()。

实测：

- 提交结果：RejectedExecutionException
- 任务状态：FAILED
- 残留记录：true
- Future 完成状态：false

结论：

旧实现存在拒绝后残留记录的问题。

### EXP-029-B：拒绝后清理记录

实现方式：

提交被拒绝时执行：

records.remove(taskId, newRecord)。

实测：

- 记录存在：false
- 记录数量：0

结论：

拒绝后清理能够解决当前线程产生的残留记录问题，但无法解决已经被其他线程取得的 Future 引用问题。

### EXP-029-C：拒绝后重试

实验步骤：

1. 制造线程池饱和。
2. 使用 ORDER-C 提交任务。
3. 验证第一次提交被拒绝。
4. 释放线程池资源。
5. 使用相同 taskId 再次提交。

实测：

- 重试结果：DONE-ORDER-C
- 执行次数：1
- 任务状态：SUCCESS
- 记录数量：1

结论：

清理失败记录后，可以使用相同 taskId 重新提交。

### EXP-029-D：并发拒绝一致性

实验步骤：

20 个线程同时提交相同 taskId，线程池保持饱和。

修正方案：

使用 ConcurrentHashMap.computeIfAbsent()，在映射函数中执行线程池提交，成功后才发布任务记录。

实测：

- 并发提交数：20
- 拒绝次数：20
- 返回 Future 数：0
- 最终记录数：0
- 业务执行次数：0
- 释放资源后重试：成功
- 重试执行次数：1

结论：

在当前实验场景下，拒绝时不会向其他线程暴露未提交的 Future。

### EXP-029-E：并发成功与 Future 复用

实验步骤：

1. 创建可正常接收任务的线程池。
2. 20 个线程同时提交相同 taskId。
3. 通过 CountDownLatch 控制业务任务执行时机。
4. 比较 20 个调用者拿到的 Future 对象。
5. 验证业务执行次数和最终结果。

实测：

- 并发提交数：20
- 相同 Future 数：20
- 业务执行次数：1
- 任务记录数：1
- Future 完成状态：true
- 任务结果：DONE-ORDER-E

结论：

20 个调用者拿到同一个 Future 对象，业务任务只执行一次。

## 4. 核心实现

```java
TaskRecord record = records.computeIfAbsent(taskId, key -> {
    TaskRecord newRecord = new TaskRecord(key, task);

    newRecord.markSubmitted();

    executor.execute(newRecord.future());

    return newRecord;
});

return record.future();
```

核心机制：

- 映射函数正常返回后才建立映射。
- 提交异常时不建立映射。
- 相同 key 的并发映射计算由 ConcurrentHashMap 协调。
- 已存在记录时，后续调用者复用同一个 Future。

## 5. 技术结论

本实验验证了单 JVM 内相同 taskId 的并发提交去重。

| 场景 | 预期行为 |
|---|---|
| 线程池饱和 | 提交被拒绝，不登记任务 |
| 线程池可用 | 提交成功，登记任务 |
| 相同 taskId 并发提交 | 复用同一个 Future |
| 拒绝后重试 | 可以重新提交 |
| 并发成功提交 | 业务只执行一次 |

## 6. 当前方案局限

当前实现属于单 JVM 内存级任务登记方案，不是生产级分布式任务系统。

尚未覆盖：

- 不同 taskId 使用相同业务幂等键。
- 相同 taskId 使用不同业务参数的冲突处理。
- 线程池关闭与已接受任务取消之间的一致性。
- 进程崩溃后的任务恢复。
- 多实例之间的任务幂等。
- 数据库事务与消息投递的一致性。
- 重试、租约、死信和超时恢复。

特别注意：

当前实现采用先到先得语义。相同 taskId 的后续请求会复用第一次提交的任务，不会比较后续 Callable 的业务参数。

企业级实现还需要增加持久化任务表、唯一约束、业务参数摘要校验、状态条件更新和恢复机制。

## 7. 最终结论

EXP-029-A、B、C、D、E 五组实验完成。

修正后的 SafeTaskRegistry 在当前实验范围内实现：

1. 拒绝后不残留任务记录。
2. 拒绝后允许重试。
3. 并发拒绝时不暴露悬空 Future。
4. 并发成功时复用同一个 Future。
5. 相同 taskId 的业务任务只执行一次。

EXP-029 实验完成。
