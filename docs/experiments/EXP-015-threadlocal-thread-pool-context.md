# EXP-015：ThreadLocal 在线程池中的数据污染与清理

## 1. 实验目标

研究 ThreadLocal 与线程池线程复用之间的关系，验证任务执行结束后不清理 ThreadLocal 可能导致的数据污染，并实践 finally + remove() 和上下文包装器。

## 2. 实验环境

- JDK：OpenJDK 25.0.2
- Maven：3.9.12
- 测试框架：JUnit 5
- 实验类：ThreadLocalThreadPoolExperimentTest

## 3. 场景 A：ThreadLocal 数据污染

实验配置：

- corePoolSize = 1
- maximumPoolSize = 1
- 使用同一个线程依次执行两个任务
- Task A 设置用户和 Trace 上下文，但不清理
- Task B 读取上下文

实测结果：

```text
Task A thread = pool-3-thread-1
Task B thread = pool-3-thread-1
Task B read context = user-A:trace-A
```

结论：

两个任务由同一个 Worker 线程执行。

Task A 设置的 ThreadLocal 数据没有清理，Task B 因此读取到了 Task A 的上下文。

ThreadLocal 隔离的是线程，不是任务。在线程池中，线程复用使得未清理的 ThreadLocal 数据可能污染后续任务。

## 4. 场景 B：finally + remove()

实验配置：

- 使用单线程线程池
- Task A 设置上下文
- finally 中执行 remove()
- Task B 再次读取上下文

实测结果：

```text
Task B user = null
Task B trace = null
```

结论：

finally + remove() 可以清理任务执行期间设置的 ThreadLocal 数据，避免后续任务读取旧上下文。

推荐模式：

```java
try {
    USER_CONTEXT.set(userId);
    businessService.execute();
} finally {
    USER_CONTEXT.remove();
}
```

## 5. 场景 C：任务上下文包装器

实验配置：

- 调用线程预先设置 caller-user 和 caller-trace
- 包装任务使用 worker-user 和 worker-trace
- 执行前保存工作线程原上下文
- 执行后恢复工作线程原上下文

实测结果：

```text
Task context user = worker-user
Task context trace = worker-trace
```

并且后续断言验证：

- 工作线程上下文恢复为空。
- 调用线程仍然保留 caller-user 和 caller-trace。

结论：

上下文包装器可以统一管理任务上下文的设置与恢复，避免每个业务任务重复编写上下文清理代码。

## 6. 核心知识点

### 6.1 ThreadLocal 的隔离边界

ThreadLocal 数据绑定在线程上，而不是绑定在一次任务上。

线程池中的 Worker 线程会被多个任务复用，因此 ThreadLocal 数据可能跨任务保留。

### 6.2 remove() 的作用

任务结束时调用 remove()，清理当前线程中对应的 ThreadLocal 数据。

### 6.3 上下文包装器

包装器可以统一完成：

1. 保存工作线程原上下文。
2. 设置当前任务上下文。
3. 执行业务任务。
4. finally 中恢复原上下文。

## 7. 工程实践结论

1. 在线程池任务中使用 ThreadLocal，必须明确上下文清理责任。
2. 优先使用 try-finally 保证清理动作执行。
3. 对需要上下文传递的异步任务，不能假设 ThreadLocal 会自动跨线程传播。
4. 可以通过 Runnable 或 Callable 包装器统一管理上下文。
5. 支持嵌套上下文时，应考虑恢复旧值，而不只是无条件 remove()。
6. 用户身份、租户、TraceId 等上下文必须避免在线程复用时串扰。

## 8. 本次实验结论

ThreadLocal 是线程级变量存储机制，不是任务级上下文存储机制。

线程池复用 Worker 线程时，ThreadLocal 的生命周期可能超过单个任务，因此必须显式管理上下文的设置、传递和清理。
