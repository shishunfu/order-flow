# EXP-016：ThreadLocal 异步任务上下文传播与隔离

## 1. 实验目标

研究 ThreadLocal 在跨线程异步执行时的上下文传播机制，验证普通 ThreadLocal 不会自动跨线程传递，并实现任务提交时捕获上下文、执行时设置上下文、结束后恢复上下文的包装器。

## 2. 实验环境

- JDK：OpenJDK 25.0.2
- Maven：3.9.12
- 测试框架：JUnit 5
- 实验类：ThreadLocalContextPropagationExperimentTest
- 并发任务数：20
- 场景 C 线程池大小：4

## 3. 场景 A：ThreadLocal 不会自动跨线程传播

实验配置：

- 主线程设置 user-A、trace-A。
- 通过单线程线程池提交任务。
- Worker 线程直接读取 ThreadLocal。

实测结果：

```text
Caller thread = main
Worker thread = pool-3-thread-1
Worker read context = null:null
```

结论：

ThreadLocal 绑定当前线程。主线程和 Worker 线程拥有各自独立的 ThreadLocalMap，因此主线程中的数据不会自动传播到 Worker。

## 4. 场景 B：提交时捕获上下文

实验配置：

1. 调用线程设置 user-A、trace-A。
2. 创建包装任务并捕获上下文。
3. 调用线程将上下文修改为 user-B、trace-B。
4. Worker 执行包装任务。
5. 任务结束后恢复 Worker 原上下文。

实测结果：

```text
Worker user = user-A
Worker trace = trace-A
```

结论：

Worker 读取到 user-A、trace-A，证明上下文在创建包装任务时被捕获，而不是在 Worker 执行时读取调用线程的当前上下文。

包装器的核心流程：

1. 提交时捕获调用线程上下文。
2. 执行时保存 Worker 原上下文。
3. 设置任务上下文。
4. 执行业务任务。
5. finally 中恢复 Worker 原上下文。

## 5. 场景 C：多任务并发上下文隔离

实验配置：

- 20 个异步任务。
- 4 个 Worker 线程。
- 每个任务具有独立 userId 和 traceId。
- 使用上下文包装器传播数据。
- 通过 CountDownLatch 控制任务并发执行时序。

实测结果：

```text
Concurrent task count = 20
All task contexts isolated successfully
```

结论：

20 个任务的上下文均通过断言验证，没有出现任务间上下文串扰。

这说明任务上下文应与任务绑定，而不应依赖 Worker 线程中残留的数据。

## 6. 核心知识点

### 6.1 ThreadLocal 不会自动跨线程传播

ThreadLocal 的存储边界是线程。线程池中的异步任务不会自动继承提交线程的普通 ThreadLocal 数据。

### 6.2 上下文捕获时机

应在任务提交时捕获上下文快照，避免调用线程后续修改影响已提交任务。

### 6.3 上下文恢复

执行异步任务时，应保存 Worker 原上下文，在 finally 中恢复，避免线程复用时发生数据污染。

### 6.4 并发隔离

不同任务必须使用各自捕获的上下文。不能让多个任务共享可变的上下文对象。

## 7. 工程实践结论

1. 不要假设 ThreadLocal 会自动跨线程传播。
2. 异步任务需要显式捕获和传递必要的上下文。
3. 上下文应在提交任务时捕获。
4. Worker 执行任务前设置上下文，执行结束后恢复或清理。
5. 并发场景必须验证不同任务之间的上下文隔离。
6. 上下文传播应明确数据范围，避免传递不必要的请求对象或敏感信息。
7. 对 SecurityContext、MDC 等框架上下文，应使用对应框架提供的传播机制或经过验证的集成方案。

## 8. 本次实验结论

ThreadLocal 是线程级存储机制，不是跨线程上下文传播机制。

通过任务包装器，可以实现提交时捕获、执行时设置、执行后恢复的上下文传播流程。

这为后续理解 Spring 异步任务、日志链路追踪和多租户上下文管理打下基础。
