# EXP-013：线程池任务异常处理与异常传播实验

## 1. 实验目的

研究 execute() 与 submit() 两种任务提交方式的异常传播差异，理解 FutureTask 如何保存异常，以及 ThreadPoolExecutor.afterExecute() 如何获取异步任务异常。

## 2. 实验环境

- JDK：OpenJDK 25.0.2
- AvailableProcessors：16
- 测试方式：JUnit 5
- 实验 A：execute() + UncaughtExceptionHandler
- 实验 B：submit() 不调用 get()
- 实验 C：submit() 调用 get()
- 实验 D：submit() + afterExecute()

## 3. 实验结果

### A：execute() 提交异常任务

实际结果：

```text
execute task running
UncaughtExceptionHandler thread=exp013-execute-worker
Uncaught exception=IllegalStateException: execute failure
Uncaught count=1
```

结论：

execute() 提交的任务发生未捕获异常时，异常会传播到 Worker 执行流程，并触发线程的 UncaughtExceptionHandler。

### B：submit() 不调用 get()

实际结果：

```text
submit task running
Future isDone=true
Future isCancelled=false
Uncaught count=0
```

结论：

submit() 通常通过 FutureTask 包装任务。FutureTask 捕获并保存任务异常，因此不会像 execute() 那样直接触发 UncaughtExceptionHandler。

如果调用方不检查 Future，也没有统一异常收集机制，异步任务失败可能被遗漏。

### C：submit() 调用 get()

实际结果：

```text
Future isDone=true
Exception from get=ExecutionException
Root cause=IllegalArgumentException: get failure
```

结论：

Future.get() 获取异步任务执行结果。如果任务异常，get() 抛出 ExecutionException，原始异常可以通过 getCause() 获取。

### D：afterExecute() 获取异常

实际结果：

```text
afterExecute runnable=FutureTask
afterExecute throwable=null
afterExecute root cause=IllegalStateException: afterExecute failure
```

结论：

submit() 提交的 FutureTask 会内部捕获任务异常，因此 afterExecute() 收到的 Throwable 通常为 null。

如果需要在线程池层面统一采集 submit() 的异常，需要检查 Future，并通过 get() 获取 ExecutionException。

## 4. 核心知识点

### 4.1 execute() 和 submit() 的区别

- execute()：直接提交 Runnable。
- submit()：通常通过 FutureTask 包装任务。
- execute() 的未捕获任务异常可能导致 Worker 异常退出。
- submit() 的任务异常通常保存在 Future 中。

### 4.2 Future.get()

任务正常完成时返回结果。

任务异常完成时抛出 ExecutionException，真实异常位于 getCause()。

### 4.3 afterExecute()

afterExecute(Runnable r, Throwable t) 是 ThreadPoolExecutor 提供的任务执行后钩子。

- execute() 的未捕获异常可能通过 t 传入。
- submit() 的 FutureTask 通常会使 t 为 null。
- 可以通过检查 Future 并调用 get() 获取保存的异常。

### 4.4 UncaughtExceptionHandler

这是线程级别的未捕获异常处理机制，不是统一的业务任务结果收集机制。

## 5. 源码阅读重点

1. AbstractExecutorService.submit()
2. AbstractExecutorService.newTaskFor()
3. FutureTask.run()
4. FutureTask.setException()
5. FutureTask.get()
6. ThreadPoolExecutor.runWorker()
7. ThreadPoolExecutor.afterExecute()

重点理解 FutureTask 如何捕获异常，以及 ThreadPoolExecutor 如何调用 afterExecute()。

## 6. 工程实践结论

1. 异步任务必须设计明确的异常处理机制。
2. 不能假设 submit() 任务异常会自动打印。
3. 需要任务结果时，应通过 Future 或异步组合机制获取结果。
4. 不需要等待结果的后台任务，也应有统一异常记录机制。
5. afterExecute() 可以作为线程池级别的异常采集入口，但不适合执行耗时业务逻辑。
6. 需要可靠重试和补偿的任务，不能只依赖线程池异常日志。

## 7. 实验局限

- 本实验没有比较 execute() 与 submit() 对 Worker 替换行为的完整生命周期。
- 没有测试批量任务和多个任务同时失败。
- 没有测试任务异常后的重试与补偿。
- 没有验证异常日志系统自身失败的场景。

## 8. 最终结论

**execute() 更直接地暴露未捕获异常；submit() 将任务异常保存在 Future 中，需要调用方或统一异常收集机制主动获取。**

线程池异常处理设计的核心不是让异常打印出来，而是确保异步任务失败能够被发现、记录，并根据业务要求进行恢复或补偿。
