# EXP-012：线程池任务取消与中断机制实验

## 1. 实验目的

研究 Future.cancel(true) 对线程池任务的取消行为，理解 Future 状态与底层线程执行状态之间的区别。

## 2. 实验环境

- JDK：OpenJDK 25.0.2
- 线程池：Executors.newSingleThreadExecutor()
- Worker 数量：1
- 实验 A：Thread.sleep(30000)
- 实验 B：CPU 循环持续约 2000ms
- 取消方式：Future.cancel(true)

## 3. 实验 A：可中断任务

实际结果：

```text
Before cancel: isDone=false
cancel(true) result=true
Task received InterruptedException
Interruptible task finished
After cancel: isCancelled=true
After cancel: isDone=true
Task interrupted=true
```

分析：

1. 任务进入 Thread.sleep()。
2. 调用 cancel(true) 后，执行线程收到中断请求。
3. sleep() 抛出 InterruptedException。
4. 任务捕获异常，并恢复中断标志。
5. 任务退出，finally 正常执行。

结论：

Thread.sleep() 是可中断阻塞操作，任务可以通过处理中断异常响应取消请求。

## 4. 实验 B：不主动响应中断的任务

实际结果：

```text
cancel(true) result=true
Immediately after cancel: isCancelled=true
Immediately after cancel: isDone=true

CPU task actually finished
Probe task started: pool-2-thread-1

Interrupt observed by CPU task=true
Time from cancel to actual task finish=2000 ms
Time from cancel to probe start=2000 ms
```

分析：

1. cancel(true) 成功将 Future 转为取消状态。
2. 执行线程收到了中断请求。
3. CPU 循环检测到了中断标志，但没有根据标志退出。
4. CPU 任务继续执行，直到达到预定结束时间。
5. 单线程池的 Worker 必须等 CPU 任务真正退出，才能执行 Probe task。

结论：

Future 被取消不代表底层业务代码已经停止。Java 中断是协作式机制，任务必须主动响应中断。

## 5. 核心知识点

### 5.1 Future 状态与实际执行状态不同

- isCancelled()：Future 是否已被取消。
- isDone()：Future 是否进入终态，包括正常完成、异常完成或取消。
- 这两个方法不能证明业务代码已经完成清理或停止运行。

### 5.2 cancel(true) 的含义

- 尝试取消任务。
- 如果任务已经运行，尝试中断执行线程。
- 不会强制终止 Java 线程。
- 返回 true 表示取消操作成功，不代表业务代码已经退出。

### 5.3 中断相关 API

| API | 作用 |
|---|---|
| interrupt() | 向线程发送中断请求 |
| isInterrupted() | 检查中断标志，不清除 |
| interrupted() | 检查当前线程的中断标志，并清除 |
| InterruptedException | 可中断阻塞操作响应中断时抛出 |

### 5.4 正确处理中断

阻塞操作：

```java
try {
    Thread.sleep(10000);
} catch (InterruptedException e) {
    Thread.currentThread().interrupt();
    return;
}
```

CPU 密集型循环：

```java
while (!Thread.currentThread().isInterrupted()) {
    doCalculation();
}
```

不要无意中吞掉 InterruptedException。

## 6. 工程实践结论

1. 取消 Future 不等于强制停止线程。
2. 任务代码需要主动支持中断。
3. Future 的取消状态不能替代任务实际退出状态。
4. 长时间运行的 CPU 任务应定期检查中断标志。
5. 阻塞任务应正确处理中断异常。
6. 任务退出时应完成必要的资源清理。
7. 业务超时、任务取消和底层 HTTP/RPC 超时需要协同设计。

## 7. 实验局限

- 使用单线程池，便于观察 Worker 是否被占用。
- CPU 循环为本地模拟任务，不代表真实业务负载。
- 实验没有覆盖任务尚未开始时的取消行为。
- 实验没有覆盖 cancel(false)。
- 实验没有覆盖多个任务之间的取消传播。

## 8. 最终结论

**Future 负责提供任务控制接口，Thread.interrupt() 负责传递中断请求，任务代码负责决定如何响应中断。**

取消机制能否真正释放线程池资源，最终取决于任务实现是否遵守协作式中断约定。
