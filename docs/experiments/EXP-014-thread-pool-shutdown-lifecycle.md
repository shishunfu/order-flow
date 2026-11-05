# EXP-014：线程池关闭与任务生命周期管理

## 1. 实验目标

研究 Java ThreadPoolExecutor 的关闭机制，理解 shutdown、shutdownNow 和 awaitTermination 的区别，以及线程池关闭过程中任务与 Future 的生命周期。

## 2. 实验环境

- JDK：OpenJDK 25.0.2
- Maven：3.9.12
- 测试框架：JUnit 5
- 实验类：ThreadPoolShutdownExperimentTest

## 3. 场景 A：shutdown() 优雅关闭

实验配置：

- corePoolSize = 2
- maximumPoolSize = 2
- 提交 4 个任务
- 前两个任务运行时阻塞，后两个任务进入队列
- 调用 shutdown() 后释放阻塞任务

实测结果：

| 观察项 | 实测结果 |
|---|---|
| shutdown 后 isShutdown | true |
| shutdown 后 isTerminated | false |
| shutdown 后队列任务数 | 2 |
| 最终完成任务数 | 4 |
| 最终 isTerminated | true |
| 关闭后提交新任务 | RejectedExecutionException |

结论：

shutdown() 停止接收新任务，但不会丢弃已经提交的任务。正在执行的任务和队列任务都会继续执行，直到全部完成。

## 4. 场景 B：shutdownNow() 立即关闭

实验配置：

- corePoolSize = 1
- maximumPoolSize = 1
- 一个任务运行并阻塞
- 两个任务通过 submit() 提交并进入队列
- 调用 shutdownNow()

实测结果：

| 观察项 | 实测结果 |
|---|---|
| 调用前队列任务数 | 2 |
| shutdownNow 返回任务数 | 2 |
| 返回对象类型 | FutureTask |
| 正在执行的任务收到中断 | 是 |
| 正在执行的任务退出 | 是 |
| 排队 Future 自动取消 | 否 |
| 手动 cancel 后 Future 取消 | 是 |

重要发现：

shutdownNow() 会尝试中断正在执行的任务，并返回尚未开始执行的任务。

但对于通过 submit() 提交的排队任务，shutdownNow() 将 FutureTask 从队列移除，并不会自动调用 FutureTask.cancel()。

因此：

- 任务已经离开线程池队列，不代表 Future 已完成。
- Future.isDone() 仍可能为 false。
- Future.isCancelled() 仍可能为 false。
- 应用需要根据返回的任务列表决定如何取消 Future、释放资源或执行补偿。

## 5. 场景 C：awaitTermination()

实验配置：

- 一个线程执行阻塞任务
- 调用 shutdown()
- 第一次等待 100ms
- 释放任务后再次等待

实测结果：

| 观察项 | 第一次等待 | 第二次等待 |
|---|---|---|
| awaitTermination | false | true |
| isShutdown | true | true |
| isTerminated | false | true |

结论：

awaitTermination() 负责等待线程池进入终止状态，不负责发起关闭。

## 6. 三个 API 的职责

| API | 主要职责 |
|---|---|
| shutdown() | 停止接收新任务，允许已提交任务执行完毕 |
| shutdownNow() | 尝试中断运行任务，移除并返回等待任务 |
| awaitTermination() | 等待线程池终止，支持超时 |

## 7. 工程实践结论

1. 正常应用关闭优先使用 shutdown()。
2. 通过 awaitTermination() 给任务预留合理的完成时间。
3. 超时后可以调用 shutdownNow() 尝试中断任务。
4. 任务必须主动响应中断，不能假设 shutdownNow() 可以强制杀死线程。
5. 对 shutdownNow() 返回的 FutureTask，需要考虑取消 Future 和清理业务资源。
6. Future 进入取消状态不代表业务代码一定已经停止执行，仍需关注实际任务退出情况。

## 8. 本次实验结论

线程池关闭不是简单地调用一个 API，而是一个涉及任务接收、排队、执行、中断、取消和终止的生命周期管理过程。

本次实验验证了 shutdown() 的优雅关闭行为、shutdownNow() 的中断与队列清理行为，以及 awaitTermination() 的等待语义。
