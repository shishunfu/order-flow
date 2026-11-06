# EXP-021：CompletableFuture 超时控制与任务取消

## 1. 实验目标

验证 CompletableFuture 的超时控制和任务取消机制，重点区分 Future 完成状态与实际业务任务执行状态。

本次验证：
- `orTimeout()`：超时后 Future 异常完成。
- `completeOnTimeout()`：超时后 Future 使用默认值正常完成。
- `get(timeout, unit)`：调用方等待超时，不改变 Future 的完成状态。
- `cancel(true)`：Future 取消不保证中断正在运行的 supplier。

## 2. 实验环境

- JDK：OpenJDK 25.0.2
- Maven：3.9.12
- 测试框架：JUnit 5
- 线程执行器：自定义单线程 ExecutorService
- 实验方式：CountDownLatch 控制任务执行时机

## 3. 实验结果

### EXP-021-A：orTimeout

关键输出：

```text
A cause = java.util.concurrent.TimeoutException
A isCompletedExceptionally = true
A task finished = true
A future result remains timeout = true
```

结论：

1. `orTimeout()` 超时后，原 CompletableFuture 异常完成。
2. `get()` 抛出 ExecutionException，其 cause 为 TimeoutException。
3. 超时后释放任务闸门，supplier 仍然能够执行完毕。
4. 后台任务完成不会覆盖已经发生的超时结果。

### EXP-021-B：completeOnTimeout

关键输出：

```text
B result = DEFAULT-ORDER
B task finished before release = false
B supplier really returned
B task finished after release = true
```

结论：

1. `completeOnTimeout()` 超时后，Future 正常完成并返回备用结果。
2. Future 已经返回默认值时，supplier 仍可能在后台执行。
3. supplier 后续返回真实结果，不会覆盖已经完成的默认结果。

### EXP-021-C：get(timeout)

关键输出：

```text
C get timeout = java.util.concurrent.TimeoutException
C future isDone after get timeout = false
```

释放闸门后，Future 成功返回 `REAL-ORDER`。

结论：

1. `get(timeout, unit)` 只限制当前调用的等待时间。
2. 等待超时不会自动取消任务。
3. 等待超时不会自动将 Future 标记为完成。
4. 后台任务仍可正常完成并返回真实结果。

### EXP-021-D：cancel(true)

关键输出：

```text
D cancel returned = true
D isCancelled = true
D isDone = true
D interrupt count before release = 0
D task finished before release = false
D task really finished = true
D interrupt count after release = 0
```

结论：

1. `cancel(true)` 成功取消 CompletableFuture。
2. Future 的 isCancelled 和 isDone 均为 true。
3. 正在执行的 supplier 没有因此收到中断。
4. 释放任务闸门后，supplier 仍然正常结束。
5. `CompletableFuture.cancel(true)` 不能作为可靠的底层线程中断机制。

## 4. API 行为对比

| API | Future 结果 | 是否自动停止 supplier |
|---|---|---|
| orTimeout | 异常完成，TimeoutException | 不保证 |
| completeOnTimeout | 正常完成，返回默认值 | 不保证 |
| get(timeout) | Future 状态不变 | 不会 |
| cancel(true) | 取消状态 | 不保证中断 supplier |

## 5. 关键技术结论

### 5.1 Future 生命周期与业务任务生命周期分离

Future 已完成、超时或取消，不代表底层任务已经停止。

在生产环境中，如果超时后仍有大量任务继续运行，可能造成：
- 线程池工作线程被长期占用。
- 数据库连接和 HTTP 连接持续占用。
- 下游服务承受额外压力。
- 请求已经返回，但后台工作仍然消耗系统资源。

### 5.2 调用方超时不等于任务执行超时

`get(timeout)` 是等待方的等待期限。

`orTimeout()` 和 `completeOnTimeout()` 是 CompletableFuture 的完成期限。

它们都不能替代底层 HTTP 客户端、数据库驱动或业务任务自身的超时控制。

### 5.3 取消需要协作式设计

对于需要真正停止的业务任务，应设计明确的取消协作机制，例如：
- 检查中断标志。
- 响应取消令牌。
- 设置底层网络请求超时。
- 在任务内部检查任务是否仍然有效。
- 对资源使用 try-with-resources 或 finally 释放。

不能只依赖 Future 的取消状态判断业务任务是否停止。

## 6. 与 EXP-012 的联系

EXP-012 验证了 FutureTask 的 `cancel(true)` 可以尝试中断执行线程，但任务仍必须响应中断。

EXP-021 进一步验证：CompletableFuture 的 `cancel(true)` 不保证向正在执行的 supplier 发送线程中断。

因此，不应将 FutureTask 和 CompletableFuture 的取消行为视为完全相同。

## 7. 实验验收

- [x] orTimeout 异常完成验证
- [x] completeOnTimeout 默认结果验证
- [x] get(timeout) 仅等待超时验证
- [x] cancel(true) 与实际任务停止的区别验证
- [x] 后台任务最终完成验证
- [x] 线程池释放与关闭验证

测试结果：

```text
Tests run: 4
Failures: 0
Errors: 0
Skipped: 0
BUILD SUCCESS
```

## 8. 最终结论

本实验验证了 CompletableFuture 的超时与取消语义。

生产系统必须分别管理：
1. 调用方等待期限。
2. Future 完成期限。
3. 底层任务实际执行期限。
4. 任务取消与资源释放。

不能将 Future 超时或取消直接等同于业务任务停止。
