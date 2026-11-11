# EXP-027：异步任务状态机与可靠取消管理器

## 1. 实验目标

使用 AtomicReference 和 CAS 实现异步任务状态机，验证合法状态转换、并发取消、完成与取消竞争，以及任务清理的一致性。

## 2. 状态定义

CREATED、SUBMITTED、RUNNING、SUCCESS、FAILED、CANCELLING、CANCELLED。

核心流转：

- CREATED → SUBMITTED
- CREATED → CANCELLED
- SUBMITTED → RUNNING / FAILED / CANCELLED
- RUNNING → SUCCESS / FAILED / CANCELLING
- CANCELLING → CANCELLED

SUCCESS、FAILED、CANCELLED 为终态，不允许再次转换。

## 3. 实验结果

| 场景 | 结果 |
|---|---|
| A 正常执行 | SUCCESS，清理 1 次 |
| B 8 线程并发取消 | 仅 1 个取消请求成功，最终 CANCELLED，清理 1 次 |
| C 完成与取消竞争 | 100 轮，SUCCESS 85 次，CANCELLED 15 次 |
| D 非法状态转换 | 非法转换被拒绝，终态不可逆 |

Maven 测试：4/4 通过，0 失败，0 错误。

测试耗时：0.127 秒。

## 4. 核心技术结论

1. AtomicReference.compareAndSet 可以实现原子状态竞争。
2. 状态机需要显式维护合法转换关系，不能只依赖 CAS。
3. 完成和取消必须竞争同一个 RUNNING 状态。
4. CANCELLING 与 CANCELLED 必须区分：发出取消请求不等于任务已经退出。
5. finally 可用于统一资源清理，并通过计数验证清理次数。
6. 业务成功必须建立在真实业务执行结果确定的基础上，不能仅凭状态转换宣布成功。

## 5. 实验局限

本实验是状态竞争原型，不是可直接投入生产的任务管理器。实际系统还需要协调 Future 取消、任务提交、任务启动、业务结果提交及资源释放之间的竞态。

## 6. 验收结论

EXP-027 修正版本通过。A～D 四项实验全部通过，完成与取消竞争验证成功，耗时异常问题已修正。
