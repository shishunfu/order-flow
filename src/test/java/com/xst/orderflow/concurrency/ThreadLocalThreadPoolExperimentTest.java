package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ThreadLocal 在线程池复用场景下的行为实验测试类
 * <p>
 * 实验目的：
 * 演示线程池线程复用机制下 ThreadLocal 最常见的三类问题：
 * 1. 上下文泄漏（context leak）：前一个任务的上下文残留到下一个复用同一线程的任务中
 * 2. 内存泄漏：ThreadLocal 绑定的 value 对象无法被 GC 回收
 * 3. 上下文污染：不同任务的上下文数据发生意外串扰
 * <p>
 * 同时验证三种不同的解决方案的正确性和适用场景。
 */
class ThreadLocalThreadPoolExperimentTest {

    // 用户身份上下文：存储当前请求的登录用户标识，典型的业务透传字段
    private static final ThreadLocal<String> USER_CONTEXT =
        new ThreadLocal<>();

    // 链路追踪上下文：存储当前请求的全链路追踪ID，用于日志链路串联
    private static final ThreadLocal<String> TRACE_CONTEXT =
        new ThreadLocal<>();

    /**
     * 安全恢复 ThreadLocal 原有值的工具方法
     * <p>
     * 处理逻辑：
     * - 如果任务执行前的原有值是 null，调用 remove() 彻底清除 ThreadLocal，避免内存泄漏
     * - 如果任务执行前的原有值不为 null，调用 set() 恢复到之前的值，支持嵌套上下文场景
     *
     * @param threadLocal   目标 ThreadLocal 对象
     * @param previousValue 任务执行前保存的原有上下文值
     */
    private static void restore(
        ThreadLocal<String> threadLocal,
        String previousValue
    ) {
        if (previousValue == null) {
            threadLocal.remove();
        } else {
            threadLocal.set(previousValue);
        }
    }

    /**
     * 场景 A：
     * 第一个任务设置 ThreadLocal，但不清理。
     * 第二个任务复用同一个线程，读取到上一个任务的数据。
     * <p>
     * 验证点：
     * 1. 线程池的线程复用机制会导致 ThreadLocal 数据跨任务残留
     * 2. 任务执行完成后如果不主动调用 remove()，下一个复用该线程的任务会意外读取到前一个任务的上下文
     * 3. 这是生产环境中最常见的"上下文串扰"bug 根源，会导致用户身份错乱、日志链路混乱等严重问题
     */
    @Test
    void threadLocalShouldLeakBetweenTasksWhenNotRemoved()
        throws Exception {

        // 创建单线程线程池，保证所有任务都严格复用同一个工作线程
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            1, 1,
            0, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>()
        );

        try {
            // 提交第一个任务：设置上下文但完全不做清理
            Future<String> first = executor.submit(() -> {
                USER_CONTEXT.set("user-A");
                TRACE_CONTEXT.set("trace-A");

                System.out.println("Task A thread = "
                    + Thread.currentThread().getName());

                return USER_CONTEXT.get() + ":" + TRACE_CONTEXT.get();
            });

            assertEquals("user-A:trace-A",
                first.get(3, TimeUnit.SECONDS));

            // 提交第二个任务：复用同一个线程，此时任务本身没有设置任何上下文
            Future<String> second = executor.submit(() -> {
                System.out.println("Task B thread = "
                    + Thread.currentThread().getName());

                String user = USER_CONTEXT.get();
                String trace = TRACE_CONTEXT.get();

                return user + ":" + trace;
            });

            String result = second.get(3, TimeUnit.SECONDS);

            System.out.println("Task B read context = " + result);

            // 断言验证：第二个任务意外读取到了第一个任务残留的上下文数据
            assertEquals("user-A:trace-A", result);

        } finally {
            // 测试结束时清理，避免污染其他测试。
            executor.shutdownNow();
            executor.awaitTermination(3, TimeUnit.SECONDS);
            USER_CONTEXT.remove();
            TRACE_CONTEXT.remove();
        }
    }

    /**
     * 场景 B：
     * 每个任务通过 finally + remove() 清理上下文。
     * <p>
     * 验证点：
     * 1. 在 finally 块中无条件调用 ThreadLocal.remove() 可以彻底清除上下文残留
     * 2. 任务执行完成后，复用该线程的下一个任务读取到的上下文为 null
     * 3. 这是最基础的 ThreadLocal 清理方案，适用于简单场景
     */
    @Test
    void threadLocalShouldBeRemovedAfterTask()
        throws Exception {

        // 同样使用单线程线程池，强制线程复用
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            1, 1,
            0, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>()
        );

        try {
            Future<String> first = executor.submit(() -> {
                try {
                    USER_CONTEXT.set("user-A");
                    TRACE_CONTEXT.set("trace-A");

                    return USER_CONTEXT.get()
                        + ":" + TRACE_CONTEXT.get();
                } finally {
                    // 无论任务执行成功还是抛出异常，都保证上下文被彻底清除
                    USER_CONTEXT.remove();
                    TRACE_CONTEXT.remove();
                }
            });

            assertEquals("user-A:trace-A",
                first.get(3, TimeUnit.SECONDS));

            // 第二个任务验证上下文已经被完全清空
            Future<Boolean> second = executor.submit(() -> {
                String user = USER_CONTEXT.get();
                String trace = TRACE_CONTEXT.get();

                System.out.println("Task B user = " + user);
                System.out.println("Task B trace = " + trace);

                return user == null && trace == null;
            });

            assertTrue(second.get(3, TimeUnit.SECONDS));

        } finally {
            executor.shutdownNow();
            executor.awaitTermination(3, TimeUnit.SECONDS);
            USER_CONTEXT.remove();
            TRACE_CONTEXT.remove();
        }
    }

    /**
     * 场景 C：
     * 使用 Runnable 包装器统一设置和清理上下文。
     * <p>
     * 验证点：
     * 1. 包装器模式可以将上下文设置/恢复逻辑与业务任务完全解耦
     * 2. 任务执行完成后，工作线程的上下文被精确恢复到任务执行之前的状态
     * 3. 调用线程自身的上下文完全不受工作线程任务执行的影响
     * 4. 这种方案支持嵌套上下文场景，比简单的 remove() 方案更健壮
     */
    @Test
    void contextWrapperShouldSetAndRestoreContext()
        throws Exception {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            1, 1,
            0, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>()
        );

        try {
            // 模拟调用线程原本持有的上下文。
            USER_CONTEXT.set("caller-user");
            TRACE_CONTEXT.set("caller-trace");

            // 使用上下文包装器创建任务，传入任务专属的上下文值
            Runnable task = withContext(
                "worker-user",
                "worker-trace",
                () -> {
                    // 业务任务执行期间可以正确读取到包装器设置的上下文
                    assertEquals("worker-user", USER_CONTEXT.get());
                    assertEquals("worker-trace", TRACE_CONTEXT.get());

                    System.out.println("Task context user = "
                        + USER_CONTEXT.get());

                    System.out.println("Task context trace = "
                        + TRACE_CONTEXT.get());
                }
            );

            executor.submit(task).get(3, TimeUnit.SECONDS);

            // 工作线程上下文已清理。
            Future<Boolean> checkWorker = executor.submit(() ->
                USER_CONTEXT.get() == null
                    && TRACE_CONTEXT.get() == null
            );

            assertTrue(checkWorker.get(3, TimeUnit.SECONDS));

            // 调用线程上下文没有被工作任务修改。
            assertEquals("caller-user", USER_CONTEXT.get());
            assertEquals("caller-trace", TRACE_CONTEXT.get());

        } finally {
            executor.shutdownNow();
            executor.awaitTermination(3, TimeUnit.SECONDS);
            USER_CONTEXT.remove();
            TRACE_CONTEXT.remove();
        }
    }

    /**
     * 上下文包装器：
     * 执行前保存工作线程原上下文，
     * 执行时设置任务上下文，
     * 执行后恢复原上下文。
     * <p>
     * 这里选择恢复，而不是无条件 remove，
     * 是为了正确处理可能存在的嵌套上下文。
     * <p>
     * 核心设计优势：
     * 1. 支持嵌套调用：如果工作线程在调用 withContext 之前已经有上下文，执行完成后会精确恢复原值
     * 2. 异常安全：finally 块保证即使业务任务抛出异常，上下文也能正确恢复
     * 3. 透明无侵入：业务代码完全感知不到上下文设置/恢复逻辑，专注于业务逻辑本身
     */
    private Runnable withContext(
        String user,
        String trace,
        Runnable task
    ) {
        return () -> {
            // 任务执行前，先保存工作线程当前已有的上下文快照
            String previousUser = USER_CONTEXT.get();
            String previousTrace = TRACE_CONTEXT.get();

            try {
                // 将任务专属的上下文设置到工作线程
                USER_CONTEXT.set(user);
                TRACE_CONTEXT.set(trace);

                // 执行业务任务，此时业务代码可以正常读取到上下文
                task.run();

            } finally {
                // 无论任务执行结果如何，都精确恢复工作线程执行前的原有上下文
                restore(USER_CONTEXT, previousUser);
                restore(TRACE_CONTEXT, previousTrace);
            }
        };
    }
}
