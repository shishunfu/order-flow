package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ThreadLocalThreadPoolExperimentTest {

    private static final ThreadLocal<String> USER_CONTEXT =
            new ThreadLocal<>();

    private static final ThreadLocal<String> TRACE_CONTEXT =
            new ThreadLocal<>();

    /**
     * 场景 A：
     * 第一个任务设置 ThreadLocal，但不清理。
     * 第二个任务复用同一个线程，读取到上一个任务的数据。
     */
    @Test
    void threadLocalShouldLeakBetweenTasksWhenNotRemoved()
            throws Exception {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1,
                0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>()
        );

        try {
            Future<String> first = executor.submit(() -> {
                USER_CONTEXT.set("user-A");
                TRACE_CONTEXT.set("trace-A");

                System.out.println("Task A thread = "
                        + Thread.currentThread().getName());

                return USER_CONTEXT.get() + ":" + TRACE_CONTEXT.get();
            });

            assertEquals("user-A:trace-A",
                    first.get(3, TimeUnit.SECONDS));

            Future<String> second = executor.submit(() -> {
                System.out.println("Task B thread = "
                        + Thread.currentThread().getName());

                String user = USER_CONTEXT.get();
                String trace = TRACE_CONTEXT.get();

                return user + ":" + trace;
            });

            String result = second.get(3, TimeUnit.SECONDS);

            System.out.println("Task B read context = " + result);

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
     */
    @Test
    void threadLocalShouldBeRemovedAfterTask()
            throws Exception {

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
                    USER_CONTEXT.remove();
                    TRACE_CONTEXT.remove();
                }
            });

            assertEquals("user-A:trace-A",
                    first.get(3, TimeUnit.SECONDS));

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

            Runnable task = withContext(
                    "worker-user",
                    "worker-trace",
                    () -> {
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
     *
     * 这里选择恢复，而不是无条件 remove，
     * 是为了正确处理可能存在的嵌套上下文。
     */
    private Runnable withContext(
            String user,
            String trace,
            Runnable task
    ) {
        return () -> {
            String previousUser = USER_CONTEXT.get();
            String previousTrace = TRACE_CONTEXT.get();

            try {
                USER_CONTEXT.set(user);
                TRACE_CONTEXT.set(trace);

                task.run();

            } finally {
                restore(USER_CONTEXT, previousUser);
                restore(TRACE_CONTEXT, previousTrace);
            }
        };
    }

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
}
