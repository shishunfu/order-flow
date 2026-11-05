package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class ThreadLocalContextPropagationExperimentTest {

    private static final ThreadLocal<String> USER_CONTEXT =
            new ThreadLocal<>();

    private static final ThreadLocal<String> TRACE_CONTEXT =
            new ThreadLocal<>();

    /**
     * 场景 A：
     * 主线程设置 ThreadLocal，
     * Worker 线程不能自动读取主线程上下文。
     */
    @Test
    void threadLocalShouldNotPropagateAutomatically()
            throws Exception {

        ExecutorService executor =
                Executors.newSingleThreadExecutor();

        try {
            USER_CONTEXT.set("user-A");
            TRACE_CONTEXT.set("trace-A");

            System.out.println("Caller thread = "
                    + Thread.currentThread().getName());

            Future<String> future = executor.submit(() -> {
                System.out.println("Worker thread = "
                        + Thread.currentThread().getName());

                String user = USER_CONTEXT.get();
                String trace = TRACE_CONTEXT.get();

                return user + ":" + trace;
            });

            String result = future.get(3, TimeUnit.SECONDS);

            System.out.println("Worker read context = " + result);

            assertEquals("null:null", result);

            // 调用线程自己的上下文仍然存在。
            assertEquals("user-A", USER_CONTEXT.get());
            assertEquals("trace-A", TRACE_CONTEXT.get());

        } finally {
            USER_CONTEXT.remove();
            TRACE_CONTEXT.remove();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景 B：
     * 提交任务时捕获上下文，
     * Worker 执行时设置上下文，
     * 执行结束后恢复 Worker 原上下文。
     */
    @Test
    void contextShouldPropagateThroughTaskWrapper()
            throws Exception {

        ExecutorService executor =
                Executors.newSingleThreadExecutor();

        try {
            USER_CONTEXT.set("user-A");
            TRACE_CONTEXT.set("trace-A");

            Runnable task = captureContext(() -> {
                assertEquals("user-A", USER_CONTEXT.get());
                assertEquals("trace-A", TRACE_CONTEXT.get());

                System.out.println("Worker user = "
                        + USER_CONTEXT.get());

                System.out.println("Worker trace = "
                        + TRACE_CONTEXT.get());
            });

            // 任务提交之后，修改调用线程上下文。
            USER_CONTEXT.set("user-B");
            TRACE_CONTEXT.set("trace-B");

            executor.submit(task).get(3, TimeUnit.SECONDS);

            // 证明捕获的是提交包装任务时的上下文，
            // 而不是 Worker 执行时或调用线程后续的上下文。
            assertEquals("user-B", USER_CONTEXT.get());
            assertEquals("trace-B", TRACE_CONTEXT.get());

            // Worker 执行完后，检查其上下文已恢复为空。
            Future<Boolean> checkWorker = executor.submit(() ->
                    USER_CONTEXT.get() == null
                            && TRACE_CONTEXT.get() == null
            );

            assertTrue(checkWorker.get(3, TimeUnit.SECONDS));

        } finally {
            USER_CONTEXT.remove();
            TRACE_CONTEXT.remove();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景 C：
     * 多个任务使用不同上下文，
     * 在线程池中并发执行，
     * 验证任务上下文不会互相污染。
     */
    @Test
    void concurrentTasksShouldKeepTheirOwnContext()
            throws Exception {

        int taskCount = 20;

        ExecutorService executor =
                Executors.newFixedThreadPool(4);

        CountDownLatch allStarted = new CountDownLatch(4);
        CountDownLatch release = new CountDownLatch(1);

        try {
            List<Future<String>> futures = new ArrayList<>();

            for (int i = 1; i <= taskCount; i++) {
                String user = "user-" + i;
                String trace = "trace-" + i;

                // 模拟每个调用线程提交前设置不同上下文。
                USER_CONTEXT.set(user);
                TRACE_CONTEXT.set(trace);

                Runnable wrappedTask = captureContext(() -> {
                    allStarted.countDown();

                    try {
                        assertTrue(release.await(3, TimeUnit.SECONDS));

                        assertEquals(user, USER_CONTEXT.get());
                        assertEquals(trace, TRACE_CONTEXT.get());

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "Task interrupted", e
                        );
                    }
                });

                futures.add(executor.submit(() -> {
                    wrappedTask.run();

                    return USER_CONTEXT.get()
                            + ":" + TRACE_CONTEXT.get();
                }));

                // 模拟下一次提交前调用线程上下文变化。
                USER_CONTEXT.remove();
                TRACE_CONTEXT.remove();
            }

            assertTrue(allStarted.await(3, TimeUnit.SECONDS));

            release.countDown();

            for (int i = 0; i < taskCount; i++) {
                String result = futures.get(i).get(
                        5, TimeUnit.SECONDS
                );

                // wrappedTask 执行后恢复 Worker 原上下文，
                // 本实验中原上下文为空。
                assertEquals("null:null", result);
            }

            System.out.println("Concurrent task count = " + taskCount);
            System.out.println("All task contexts isolated successfully");

        } finally {
            release.countDown();
            USER_CONTEXT.remove();
            TRACE_CONTEXT.remove();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * 提交时捕获上下文。
     */
    private Runnable captureContext(Runnable task) {
        String capturedUser = USER_CONTEXT.get();
        String capturedTrace = TRACE_CONTEXT.get();

        return () -> {
            String previousUser = USER_CONTEXT.get();
            String previousTrace = TRACE_CONTEXT.get();

            try {
                setOrRemove(USER_CONTEXT, capturedUser);
                setOrRemove(TRACE_CONTEXT, capturedTrace);

                task.run();

            } finally {
                setOrRemove(USER_CONTEXT, previousUser);
                setOrRemove(TRACE_CONTEXT, previousTrace);
            }
        };
    }

    private static void setOrRemove(
            ThreadLocal<String> threadLocal,
            String value
    ) {
        if (value == null) {
            threadLocal.remove();
        } else {
            threadLocal.set(value);
        }
    }
}
