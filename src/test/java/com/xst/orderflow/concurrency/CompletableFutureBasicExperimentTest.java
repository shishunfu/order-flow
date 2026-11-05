package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CompletableFutureBasicExperimentTest {

    /**
     * 场景 A：
     * supplyAsync 异步执行任务，并返回计算结果。
     */
    @Test
    void supplyAsyncShouldReturnResult() throws Exception {

        String callerThread = Thread.currentThread().getName();

        AtomicReference<String> workerThread = new AtomicReference<>();

        CompletableFuture<Integer> future =
                CompletableFuture.supplyAsync(() -> {

                    workerThread.set(Thread.currentThread().getName());

                    System.out.println("supplyAsync thread = " + workerThread.get());

                    return 100 + 200;
                });

        Integer result = future.get(3, TimeUnit.SECONDS);

        System.out.println("Caller thread = " + callerThread);
        System.out.println("Result = " + result);

        assertEquals(300, result);
        assertNotNull(workerThread.get());
        assertNotEquals(callerThread, workerThread.get());
    }

    /**
     * 场景 B：
     * runAsync 异步执行任务，但不返回业务计算结果。
     */
    @Test
    void runAsyncShouldExecuteWithoutResult() throws Exception {

        AtomicBoolean executed = new AtomicBoolean(false);
        AtomicReference<String> workerThread = new AtomicReference<>();

        CompletableFuture<Void> future =
                CompletableFuture.runAsync(() -> {

                    workerThread.set(Thread.currentThread().getName());

                    executed.set(true);

                    System.out.println("runAsync thread = " + workerThread.get());
                });

        Void result = future.get(3, TimeUnit.SECONDS);

        System.out.println("runAsync result = " + result);
        System.out.println("executed = " + executed.get());

        assertNull(result);
        assertTrue(executed.get());
        assertNotNull(workerThread.get());
    }

    /**
     * 场景 C：
     * 使用自定义 Executor 执行 CompletableFuture 任务。
     */
    @Test
    void shouldUseCustomExecutor() throws Exception {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2,
                2,
                0,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName("order-flow-worker");
                    return thread;
                }
        );

        try {
            CompletableFuture<String> future =
                    CompletableFuture.supplyAsync(() -> {

                        String threadName =
                                Thread.currentThread().getName();

                        System.out.println("Custom executor thread = "
                                + threadName);

                        return threadName;
                    }, executor);

            String result = future.get(3, TimeUnit.SECONDS);

            assertTrue(result.startsWith("order-flow-worker"));

        } finally {
            executor.shutdown();
            assertTrue(
                    executor.awaitTermination(3, TimeUnit.SECONDS)
            );
        }
    }

    /**
     * 场景 D：
     * 验证异步任务提交后，调用线程不需要等待任务执行完成。
     */
    @Test
    void asyncTaskShouldNotBlockCallerThread() throws Exception {

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);

        CompletableFuture<String> future =
                CompletableFuture.supplyAsync(() -> {

                    taskStarted.countDown();

                    try {
                        releaseTask.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "Task interrupted", e
                        );
                    }

                    return "Order-1001";
                });

        try {
            assertTrue(taskStarted.await(3, TimeUnit.SECONDS));

            // 任务仍然阻塞，Future 尚未完成。
            assertFalse(future.isDone());

            System.out.println("Future isDone before release = " + future.isDone());

            // 释放异步任务。
            releaseTask.countDown();

            String result = future.get(3, TimeUnit.SECONDS);

            System.out.println("Future result = " + result);
            System.out.println("Future isDone after release = " + future.isDone());

            assertEquals("Order-1001", result);
            assertTrue(future.isDone());

        } finally {
            releaseTask.countDown();
        }
    }
}
