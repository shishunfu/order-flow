package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class AsyncTaskIdempotencyExperimentTest {

    /**
     * 模拟订单异步任务执行器。
     *
     * taskId 作为幂等键。
     * 同一个 taskId 只创建一个 FutureTask。
     */
    static class IdempotentTaskExecutor {

        private final ConcurrentHashMap<String, FutureTask<String>> tasks =
                new ConcurrentHashMap<>();

        private final ExecutorService executor;

        private final AtomicInteger executionCount = new AtomicInteger();

        IdempotentTaskExecutor(ExecutorService executor) {
            this.executor = executor;
        }

        Future<String> submit(String taskId) {
            FutureTask<String> newTask = new FutureTask<>(() -> {
                executionCount.incrementAndGet();

                // 模拟订单异步处理
                Thread.sleep(200);

                return "ORDER-PROCESSED-" + taskId;
            });

            FutureTask<String> existingTask =
                    tasks.putIfAbsent(taskId, newTask);

            if (existingTask != null) {
                return existingTask;
            }

            executor.execute(newTask);
            return newTask;
        }

        int executionCount() {
            return executionCount.get();
        }

        int taskCount() {
            return tasks.size();
        }

        Map<String, FutureTask<String>> tasks() {
            return tasks;
        }
    }

    /**
     * A：顺序重复提交
     */
    @Test
    void shouldExecuteSameTaskOnlyOnce() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);

        try {
            IdempotentTaskExecutor taskExecutor =
                    new IdempotentTaskExecutor(executor);

            Future<String> first = taskExecutor.submit("ORDER-1001");

            for (int i = 0; i < 9; i++) {
                Future<String> duplicate =
                        taskExecutor.submit("ORDER-1001");

                assertSame(first, duplicate);
            }

            String result = first.get(2, TimeUnit.SECONDS);

            assertEquals("ORDER-PROCESSED-ORDER-1001", result);
            assertEquals(1, taskExecutor.executionCount());
            assertEquals(1, taskExecutor.taskCount());

            System.out.println("A execution count = "
                    + taskExecutor.executionCount());
            System.out.println("A task count = "
                    + taskExecutor.taskCount());
            System.out.println("A result = " + result);

        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    /**
     * B：100 个线程并发提交同一个任务
     */
    @Test
    void shouldExecuteConcurrentDuplicateTaskOnlyOnce()
            throws Exception {

        int submitterCount = 100;

        ExecutorService worker = Executors.newFixedThreadPool(4);
        ExecutorService submitters =
                Executors.newFixedThreadPool(submitterCount);

        try {
            IdempotentTaskExecutor taskExecutor =
                    new IdempotentTaskExecutor(worker);

            CountDownLatch ready = new CountDownLatch(submitterCount);
            CountDownLatch start = new CountDownLatch(1);

            ConcurrentLinkedQueue<Future<String>> results =
                    new ConcurrentLinkedQueue<>();

            for (int i = 0; i < submitterCount; i++) {
                submitters.submit(() -> {
                    ready.countDown();

                    try {
                        start.await();

                        results.add(
                                taskExecutor.submit("ORDER-2001")
                        );
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();

            submitters.shutdown();
            assertTrue(submitters.awaitTermination(10, TimeUnit.SECONDS));

            assertEquals(submitterCount, results.size());

            Future<String> first = results.peek();

            for (Future<String> future : results) {
                assertSame(first, future);
                assertEquals(
                        "ORDER-PROCESSED-ORDER-2001",
                        future.get(2, TimeUnit.SECONDS)
                );
            }

            assertEquals(1, taskExecutor.executionCount());
            assertEquals(1, taskExecutor.taskCount());

            System.out.println("B submit count = " + results.size());
            System.out.println("B execution count = "
                    + taskExecutor.executionCount());
            System.out.println("B task count = "
                    + taskExecutor.taskCount());

        } finally {
            submitters.shutdownNow();
            worker.shutdownNow();

            assertTrue(submitters.awaitTermination(2, TimeUnit.SECONDS));
            assertTrue(worker.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    /**
     * C：不同任务可以并行执行
     */
    @Test
    void shouldExecuteDifferentTasksIndependently()
            throws Exception {

        ExecutorService executor = Executors.newFixedThreadPool(4);

        try {
            IdempotentTaskExecutor taskExecutor =
                    new IdempotentTaskExecutor(executor);

            Future<String> order1 =
                    taskExecutor.submit("ORDER-3001");

            Future<String> order2 =
                    taskExecutor.submit("ORDER-3002");

            Future<String> order3 =
                    taskExecutor.submit("ORDER-3003");

            assertEquals(
                    "ORDER-PROCESSED-ORDER-3001",
                    order1.get(2, TimeUnit.SECONDS)
            );

            assertEquals(
                    "ORDER-PROCESSED-ORDER-3002",
                    order2.get(2, TimeUnit.SECONDS)
            );

            assertEquals(
                    "ORDER-PROCESSED-ORDER-3003",
                    order3.get(2, TimeUnit.SECONDS)
            );

            assertEquals(3, taskExecutor.executionCount());
            assertEquals(3, taskExecutor.taskCount());

            System.out.println("C execution count = "
                    + taskExecutor.executionCount());
            System.out.println("C task count = "
                    + taskExecutor.taskCount());

        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }
}
