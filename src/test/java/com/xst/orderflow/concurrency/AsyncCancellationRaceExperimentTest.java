package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class AsyncCancellationRaceExperimentTest {

    private ExecutorService newExecutor(String name, int size) {
        AtomicInteger sequence = new AtomicInteger();

        return Executors.newFixedThreadPool(size, runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName(name + "-" + sequence.incrementAndGet());
            return thread;
        });
    }

    /**
     * 场景 A：
     * 一个任务已经完成，另一个任务仍在执行。
     * 整体失败时，只取消未完成任务。
     */
    @Test
    void shouldNotCancelCompletedTask() throws Exception {

        ExecutorService executor = newExecutor("race-a", 2);

        CountDownLatch slowStarted = new CountDownLatch(1);
        CountDownLatch slowFinished = new CountDownLatch(1);

        try {
            Future<String> completedFuture =
                executor.submit(() -> "ORDER-SUCCESS");

            Future<String> slowFuture = executor.submit(() -> {
                slowStarted.countDown();

                try {
                    Thread.sleep(30_000);
                    return "INVENTORY-SUCCESS";
                } finally {
                    slowFinished.countDown();
                }
            });

            assertEquals("ORDER-SUCCESS",
                completedFuture.get(1, TimeUnit.SECONDS));

            assertTrue(slowStarted.await(1, TimeUnit.SECONDS));

            // 模拟整体请求失败，只取消尚未完成的任务。
            List<Future<?>> futures =
                List.of(completedFuture, slowFuture);

            for (Future<?> future : futures) {
                if (!future.isDone()) {
                    future.cancel(true);
                }
            }

            assertTrue(completedFuture.isDone());
            assertFalse(completedFuture.isCancelled());

            assertTrue(slowFuture.isCancelled());
            assertTrue(slowFinished.await(2, TimeUnit.SECONDS));

            System.out.println("A completed result = "
                + completedFuture.get());

            System.out.println("A completed task cancelled = "
                + completedFuture.isCancelled());

            System.out.println("A slow task cancelled = "
                + slowFuture.isCancelled());

            System.out.println("A slow task finished = "
                + (slowFinished.getCount() == 0));

        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景 B：
     * 多个线程同时取消同一个任务。
     * finally 资源清理只能执行一次。
     */
    @Test
    void shouldExecuteCleanupOnlyOnceWhenCancelledRepeatedly()
        throws Exception {

        ExecutorService taskExecutor = newExecutor("race-b-task", 1);
        ExecutorService cancelExecutor = newExecutor("race-b-cancel", 8);

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);

        AtomicInteger cleanupCount = new AtomicInteger();

        try {
            Future<?> future = taskExecutor.submit(() -> {
                taskStarted.countDown();

                try {
                    Thread.sleep(30_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    cleanupCount.incrementAndGet();
                    taskFinished.countDown();
                }
            });

            assertTrue(taskStarted.await(1, TimeUnit.SECONDS));

            CountDownLatch cancelStart = new CountDownLatch(1);
            CountDownLatch cancelFinished = new CountDownLatch(8);

            List<Future<?>> cancelResults = new ArrayList<>();

            for (int i = 0; i < 8; i++) {
                cancelResults.add(cancelExecutor.submit(() -> {
                    try {
                        cancelStart.await();
                        future.cancel(true);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        cancelFinished.countDown();
                    }
                }));
            }

            cancelStart.countDown();

            assertTrue(cancelFinished.await(2, TimeUnit.SECONDS));
            assertTrue(taskFinished.await(2, TimeUnit.SECONDS));

            for (Future<?> cancelResult : cancelResults) {
                cancelResult.get();
            }

            System.out.println("B future cancelled = " + future.isCancelled());
            System.out.println("B cleanup count = " + cleanupCount.get());

            assertTrue(future.isCancelled());
            assertEquals(1, cleanupCount.get());

        } finally {
            cancelExecutor.shutdownNow();
            taskExecutor.shutdownNow();

            assertTrue(cancelExecutor.awaitTermination(3, TimeUnit.SECONDS));
            assertTrue(taskExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景 C：
     * Future 正常完成与取消操作并发竞争。
     * <p>
     * 注意：
     * Future 的完成状态与底层业务代码是否已经开始执行，
     * 不是同一个概念。
     */
    @Test
    void shouldKeepFutureStateConsistentDuringCompletionAndCancellationRace()
        throws Exception {

        ExecutorService executor = newExecutor("race-c", 2);

        AtomicInteger cleanupCount = new AtomicInteger();
        AtomicReference<String> outcome = new AtomicReference<>();

        try {
            for (int i = 0; i < 100; i++) {

                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch taskFinished = new CountDownLatch(1);

                Future<String> future = executor.submit(() -> {
                    try {
                        start.await();
                        return "SUCCESS";
                    } finally {
                        cleanupCount.incrementAndGet();
                        taskFinished.countDown();
                    }
                });

                CountDownLatch cancelFinished = new CountDownLatch(1);

                Thread cancelThread = new Thread(() -> {
                    try {
                        start.await();
                        future.cancel(true);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        cancelFinished.countDown();
                    }
                });

                cancelThread.start();
                start.countDown();

                assertTrue(cancelFinished.await(2, TimeUnit.SECONDS));
                assertTrue(taskFinished.await(2, TimeUnit.SECONDS));

                if (future.isCancelled()) {
                    assertTrue(future.isDone());

                    try {
                        future.get();
                        fail("Cancelled Future should not return a result");
                    } catch (CancellationException expected) {
                        // Expected.
                    }

                    outcome.set("CANCELLED");

                } else {
                    assertTrue(future.isDone());
                    assertEquals("SUCCESS", future.get());
                    outcome.set("SUCCESS");
                }

                cancelThread.join(2000);
                assertFalse(cancelThread.isAlive());
            }

            System.out.println("C last outcome = " + outcome.get());
            System.out.println("C cleanup count = " + cleanupCount.get());

            assertEquals(100, cleanupCount.get());

        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景 D：
     * 订单和商品已完成，库存仍在执行。
     * 模拟库存失败，取消剩余任务。
     */
    @Test
    void shouldPreserveCompletedResultsAndCancelRemainingTasks()
        throws Exception {

        ExecutorService executor = newExecutor("race-d", 3);

        CountDownLatch inventoryStarted = new CountDownLatch(1);
        CountDownLatch inventoryFinished = new CountDownLatch(1);

        try {
            Future<String> orderFuture =
                executor.submit(() -> "ORDER-1001");

            Future<String> productFuture =
                executor.submit(() -> "PRODUCT-2001");

            Future<String> inventoryFuture = executor.submit(() -> {
                inventoryStarted.countDown();

                try {
                    Thread.sleep(30_000);
                    return "INVENTORY-3001";
                } finally {
                    inventoryFinished.countDown();
                }
            });

            String order = orderFuture.get(1, TimeUnit.SECONDS);
            String product = productFuture.get(1, TimeUnit.SECONDS);

            assertTrue(inventoryStarted.await(1, TimeUnit.SECONDS));

            // 模拟库存服务失败导致整体请求失败。
            inventoryFuture.cancel(true);

            assertTrue(inventoryFinished.await(2, TimeUnit.SECONDS));

            System.out.println("D preserved order = " + order);
            System.out.println("D preserved product = " + product);
            System.out.println("D inventory cancelled = "
                + inventoryFuture.isCancelled());

            assertEquals("ORDER-1001", order);
            assertEquals("PRODUCT-2001", product);
            assertTrue(inventoryFuture.isCancelled());

        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }
}
