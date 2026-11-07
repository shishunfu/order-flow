package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class AsyncDeadlinePropagationExperimentTest {

    /**
     * 使用 System.nanoTime() 计算单调时间，
     * 避免系统时钟调整影响 Deadline。
     */
    static class Deadline {

        private final long deadlineNanos;

        private Deadline(long deadlineNanos) {
            this.deadlineNanos = deadlineNanos;
        }

        static Deadline after(Duration duration) {
            return new Deadline(
                    System.nanoTime() + duration.toNanos()
            );
        }

        long remainingMillis() {
            long remaining = deadlineNanos - System.nanoTime();

            if (remaining <= 0) {
                return 0;
            }

            return TimeUnit.NANOSECONDS.toMillis(remaining);
        }

        long remainingNanos() {
            return Math.max(
                    0,
                    deadlineNanos - System.nanoTime()
            );
        }

        boolean isExpired() {
            return deadlineNanos - System.nanoTime() <= 0;
        }

        void throwIfExpired() throws TimeoutException {
            if (isExpired()) {
                throw new TimeoutException("Request deadline exceeded");
            }
        }
    }

    private ExecutorService newExecutor(String name) {
        return Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setName(name);
            return thread;
        });
    }

    /**
     * 场景 A：
     * 多个下游共享同一个整体 Deadline。
     */
    @Test
    void shouldPropagateRemainingDeadlineToDownstream() throws Exception {

        Deadline deadline = Deadline.after(Duration.ofMillis(800));

        Thread.sleep(200);

        long orderRemaining = deadline.remainingMillis();

        Thread.sleep(150);

        long productRemaining = deadline.remainingMillis();

        Thread.sleep(100);

        long inventoryRemaining = deadline.remainingMillis();

        System.out.println("A order remaining = " + orderRemaining + "ms");
        System.out.println("A product remaining = " + productRemaining + "ms");
        System.out.println("A inventory remaining = " + inventoryRemaining + "ms");

        assertTrue(orderRemaining > productRemaining);
        assertTrue(productRemaining > inventoryRemaining);
        assertTrue(inventoryRemaining > 0);
        assertTrue(inventoryRemaining < 500);
    }

    /**
     * 场景 B：
     * 下游任务在超时后响应 interrupt，
     * 并通过 finally 释放任务资源。
     */
    @Test
    void shouldStopDownstreamTaskWhenInterrupted() throws Exception {

        ExecutorService executor = newExecutor("deadline-cancel-worker");

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);

        AtomicBoolean interrupted = new AtomicBoolean(false);
        AtomicBoolean resourceReleased = new AtomicBoolean(false);

        try {
            Future<?> future = executor.submit(() -> {
                started.countDown();

                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        Thread.sleep(100);
                    }
                } catch (InterruptedException e) {
                    interrupted.set(true);
                    Thread.currentThread().interrupt();
                } finally {
                    resourceReleased.set(true);
                    finished.countDown();
                }
            });

            assertTrue(started.await(1, TimeUnit.SECONDS));

            Thread.sleep(100);

            boolean cancelResult = future.cancel(true);

            assertTrue(finished.await(2, TimeUnit.SECONDS));

            System.out.println("B cancel result = " + cancelResult);
            System.out.println("B interrupted = " + interrupted.get());
            System.out.println("B resource released = " + resourceReleased.get());
            System.out.println("B future cancelled = " + future.isCancelled());

            assertTrue(cancelResult);
            assertTrue(interrupted.get());
            assertTrue(resourceReleased.get());
            assertTrue(future.isCancelled());

        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景 C：
     * 整体请求超时后，主动取消其他下游任务。
     */
    @Test
    void shouldCancelOtherTasksWhenOverallDeadlineExpires() throws Exception {

        ExecutorService orderExecutor = newExecutor("deadline-order-worker");
        ExecutorService productExecutor = newExecutor("deadline-product-worker");
        ExecutorService inventoryExecutor = newExecutor("deadline-inventory-worker");

        CountDownLatch orderStarted = new CountDownLatch(1);
        CountDownLatch productStarted = new CountDownLatch(1);
        CountDownLatch inventoryStarted = new CountDownLatch(1);

        CountDownLatch orderFinished = new CountDownLatch(1);
        CountDownLatch productFinished = new CountDownLatch(1);
        CountDownLatch inventoryFinished = new CountDownLatch(1);

        try {
            Future<String> orderFuture = orderExecutor.submit(() -> {
                orderStarted.countDown();
                try {
                    Thread.sleep(30_000);
                    return "ORDER";
                } finally {
                    orderFinished.countDown();
                }
            });

            Future<String> productFuture = productExecutor.submit(() -> {
                productStarted.countDown();
                try {
                    Thread.sleep(30_000);
                    return "PRODUCT";
                } finally {
                    productFinished.countDown();
                }
            });

            Future<String> inventoryFuture = inventoryExecutor.submit(() -> {
                inventoryStarted.countDown();
                try {
                    Thread.sleep(30_000);
                    return "INVENTORY";
                } finally {
                    inventoryFinished.countDown();
                }
            });

            assertTrue(orderStarted.await(1, TimeUnit.SECONDS));
            assertTrue(productStarted.await(1, TimeUnit.SECONDS));
            assertTrue(inventoryStarted.await(1, TimeUnit.SECONDS));

            Deadline deadline = Deadline.after(Duration.ofMillis(200));

            while (!deadline.isExpired()) {
                Thread.sleep(10);
            }

            orderFuture.cancel(true);
            productFuture.cancel(true);
            inventoryFuture.cancel(true);

            assertTrue(orderFinished.await(2, TimeUnit.SECONDS));
            assertTrue(productFinished.await(2, TimeUnit.SECONDS));
            assertTrue(inventoryFinished.await(2, TimeUnit.SECONDS));

            System.out.println("C order cancelled = " + orderFuture.isCancelled());
            System.out.println("C product cancelled = " + productFuture.isCancelled());
            System.out.println("C inventory cancelled = " + inventoryFuture.isCancelled());

            System.out.println("C order finished = " + (orderFinished.getCount() == 0));
            System.out.println("C product finished = " + (productFinished.getCount() == 0));
            System.out.println("C inventory finished = " + (inventoryFinished.getCount() == 0));

            assertTrue(orderFuture.isCancelled());
            assertTrue(productFuture.isCancelled());
            assertTrue(inventoryFuture.isCancelled());

        } finally {
            orderExecutor.shutdownNow();
            productExecutor.shutdownNow();
            inventoryExecutor.shutdownNow();

            assertTrue(orderExecutor.awaitTermination(3, TimeUnit.SECONDS));
            assertTrue(productExecutor.awaitTermination(3, TimeUnit.SECONDS));
            assertTrue(inventoryExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景 D：
     * 取消慢任务后，线程池 Worker 可以继续处理新任务。
     */
    @Test
    void shouldReleaseWorkerAfterCancelledTask() throws Exception {

        ExecutorService executor = newExecutor("deadline-recovery-worker");

        CountDownLatch slowTaskStarted = new CountDownLatch(1);
        CountDownLatch slowTaskFinished = new CountDownLatch(1);

        AtomicBoolean slowTaskInterrupted = new AtomicBoolean(false);

        try {
            Future<?> slowFuture = executor.submit(() -> {
                slowTaskStarted.countDown();

                try {
                    Thread.sleep(30_000);
                } catch (InterruptedException e) {
                    slowTaskInterrupted.set(true);
                    Thread.currentThread().interrupt();
                } finally {
                    slowTaskFinished.countDown();
                }
            });

            assertTrue(slowTaskStarted.await(1, TimeUnit.SECONDS));

            slowFuture.cancel(true);

            assertTrue(slowTaskFinished.await(2, TimeUnit.SECONDS));

            Future<String> probeFuture = executor.submit(() -> {
                System.out.println("D probe thread = "
                        + Thread.currentThread().getName());
                return "PROBE-SUCCESS";
            });

            String result = probeFuture.get(2, TimeUnit.SECONDS);

            System.out.println("D slow task interrupted = "
                    + slowTaskInterrupted.get());
            System.out.println("D probe result = " + result);

            assertTrue(slowTaskInterrupted.get());
            assertEquals("PROBE-SUCCESS", result);

        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }
}
