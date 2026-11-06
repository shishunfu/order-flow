package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CompletableFutureRejectionExperimentTest {

    private ThreadPoolExecutor newExecutor() {
        return new ThreadPoolExecutor(
                1,
                1,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1),
                r -> new Thread(r, "order-flow-worker"),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    /**
     * EXP-023-A
     * 验证线程池拒绝任务时，supplyAsync 的提交行为。
     */
    @Test
    void submissionRejectionExperiment() throws Exception {

        ThreadPoolExecutor executor = newExecutor();

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);

        try {
            // 第一个任务占用唯一 Worker。
            CompletableFuture<String> firstFuture =
                    CompletableFuture.supplyAsync(() -> {
                        taskStarted.countDown();

                        try {
                            releaseTask.await();
                            return "ORDER";
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }, executor);

            assertTrue(taskStarted.await(2, TimeUnit.SECONDS));

            // 第二个任务进入唯一队列位置。
            CompletableFuture<String> secondFuture =
                    CompletableFuture.supplyAsync(
                            () -> "PRODUCT",
                            executor
                    );

            System.out.println("A active = " + executor.getActiveCount());
            System.out.println("A queue size = " + executor.getQueue().size());

            // 第三个任务提交时，Worker 和队列都已满。
            assertThrows(
                    RejectedExecutionException.class,
                    () -> CompletableFuture.supplyAsync(
                            () -> "INVENTORY",
                            executor
                    )
            );

            System.out.println("A rejected count = 1");

            releaseTask.countDown();

            assertEquals("ORDER", firstFuture.get(2, TimeUnit.SECONDS));
            assertEquals("PRODUCT", secondFuture.get(2, TimeUnit.SECONDS));

        } finally {
            releaseTask.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * EXP-023-B
     * 订单详情组合中，一个异步任务被拒绝。
     * 验证整体组合结果异常完成。
     */
    @Test
    void combinedFutureRejectionExperiment() throws Exception {

        ThreadPoolExecutor executor = newExecutor();

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);

        try {
            CompletableFuture<String> orderFuture =
                    CompletableFuture.supplyAsync(() -> {
                        taskStarted.countDown();

                        try {
                            releaseTask.await();
                            return "ORDER-1001";
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }, executor);

            assertTrue(taskStarted.await(2, TimeUnit.SECONDS));

            CompletableFuture<String> productFuture =
                    CompletableFuture.supplyAsync(
                            () -> "PRODUCT-2001",
                            executor
                    );

            // 将提交拒绝转换为异常完成的 Future，
            // 使其能够参与后续异步组合。
            CompletableFuture<String> inventoryFuture =
                    submitOrFailedFuture(
                            executor,
                            () -> "INVENTORY-3001"
                    );

            assertTrue(inventoryFuture.isCompletedExceptionally());

            CompletableFuture<String> detailFuture =
                    orderFuture
                            .thenCombine(productFuture,
                                    (order, product) ->
                                            order + " + " + product)
                            .thenCombine(inventoryFuture,
                                    (partial, inventory) ->
                                            partial + " + " + inventory);

            System.out.println("B order done = " + orderFuture.isDone());
            System.out.println("B product done = " + productFuture.isDone());
            System.out.println("B inventory exceptional = "
                    + inventoryFuture.isCompletedExceptionally());

            releaseTask.countDown();

            ExecutionException exception = assertThrows(
                    ExecutionException.class,
                    () -> detailFuture.get(2, TimeUnit.SECONDS)
            );

            System.out.println("B detail cause = " + exception.getCause());

            assertEquals("ORDER-1001",
                    orderFuture.get(2, TimeUnit.SECONDS));

            assertEquals("PRODUCT-2001",
                    productFuture.get(2, TimeUnit.SECONDS));

            assertTrue(detailFuture.isCompletedExceptionally());

        } finally {
            releaseTask.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * EXP-023-C
     * 对拒绝任务提供降级结果，完成订单详情组合。
     */
    @Test
    void rejectionFallbackExperiment() throws Exception {

        ThreadPoolExecutor executor = newExecutor();

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        AtomicInteger fallbackCount = new AtomicInteger();

        try {
            CompletableFuture<String> orderFuture =
                    CompletableFuture.supplyAsync(() -> {
                        taskStarted.countDown();

                        try {
                            releaseTask.await();
                            return "ORDER-1002";
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }, executor);

            assertTrue(taskStarted.await(2, TimeUnit.SECONDS));

            CompletableFuture<String> productFuture =
                    CompletableFuture.supplyAsync(
                            () -> "PRODUCT-2002",
                            executor
                    );

            CompletableFuture<String> inventoryFuture =
                    submitOrFailedFuture(
                            executor,
                            () -> "INVENTORY-3002"
                    ).exceptionally(exception -> {
                        fallbackCount.incrementAndGet();

                        System.out.println("C fallback cause = "
                                + exception);

                        return "INVENTORY-DEFAULT";
                    });

            CompletableFuture<String> detailFuture =
                    orderFuture
                            .thenCombine(productFuture,
                                    (order, product) ->
                                            order + " + " + product)
                            .thenCombine(inventoryFuture,
                                    (partial, inventory) ->
                                            partial + " + " + inventory);

            releaseTask.countDown();

            String detail = detailFuture.get(2, TimeUnit.SECONDS);

            System.out.println("C detail = " + detail);
            System.out.println("C fallback count = "
                    + fallbackCount.get());

            assertEquals(
                    "ORDER-1002 + PRODUCT-2002 + INVENTORY-DEFAULT",
                    detail
            );

            assertEquals(1, fallbackCount.get());

            assertFalse(detailFuture.isCompletedExceptionally());

        } finally {
            releaseTask.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * 将提交阶段发生的拒绝异常转换为异常完成的 Future。
     */
    private <T> CompletableFuture<T> submitOrFailedFuture(
            Executor executor,
            java.util.function.Supplier<T> supplier) {

        try {
            return CompletableFuture.supplyAsync(supplier, executor);
        } catch (RejectedExecutionException e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
