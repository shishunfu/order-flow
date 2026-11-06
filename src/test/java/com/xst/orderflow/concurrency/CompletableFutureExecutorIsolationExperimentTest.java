package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CompletableFutureExecutorIsolationExperimentTest {

    /**
     * EXP-022-A
     * 不指定 Executor：观察默认异步执行线程。
     */
    @Test
    void defaultExecutorExperiment() throws Exception {

        String callerThread = Thread.currentThread().getName();

        CompletableFuture<String> future =
            CompletableFuture.supplyAsync(() -> {
                String workerThread = Thread.currentThread().getName();

                System.out.println("A caller thread = " + callerThread);
                System.out.println("A worker thread = " + workerThread);

                return workerThread;
            });

        String workerThread = future.get(3, TimeUnit.SECONDS);

        assertNotEquals(callerThread, workerThread);
        assertTrue(
            workerThread.startsWith("ForkJoinPool.commonPool-worker")
                || workerThread.equals("Thread-0")
                || workerThread.startsWith("Thread-")
        );

        System.out.println("A result = " + workerThread);
    }

    /**
     * EXP-022-B
     * thenApply 与 thenApplyAsync 的线程执行差异。
     */
    @Test
    void stageExecutionThreadExperiment() throws Exception {

        ExecutorService executor =
            Executors.newSingleThreadExecutor(r ->
                new Thread(r, "order-flow-worker")
            );

        try {
            AtomicReference<String> thenApplyThread = new AtomicReference<>();

            AtomicReference<String> thenApplyAsyncThread = new AtomicReference<>();

            CompletableFuture<String> source =
                CompletableFuture.supplyAsync(() -> {
                    System.out.println("B supplyAsync thread = " + Thread.currentThread().getName());
                    return "Order-1001";
                }, executor);

            CompletableFuture<String> normalStage =
                source.thenApply(order -> {
                    String thread = Thread.currentThread().getName();
                    thenApplyThread.set(thread);

                    System.out.println("B thenApply thread = " + thread);

                    return order + "-PROCESSED";
                });

            CompletableFuture<String> asyncStage =
                source.thenApplyAsync(order -> {
                    String thread = Thread.currentThread().getName();
                    thenApplyAsyncThread.set(thread);

                    System.out.println("B thenApplyAsync thread = " + thread);

                    return order + "-ASYNC-PROCESSED";
                });

            assertEquals("Order-1001-PROCESSED",
                normalStage.get(3, TimeUnit.SECONDS));

            assertEquals("Order-1001-ASYNC-PROCESSED",
                asyncStage.get(3, TimeUnit.SECONDS));

            assertEquals("order-flow-worker", thenApplyThread.get());

            assertTrue(thenApplyAsyncThread.get().startsWith(
                "ForkJoinPool.commonPool-worker"
            ));

        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * EXP-022-C
     * thenApplyAsync 指定自定义 Executor。
     */
    @Test
    void customExecutorExperiment() throws Exception {

        ExecutorService executor =
            Executors.newFixedThreadPool(2, r ->
                new Thread(r, "order-detail-worker")
            );

        try {
            CompletableFuture<String> future =
                CompletableFuture.supplyAsync(
                    () -> "Order-1002"
                ).thenApplyAsync(order -> {
                    String thread = Thread.currentThread().getName();

                    System.out.println("C processing thread = " + thread);

                    return order + "-DETAIL";
                }, executor);

            String result = future.get(3, TimeUnit.SECONDS);

            assertEquals("Order-1002-DETAIL", result);

            System.out.println("C result = " + result);

        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * EXP-022-D
     * 两个业务线程池相互隔离。
     * <p>
     * 订单池中的任务被阻塞时，商品池仍应能够完成任务。
     */
    @Test
    void businessExecutorIsolationExperiment() throws Exception {

        ExecutorService orderExecutor =
            Executors.newSingleThreadExecutor(r ->
                new Thread(r, "order-query-worker")
            );

        ExecutorService productExecutor =
            Executors.newSingleThreadExecutor(r ->
                new Thread(r, "product-query-worker")
            );

        CountDownLatch orderStarted = new CountDownLatch(1);
        CountDownLatch releaseOrder = new CountDownLatch(1);

        try {
            CompletableFuture<String> orderFuture =
                CompletableFuture.supplyAsync(() -> {

                    orderStarted.countDown();

                    try {
                        releaseOrder.await();
                        return "Order-1003";
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }

                }, orderExecutor);

            assertTrue(orderStarted.await(2, TimeUnit.SECONDS));

            CompletableFuture<String> productFuture =
                CompletableFuture.supplyAsync(() -> {

                    System.out.println("D product thread = "
                        + Thread.currentThread().getName());

                    return "Product-2003";

                }, productExecutor);

            // 订单池任务仍被阻塞，但商品池应该独立完成。
            String product = productFuture.get(2, TimeUnit.SECONDS);

            System.out.println("D product result = " + product);
            System.out.println("D order future done before release = "
                + orderFuture.isDone());

            assertEquals("Product-2003", product);
            assertFalse(orderFuture.isDone());

            releaseOrder.countDown();

            String order = orderFuture.get(2, TimeUnit.SECONDS);

            System.out.println("D order result = " + order);

            assertEquals("Order-1003", order);

        } finally {
            releaseOrder.countDown();

            orderExecutor.shutdown();
            productExecutor.shutdown();

            assertTrue(orderExecutor.awaitTermination(
                3, TimeUnit.SECONDS));

            assertTrue(productExecutor.awaitTermination(
                3, TimeUnit.SECONDS));
        }
    }
}
