package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class CompletableFutureCombineExperimentTest {

    /**
     * 场景 A：thenCombine
     * 两个独立任务并行执行，完成后合并结果。
     */
    @Test
    void thenCombineShouldMergeTwoResults()
            throws ExecutionException, InterruptedException {

        long start = System.nanoTime();

        CompletableFuture<String> orderFuture =
                CompletableFuture.supplyAsync(() -> {
                    sleep(300);
                    System.out.println("Order query finished");
                    return "Order-1001";
                });

        CompletableFuture<String> productFuture =
                CompletableFuture.supplyAsync(() -> {
                    sleep(300);
                    System.out.println("Product query finished");
                    return "Product-2001";
                });

        CompletableFuture<String> combinedFuture =
                orderFuture.thenCombine(
                        productFuture,
                        (order, product) -> {
                            System.out.println("Combining results");
                            return order + " + " + product;
                        }
                );

        String result = combinedFuture.get();

        long durationMs =
                TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime() - start);

        System.out.println("Combined result = " + result);
        System.out.println("Parallel duration = " + durationMs + "ms");

        assertEquals(
                "Order-1001 + Product-2001",
                result
        );

        // 两个任务各等待约 300ms，合理情况下总时间明显低于 600ms。
        assertTrue(durationMs < 550,
                "Expected parallel execution, actual = " + durationMs);
    }

    /**
     * 场景 B：allOf
     * 等待三个任务全部完成，再分别读取结果。
     */
    @Test
    void allOfShouldWaitForAllTasks()
            throws ExecutionException, InterruptedException {

        AtomicLong completedCount = new AtomicLong();

        CompletableFuture<String> orderFuture =
                CompletableFuture.supplyAsync(() -> {
                    sleep(200);
                    completedCount.incrementAndGet();
                    System.out.println("Order task completed");
                    return "Order-1002";
                });

        CompletableFuture<String> productFuture =
                CompletableFuture.supplyAsync(() -> {
                    sleep(300);
                    completedCount.incrementAndGet();
                    System.out.println("Product task completed");
                    return "Product-2002";
                });

        CompletableFuture<String> customerFuture =
                CompletableFuture.supplyAsync(() -> {
                    sleep(400);
                    completedCount.incrementAndGet();
                    System.out.println("Customer task completed");
                    return "Customer-3002";
                });

        CompletableFuture<Void> allFuture =
                CompletableFuture.allOf(
                        orderFuture,
                        productFuture,
                        customerFuture
                );

        allFuture.get();

        System.out.println("All tasks completed");
        System.out.println("Completed count = "
                + completedCount.get());

        System.out.println("Order = " + orderFuture.get());
        System.out.println("Product = " + productFuture.get());
        System.out.println("Customer = " + customerFuture.get());

        assertEquals(3, completedCount.get());
        assertTrue(allFuture.isDone());
        assertEquals("Order-1002", orderFuture.get());
        assertEquals("Product-2002", productFuture.get());
        assertEquals("Customer-3002", customerFuture.get());
    }

    /**
     * 场景 C：anyOf
     * 两个任务中，先完成的任务触发后续处理。
     */
    @Test
    void anyOfShouldReturnFirstCompletedResult()
            throws ExecutionException, InterruptedException {

        CompletableFuture<String> fastFuture =
                CompletableFuture.supplyAsync(() -> {
                    sleep(100);
                    System.out.println("Fast task completed");
                    return "Fast-Result";
                });

        CompletableFuture<String> slowFuture =
                CompletableFuture.supplyAsync(() -> {
                    sleep(500);
                    System.out.println("Slow task completed");
                    return "Slow-Result";
                });

        CompletableFuture<Object> anyFuture =
                CompletableFuture.anyOf(
                        fastFuture,
                        slowFuture
                );

        Object result = anyFuture.get();

        System.out.println("Any result = " + result);

        assertEquals("Fast-Result", result);
    }

    /**
     * 场景 D：allOf 汇总三个异步结果
     * 模拟订单详情聚合。
     */
    @Test
    void allOfShouldAggregateOrderDetail()
            throws ExecutionException, InterruptedException {

        CompletableFuture<String> orderFuture =
                CompletableFuture.supplyAsync(
                        () -> "Order-1004"
                );

        CompletableFuture<String> productFuture =
                CompletableFuture.supplyAsync(
                        () -> "Product-2004"
                );

        CompletableFuture<String> customerFuture =
                CompletableFuture.supplyAsync(
                        () -> "Customer-3004"
                );

        CompletableFuture<String> detailFuture =
                CompletableFuture.allOf(
                        orderFuture,
                        productFuture,
                        customerFuture
                ).thenApply(ignored ->
                        "OrderDetail{"
                                + orderFuture.join()
                                + ", "
                                + productFuture.join()
                                + ", "
                                + customerFuture.join()
                                + "}"
                );

        String detail = detailFuture.get();

        System.out.println("Order detail = " + detail);

        assertEquals(
                "OrderDetail{Order-1004, Product-2004, Customer-3004}",
                detail
        );
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Task interrupted", e);
        }
    }
}
