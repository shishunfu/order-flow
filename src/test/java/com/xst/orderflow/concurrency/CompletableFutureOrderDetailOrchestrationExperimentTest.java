package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class CompletableFutureOrderDetailOrchestrationExperimentTest {

    private ExecutorService newExecutor(String threadName) {
        return Executors.newSingleThreadExecutor(r ->
                new Thread(r, threadName));
    }

    private CompletableFuture<String> buildDetail(
            CompletableFuture<String> orderFuture,
            CompletableFuture<String> productFuture,
            CompletableFuture<String> inventoryFuture,
            long overallTimeoutMs) {

        // 商品信息允许降级。
        CompletableFuture<String> safeProductFuture =
                productFuture.exceptionally(exception -> {
                    System.out.println("Product fallback: " + exception);
                    return "PRODUCT-DEFAULT";
                });

        // 订单、库存是核心依赖，不做默认值降级。
        CompletableFuture<String> detailFuture =
                orderFuture.thenCombine(
                        safeProductFuture,
                        (order, product) -> order + " | " + product
                ).thenCombine(
                        inventoryFuture,
                        (partial, inventory) ->
                                partial + " | " + inventory
                );

        // 整个订单详情组合使用一个总超时期限。
        return detailFuture.orTimeout(
                overallTimeoutMs,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * EXP-024-A
     * 三个下游任务正常完成。
     */
    @Test
    void allServicesSuccessExperiment() throws Exception {

        ExecutorService orderExecutor = newExecutor("order-worker");
        ExecutorService productExecutor = newExecutor("product-worker");
        ExecutorService inventoryExecutor = newExecutor("inventory-worker");

        try {
            CompletableFuture<String> orderFuture =
                    CompletableFuture.supplyAsync(
                            () -> "ORDER-1001",
                            orderExecutor
                    );

            CompletableFuture<String> productFuture =
                    CompletableFuture.supplyAsync(
                            () -> "PRODUCT-2001",
                            productExecutor
                    );

            CompletableFuture<String> inventoryFuture =
                    CompletableFuture.supplyAsync(
                            () -> "INVENTORY-3001",
                            inventoryExecutor
                    );

            CompletableFuture<String> detailFuture = buildDetail(
                    orderFuture,
                    productFuture,
                    inventoryFuture,
                    800
            );

            String detail = detailFuture.get(2, TimeUnit.SECONDS);

            System.out.println("A detail = " + detail);

            assertEquals(
                    "ORDER-1001 | PRODUCT-2001 | INVENTORY-3001",
                    detail
            );

        } finally {
            shutdown(orderExecutor, productExecutor, inventoryExecutor);
        }
    }

    /**
     * EXP-024-B
     * 商品服务失败，使用默认信息降级。
     */
    @Test
    void productFailureFallbackExperiment() throws Exception {

        ExecutorService orderExecutor = newExecutor("order-worker");
        ExecutorService productExecutor = newExecutor("product-worker");
        ExecutorService inventoryExecutor = newExecutor("inventory-worker");

        try {
            CompletableFuture<String> orderFuture =
                    CompletableFuture.supplyAsync(
                            () -> "ORDER-1002",
                            orderExecutor
                    );

            CompletableFuture<String> productFuture =
                    CompletableFuture.supplyAsync(() -> {
                        throw new IllegalStateException(
                                "Product service unavailable"
                        );
                    }, productExecutor);

            CompletableFuture<String> inventoryFuture =
                    CompletableFuture.supplyAsync(
                            () -> "INVENTORY-3002",
                            inventoryExecutor
                    );

            String detail = buildDetail(
                    orderFuture,
                    productFuture,
                    inventoryFuture,
                    800
            ).get(2, TimeUnit.SECONDS);

            System.out.println("B detail = " + detail);

            assertEquals(
                    "ORDER-1002 | PRODUCT-DEFAULT | INVENTORY-3002",
                    detail
            );

        } finally {
            shutdown(orderExecutor, productExecutor, inventoryExecutor);
        }
    }

    /**
     * EXP-024-C
     * 库存服务超时，整体详情查询失败。
     */
    @Test
    void inventoryTimeoutExperiment() throws Exception {

        ExecutorService orderExecutor = newExecutor("order-worker");
        ExecutorService productExecutor = newExecutor("product-worker");
        ExecutorService inventoryExecutor = newExecutor("inventory-worker");

        CountDownLatch inventoryStarted = new CountDownLatch(1);
        CountDownLatch releaseInventory = new CountDownLatch(1);
        CountDownLatch inventoryFinished = new CountDownLatch(1);

        try {
            CompletableFuture<String> orderFuture =
                    CompletableFuture.supplyAsync(
                            () -> "ORDER-1003",
                            orderExecutor
                    );

            CompletableFuture<String> productFuture =
                    CompletableFuture.supplyAsync(
                            () -> "PRODUCT-2003",
                            productExecutor
                    );

            CompletableFuture<String> inventoryFuture =
                    CompletableFuture.supplyAsync(() -> {
                        inventoryStarted.countDown();

                        try {
                            releaseInventory.await();
                            return "INVENTORY-3003";
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } finally {
                            inventoryFinished.countDown();
                        }
                    }, inventoryExecutor);

            assertTrue(inventoryStarted.await(2, TimeUnit.SECONDS));

            // 库存依赖自己的超时期限。
            inventoryFuture.orTimeout(150, TimeUnit.MILLISECONDS);

            CompletableFuture<String> detailFuture = buildDetail(
                    orderFuture,
                    productFuture,
                    inventoryFuture,
                    800
            );

            ExecutionException exception = assertThrows(
                    ExecutionException.class,
                    () -> detailFuture.get(2, TimeUnit.SECONDS)
            );

            System.out.println("C detail cause = " + exception.getCause());

            assertInstanceOf(
                    TimeoutException.class,
                    exception.getCause()
            );

            System.out.println("C inventory task finished before release = "
                    + (inventoryFinished.getCount() == 0));

            assertFalse(inventoryFinished.getCount() == 0);

            // 超时并没有自动停止库存 supplier。
            releaseInventory.countDown();

            assertTrue(inventoryFinished.await(2, TimeUnit.SECONDS));

            System.out.println("C inventory task finished after release = "
                    + (inventoryFinished.getCount() == 0));

        } finally {
            releaseInventory.countDown();
            shutdown(orderExecutor, productExecutor, inventoryExecutor);
        }
    }

    /**
     * EXP-024-D
     * 整体订单详情查询超过总响应期限。
     */
    @Test
    void overallTimeoutExperiment() throws Exception {

        ExecutorService orderExecutor = newExecutor("order-worker");
        ExecutorService productExecutor = newExecutor("product-worker");
        ExecutorService inventoryExecutor = newExecutor("inventory-worker");

        CountDownLatch orderStarted = new CountDownLatch(1);
        CountDownLatch releaseOrder = new CountDownLatch(1);
        CountDownLatch orderFinished = new CountDownLatch(1);

        try {
            CompletableFuture<String> orderFuture =
                    CompletableFuture.supplyAsync(() -> {
                        orderStarted.countDown();

                        try {
                            releaseOrder.await();
                            return "ORDER-1004";
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } finally {
                            orderFinished.countDown();
                        }
                    }, orderExecutor);

            assertTrue(orderStarted.await(2, TimeUnit.SECONDS));

            CompletableFuture<String> productFuture =
                    CompletableFuture.supplyAsync(
                            () -> "PRODUCT-2004",
                            productExecutor
                    );

            CompletableFuture<String> inventoryFuture =
                    CompletableFuture.supplyAsync(
                            () -> "INVENTORY-3004",
                            inventoryExecutor
                    );

            CompletableFuture<String> detailFuture = buildDetail(
                    orderFuture,
                    productFuture,
                    inventoryFuture,
                    150
            );

            ExecutionException exception = assertThrows(
                    ExecutionException.class,
                    () -> detailFuture.get(2, TimeUnit.SECONDS)
            );

            System.out.println("D detail cause = " + exception.getCause());

            assertInstanceOf(
                    TimeoutException.class,
                    exception.getCause()
            );

            System.out.println("D order task finished before release = "
                    + (orderFinished.getCount() == 0));

            assertFalse(orderFinished.getCount() == 0);

            releaseOrder.countDown();

            assertTrue(orderFinished.await(2, TimeUnit.SECONDS));

            System.out.println("D order task finished after release = "
                    + (orderFinished.getCount() == 0));

        } finally {
            releaseOrder.countDown();
            shutdown(orderExecutor, productExecutor, inventoryExecutor);
        }
    }

    /**
     * EXP-024-E
     * 核心订单服务失败，不允许降级。
     */
    @Test
    void orderFailureMustFailExperiment() throws Exception {

        ExecutorService orderExecutor = newExecutor("order-worker");
        ExecutorService productExecutor = newExecutor("product-worker");
        ExecutorService inventoryExecutor = newExecutor("inventory-worker");

        try {
            CompletableFuture<String> orderFuture =
                    CompletableFuture.supplyAsync(() -> {
                        throw new IllegalStateException(
                                "Order service unavailable"
                        );
                    }, orderExecutor);

            CompletableFuture<String> productFuture =
                    CompletableFuture.supplyAsync(
                            () -> "PRODUCT-2005",
                            productExecutor
                    );

            CompletableFuture<String> inventoryFuture =
                    CompletableFuture.supplyAsync(
                            () -> "INVENTORY-3005",
                            inventoryExecutor
                    );

            CompletableFuture<String> detailFuture = buildDetail(
                    orderFuture,
                    productFuture,
                    inventoryFuture,
                    800
            );

            ExecutionException exception = assertThrows(
                    ExecutionException.class,
                    () -> detailFuture.get(2, TimeUnit.SECONDS)
            );

            System.out.println("E detail cause = " + exception.getCause());

            assertInstanceOf(
                    IllegalStateException.class,
                    exception.getCause()
            );

            assertTrue(detailFuture.isCompletedExceptionally());

        } finally {
            shutdown(orderExecutor, productExecutor, inventoryExecutor);
        }
    }

    private void shutdown(ExecutorService... executors)
            throws InterruptedException {

        for (ExecutorService executor : executors) {
            executor.shutdown();
        }

        for (ExecutorService executor : executors) {
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }
}
