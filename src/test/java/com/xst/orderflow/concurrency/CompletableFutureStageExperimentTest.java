package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CompletableFutureStageExperimentTest {

    /**
     * 场景 A：thenApply
     * 接收上一步结果，并转换为新的结果。
     */
    @Test
    void thenApplyShouldTransformPreviousResult()
        throws ExecutionException, InterruptedException {

        CompletableFuture<String> future =
            CompletableFuture.supplyAsync(() -> "Order-1001")
                .thenApply(order -> {
                    System.out.println(
                        "thenApply input = " + order);
                    return order.replace("Order-", "");
                });

        String result = future.get();

        System.out.println("thenApply result = " + result);

        assertEquals("1001", result);
    }

    /**
     * 场景 B：thenAccept
     * 接收上一步结果，只消费，不返回新业务结果。
     */
    @Test
    void thenAcceptShouldConsumePreviousResult()
        throws ExecutionException, InterruptedException {

        AtomicReference<String> consumed =
            new AtomicReference<>();

        CompletableFuture<Void> future =
            CompletableFuture.supplyAsync(() -> "Order-1002")
                .thenAccept(order -> {
                    System.out.println(
                        "thenAccept input = " + order);
                    consumed.set(order);
                });

        future.get();

        System.out.println("consumed = " + consumed.get());
        System.out.println("thenAccept result = " + future.get());

        assertEquals("Order-1002", consumed.get());
        assertNull(future.get());
    }

    /**
     * 场景 C：thenRun
     * 不接收上一步结果，只执行后续动作。
     */
    @Test
    void thenRunShouldExecuteWithoutPreviousResult()
        throws ExecutionException, InterruptedException {

        AtomicBoolean executed = new AtomicBoolean(false);

        CompletableFuture<Void> future =
            CompletableFuture.supplyAsync(() -> "Order-1003")
                .thenRun(() -> {
                    System.out.println(
                        "thenRun executed");
                    executed.set(true);
                });

        future.get();

        System.out.println("executed = " + executed.get());

        assertTrue(executed.get());
        assertNull(future.get());
    }

    /**
     * 场景 D：组合多个阶段
     * supplyAsync -> thenApply -> thenApply -> thenAccept
     */
    @Test
    void shouldComposeMultipleStages()
        throws ExecutionException, InterruptedException {

        AtomicReference<String> notification =
            new AtomicReference<>();

        CompletableFuture<Void> future =
            CompletableFuture.supplyAsync(() -> {
                    System.out.println("Stage 1: create order");
                    return "Order-1004";
                })
                .thenApply(order -> {
                    System.out.println("Stage 2: convert order");
                    return order.replace("Order-", "");
                })
                .thenApply(orderId -> {
                    System.out.println("Stage 3: build message");
                    return "Order created, id = " + orderId;
                })
                .thenAccept(message -> {
                    System.out.println("Stage 4: consume message");
                    notification.set(message);
                });

        future.get();

        System.out.println(
            "Final notification = " + notification.get());

        assertEquals(
            "Order created, id = 1004",
            notification.get()
        );
    }
}
