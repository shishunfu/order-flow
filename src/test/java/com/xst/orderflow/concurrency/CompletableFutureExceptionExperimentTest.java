package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CompletableFutureExceptionExperimentTest {

    /**
     * 场景 A：exceptionally
     * 任务失败后返回备用结果。
     */
    @Test
    void exceptionallyShouldRecoverWithFallback()
        throws ExecutionException, InterruptedException {

        CompletableFuture<Object> future =
            CompletableFuture.supplyAsync(() -> {
                throw new IllegalStateException("Order service unavailable");
            }).exceptionally(ex -> {
                System.out.println("exceptionally exception = " + ex);
                return "DEFAULT-ORDER";
            });

        Object result = future.get();

        System.out.println("Recovered result = " + result);

        assertEquals("DEFAULT-ORDER", result);
        assertFalse(future.isCompletedExceptionally());
    }

    /**
     * 场景 B：handle
     * 无论成功还是失败都会执行。
     */
    @Test
    void handleShouldProcessSuccessAndFailure()
        throws ExecutionException, InterruptedException {

        CompletableFuture<String> successFuture =
            CompletableFuture.supplyAsync(
                () -> "Order-1001"
            ).handle((result, ex) -> {
                if (ex != null) {
                    return "FALLBACK";
                }
                return result + "-PROCESSED";
            });

        CompletableFuture<Object> failureFuture =
            CompletableFuture.supplyAsync(() -> {
                throw new IllegalArgumentException("Invalid order");
            }).handle((result, ex) -> {
                if (ex != null) {
                    System.out.println("handle exception = " + ex);
                    return "FALLBACK";
                }
                return result;
            });

        assertEquals("Order-1001-PROCESSED", successFuture.get());

        assertEquals("FALLBACK", failureFuture.get());

        System.out.println("Success handle result = " + successFuture.get());

        System.out.println("Failure handle result = " + failureFuture.get());
    }

    /**
     * 场景 C：whenComplete
     * 观察成功或失败，不主动恢复异常。
     */
    @Test
    void whenCompleteShouldObserveFailure()
        throws InterruptedException {

        AtomicReference<Throwable> observedException = new AtomicReference<>();

        CompletableFuture<String> originalFuture =
            CompletableFuture.supplyAsync(() -> {
                throw new IllegalStateException("Database timeout");
            });

        CompletableFuture<String> observedFuture =
            originalFuture.whenComplete((result, ex) -> {
                System.out.println("whenComplete result = " + result);
                System.out.println("whenComplete exception = " + ex);
                observedException.set(ex);
            });

        ExecutionException exception =
            assertThrows(
                ExecutionException.class,
                observedFuture::get
            );

        System.out.println("Final exception cause = " + exception.getCause());

        assertNotNull(observedException.get());
        assertInstanceOf(
            CompletionException.class,
            observedException.get()
        );

        assertTrue(observedFuture.isCompletedExceptionally());
    }

    /**
     * 场景 D：whenComplete 后继续异常传播
     * 验证观察异常并不等于恢复异常。
     */
    @Test
    void whenCompleteShouldNotRecoverException()
        throws InterruptedException {

        CompletableFuture<Object> future =
            CompletableFuture.supplyAsync(() -> {
                throw new IllegalStateException("Payment failed");
            }).whenComplete((result, ex) -> {
                System.out.println("Observed payment exception = " + ex);
            });

        ExecutionException exception =
            assertThrows(
                ExecutionException.class,
                future::get
            );

        assertInstanceOf(
            IllegalStateException.class,
            exception.getCause()
        );

        assertTrue(future.isCompletedExceptionally());
    }

    /**
     * 场景 E：异常包装
     * 观察 CompletionException 与 ExecutionException。
     */
    @Test
    void shouldUnderstandExceptionWrapping()
        throws InterruptedException {

        CompletableFuture<String> future =
            CompletableFuture.supplyAsync(() -> {
                throw new IllegalArgumentException("Original business error");
            });

        CompletionException joinException =
            assertThrows(
                CompletionException.class,
                future::join
            );

        System.out.println("join exception = " + joinException);

        System.out.println("join root cause = " + joinException.getCause());

        ExecutionException getException =
            assertThrows(
                ExecutionException.class,
                future::get
            );

        System.out.println("get exception = " + getException);

        System.out.println("get root cause = " + getException.getCause());

        assertInstanceOf(
            IllegalArgumentException.class,
            joinException.getCause()
        );

        assertInstanceOf(
            IllegalArgumentException.class,
            getException.getCause()
        );
    }
}
