package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ThreadPoolExceptionHandlingExperimentTest {

    private static final RuntimeException BUSINESS_EXCEPTION =
            new IllegalStateException("Simulated business failure");

    @Test
    void compareExecuteSubmitAndAfterExecute() throws Exception {
        System.out.println("\n========== EXP-013 Environment ==========");
        System.out.println("AvailableProcessors: "
                + Runtime.getRuntime().availableProcessors());

        experimentAExecute();
        experimentBSubmitWithoutGet();
        experimentCSubmitWithGet();
        experimentDAfterExecute();
    }

    /**
     * A：execute() 提交任务，异常未捕获。
     */
    private void experimentAExecute() throws Exception {
        System.out.println("\n========== EXP-013-A execute() ==========");

        CountDownLatch exceptionHandled = new CountDownLatch(1);
        AtomicInteger uncaughtCount = new AtomicInteger();

        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "exp013-execute-worker");

            thread.setUncaughtExceptionHandler((t, e) -> {
                uncaughtCount.incrementAndGet();

                System.out.println("UncaughtExceptionHandler thread="
                        + t.getName());
                System.out.println("Uncaught exception="
                        + e.getClass().getSimpleName() + ": "
                        + e.getMessage());

                exceptionHandled.countDown();
            });

            return thread;
        };

        ExecutorService executor = new ThreadPoolExecutor(
                1, 1,
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                factory
        );

        try {
            executor.execute(() -> {
                System.out.println("execute task running");
                throw new IllegalStateException("execute failure");
            });

            assertTrue(exceptionHandled.await(3, TimeUnit.SECONDS));

            System.out.println("Uncaught count=" + uncaughtCount.get());

            assertEquals(1, uncaughtCount.get());
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }

        System.out.println("========== EXP-013-A Finished ==========");
    }

    /**
     * B：submit() 提交任务，不调用 Future.get()。
     */
    private void experimentBSubmitWithoutGet() throws Exception {
        System.out.println("\n========== EXP-013-B submit() without get() ==========");

        CountDownLatch taskFinished = new CountDownLatch(1);
        AtomicInteger uncaughtCount = new AtomicInteger();

        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "exp013-submit-no-get");

            thread.setUncaughtExceptionHandler((t, e) -> {
                uncaughtCount.incrementAndGet();
                System.out.println("Unexpected uncaught exception="
                        + e.getMessage());
            });

            return thread;
        };

        ExecutorService executor = new ThreadPoolExecutor(
                1, 1,
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                factory
        );

        try {
            Future<?> future = executor.submit(() -> {
                try {
                    System.out.println("submit task running");
                    throw new IllegalStateException("submit failure");
                } finally {
                    taskFinished.countDown();
                }
            });

            assertTrue(taskFinished.await(3, TimeUnit.SECONDS));

            System.out.println("Future isDone=" + future.isDone());
            System.out.println("Future isCancelled=" + future.isCancelled());
            System.out.println("Uncaught count=" + uncaughtCount.get());

            assertTrue(future.isDone());
            assertFalse(future.isCancelled());
            assertEquals(0, uncaughtCount.get());
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }

        System.out.println("========== EXP-013-B Finished ==========");
    }

    /**
     * C：submit() 提交任务，通过 Future.get() 获取异常。
     */
    private void experimentCSubmitWithGet() throws Exception {
        System.out.println("\n========== EXP-013-C submit() with get() ==========");

        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            Future<?> future = executor.submit(() -> {
                System.out.println("submit task with get running");
                throw new IllegalArgumentException("get failure");
            });

            ExecutionException executionException = assertThrows(
                    ExecutionException.class,
                    future::get
            );

            System.out.println("Future isDone=" + future.isDone());
            System.out.println("Exception from get="
                    + executionException.getClass().getSimpleName());
            System.out.println("Root cause="
                    + executionException.getCause().getClass().getSimpleName()
                    + ": " + executionException.getCause().getMessage());

            assertInstanceOf(
                    IllegalArgumentException.class,
                    executionException.getCause()
            );
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }

        System.out.println("========== EXP-013-C Finished ==========");
    }

    /**
     * D：通过 ThreadPoolExecutor.afterExecute() 观察异常。
     */
    private void experimentDAfterExecute() throws Exception {
        System.out.println("\n========== EXP-013-D afterExecute() ==========");

        CountDownLatch afterExecuteCalled = new CountDownLatch(1);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1,
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>()
        ) {
            @Override
            protected void afterExecute(Runnable runnable, Throwable throwable) {
                super.afterExecute(runnable, throwable);

                System.out.println("afterExecute runnable="
                        + runnable.getClass().getSimpleName());
                System.out.println("afterExecute throwable="
                        + (throwable == null
                        ? "null"
                        : throwable.getClass().getSimpleName()));

                if (runnable instanceof Future<?> future
                        && future.isDone()
                        && !future.isCancelled()) {
                    try {
                        future.get();
                    } catch (ExecutionException e) {
                        System.out.println("afterExecute root cause="
                                + e.getCause().getClass().getSimpleName()
                                + ": " + e.getCause().getMessage());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (CancellationException e) {
                        System.out.println("Task was cancelled");
                    }
                }

                afterExecuteCalled.countDown();
            }
        };

        try {
            executor.submit(() -> {
                System.out.println("afterExecute test task running");
                throw new IllegalStateException("afterExecute failure");
            });

            assertTrue(afterExecuteCalled.await(3, TimeUnit.SECONDS));
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }

        System.out.println("========== EXP-013-D Finished ==========");
    }
}
