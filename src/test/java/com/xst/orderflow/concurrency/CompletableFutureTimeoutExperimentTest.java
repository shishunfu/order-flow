package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CompletableFutureTimeoutExperimentTest {

    /**
     * EXP-021-A
     * orTimeout：超时后 Future 异常完成。
     */
    @Test
    void orTimeoutExperiment() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);

        try {
            CompletableFuture<String> future =
                    CompletableFuture.supplyAsync(() -> {
                        taskStarted.countDown();

                        try {
                            releaseTask.await();
                            return "REAL-ORDER";
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } finally {
                            taskFinished.countDown();
                        }
                    }, executor);

            assertTrue(taskStarted.await(2, TimeUnit.SECONDS));

            // 超时后，这个 Future 会异常完成。
            CompletableFuture<String> timeoutFuture = future.orTimeout(100, TimeUnit.MILLISECONDS);

            assertSame(future, timeoutFuture);

            try {
                future.get(2, TimeUnit.SECONDS);
                fail("Expected timeout exception");
            } catch (ExecutionException e) {
                System.out.println("A cause = " + e.getCause());
                assertInstanceOf(TimeoutException.class, e.getCause());
            }

            System.out.println("A isCompletedExceptionally = " + future.isCompletedExceptionally());

            // 放行后台任务，观察任务是否仍然能够执行完毕。
            releaseTask.countDown();

            assertTrue(taskFinished.await(2, TimeUnit.SECONDS));

            System.out.println("A task finished = " + (taskFinished.getCount() == 0));
            System.out.println("A future result remains timeout = "
                    + future.isCompletedExceptionally());

        } finally {
            releaseTask.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * EXP-021-B
     * completeOnTimeout：超时后以默认值正常完成 Future。
     */
    @Test
    void completeOnTimeoutExperiment() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);

        try {
            CompletableFuture<String> future =
                    CompletableFuture.supplyAsync(() -> {
                        taskStarted.countDown();

                        try {
                            releaseTask.await();
                            System.out.println("B supplier really returned");
                            return "REAL-ORDER";
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } finally {
                            taskFinished.countDown();
                        }
                    }, executor);

            assertTrue(taskStarted.await(2, TimeUnit.SECONDS));

            CompletableFuture<String> timeoutFuture =
                    future.completeOnTimeout(
                            "DEFAULT-ORDER",
                            100,
                            TimeUnit.MILLISECONDS
                    );

            assertSame(future, timeoutFuture);

            String result = future.get(2, TimeUnit.SECONDS);

            System.out.println("B result = " + result);
            assertEquals("DEFAULT-ORDER", result);

            // 备用结果已经返回，但原 supplier 仍然可能在执行。
            System.out.println("B task finished before release = "
                    + (taskFinished.getCount() == 0));

            releaseTask.countDown();

            assertTrue(taskFinished.await(2, TimeUnit.SECONDS));

            System.out.println("B task finished after release = "
                    + (taskFinished.getCount() == 0));

            // supplier 后续返回 REAL-ORDER，也不会覆盖已经完成的 DEFAULT-ORDER。
            assertEquals("DEFAULT-ORDER", future.get());

        } finally {
            releaseTask.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * EXP-021-C
     * get(timeout)：只限制当前调用的等待时间。
     */
    @Test
    void getTimeoutExperiment() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);

        try {
            CompletableFuture<String> future =
                    CompletableFuture.supplyAsync(() -> {
                        taskStarted.countDown();

                        try {
                            releaseTask.await();
                            return "REAL-ORDER";
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }, executor);

            assertTrue(taskStarted.await(2, TimeUnit.SECONDS));

            try {
                future.get(100, TimeUnit.MILLISECONDS);
                fail("Expected waiting timeout");
            } catch (TimeoutException e) {
                System.out.println("C get timeout = " + e);
            }

            System.out.println("C future isDone after get timeout = "
                    + future.isDone());

            assertFalse(future.isDone());

            releaseTask.countDown();

            assertEquals("REAL-ORDER", future.get(2, TimeUnit.SECONDS));
            assertTrue(future.isDone());

        } finally {
            releaseTask.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * EXP-021-D
     * CompletableFuture.cancel(true)：
     * Future 被取消，但 mayInterruptIfRunning 不保证中断 supplier。
     */
    @Test
    void cancelExperiment() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);

        AtomicInteger interruptCount = new AtomicInteger();
        AtomicBoolean taskReallyFinished = new AtomicBoolean(false);

        try {
            CompletableFuture<String> future =
                    CompletableFuture.supplyAsync(() -> {
                        taskStarted.countDown();

                        try {
                            releaseTask.await();
                            return "REAL-ORDER";
                        } catch (InterruptedException e) {
                            interruptCount.incrementAndGet();
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } finally {
                            taskReallyFinished.set(true);
                            taskFinished.countDown();
                        }
                    }, executor);

            assertTrue(taskStarted.await(2, TimeUnit.SECONDS));

            boolean cancelled = future.cancel(true);

            System.out.println("D cancel returned = " + cancelled);
            System.out.println("D isCancelled = " + future.isCancelled());
            System.out.println("D isDone = " + future.isDone());

            assertTrue(cancelled);
            assertTrue(future.isCancelled());
            assertTrue(future.isDone());

            // 给任务一点时间，观察 cancel(true) 是否中断了 supplier。
            Thread.sleep(100);

            System.out.println("D interrupt count before release = "
                    + interruptCount.get());
            System.out.println("D task finished before release = "
                    + taskReallyFinished.get());

            assertEquals(0, interruptCount.get());
            assertFalse(taskReallyFinished.get());

            // 释放业务任务，让它正常结束。
            releaseTask.countDown();

            assertTrue(taskFinished.await(2, TimeUnit.SECONDS));

            System.out.println("D task really finished = "
                    + taskReallyFinished.get());
            System.out.println("D interrupt count after release = "
                    + interruptCount.get());

            assertTrue(taskReallyFinished.get());
            assertEquals(0, interruptCount.get());

            assertThrows(CancellationException.class, future::join);

        } finally {
            releaseTask.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }
}
