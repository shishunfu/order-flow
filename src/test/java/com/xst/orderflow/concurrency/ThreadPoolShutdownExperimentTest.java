package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ThreadPoolShutdownExperimentTest {

    /**
     * 场景 A：
     * shutdown() 后，已经提交的任务继续执行，
     * 但线程池不再接受新任务。
     */
    @Test
    void shutdownShouldFinishSubmittedTasks() throws Exception {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2, 2,
                0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadPoolExecutor.AbortPolicy()
        );

        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger completed = new AtomicInteger();

        try {
            // 提交两个正在运行的任务
            for (int i = 1; i <= 2; i++) {
                int taskId = i;
                executor.execute(() -> {
                    started.countDown();
                    try {
                        release.await();
                        System.out.println("Running task " + taskId + " finished");
                        completed.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            // 提交两个排队任务
            for (int i = 3; i <= 4; i++) {
                int taskId = i;
                executor.execute(() -> {
                    System.out.println("Queued task " + taskId + " finished");
                    completed.incrementAndGet();
                });
            }

            assertTrue(started.await(3, TimeUnit.SECONDS));

            executor.shutdown();

            System.out.println("shutdown called");
            System.out.println("isShutdown = " + executor.isShutdown());
            System.out.println("isTerminated = " + executor.isTerminated());
            System.out.println("queue size = " + executor.getQueue().size());

            assertTrue(executor.isShutdown());
            assertFalse(executor.isTerminated());

            // shutdown 后不允许提交新任务
            assertThrows(
                    RejectedExecutionException.class,
                    () -> executor.execute(() ->
                            System.out.println("This task should be rejected"))
            );

            // 释放两个正在运行的任务
            release.countDown();

            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));

            System.out.println("completed = " + completed.get());
            System.out.println("isTerminated = " + executor.isTerminated());

            assertEquals(4, completed.get());
            assertTrue(executor.isTerminated());

        } finally {
            release.countDown();
            executor.shutdownNow();
            executor.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    /**
     * 场景 B：
     * shutdownNow() 尝试中断运行任务，
     * 并返回尚未开始执行的排队任务。
     */
    @Test
    void shutdownNowShouldInterruptRunningTaskAndDrainQueue()
            throws Exception {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1,
                0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadPoolExecutor.AbortPolicy()
        );

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        AtomicBoolean runningTaskFinished = new AtomicBoolean();

        Future<?> queuedFuture1 = null;
        Future<?> queuedFuture2 = null;

        try {
            // 任务1：运行中，等待中断
            executor.execute(() -> {
                started.countDown();

                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException e) {
                    System.out.println("Running task received interrupt");
                    interrupted.countDown();
                    Thread.currentThread().interrupt();
                } finally {
                    runningTaskFinished.set(true);
                    System.out.println("Running task finished");
                }
            });

            assertTrue(started.await(3, TimeUnit.SECONDS));

            // 任务2、3：进入等待队列
            queuedFuture1 = executor.submit(
                    () -> System.out.println("Queued task 2 executed")
            );

            queuedFuture2 = executor.submit(
                    () -> System.out.println("Queued task 3 executed")
            );

            System.out.println("Before shutdownNow:");
            System.out.println("queue size = " + executor.getQueue().size());

            // 尝试立即关闭
            List<Runnable> notStartedTasks = executor.shutdownNow();

            System.out.println("shutdownNow returned tasks = "
                    + notStartedTasks.size());

            for (Runnable task : notStartedTasks) {
                System.out.println("Returned task type = "
                        + task.getClass().getName());
            }

            assertTrue(executor.isShutdown());
            assertEquals(2, notStartedTasks.size());
            assertTrue(notStartedTasks.contains(queuedFuture1));
            assertTrue(notStartedTasks.contains(queuedFuture2));

            // 等待运行任务响应中断
            assertTrue(interrupted.await(3, TimeUnit.SECONDS));
            assertTrue(runningTaskFinished.get());

            System.out.println("Future states before manual cancellation:");
            System.out.println("future1 isDone = " + queuedFuture1.isDone());
            System.out.println("future1 isCancelled = "
                    + queuedFuture1.isCancelled());
            System.out.println("future2 isDone = " + queuedFuture2.isDone());
            System.out.println("future2 isCancelled = "
                    + queuedFuture2.isCancelled());

            // shutdownNow 从队列移除 FutureTask，
            // 但不会自动将这些 Future 标记为 cancelled。
            queuedFuture1.cancel(false);
            queuedFuture2.cancel(false);

            System.out.println("Future states after manual cancellation:");
            System.out.println("future1 isCancelled = "
                    + queuedFuture1.isCancelled());
            System.out.println("future2 isCancelled = "
                    + queuedFuture2.isCancelled());

            assertTrue(queuedFuture1.isCancelled());
            assertTrue(queuedFuture2.isCancelled());

            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));

        } finally {
            executor.shutdownNow();
            executor.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    /**
     * 场景 C：
     * awaitTermination 只负责等待，
     * 不会自动触发线程池关闭。
     */
    @Test
    void awaitTerminationShouldOnlyWait() throws Exception {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1,
                0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadPoolExecutor.AbortPolicy()
        );

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        try {
            executor.execute(() -> {
                started.countDown();

                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            assertTrue(started.await(3, TimeUnit.SECONDS));

            // 先发起关闭
            executor.shutdown();

            // 任务仍被阻塞，等待100ms后应超时
            boolean terminated = executor.awaitTermination(
                    100, TimeUnit.MILLISECONDS
            );

            System.out.println("First awaitTermination = " + terminated);
            System.out.println("isShutdown = " + executor.isShutdown());
            System.out.println("isTerminated = " + executor.isTerminated());

            assertFalse(terminated);
            assertTrue(executor.isShutdown());
            assertFalse(executor.isTerminated());

            // 释放任务
            release.countDown();

            boolean terminatedAfterRelease =
                    executor.awaitTermination(3, TimeUnit.SECONDS);

            System.out.println("Second awaitTermination = "
                    + terminatedAfterRelease);

            assertTrue(terminatedAfterRelease);
            assertTrue(executor.isTerminated());

        } finally {
            release.countDown();
            executor.shutdownNow();
            executor.awaitTermination(3, TimeUnit.SECONDS);
        }
    }
}
