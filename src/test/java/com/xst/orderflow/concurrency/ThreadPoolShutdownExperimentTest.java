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
     * <p>
     * 验证点：
     * 1. 调用 shutdown() 后线程池进入 SHUTDOWN 状态，不再接受新任务
     * 2. 正在运行的任务会继续执行直到完成
     * 3. 队列中排队的任务也会被正常执行
     * 4. 所有任务完成后线程池最终进入 TERMINATED 状态
     */
    @Test
    void shutdownShouldFinishSubmittedTasks() throws Exception {
        // 创建核心线程数和最大线程数都为2的线程池，使用无界队列
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            2, 2,
            0, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadPoolExecutor.AbortPolicy()
        );

        // 用于等待两个工作线程都成功启动
        CountDownLatch started = new CountDownLatch(2);
        // 用于控制两个工作线程同时释放，模拟任务长时间运行的场景
        CountDownLatch release = new CountDownLatch(1);
        // 统计已完成的任务数量
        AtomicInteger completed = new AtomicInteger();

        try {
            // 提交两个任务，这两个任务会立即被核心线程执行
            for (int i = 1; i <= 2; i++) {
                int taskId = i;
                executor.execute(() -> {
                    // 标记任务已开始执行
                    started.countDown();
                    try {
                        // 阻塞等待外部信号，让任务保持运行状态
                        release.await();
                        System.out.println("Running task " + taskId + " finished");
                        completed.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            // 再提交两个任务，由于核心线程已满，这两个任务会进入阻塞队列排队
            for (int i = 3; i <= 4; i++) {
                int taskId = i;
                executor.execute(() -> {
                    System.out.println("Queued task " + taskId + " finished");
                    completed.incrementAndGet();
                });
            }

            // 等待两个运行中的任务都成功启动
            assertTrue(started.await(3, TimeUnit.SECONDS));

            // 调用 shutdown() 发起优雅关闭
            executor.shutdown();

            System.out.println("shutdown called");
            System.out.println("isShutdown = " + executor.isShutdown());
            System.out.println("isTerminated = " + executor.isTerminated());
            System.out.println("queue size = " + executor.getQueue().size());

            // shutdown() 调用后线程池状态变为 SHUTDOWN
            assertTrue(executor.isShutdown());
            // 此时还有任务在运行，线程池尚未终止
            assertFalse(executor.isTerminated());

            // 验证 shutdown() 之后提交新任务会被拒绝
            assertThrows(
                RejectedExecutionException.class,
                () -> executor.execute(() ->
                    System.out.println("This task should be rejected"))
            );

            // 释放阻塞的两个运行任务
            release.countDown();

            // 等待线程池所有任务执行完毕
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));

            System.out.println("completed = " + completed.get());
            System.out.println("isTerminated = " + executor.isTerminated());

            // 所有4个任务都应该被完整执行
            assertEquals(4, completed.get());
            assertTrue(executor.isTerminated());

        } finally {
            // 确保无论测试结果如何都释放锁，避免线程永久阻塞
            release.countDown();
            // 尝试强制关闭，作为安全兜底
            executor.shutdownNow();
            executor.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    /**
     * 场景 B：
     * shutdownNow() 尝试中断运行任务，
     * 并返回尚未开始执行的排队任务。
     * <p>
     * 验证点：
     * 1. shutdownNow() 会尝试中断正在运行的任务（通过设置中断标志位）
     * 2. 队列中尚未开始执行的任务会被直接从队列中移除并返回
     * 3. 注意：shutdownNow() 不会自动将返回的 Future 任务标记为取消，需要手动处理
     */
    @Test
    void shutdownNowShouldInterruptRunningTaskAndDrainQueue()
        throws Exception {

        // 创建单线程线程池
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            1, 1,
            0, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadPoolExecutor.AbortPolicy()
        );

        // 等待工作线程成功启动
        CountDownLatch started = new CountDownLatch(1);
        // 等待运行中的任务收到中断信号
        CountDownLatch interrupted = new CountDownLatch(1);
        // 标记运行中的任务是否最终完成
        AtomicBoolean runningTaskFinished = new AtomicBoolean();

        Future<?> queuedFuture1 = null;
        Future<?> queuedFuture2 = null;

        try {
            // 提交第一个任务，它会立即被唯一的工作线程执行
            executor.execute(() -> {
                started.countDown();

                try {
                    // 任务在这里阻塞等待，直到被中断
                    new CountDownLatch(1).await();
                } catch (InterruptedException e) {
                    System.out.println("Running task received interrupt");
                    interrupted.countDown();
                    // 恢复线程的中断状态
                    Thread.currentThread().interrupt();
                } finally {
                    runningTaskFinished.set(true);
                    System.out.println("Running task finished");
                }
            });

            // 确保第一个任务已经开始运行
            assertTrue(started.await(3, TimeUnit.SECONDS));

            // 再提交两个任务，它们会进入等待队列排队
            queuedFuture1 = executor.submit(
                () -> System.out.println("Queued task 2 executed")
            );

            queuedFuture2 = executor.submit(
                () -> System.out.println("Queued task 3 executed")
            );

            System.out.println("Before shutdownNow:");
            System.out.println("queue size = " + executor.getQueue().size());

            // 调用 shutdownNow() 尝试立即关闭：中断运行任务 + 清空队列
            List<Runnable> notStartedTasks = executor.shutdownNow();

            System.out.println("shutdownNow returned tasks = "
                + notStartedTasks.size());

            // 打印被移除的任务类型，可以看到它们是 FutureTask 包装对象
            for (Runnable task : notStartedTasks) {
                System.out.println("Returned task type = "
                    + task.getClass().getName());
            }

            assertTrue(executor.isShutdown());
            // 队列中2个排队任务都应该被返回
            assertEquals(2, notStartedTasks.size());
            assertTrue(notStartedTasks.contains(queuedFuture1));
            assertTrue(notStartedTasks.contains(queuedFuture2));

            // 等待运行中的任务响应中断并退出
            assertTrue(interrupted.await(3, TimeUnit.SECONDS));
            assertTrue(runningTaskFinished.get());

            System.out.println("Future states before manual cancellation:");
            System.out.println("future1 isDone = " + queuedFuture1.isDone());
            System.out.println("future1 isCancelled = "
                + queuedFuture1.isCancelled());
            System.out.println("future2 isDone = " + queuedFuture2.isDone());
            System.out.println("future2 isCancelled = "
                + queuedFuture2.isCancelled());

            // shutdownNow 只是把 FutureTask 从队列中移除，
            // 但不会自动将这些 Future 标记为 cancelled，需要手动取消
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
     * <p>
     * 验证点：
     * 1. awaitTermination() 本身不会发起关闭操作，它只是阻塞等待直到线程池终止
     * 2. 如果在指定时间内线程池没有终止，方法会返回 false
     * 3. 必须先调用 shutdown() 或 shutdownNow()，线程池才会开始关闭流程
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
                    // 任务在这里阻塞，等待外部释放信号
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            // 确保任务已经开始运行
            assertTrue(started.await(3, TimeUnit.SECONDS));

            // 先发起优雅关闭
            executor.shutdown();

            // 此时任务仍在阻塞中，等待100ms后应该超时返回false
            boolean terminated = executor.awaitTermination(
                100, TimeUnit.MILLISECONDS
            );

            System.out.println("First awaitTermination = " + terminated);
            System.out.println("isShutdown = " + executor.isShutdown());
            System.out.println("isTerminated = " + executor.isTerminated());

            // 100ms等待超时，线程池尚未终止
            assertFalse(terminated);
            assertTrue(executor.isShutdown());
            assertFalse(executor.isTerminated());

            // 释放阻塞的任务，让它执行完成
            release.countDown();

            // 再次等待，这次线程池应该能在3秒内正常终止
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
