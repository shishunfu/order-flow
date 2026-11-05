package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class ThreadPoolTaskCancellationExperimentTest {

    private static final long NON_INTERRUPTIBLE_DURATION_MS = 2_000;

    @Test
    void compareInterruptibleAndNonInterruptibleTasks() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            runInterruptibleTaskExperiment(executor);
            runNonInterruptibleTaskExperiment(executor);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /**
     * 实验一：Thread.sleep() 是可中断阻塞操作。
     */
    private void runInterruptibleTaskExperiment(ExecutorService executor)
            throws Exception {

        System.out.println("\n========== EXP-012-A Interruptible Task ==========");

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean(false);

        Future<?> future = executor.submit(() -> {
            started.countDown();

            try {
                System.out.println("Interruptible task started: "
                        + Thread.currentThread().getName());

                Thread.sleep(30_000);

                System.out.println("Unexpected: sleep completed normally");
            } catch (InterruptedException e) {
                interrupted.set(true);

                System.out.println("Task received InterruptedException");

                // 恢复中断标志，避免吞掉中断信号
                Thread.currentThread().interrupt();
            } finally {
                finished.countDown();
                System.out.println("Interruptible task finished");
            }
        });

        assertTrue(started.await(3, TimeUnit.SECONDS));

        System.out.println("Before cancel: isDone=" + future.isDone());
        System.out.println("cancel(true) result=" + future.cancel(true));

        assertTrue(finished.await(3, TimeUnit.SECONDS));

        System.out.println("After cancel: isCancelled=" + future.isCancelled());
        System.out.println("After cancel: isDone=" + future.isDone());
        System.out.println("Task interrupted=" + interrupted.get());

        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
        assertTrue(interrupted.get());

        System.out.println("========== EXP-012-A Finished ==========");
    }

    /**
     * 实验二：CPU循环不主动响应中断。
     *
     * cancel(true) 会尝试中断执行线程，但任务本身不检查中断标志并退出。
     */
    private void runNonInterruptibleTaskExperiment(ExecutorService executor)
            throws Exception {

        System.out.println("\n========== EXP-012-B Non-interruptible Task ==========");

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);

        AtomicBoolean interruptObserved = new AtomicBoolean(false);
        AtomicLong actualFinishTime = new AtomicLong(0);

        Future<?> future = executor.submit(() -> {
            started.countDown();

            long start = System.nanoTime();
            long deadline = start
                    + TimeUnit.MILLISECONDS.toNanos(
                    NON_INTERRUPTIBLE_DURATION_MS);

            System.out.println("CPU task started: "
                    + Thread.currentThread().getName());

            // 故意不根据 interrupt 状态退出
            while (System.nanoTime() < deadline) {
                if (Thread.currentThread().isInterrupted()) {
                    interruptObserved.set(true);
                }

                // 模拟CPU计算，防止循环完全没有业务操作
                Math.sqrt(System.nanoTime());
            }

            actualFinishTime.set(System.nanoTime());
            finished.countDown();

            System.out.println("CPU task actually finished");
        });

        assertTrue(started.await(3, TimeUnit.SECONDS));

        long cancelTime = System.nanoTime();

        System.out.println("Before cancel: isDone=" + future.isDone());
        System.out.println("cancel(true) result=" + future.cancel(true));
        System.out.println("Immediately after cancel: isCancelled="
                + future.isCancelled());
        System.out.println("Immediately after cancel: isDone="
                + future.isDone());

        // Future 被取消，不代表底层任务已经停止。
        // 提交一个探测任务，观察单线程池何时能执行它。
        CountDownLatch probeStarted = new CountDownLatch(1);
        AtomicLong probeStartTime = new AtomicLong(0);

        executor.submit(() -> {
            probeStartTime.set(System.nanoTime());
            probeStarted.countDown();
            System.out.println("Probe task started: "
                    + Thread.currentThread().getName());
        });

        assertTrue(finished.await(5, TimeUnit.SECONDS));
        assertTrue(probeStarted.await(3, TimeUnit.SECONDS));

        long actualTaskDurationMs = TimeUnit.NANOSECONDS.toMillis(
                actualFinishTime.get() - cancelTime);

        long probeDelayMs = TimeUnit.NANOSECONDS.toMillis(
                probeStartTime.get() - cancelTime);

        System.out.println("Interrupt observed by CPU task="
                + interruptObserved.get());
        System.out.println("Future isCancelled=" + future.isCancelled());
        System.out.println("Future isDone=" + future.isDone());
        System.out.println("Time from cancel to actual task finish="
                + actualTaskDurationMs + " ms");
        System.out.println("Time from cancel to probe start="
                + probeDelayMs + " ms");

        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
        assertTrue(interruptObserved.get());

        System.out.println("========== EXP-012-B Finished ==========");
    }
}
