package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadPoolMetricsExperimentTest {

    private static final int TASK_COUNT = 10;

    @Test
    void shouldMonitorThreadPoolRuntimeMetrics() throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2,
                2,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(8),
                new ThreadPoolExecutor.AbortPolicy()
        );

        // 确保两个工作线程都已启动并进入任务。
        CountDownLatch workersStarted = new CountDownLatch(2);

        // 控制工作线程，先制造稳定的任务堆积状态。
        CountDownLatch releaseWorkers = new CountDownLatch(1);

        // 等待所有任务完成。
        CountDownLatch completed = new CountDownLatch(TASK_COUNT);

        AtomicInteger rejectedCount = new AtomicInteger();

        ConcurrentLinkedQueue<Long> waitTimes = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Long> executionTimes = new ConcurrentLinkedQueue<>();

        try {
            for (int i = 1; i <= TASK_COUNT; i++) {
                final int taskId = i;
                final long submitTime = System.nanoTime();

                try {
                    executor.execute(() -> {
                        long startTime = System.nanoTime();

                        if (taskId <= 2) {
                            workersStarted.countDown();
                        }

                        try {
                            // 前两个任务占住工作线程，制造排队。
                            if (taskId <= 2) {
                                releaseWorkers.await();
                            }

                            long executionStart = System.nanoTime();

                            // 模拟业务执行耗时。
                            Thread.sleep(500);

                            long endTime = System.nanoTime();

                            waitTimes.add(
                                    TimeUnit.NANOSECONDS.toMillis(startTime - submitTime)
                            );

                            executionTimes.add(
                                    TimeUnit.NANOSECONDS.toMillis(endTime - executionStart)
                            );

                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            completed.countDown();
                        }
                    });
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    rejectedCount.incrementAndGet();
                    completed.countDown();
                }

                // 提交前两个任务后，等待它们确实占住工作线程。
                if (i == 2) {
                    assertTrue(
                            workersStarted.await(5, TimeUnit.SECONDS),
                            "两个工作线程未能及时启动"
                    );
                }
            }

            // 采样点 A：两个线程被占用，其他任务已进入队列。
            printMetrics("任务堆积时", executor, rejectedCount.get());

            // 放开两个工作线程。
            releaseWorkers.countDown();

            assertTrue(
                    completed.await(15, TimeUnit.SECONDS),
                    "任务未能在规定时间内完成"
            );

            executor.shutdown();

            assertTrue(
                    executor.awaitTermination(5, TimeUnit.SECONDS),
                    "线程池未能正常关闭"
            );

            // 采样点 B：所有任务完成，线程池已关闭。
            printMetrics("任务完成后", executor, rejectedCount.get());

            printTimingMetrics(waitTimes, executionTimes);

            // 本实验容量恰好容纳 8 个排队任务，预期无拒绝。
            assertEquals(0, rejectedCount.get());
            assertEquals(TASK_COUNT, executor.getCompletedTaskCount());
            assertEquals(TASK_COUNT, executor.getTaskCount());

        } finally {
            // 避免断言失败或异常时，工作线程一直被闸门阻塞。
            releaseWorkers.countDown();

            if (!executor.isShutdown()) {
                executor.shutdownNow();
            }
        }
    }

    private void printMetrics(
            String stage,
            ThreadPoolExecutor executor,
            int rejectedCount
    ) {
        System.out.println();
        System.out.println("========== " + stage + " ==========");
        System.out.println("PoolSize             : " + executor.getPoolSize());
        System.out.println("ActiveCount          : " + executor.getActiveCount());
        System.out.println("CorePoolSize         : " + executor.getCorePoolSize());
        System.out.println("MaximumPoolSize      : " + executor.getMaximumPoolSize());
        System.out.println("LargestPoolSize      : " + executor.getLargestPoolSize());
        System.out.println("QueueSize            : " + executor.getQueue().size());
        System.out.println("QueueRemainingCapacity: " + executor.getQueue().remainingCapacity());
        System.out.println("TaskCount            : " + executor.getTaskCount());
        System.out.println("CompletedTaskCount   : " + executor.getCompletedTaskCount());
        System.out.println("RejectedCount        : " + rejectedCount);
    }

    private void printTimingMetrics(
            ConcurrentLinkedQueue<Long> waitTimes,
            ConcurrentLinkedQueue<Long> executionTimes
    ) {
        printAverageAndMax("任务等待时间", waitTimes);
        printAverageAndMax("任务执行时间", executionTimes);
    }

    private void printAverageAndMax(String name, ConcurrentLinkedQueue<Long> values) {
        List<Long> list = List.copyOf(values);

        if (list.isEmpty()) {
            System.out.println(name + "：无数据");
            return;
        }

        double average = list.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0);

        long max = list.stream()
                .mapToLong(Long::longValue)
                .max()
                .orElse(0);

        System.out.printf(
                "%s：样本数=%d，平均值=%.2fms，最大值=%dms%n",
                name, list.size(), average, max
        );
    }
}
