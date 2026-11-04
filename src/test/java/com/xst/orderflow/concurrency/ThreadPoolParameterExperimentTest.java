package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadPoolParameterExperimentTest {

    private static final int TASK_COUNT = 40;
    private static final long TASK_EXECUTION_MS = 200;

    @Test
    void shouldCompareThreadPoolParameters() throws InterruptedException {

        // A-D：固定队列容量，改变线程数
        runExperiment("A", 1, 40);
        runExperiment("B", 2, 40);
        runExperiment("C", 4, 40);
        runExperiment("D", 8, 40);

        // B、E、F：固定线程数，改变队列容量
        runExperiment("E", 2, 4);
        runExperiment("F", 2, 20);
    }

    private void runExperiment(
            String group,
            int workerCount,
            int queueCapacity
    ) throws InterruptedException {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                workerCount,
                workerCount,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                new ThreadPoolExecutor.AbortPolicy()
        );

        AtomicInteger submitted = new AtomicInteger();
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger executed = new AtomicInteger();

        long[] submitTimes = new long[TASK_COUNT];
        long[] startTimes = new long[TASK_COUNT];
        long[] endTimes = new long[TASK_COUNT];

        Arrays.fill(submitTimes, -1L);
        Arrays.fill(startTimes, -1L);
        Arrays.fill(endTimes, -1L);

        long experimentStart = System.nanoTime();

        try {
            for (int i = 0; i < TASK_COUNT; i++) {
                final int taskId = i;

                submitTimes[taskId] = System.nanoTime();
                submitted.incrementAndGet();

                Runnable task = () -> {
                    startTimes[taskId] = System.nanoTime();

                    try {
                        Thread.sleep(TASK_EXECUTION_MS);
                        executed.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        endTimes[taskId] = System.nanoTime();
                    }
                };

                try {
                    executor.execute(task);
                    accepted.incrementAndGet();
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    rejected.incrementAndGet();
                }
            }
        } finally {
            executor.shutdown();

            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();

                assertTrue(
                        executor.awaitTermination(5, TimeUnit.SECONDS),
                        "线程池未能在规定时间内结束"
                );
            }
        }

        long experimentEnd = System.nanoTime();

        long totalDurationMs =
                TimeUnit.NANOSECONDS.toMillis(
                        experimentEnd - experimentStart
                );

        long totalWaitMs = 0;
        long maxWaitMs = 0;
        long totalExecutionMs = 0;
        long maxExecutionMs = 0;
        int completedSamples = 0;

        for (int i = 0; i < TASK_COUNT; i++) {
            if (startTimes[i] < 0 || endTimes[i] < 0) {
                continue;
            }

            long waitMs = TimeUnit.NANOSECONDS.toMillis(
                    startTimes[i] - submitTimes[i]
            );

            long executionMs = TimeUnit.NANOSECONDS.toMillis(
                    endTimes[i] - startTimes[i]
            );

            totalWaitMs += waitMs;
            maxWaitMs = Math.max(maxWaitMs, waitMs);

            totalExecutionMs += executionMs;
            maxExecutionMs = Math.max(maxExecutionMs, executionMs);

            completedSamples++;
        }

        double averageWaitMs = completedSamples == 0
                ? 0
                : (double) totalWaitMs / completedSamples;

        double averageExecutionMs = completedSamples == 0
                ? 0
                : (double) totalExecutionMs / completedSamples;

        double throughput = totalDurationMs == 0
                ? 0
                : executed.get() * 1000.0 / totalDurationMs;

        System.out.println();
        System.out.println("========== Group " + group + " ==========");
        System.out.println("CorePoolSize       : " + workerCount);
        System.out.println("MaximumPoolSize    : " + workerCount);
        System.out.println("QueueCapacity      : " + queueCapacity);
        System.out.println("Submitted          : " + submitted.get());
        System.out.println("Accepted           : " + accepted.get());
        System.out.println("Rejected           : " + rejected.get());
        System.out.println("Executed           : " + executed.get());
        System.out.println("CompletedSamples   : " + completedSamples);
        System.out.println("AverageWait        : " + String.format("%.2f", averageWaitMs) + " ms");
        System.out.println("MaxWait            : " + maxWaitMs + " ms");
        System.out.println("AverageExecution   : " + String.format("%.2f", averageExecutionMs) + " ms");
        System.out.println("MaxExecution       : " + maxExecutionMs + " ms");
        System.out.println("TotalDuration      : " + totalDurationMs + " ms");
        System.out.println("ActualThroughput   : " + String.format("%.2f", throughput) + " tasks/s");

        assertEquals(TASK_COUNT, submitted.get());
        assertEquals(TASK_COUNT, accepted.get() + rejected.get());
        assertEquals(accepted.get(), executed.get());
        assertEquals(accepted.get(), completedSamples);
    }
}
