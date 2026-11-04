package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadPoolCpuSaturationExperimentTest {

    private static final int TASK_COUNT = 200;
    private static final int QUEUE_CAPACITY = TASK_COUNT;

    private static final int CPU_ITERATIONS = 5_000_000;
    private static final int WARMUP_TASKS = 20;

    private static volatile long blackhole;

    @Test
    void shouldFindCpuThreadPoolSaturationPoint()
            throws InterruptedException {

        int processors =
                Runtime.getRuntime().availableProcessors();

        System.out.println();
        System.out.println("========== JVM Environment ==========");
        System.out.println("AvailableProcessors : " + processors);
        System.out.println("TaskCount            : " + TASK_COUNT);
        System.out.println("CpuIterations        : " + CPU_ITERATIONS);

        warmUp();

        // 使用 LinkedHashSet 去重，同时保留测试顺序
        LinkedHashSet<Integer> threadCounts = new LinkedHashSet<>();

        threadCounts.add(1);
        threadCounts.add(2);
        threadCounts.add(4);
        threadCounts.add(8);
        threadCounts.add(processors);
        threadCounts.add(processors * 2);

        for (int workerCount : threadCounts) {
            runExperiment(workerCount);
        }
    }

    private void warmUp() {
        System.out.println();
        System.out.println("========== JVM Warmup ==========");

        long result = 0;

        for (int i = 0; i < WARMUP_TASKS; i++) {
            result ^= cpuWork(i);
        }

        blackhole = result;

        System.out.println("WarmupTasks         : " + WARMUP_TASKS);
        System.out.println("Warmup completed");
    }

    private long cpuWork(long seed) {
        long value = seed + 1;

        for (int i = 0; i < CPU_ITERATIONS; i++) {
            value = (value ^ (value >>> 13))
                    * 0x5DEECE66DL + i;
        }

        return value;
    }

    private void runExperiment(int workerCount)
            throws InterruptedException {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                workerCount,
                workerCount,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(QUEUE_CAPACITY),
                new ThreadPoolExecutor.AbortPolicy()
        );

        AtomicInteger submitted = new AtomicInteger();
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger executed = new AtomicInteger();

        long[] results = new long[TASK_COUNT];
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
                        results[taskId] = cpuWork(taskId);
                        executed.incrementAndGet();
                    } finally {
                        endTimes[taskId] = System.nanoTime();
                    }
                };

                try {
                    executor.execute(task);
                    accepted.incrementAndGet();
                } catch (RejectedExecutionException e) {
                    rejected.incrementAndGet();
                }
            }
        } finally {
            executor.shutdown();

            if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
                executor.shutdownNow();

                assertTrue(
                        executor.awaitTermination(5, TimeUnit.SECONDS),
                        "线程池未能在规定时间内结束"
                );
            }
        }

        long experimentEnd = System.nanoTime();

        // 所有任务结束后再汇总结果，避免任务之间争用共享变量
        long checksum = 0;

        for (long result : results) {
            checksum ^= result;
        }

        blackhole = checksum;

        long totalDurationNs = experimentEnd - experimentStart;

        long totalWaitNs = 0;
        long maxWaitNs = 0;
        long totalExecutionNs = 0;
        long maxExecutionNs = 0;
        int completedSamples = 0;

        for (int i = 0; i < TASK_COUNT; i++) {
            if (startTimes[i] < 0 || endTimes[i] < 0) {
                continue;
            }

            long waitNs = startTimes[i] - submitTimes[i];
            long executionNs = endTimes[i] - startTimes[i];

            totalWaitNs += waitNs;
            maxWaitNs = Math.max(maxWaitNs, waitNs);

            totalExecutionNs += executionNs;
            maxExecutionNs = Math.max(maxExecutionNs, executionNs);

            completedSamples++;
        }

        double averageWaitMs = completedSamples == 0
                ? 0
                : totalWaitNs / 1_000_000.0 / completedSamples;

        double maxWaitMs = maxWaitNs / 1_000_000.0;

        double averageExecutionMs = completedSamples == 0
                ? 0
                : totalExecutionNs / 1_000_000.0 / completedSamples;

        double maxExecutionMs = maxExecutionNs / 1_000_000.0;

        double totalDurationMs = totalDurationNs / 1_000_000.0;

        double throughput = totalDurationNs == 0
                ? 0
                : executed.get() * 1_000_000_000.0 / totalDurationNs;

        System.out.println();
        System.out.println("========== CPU Workers: "
                + workerCount + " ==========");
        System.out.println("Submitted          : " + submitted.get());
        System.out.println("Accepted           : " + accepted.get());
        System.out.println("Rejected           : " + rejected.get());
        System.out.println("Executed           : " + executed.get());
        System.out.println("CompletedSamples   : " + completedSamples);
        System.out.println("AverageWait        : "
                + String.format("%.2f", averageWaitMs) + " ms");
        System.out.println("MaxWait            : "
                + String.format("%.2f", maxWaitMs) + " ms");
        System.out.println("AverageExecution   : "
                + String.format("%.2f", averageExecutionMs) + " ms");
        System.out.println("MaxExecution       : "
                + String.format("%.2f", maxExecutionMs) + " ms");
        System.out.println("TotalDuration      : "
                + String.format("%.2f", totalDurationMs) + " ms");
        System.out.println("ActualThroughput   : "
                + String.format("%.2f", throughput) + " tasks/s");

        assertEquals(TASK_COUNT, submitted.get());
        assertEquals(TASK_COUNT, accepted.get() + rejected.get());
        assertEquals(accepted.get(), executed.get());
        assertEquals(executed.get(), completedSamples);
        assertEquals(TASK_COUNT, completedSamples);
    }
}
