package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadPoolTaskTypeExperimentTest {

    private static final int TASK_COUNT = 40;
    private static final int QUEUE_CAPACITY = 40;

    private static final int CPU_ITERATIONS = 5_000_000;
    private static final long IO_WAIT_MS = 200;

    // 保存计算结果，避免 CPU 计算完全没有可观察副作用
    private static volatile long blackhole;

    @Test
    void shouldCompareCpuAndIoBoundTasks() throws InterruptedException {

        // CPU 预热：降低首次 JIT 编译对正式实验的影响
        warmUpCpu();

        // CPU 密集型：改变线程数
        runExperiment("A-CPU", "CPU", 1);
        runExperiment("B-CPU", "CPU", 2);
        runExperiment("C-CPU", "CPU", 4);
        runExperiment("D-CPU", "CPU", 8);

        // I/O 密集型：改变线程数
        runExperiment("E-IO", "IO", 1);
        runExperiment("F-IO", "IO", 2);
        runExperiment("G-IO", "IO", 4);
        runExperiment("H-IO", "IO", 8);
    }

    private void warmUpCpu() {
        for (int i = 0; i < 10; i++) {
            blackhole = cpuWork(i);
        }
    }

    private long cpuWork(long seed) {
        long value = seed + 1;

        for (int i = 0; i < CPU_ITERATIONS; i++) {
            value = (value ^ (value >>> 13))
                * 0x5DEECE66DL + i;
        }

        return value;
    }

    private void runExperiment(
        String group,
        String taskType,
        int workerCount
    ) throws InterruptedException {

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
                        if ("CPU".equals(taskType)) {
                            blackhole = cpuWork(taskId);
                        } else {
                            Thread.sleep(IO_WAIT_MS);
                        }

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

        long totalDurationMs = TimeUnit.NANOSECONDS.toMillis(
            experimentEnd - experimentStart
        );

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

        double throughput = totalDurationMs == 0
            ? 0
            : executed.get() * 1000.0 / totalDurationMs;

        System.out.println();
        System.out.println("========== Group " + group + " ==========");
        System.out.println("TaskType           : " + taskType);
        System.out.println("WorkerCount        : " + workerCount);
        System.out.println("QueueCapacity      : " + QUEUE_CAPACITY);
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
        System.out.println("TotalDuration      : " + totalDurationMs + " ms");
        System.out.println("ActualThroughput   : "
            + String.format("%.2f", throughput) + " tasks/s");

        assertEquals(TASK_COUNT, submitted.get());
        assertEquals(TASK_COUNT, accepted.get() + rejected.get());
        assertEquals(accepted.get(), executed.get());
        assertEquals(executed.get(), completedSamples);
    }
}
