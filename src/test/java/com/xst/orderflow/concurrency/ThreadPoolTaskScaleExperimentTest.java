package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadPoolTaskScaleExperimentTest {

    private static final int CPU_ITERATIONS = 5_000_000;
    private static final int WARMUP_TASKS = 20;
    private static final int REPEAT_COUNT = 5;

    private static final int[] WORKER_COUNTS = {8, 16, 32};
    private static final int[] TASK_COUNTS = {200, 1000, 2000};

    private static volatile long blackhole;

    @Test
    void shouldCompareWorkerCountAndTaskScale()
            throws InterruptedException {

        System.out.println();
        System.out.println("========== EXP-010 Environment ==========");
        System.out.println("AvailableProcessors: "
                + Runtime.getRuntime().availableProcessors());
        System.out.println("CpuIterations      : " + CPU_ITERATIONS);
        System.out.println("RepeatCount        : " + REPEAT_COUNT);

        List<TestCase> testCases = new ArrayList<>();

        for (int taskCount : TASK_COUNTS) {
            for (int workerCount : WORKER_COUNTS) {
                testCases.add(new TestCase(workerCount, taskCount));
            }
        }

        // 固定随机种子，保证测试顺序可以复现
        Collections.shuffle(testCases, new Random(20261104));

        List<Result> allResults = new ArrayList<>();

        for (int round = 1; round <= REPEAT_COUNT; round++) {

            System.out.println();
            System.out.println("========== ROUND "
                    + round + " ==========");

            List<TestCase> roundCases = new ArrayList<>(testCases);

            // 每轮改变测试顺序，减少固定顺序影响
            Collections.shuffle(
                    roundCases,
                    new Random(20261104L + round)
            );

            for (TestCase testCase : roundCases) {

                warmUp();

                Result result = runExperiment(
                        testCase.workerCount,
                        testCase.taskCount
                );

                result.round = round;
                allResults.add(result);

                System.out.printf(
                        "Workers=%2d Tasks=%4d"
                                + " Duration=%8.2f ms"
                                + " AvgWait=%8.2f ms"
                                + " AvgExec=%6.2f ms"
                                + " Throughput=%8.2f tasks/s%n",
                        result.workerCount,
                        result.taskCount,
                        result.durationMs,
                        result.averageWaitMs,
                        result.averageExecutionMs,
                        result.throughput
                );
            }
        }

        printSummary(allResults);
    }

    private void warmUp() {

        long result = 0;

        for (int i = 0; i < WARMUP_TASKS; i++) {
            result ^= cpuWork(i);
        }

        blackhole = result;
    }

    private long cpuWork(long seed) {

        long value = seed + 1;

        for (int i = 0; i < CPU_ITERATIONS; i++) {
            value = (value ^ (value >>> 13))
                    * 0x5DEECE66DL + i;
        }

        return value;
    }

    private Result runExperiment(
            int workerCount,
            int taskCount) throws InterruptedException {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                workerCount,
                workerCount,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(taskCount),
                new ThreadPoolExecutor.AbortPolicy()
        );

        long[] submitTimes = new long[taskCount];
        long[] startTimes = new long[taskCount];
        long[] endTimes = new long[taskCount];
        long[] results = new long[taskCount];

        long experimentStart = System.nanoTime();

        try {
            for (int i = 0; i < taskCount; i++) {

                final int taskId = i;

                submitTimes[taskId] = System.nanoTime();

                executor.execute(() -> {

                    startTimes[taskId] = System.nanoTime();

                    try {
                        results[taskId] = cpuWork(taskId);
                    } finally {
                        endTimes[taskId] = System.nanoTime();
                    }
                });
            }
        } finally {
            executor.shutdown();

            assertTrue(
                    executor.awaitTermination(180, TimeUnit.SECONDS),
                    "线程池未能在规定时间内结束"
            );
        }

        long experimentEnd = System.nanoTime();

        long totalWaitNs = 0;
        long totalExecutionNs = 0;

        long maxWaitNs = 0;
        long maxExecutionNs = 0;

        long checksum = 0;

        for (int i = 0; i < taskCount; i++) {

            assertTrue(startTimes[i] > 0);
            assertTrue(endTimes[i] > startTimes[i]);

            long waitNs = startTimes[i] - submitTimes[i];
            long executionNs = endTimes[i] - startTimes[i];

            totalWaitNs += waitNs;
            totalExecutionNs += executionNs;

            maxWaitNs = Math.max(maxWaitNs, waitNs);
            maxExecutionNs = Math.max(
                    maxExecutionNs,
                    executionNs
            );

            checksum ^= results[i];
        }

        blackhole = checksum;

        long durationNs = experimentEnd - experimentStart;

        Result result = new Result();

        result.workerCount = workerCount;
        result.taskCount = taskCount;

        result.durationMs = durationNs / 1_000_000.0;

        result.averageWaitMs =
                totalWaitNs / 1_000_000.0 / taskCount;

        result.maxWaitMs = maxWaitNs / 1_000_000.0;

        result.averageExecutionMs =
                totalExecutionNs / 1_000_000.0 / taskCount;

        result.maxExecutionMs =
                maxExecutionNs / 1_000_000.0;

        result.throughput =
                taskCount * 1_000_000_000.0 / durationNs;

        return result;
    }

    private void printSummary(List<Result> results) {

        System.out.println();
        System.out.println("========== SUMMARY ==========");

        for (int taskCount : TASK_COUNTS) {

            for (int workerCount : WORKER_COUNTS) {

                List<Double> throughputs = new ArrayList<>();
                List<Double> waits = new ArrayList<>();
                List<Double> durations = new ArrayList<>();

                for (Result result : results) {

                    if (result.taskCount == taskCount
                            && result.workerCount == workerCount) {

                        throughputs.add(result.throughput);
                        waits.add(result.averageWaitMs);
                        durations.add(result.durationMs);
                    }
                }

                Collections.sort(throughputs);
                Collections.sort(waits);
                Collections.sort(durations);

                System.out.printf(
                        "Tasks=%4d Workers=%2d"
                                + " MedianThroughput=%8.2f"
                                + " MedianWait=%8.2f ms"
                                + " MedianDuration=%8.2f ms%n",
                        taskCount,
                        workerCount,
                        median(throughputs),
                        median(waits),
                        median(durations)
                );
            }
        }
    }

    private double median(List<Double> values) {

        int size = values.size();

        if (size == 0) {
            return 0;
        }

        if (size % 2 == 1) {
            return values.get(size / 2);
        }

        return (
                values.get(size / 2 - 1)
                        + values.get(size / 2)
        ) / 2.0;
    }

    private static class TestCase {

        private final int workerCount;
        private final int taskCount;

        private TestCase(int workerCount, int taskCount) {
            this.workerCount = workerCount;
            this.taskCount = taskCount;
        }
    }

    private static class Result {

        private int round;
        private int workerCount;
        private int taskCount;

        private double durationMs;
        private double averageWaitMs;
        private double maxWaitMs;

        private double averageExecutionMs;
        private double maxExecutionMs;

        private double throughput;
    }
}
