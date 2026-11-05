package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadPoolQueueCapacityExperimentTest {

    private static final int TASK_COUNT = 1000;
    private static final int CPU_ITERATIONS = 5_000_000;
    private static final int WARMUP_TASKS = 20;
    private static final int REPEAT_COUNT = 3;

    private static final int[] WORKER_COUNTS = {8, 16};
    private static final int[] QUEUE_CAPACITIES = {16, 64, 256, 1000};

    private static volatile long blackhole;

    @Test
    void shouldCompareQueueCapacityAndOverloadProtection()
            throws InterruptedException {

        System.out.println();
        System.out.println("========== EXP-011 Environment ==========");
        System.out.println("AvailableProcessors: "
                + Runtime.getRuntime().availableProcessors());
        System.out.println("TaskCount          : " + TASK_COUNT);
        System.out.println("CpuIterations      : " + CPU_ITERATIONS);
        System.out.println("RepeatCount        : " + REPEAT_COUNT);

        List<TestCase> testCases = new ArrayList<>();

        for (int workerCount : WORKER_COUNTS) {
            for (int queueCapacity : QUEUE_CAPACITIES) {
                testCases.add(
                        new TestCase(workerCount, queueCapacity)
                );
            }
        }

        List<Result> allResults = new ArrayList<>();

        for (int round = 1; round <= REPEAT_COUNT; round++) {

            List<TestCase> roundCases =
                    new ArrayList<>(testCases);

            Collections.shuffle(
                    roundCases,
                    new Random(20261104L + round)
            );

            System.out.println();
            System.out.println("========== ROUND "
                    + round + " ==========");

            for (TestCase testCase : roundCases) {

                warmUp();

                Result result = runExperiment(
                        testCase.workerCount,
                        testCase.queueCapacity
                );

                result.round = round;
                allResults.add(result);

                System.out.printf(
                        "Workers=%2d Queue=%4d"
                                + " Accepted=%4d"
                                + " Rejected=%4d"
                                + " Executed=%4d"
                                + " AvgWait=%8.2f ms"
                                + " MaxWait=%8.2f ms"
                                + " AvgExec=%6.2f ms"
                                + " Duration=%8.2f ms"
                                + " Throughput=%8.2f tasks/s%n",
                        result.workerCount,
                        result.queueCapacity,
                        result.accepted,
                        result.rejected,
                        result.executed,
                        result.averageWaitMs,
                        result.maxWaitMs,
                        result.averageExecutionMs,
                        result.durationMs,
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
            int queueCapacity) throws InterruptedException {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                workerCount,
                workerCount,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                new ThreadPoolExecutor.AbortPolicy()
        );

        long[] submitTimes = new long[TASK_COUNT];
        long[] startTimes = new long[TASK_COUNT];
        long[] endTimes = new long[TASK_COUNT];
        long[] results = new long[TASK_COUNT];

        Arrays.fill(startTimes, -1L);
        Arrays.fill(endTimes, -1L);

        int accepted = 0;
        int rejected = 0;

        long experimentStart = System.nanoTime();

        try {
            for (int i = 0; i < TASK_COUNT; i++) {

                final int taskId = i;

                submitTimes[taskId] = System.nanoTime();

                Runnable task = () -> {

                    startTimes[taskId] = System.nanoTime();

                    try {
                        results[taskId] = cpuWork(taskId);
                    } finally {
                        endTimes[taskId] = System.nanoTime();
                    }
                };

                try {
                    executor.execute(task);
                    accepted++;
                } catch (RejectedExecutionException e) {
                    rejected++;
                }
            }
        } finally {
            executor.shutdown();

            assertTrue(
                    executor.awaitTermination(
                            180,
                            TimeUnit.SECONDS
                    ),
                    "线程池未能在规定时间内结束"
            );
        }

        long experimentEnd = System.nanoTime();

        long totalWaitNs = 0;
        long totalExecutionNs = 0;

        long maxWaitNs = 0;
        long maxExecutionNs = 0;

        long checksum = 0;
        int executed = 0;

        for (int i = 0; i < TASK_COUNT; i++) {

            if (startTimes[i] < 0 || endTimes[i] < 0) {
                continue;
            }

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
            executed++;
        }

        blackhole = checksum;

        long durationNs = experimentEnd - experimentStart;

        Result result = new Result();

        result.workerCount = workerCount;
        result.queueCapacity = queueCapacity;

        result.submitted = TASK_COUNT;
        result.accepted = accepted;
        result.rejected = rejected;
        result.executed = executed;

        result.averageWaitMs = executed == 0
                ? 0
                : totalWaitNs / 1_000_000.0 / executed;

        result.maxWaitMs = maxWaitNs / 1_000_000.0;

        result.averageExecutionMs = executed == 0
                ? 0
                : totalExecutionNs / 1_000_000.0 / executed;

        result.maxExecutionMs =
                maxExecutionNs / 1_000_000.0;

        result.durationMs = durationNs / 1_000_000.0;

        // 本次吞吐量定义为：实际完成任务数 / 实验总耗时
        result.throughput = executed
                * 1_000_000_000.0 / durationNs;

        assertEquals(TASK_COUNT, accepted + rejected);
        assertEquals(accepted, executed);

        return result;
    }

    private void printSummary(List<Result> results) {

        System.out.println();
        System.out.println("========== SUMMARY ==========");

        for (int workerCount : WORKER_COUNTS) {

            for (int queueCapacity : QUEUE_CAPACITIES) {

                List<Double> throughputs = new ArrayList<>();
                List<Double> waits = new ArrayList<>();
                List<Double> durations = new ArrayList<>();
                List<Integer> rejectedCounts = new ArrayList<>();

                for (Result result : results) {

                    if (result.workerCount == workerCount
                            && result.queueCapacity == queueCapacity) {

                        throughputs.add(result.throughput);
                        waits.add(result.averageWaitMs);
                        durations.add(result.durationMs);
                        rejectedCounts.add(result.rejected);
                    }
                }

                Collections.sort(throughputs);
                Collections.sort(waits);
                Collections.sort(durations);
                Collections.sort(rejectedCounts);

                System.out.printf(
                        "Workers=%2d Queue=%4d"
                                + " MedianThroughput=%8.2f"
                                + " MedianRejected=%4d"
                                + " MedianWait=%8.2f ms"
                                + " MedianDuration=%8.2f ms%n",
                        workerCount,
                        queueCapacity,
                        median(throughputs),
                        medianInt(rejectedCounts),
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

    private int medianInt(List<Integer> values) {

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
        ) / 2;
    }

    private static class TestCase {

        private final int workerCount;
        private final int queueCapacity;

        private TestCase(
                int workerCount,
                int queueCapacity) {

            this.workerCount = workerCount;
            this.queueCapacity = queueCapacity;
        }
    }

    private static class Result {

        private int round;
        private int workerCount;
        private int queueCapacity;

        private int submitted;
        private int accepted;
        private int rejected;
        private int executed;

        private double averageWaitMs;
        private double maxWaitMs;

        private double averageExecutionMs;
        private double maxExecutionMs;

        private double durationMs;
        private double throughput;
    }
}
