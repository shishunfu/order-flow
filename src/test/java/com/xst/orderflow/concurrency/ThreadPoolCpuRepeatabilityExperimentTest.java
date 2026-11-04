package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadPoolCpuRepeatabilityExperimentTest {

    private static final int TASK_COUNT = 200;
    private static final int CPU_ITERATIONS = 5_000_000;
    private static final int WARMUP_TASKS = 20;
    private static final int REPEAT_COUNT = 5;

    private static volatile long blackhole;

    @Test
    void shouldVerifyCpuThreadPoolRepeatability()
            throws InterruptedException {

        int[] workerCounts = {4, 8, 16, 32};

        System.out.println();
        System.out.println("========== EXP-009 Environment ==========");
        System.out.println("AvailableProcessors : "
                + Runtime.getRuntime().availableProcessors());
        System.out.println("TaskCount           : " + TASK_COUNT);
        System.out.println("CpuIterations       : " + CPU_ITERATIONS);
        System.out.println("RepeatCount         : " + REPEAT_COUNT);

        warmUp();

        Map<Integer, double[]> throughputResults =
                new LinkedHashMap<>();

        for (int workerCount : workerCounts) {

            double[] throughputs = new double[REPEAT_COUNT];

            System.out.println();
            System.out.println("========== Workers: "
                    + workerCount + " ==========");

            for (int round = 0; round < REPEAT_COUNT; round++) {

                warmUp();

                Result result = runExperiment(workerCount);

                throughputs[round] = result.throughput;

                System.out.printf(
                        "Round %d | Duration: %.2f ms"
                                + " | AvgExecution: %.2f ms"
                                + " | Throughput: %.2f tasks/s%n",
                        round + 1,
                        result.durationMs,
                        result.averageExecutionMs,
                        result.throughput
                );

                assertEquals(TASK_COUNT, result.executed);
            }

            throughputResults.put(workerCount, throughputs);
        }

        System.out.println();
        System.out.println("========== Summary ==========");

        for (Map.Entry<Integer, double[]> entry
                : throughputResults.entrySet()) {

            double[] values = entry.getValue().clone();
            Arrays.sort(values);

            double median = values[values.length / 2];

            double min = values[0];
            double max = values[values.length - 1];

            System.out.printf(
                    "Workers: %2d | Median: %.2f"
                            + " | Min: %.2f"
                            + " | Max: %.2f"
                            + " | Range: %.2f tasks/s%n",
                    entry.getKey(),
                    median,
                    min,
                    max,
                    max - min
            );
        }
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

    private Result runExperiment(int workerCount)
            throws InterruptedException {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                workerCount,
                workerCount,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(TASK_COUNT),
                new ThreadPoolExecutor.AbortPolicy()
        );

        long[] startTimes = new long[TASK_COUNT];
        long[] endTimes = new long[TASK_COUNT];
        long[] results = new long[TASK_COUNT];

        long experimentStart = System.nanoTime();

        try {
            for (int i = 0; i < TASK_COUNT; i++) {

                final int taskId = i;

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
                    executor.awaitTermination(120, TimeUnit.SECONDS),
                    "线程池未能在规定时间内结束"
            );
        }

        long experimentEnd = System.nanoTime();

        long totalExecutionNs = 0;
        int completed = 0;
        long checksum = 0;

        for (int i = 0; i < TASK_COUNT; i++) {

            assertTrue(startTimes[i] > 0);
            assertTrue(endTimes[i] > startTimes[i]);

            totalExecutionNs += endTimes[i] - startTimes[i];
            checksum ^= results[i];
            completed++;
        }

        blackhole = checksum;

        long durationNs = experimentEnd - experimentStart;

        double durationMs = durationNs / 1_000_000.0;

        double averageExecutionMs =
                totalExecutionNs / 1_000_000.0 / completed;

        double throughput =
                completed * 1_000_000_000.0 / durationNs;

        return new Result(
                completed,
                durationMs,
                averageExecutionMs,
                throughput
        );
    }

    private static class Result {

        private final int executed;
        private final double durationMs;
        private final double averageExecutionMs;
        private final double throughput;

        private Result(
                int executed,
                double durationMs,
                double averageExecutionMs,
                double throughput) {

            this.executed = executed;
            this.durationMs = durationMs;
            this.averageExecutionMs = averageExecutionMs;
            this.throughput = throughput;
        }
    }
}
