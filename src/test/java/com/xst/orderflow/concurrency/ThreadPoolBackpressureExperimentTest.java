package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadPoolBackpressureExperimentTest {

    private static final int TASK_COUNT = 40;
    private static final int WORKER_COUNT = 2;
    private static final int QUEUE_CAPACITY = 8;

    private static final long TASK_EXECUTION_MS = 500;
    private static final long SUBMIT_INTERVAL_MS = 50;

    @Test
    void shouldCompareAbortPolicyAndCallerRunsPolicy() throws InterruptedException {

        runExperiment("AbortPolicy", new ThreadPoolExecutor.AbortPolicy());

        runExperiment("CallerRunsPolicy", new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private void runExperiment(String policyName, RejectedExecutionHandler handler) throws InterruptedException {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                WORKER_COUNT,
                WORKER_COUNT,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(QUEUE_CAPACITY),
                handler
        );

        AtomicInteger submitted = new AtomicInteger();
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger executed = new AtomicInteger();
        AtomicInteger callerRuns = new AtomicInteger();

        AtomicInteger maxQueueSize = new AtomicInteger();

        Set<Integer> executedTaskIds = ConcurrentHashMap.newKeySet();

        String producerThreadName = Thread.currentThread().getName();

        long experimentStart = System.nanoTime();
        long producerStart = System.nanoTime();

        try {
            for (int i = 1; i <= TASK_COUNT; i++) {
                final int taskId = i;

                submitted.incrementAndGet();

                Runnable task = () -> {
                    executed.incrementAndGet();
                    executedTaskIds.add(taskId);

                    if (Thread.currentThread().getName().equals(producerThreadName)) {
                        callerRuns.incrementAndGet();
                    }

                    try {
                        Thread.sleep(TASK_EXECUTION_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                };

                try {
                    executor.execute(task);
                    accepted.incrementAndGet();
                } catch (RejectedExecutionException e) {
                    rejected.incrementAndGet();
                }

                maxQueueSize.accumulateAndGet(executor.getQueue().size(), Math::max);

                // 模拟生产者以固定间隔提交任务。
                if (i < TASK_COUNT) {
                    Thread.sleep(SUBMIT_INTERVAL_MS);
                }
            }

        } finally {
            long producerEnd = System.nanoTime();

            executor.shutdown();

            assertTrue(
                    executor.awaitTermination(60, TimeUnit.SECONDS),
                    policyName + "线程池未能在规定时间内结束"
            );

            long experimentEnd = System.nanoTime();

            printResult(
                    policyName,
                    submitted.get(),
                    accepted.get(),
                    rejected.get(),
                    executed.get(),
                    callerRuns.get(),
                    maxQueueSize.get(),
                    executedTaskIds,
                    executor,
                    producerEnd - producerStart,
                    experimentEnd - experimentStart
            );
        }

        assertEquals(TASK_COUNT, submitted.get());
        assertEquals(TASK_COUNT, executed.get() + rejected.get());
//        assertEquals(TASK_COUNT, executedTaskIds.size());
        assertTrue(maxQueueSize.get() <= QUEUE_CAPACITY);
    }

    private void printResult(
            String policyName,
            int submitted,
            int accepted,
            int rejected,
            int executed,
            int callerRuns,
            int maxQueueSize,
            Set<Integer> executedTaskIds,
            ThreadPoolExecutor executor,
            long producerNanos,
            long totalNanos
    ) {
        System.out.println();
        System.out.println("========== " + policyName + " ==========");
        System.out.println("Submitted          : " + submitted);
        System.out.println("Accepted           : " + accepted);
        System.out.println("Rejected           : " + rejected);
        System.out.println("Executed           : " + executed);
        System.out.println("CallerRuns         : " + callerRuns);
        System.out.println("MaxQueueSize       : " + maxQueueSize);
        System.out.println("ExecutedTaskIds    : " + executedTaskIds.size());
        System.out.println("PoolCompletedCount: "
                + executor.getCompletedTaskCount());
        System.out.println("PoolTaskCount      : " + executor.getTaskCount());

        System.out.printf(
                "ProducerDuration   : %.2f ms%n",
                producerNanos / 1_000_000.0
        );

        System.out.printf(
                "TotalDuration      : %.2f ms%n",
                totalNanos / 1_000_000.0
        );

        if (totalNanos > 0) {
            System.out.printf(
                    "ActualThroughput   : %.2f tasks/s%n",
                    executed / (totalNanos / 1_000_000_000.0)
            );
        }
    }
}
