package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadPoolRejectPolicyExperimentTest {

    private static final int TASK_COUNT = 8;
    private static final int WORKER_COUNT = 2;
    private static final int QUEUE_CAPACITY = 2;

    @Test
    void shouldCompareAbortPolicy() throws InterruptedException {
        runExperiment("AbortPolicy", new ThreadPoolExecutor.AbortPolicy());
    }

    @Test
    void shouldCompareCallerRunsPolicy() throws InterruptedException {
        runExperiment("CallerRunsPolicy", new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Test
    void shouldCompareDiscardPolicy() throws InterruptedException {
        runExperiment("DiscardPolicy", new ThreadPoolExecutor.DiscardPolicy());
    }

    @Test
    void shouldCompareDiscardOldestPolicy() throws InterruptedException {
        runExperiment("DiscardOldestPolicy", new ThreadPoolExecutor.DiscardOldestPolicy());
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

        CountDownLatch workersStarted = new CountDownLatch(WORKER_COUNT);
        CountDownLatch releaseWorkers = new CountDownLatch(1);

        Set<Integer> executedTasks = new ConcurrentSkipListSet<>();
        Set<Integer> rejectedExceptionTasks = new ConcurrentSkipListSet<>();

        ConcurrentHashMap<Integer, String> executionThreads = new ConcurrentHashMap<>();

        AtomicInteger callerRunsCount = new AtomicInteger();

        String submitterThread = Thread.currentThread().getName();

        try {
            for (int i = 1; i <= TASK_COUNT; i++) {
                final int taskId = i;

                Runnable task = () -> {
                    String threadName = Thread.currentThread().getName();

                    executedTasks.add(taskId);
                    executionThreads.put(taskId, threadName);

                    if (threadName.equals(submitterThread)) {
                        callerRunsCount.incrementAndGet();
                    }

                    if (taskId <= WORKER_COUNT) {
                        workersStarted.countDown();

                        try {
                            releaseWorkers.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }

                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                };

                try {
                    executor.execute(task);
                } catch (RejectedExecutionException e) {
                    rejectedExceptionTasks.add(taskId);
                }

                // 确保任务1、2已经占住两个工作线程。
                if (i == WORKER_COUNT) {
                    assertTrue(
                            workersStarted.await(5, TimeUnit.SECONDS),
                            "工作线程未能及时启动"
                    );
                }
            }

        } finally {
            // 释放任务1、2，避免工作线程一直阻塞。
            releaseWorkers.countDown();
            executor.shutdown();

            assertTrue(
                    executor.awaitTermination(15, TimeUnit.SECONDS),
                    policyName + "线程池未能正常结束"
            );
        }

        Set<Integer> notExecutedTasks = new TreeSet<>();

        for (int i = 1; i <= TASK_COUNT; i++) {
            if (!executedTasks.contains(i)) {
                notExecutedTasks.add(i);
            }
        }

        System.out.println();
        System.out.println("========== " + policyName + " ==========");
        System.out.println("ExecutedTasks      : " + executedTasks);
        System.out.println("RejectedTasks      : " + rejectedExceptionTasks);
        System.out.println("NotExecutedTasks   : " + notExecutedTasks);
        System.out.println("ExecutionThreads   : " + executionThreads);
        System.out.println("CallerRunsCount    : " + callerRunsCount.get());
        System.out.println("PoolCompletedCount : " + executor.getCompletedTaskCount());
        System.out.println("PoolTaskCount      : " + executor.getTaskCount());
    }
}
