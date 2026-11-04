

package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

class QueueWaitTimeExperimentTest {

    private static final int THREAD_COUNT = 2;
    private static final int TASK_COUNT = 10;
    private static final long TASK_DURATION_MS = 500;

    @Test
    void compareQueueCapacityAndTaskWaitTime() throws InterruptedException {
        List<Integer> capacities = List.of(3, 8, 20);

        for (int capacity : capacities) {
            runExperiment(capacity);
        }
    }

    private void runExperiment(int queueCapacity)
            throws InterruptedException {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                THREAD_COUNT,
                THREAD_COUNT,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                new ThreadPoolExecutor.AbortPolicy()
        );

        CountDownLatch firstWorkersStarted = new CountDownLatch(THREAD_COUNT);
        CountDownLatch releaseWorkers = new CountDownLatch(1);

        List<TaskMetric> metrics = new CopyOnWriteArrayList<>();
        AtomicInteger rejectedCount = new AtomicInteger();

        try {
            // 先提交两个任务，确保两个工作线程都已启动并被闸门阻塞
            for (int i = 1; i <= THREAD_COUNT; i++) {
                submitTask(
                        executor, i, firstWorkersStarted,
                        releaseWorkers, metrics, rejectedCount
                );
            }

            boolean workersReady =
                    firstWorkersStarted.await(5, TimeUnit.SECONDS);

            if (!workersReady) {
                throw new IllegalStateException("工作线程未能按时启动");
            }

            // 再提交剩余任务，此时两个工作线程被占用
            for (int i = THREAD_COUNT + 1; i <= TASK_COUNT; i++) {
                submitTask(
                        executor, i, firstWorkersStarted,
                        releaseWorkers, metrics, rejectedCount
                );
            }

            System.out.printf(
                    "%n========== 队列容量：%d ==========%n",
                    queueCapacity
            );
            System.out.println("提交任务数：" + TASK_COUNT);
            System.out.println("线程池大小：" + executor.getPoolSize());
            System.out.println("活跃线程数：" + executor.getActiveCount());
            System.out.println("当前队列长度：" + executor.getQueue().size());
            System.out.println("拒绝任务数：" + rejectedCount.get());

        } finally {
            // 放行任务，开始真正执行模拟业务
            releaseWorkers.countDown();
            executor.shutdown();

            if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                throw new IllegalStateException("线程池未能正常结束");
            }
        }

        printMetrics(metrics);
    }

    private void submitTask(
            ThreadPoolExecutor executor,
            int taskId,
            CountDownLatch firstWorkersStarted,
            CountDownLatch releaseWorkers,
            List<TaskMetric> metrics,
            AtomicInteger rejectedCount) {

        long submitTime = System.nanoTime();

        try {
            executor.execute(() -> {
                // 任务真正开始运行的时刻
                long startTime = System.nanoTime();

                // 前两个任务到达这里后通知主线程
                firstWorkersStarted.countDown();

                try {
                    // 暂停执行，保证后续任务进入队列
                    releaseWorkers.await();

                    long executionStart = System.nanoTime();

                    Thread.sleep(TASK_DURATION_MS);

                    long endTime = System.nanoTime();

                    metrics.add(new TaskMetric(
                            taskId,
                            submitTime,
                            startTime,
                            executionStart,
                            endTime
                    ));

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        } catch (RejectedExecutionException e) {
            rejectedCount.incrementAndGet();
        }
    }

    private void printMetrics(List<TaskMetric> metrics) {
        List<TaskMetric> sorted = new ArrayList<>(metrics);
        sorted.sort(Comparator.comparingInt(TaskMetric::taskId));

        System.out.println("\n任务明细：");
        System.out.printf(
                "%-8s %-16s %-16s%n",
                "任务ID", "等待时间(ms)", "执行时间(ms)"
        );

        for (TaskMetric metric : sorted) {
            System.out.printf(
                    "%-8d %-16.2f %-16.2f%n",
                    metric.taskId(),
                    metric.waitMs(),
                    metric.executionMs()
            );
        }

        double averageWait = sorted.stream()
                .mapToDouble(TaskMetric::waitMs)
                .average()
                .orElse(0);

        double maxWait = sorted.stream()
                .mapToDouble(TaskMetric::waitMs)
                .max()
                .orElse(0);

        double averageExecution = sorted.stream()
                .mapToDouble(TaskMetric::executionMs)
                .average()
                .orElse(0);

        System.out.println("\n统计结果：");
        System.out.println("实际完成任务数：" + sorted.size());
        System.out.printf("平均等待时间：%.2f ms%n", averageWait);
        System.out.printf("最大等待时间：%.2f ms%n", maxWait);
        System.out.printf("平均执行时间：%.2f ms%n", averageExecution);
    }

    private record TaskMetric(
            int taskId,
            long submitTime,
            long startTime,
            long executionStart,
            long endTime
    ) {
        double waitMs() {
            return (startTime - submitTime) / 1_000_000.0;
        }

        double executionMs() {
            return (endTime - executionStart) / 1_000_000.0;
        }
    }
}
