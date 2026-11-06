package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ThreadPoolExecutor 核心参数组合对比实验测试类
 * <p>
 * 实验目的：
 * 通过两组对照实验，系统量化分析线程池两个最核心参数对系统表现的影响：
 * <p>
 * 第一组对照（A→B→C→D）：固定队列容量不变，逐步增加工作线程数
 * 观察吞吐量随并行度提升的近似线性增长趋势，验证 IO 密集型场景下线程数与吞吐量的正相关关系
 * <p>
 * 第二组对照（E→B→F）：固定工作线程数不变，逐步增大队列容量
 * 观察队列容量如何影响任务拒绝率、平均等待时间和系统整体弹性，
 * 理解有界队列在流量洪峰下的缓冲作用和 trade-off 取舍
 * <p>
 * 实验设计特点：
 * - 所有任务都是固定 200ms 休眠的模拟 IO 密集型负载，无 CPU 计算瓶颈
 * - 任务批量一次性提交，消除生产者提交速率对实验结果的干扰
 * - 纳秒级精度记录任务提交、开始执行、执行完成三个关键时间点
 * - 所有统计计算在线程池完全关闭后进行，避免任务执行过程中争用共享变量
 */
class ThreadPoolParameterExperimentTest {

    // 单次实验一次性批量提交的总任务数量
    private static final int TASK_COUNT = 40;
    // 每个任务模拟 IO 密集型业务逻辑的执行耗时（毫秒）
    private static final long TASK_EXECUTION_MS = 200;

    /**
     * 主实验入口：运行两组完整的对照实验
     * <p>
     * 第一组：固定队列容量为40，工作线程数从1→2→4→8逐步翻倍
     * 预期观察结果：吞吐量近似跟随工作线程数线性增长
     * <p>
     * 第二组：固定工作线程数为2，队列容量从4→20→40逐步增大
     * 预期观察结果：队列容量越小，拒绝任务数越多，平均等待时间越短；
     * 队列容量越大，拒绝任务数越少，平均等待时间越长
     */
    @Test
    void shouldCompareThreadPoolParameters() throws InterruptedException {

        // Group A-D: Fixed queue capacity = 40, vary worker count
        runExperiment("A", 1, 40);
        runExperiment("B", 2, 40);
        runExperiment("C", 4, 40);
        runExperiment("D", 8, 40);

        // Group B, E, F: Fixed worker count = 2, vary queue capacity
        runExperiment("E", 2, 4);
        runExperiment("F", 2, 20);
    }

    /**
     * 使用指定的工作线程数和队列容量运行一次完整的对照实验
     * <p>
     * 实验流程：
     * 1. 创建固定大小线程池，maximumPoolSize == corePoolSize，线程池永远不会创建额外非核心线程
     * 2. 一次性批量提交全部 TASK_COUNT 个任务，记录每个任务的提交时刻
     * 3. 每个任务被工作线程取出执行时，记录开始执行时刻
     * 4. 任务模拟 200ms IO 操作完成后，记录执行结束时刻
     * 5. 优雅关闭线程池，等待所有任务执行完成
     * 6. 所有任务结束后再统一汇总所有统计指标，完全避免任务执行过程中争用共享变量
     *
     * @param group         实验组标识名称，用于输出报告区分不同配置
     * @param workerCount   本次实验使用的工作线程数量
     * @param queueCapacity 本次实验使用的有界阻塞队列容量
     */
    private void runExperiment(
        String group,
        int workerCount,
        int queueCapacity
    ) throws InterruptedException {

        // 创建固定大小线程池，核心线程数=最大线程数=workerCount，队列容量=queueCapacity
        // 使用 AbortPolicy 拒绝策略，队列满时直接抛出 RejectedExecutionException
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            workerCount,
            workerCount,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(queueCapacity),
            new ThreadPoolExecutor.AbortPolicy()
        );

        // 生产者尝试提交的任务总数
        AtomicInteger submitted = new AtomicInteger();
        // 线程池成功接收（进入队列或立即执行）的任务数量
        AtomicInteger accepted = new AtomicInteger();
        // 被拒绝策略拒绝的任务数量
        AtomicInteger rejected = new AtomicInteger();
        // 真正完整执行完成的任务数量
        AtomicInteger executed = new AtomicInteger();

        // 记录每个任务被提交给线程池的纳秒时间戳
        long[] submitTimes = new long[TASK_COUNT];
        // 记录每个任务被工作线程从队列中取出、开始执行的纳秒时间戳
        long[] startTimes = new long[TASK_COUNT];
        // 记录每个任务 IO 模拟完成的纳秒时间戳
        long[] endTimes = new long[TASK_COUNT];

        // 初始化时间戳数组为 -1，用于后续校验任务是否被完整执行
        Arrays.fill(submitTimes, -1L);
        Arrays.fill(startTimes, -1L);
        Arrays.fill(endTimes, -1L);

        // 整个实验开始的时刻（提交第一个任务之前）
        long experimentStart = System.nanoTime();

        try {
            // 一次性批量提交全部 TASK_COUNT 个任务
            for (int i = 0; i < TASK_COUNT; i++) {
                final int taskId = i;

                submitTimes[taskId] = System.nanoTime();
                submitted.incrementAndGet();

                Runnable task = () -> {
                    startTimes[taskId] = System.nanoTime();

                    try {
                        // 模拟 IO 密集型业务逻辑执行耗时
                        Thread.sleep(TASK_EXECUTION_MS);
                        executed.incrementAndGet();
                    } catch (InterruptedException e) {
                        // 恢复线程中断标记，不破坏线程池的中断语义
                        Thread.currentThread().interrupt();
                    } finally {
                        // 无论任务成功失败，都保证结束时间戳被记录
                        endTimes[taskId] = System.nanoTime();
                    }
                };

                try {
                    executor.execute(task);
                    accepted.incrementAndGet();
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    // 捕获拒绝异常，统计被拒绝的任务数量
                    rejected.incrementAndGet();
                }
            }
        } finally {
            // 所有任务提交完成后，优雅关闭线程池
            executor.shutdown();

            // 等待所有任务执行完成，超时后强制中断所有剩余任务
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();

                assertTrue(
                    executor.awaitTermination(5, TimeUnit.SECONDS),
                    "线程池未能在规定时间内结束"
                );
            }
        }

        // 所有任务全部执行完成的时刻
        long experimentEnd = System.nanoTime();

        // 整个实验从第一个任务提交到最后一个任务完成的总耗时（毫秒）
        long totalDurationMs =
            TimeUnit.NANOSECONDS.toMillis(
                experimentEnd - experimentStart
            );

        // 所有任务在队列中的总等待时间之和
        long totalWaitMs = 0;
        // 所有任务中最长的单次等待时间
        long maxWaitMs = 0;
        // 所有任务的 IO 模拟执行时间之和
        long totalExecutionMs = 0;
        // 所有任务中最长的单次 IO 执行时间
        long maxExecutionMs = 0;
        // 有效完成的任务样本数，用于后续平均值计算
        int completedSamples = 0;

        // 遍历所有任务，汇总计算各项统计指标
        for (int i = 0; i < TASK_COUNT; i++) {
            // 跳过未被完整执行的任务（被拒绝的任务）
            if (startTimes[i] < 0 || endTimes[i] < 0) {
                continue;
            }

            // 任务等待时间 = 开始执行时刻 - 提交时刻
            long waitMs = TimeUnit.NANOSECONDS.toMillis(
                startTimes[i] - submitTimes[i]
            );

            // 任务执行时间 = IO 完成时刻 - 开始执行时刻
            long executionMs = TimeUnit.NANOSECONDS.toMillis(
                endTimes[i] - startTimes[i]
            );

            totalWaitMs += waitMs;
            maxWaitMs = Math.max(maxWaitMs, waitMs);

            totalExecutionMs += executionMs;
            maxExecutionMs = Math.max(maxExecutionMs, executionMs);

            completedSamples++;
        }

        // 所有任务的平均队列等待时间（毫秒）
        double averageWaitMs = completedSamples == 0
            ? 0
            : (double) totalWaitMs / completedSamples;

        // 所有任务的平均 IO 执行时间（毫秒）
        double averageExecutionMs = completedSamples == 0
            ? 0
            : (double) totalExecutionMs / completedSamples;

        // 系统整体吞吐量：单位时间内完成的任务总数（tasks/s）
        double throughput = totalDurationMs == 0
            ? 0
            : executed.get() * 1000.0 / totalDurationMs;

        // 打印当前实验组的完整统计报告
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

        // 完整性校验：确保所有任务都有明确的去向，没有丢失
        assertEquals(TASK_COUNT, submitted.get());
        assertEquals(TASK_COUNT, accepted.get() + rejected.get());
        assertEquals(accepted.get(), executed.get());
        assertEquals(accepted.get(), completedSamples);
    }
}
