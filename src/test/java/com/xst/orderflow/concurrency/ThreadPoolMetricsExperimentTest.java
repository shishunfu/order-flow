package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ThreadPoolExecutor 运行时指标监控实验测试类
 * <p>
 * 实验目的：
 * 演示 ThreadPoolExecutor 所有核心内置监控指标的真实含义，
 * 在"任务堆积"和"任务全部完成"两个不同阶段分别采样指标快照，
 * 直观展示每个运行时指标的动态变化过程，
 * 同时统计任务队列等待时间和实际执行时间的分布特征。
 * <p>
 * 实验配置：
 * - 核心线程数 = 2，最大线程数 = 2
 * - 有界队列容量 = 8
 * - 总提交任务数 = 10
 * - 预期行为：2个任务立即执行，剩余8个任务全部进入队列排队，无任务被拒绝
 */
class ThreadPoolMetricsExperimentTest {

    // 本次实验总共提交的任务总数
    private static final int TASK_COUNT = 10;

    @Test
    void shouldMonitorThreadPoolRuntimeMetrics() throws InterruptedException {
        // 创建固定大小线程池，核心线程数=2，最大线程数=2，队列容量=8
        // 由于 maximumPoolSize == corePoolSize，线程池永远不会创建额外非核心线程
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            2,
            2,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(8),
            new ThreadPoolExecutor.AbortPolicy()
        );

        // 同步闸门1：等待2个工作线程完全启动并进入阻塞状态
        // 保证两个核心线程100%被占用，后续提交的任务只能进入队列排队
        CountDownLatch workersStarted = new CountDownLatch(2);

        // 同步闸门2：主线程统一释放所有阻塞的工作线程
        // 用于制造稳定的任务堆积状态，确保我们可以在队列填满后采样指标
        CountDownLatch releaseWorkers = new CountDownLatch(1);

        // 同步闸门3：等待全部 TASK_COUNT 个任务执行完成
        // 用于优雅等待所有任务结束后再进行最终指标采样
        CountDownLatch completed = new CountDownLatch(TASK_COUNT);

        // 统计被拒绝策略拒绝的任务数量
        AtomicInteger rejectedCount = new AtomicInteger();

        // 线程安全队列：收集所有任务的队列等待时间（毫秒）
        // 等待时间定义：从任务提交给线程池，到任务被工作线程取出开始执行的时间间隔
        ConcurrentLinkedQueue<Long> waitTimes = new ConcurrentLinkedQueue<>();
        // 线程安全队列：收集所有任务的实际业务执行时间（毫秒）
        // 执行时间定义：从任务开始执行业务逻辑，到业务逻辑执行完成的时间间隔
        ConcurrentLinkedQueue<Long> executionTimes = new ConcurrentLinkedQueue<>();

        try {
            // 循环提交全部10个任务
            for (int i = 1; i <= TASK_COUNT; i++) {
                final int taskId = i;
                // 记录任务被提交给线程池的纳秒级时间戳
                final long submitTime = System.nanoTime();

                try {
                    executor.execute(() -> {
                        // 任务被工作线程从队列中取出，开始执行的时刻
                        long startTime = System.nanoTime();

                        // 前两个任务启动后，递减启动闸门，通知主线程工作线程已就绪
                        if (taskId <= 2) {
                            workersStarted.countDown();
                        }

                        try {
                            // 前两个任务在这里阻塞等待，占住两个工作线程不释放
                            // 这样后续提交的8个任务会全部进入队列排队，制造稳定的堆积状态
                            if (taskId <= 2) {
                                releaseWorkers.await();
                            }

                            // 闸门释放后，任务正式开始执行业务逻辑
                            long executionStart = System.nanoTime();

                            // 模拟真实业务逻辑执行耗时 500ms
                            Thread.sleep(500);

                            // 业务逻辑执行完成的时刻
                            long endTime = System.nanoTime();

                            // 计算并记录该任务的队列等待时间
                            waitTimes.add(
                                TimeUnit.NANOSECONDS.toMillis(startTime - submitTime)
                            );

                            // 计算并记录该任务的实际业务执行时间
                            executionTimes.add(
                                TimeUnit.NANOSECONDS.toMillis(endTime - executionStart)
                            );

                        } catch (InterruptedException e) {
                            // 恢复线程中断标记，不破坏线程池的中断语义
                            Thread.currentThread().interrupt();
                        } finally {
                            // 无论任务成功失败，都递减完成闸门
                            completed.countDown();
                        }
                    });
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    // 捕获拒绝异常，统计被拒绝的任务数量
                    rejectedCount.incrementAndGet();
                    // 即使任务被拒绝，也要递减完成闸门，避免主线程永久等待
                    completed.countDown();
                }

                // 提交完前2个任务后，等待它们完全启动并占住工作线程
                // 这是整个实验最关键的时序控制点，保证后续提交任务时核心线程没有空闲
                if (i == 2) {
                    assertTrue(
                        workersStarted.await(5, TimeUnit.SECONDS),
                        "两个工作线程未能及时启动"
                    );
                }
            }

            // ========== 采样点 A：任务堆积稳定状态 ==========
            // 此时两个工作线程都被阻塞在闸门处，队列中已经填满了剩余8个排队任务
            printMetrics("任务堆积时", executor, rejectedCount.get());

            // 释放闸门，所有阻塞的工作线程开始执行任务
            releaseWorkers.countDown();

            // 等待全部10个任务执行完成，最多等待15秒
            assertTrue(
                completed.await(15, TimeUnit.SECONDS),
                "任务未能在规定时间内完成"
            );

            // 优雅关闭线程池，不再接受新任务
            executor.shutdown();

            // 等待线程池完全终止，最多等待5秒
            assertTrue(
                executor.awaitTermination(5, TimeUnit.SECONDS),
                "线程池未能正常关闭"
            );

            // ========== 采样点 B：所有任务执行完成，线程池已关闭 ==========
            printMetrics("任务完成后", executor, rejectedCount.get());

            // 打印等待时间和执行时间的统计汇总报告
            printTimingMetrics(waitTimes, executionTimes);

            // 完整性校验：队列容量8，总任务数10，2个运行+8个排队，没有任务会被拒绝
            assertEquals(0, rejectedCount.get());
            // ThreadPoolExecutor.getCompletedTaskCount() 返回线程池历史上累计完成的总任务数
            assertEquals(TASK_COUNT, executor.getCompletedTaskCount());
            // ThreadPoolExecutor.getTaskCount() 返回线程池历史上总接收过的任务数（包括已完成、运行中、排队中的任务）
            assertEquals(TASK_COUNT, executor.getTaskCount());

        } finally {
            // 异常安全保障：无论测试成功失败，都释放闸门，避免工作线程永久阻塞
            releaseWorkers.countDown();

            // 如果线程池还没有关闭，强制中断所有正在执行的任务
            if (!executor.isShutdown()) {
                executor.shutdownNow();
            }
        }
    }

    /**
     * 打印 ThreadPoolExecutor 所有核心运行时监控指标
     * <p>
     * 每个指标的生产环境含义：
     * - PoolSize: 当前线程池实际存活的工作线程数量
     * - ActiveCount: 当前正在执行任务的活跃线程数量
     * - CorePoolSize: 配置的核心线程数，线程池正常情况下会维持这么多线程存活
     * - MaximumPoolSize: 配置的最大线程数，队列满时最多可以扩容到这么多线程
     * - LargestPoolSize: 线程池历史上曾经达到过的最大线程数峰值，用于判断线程池是否曾经扩容过
     * - QueueSize: 当前阻塞队列中正在排队等待执行的任务数量
     * - QueueRemainingCapacity: 队列剩余可用容量，QueueSize + QueueRemainingCapacity = 队列总容量
     * - TaskCount: 线程池历史上累计接收过的总任务数（包括已完成、运行中、排队中）
     * - CompletedTaskCount: 线程池历史上累计已经执行完成的任务数
     * - RejectedCount: 本次实验统计的被拒绝策略拒绝的任务数量
     *
     * @param stage         当前采样阶段的名称，用于输出标识
     * @param executor      目标线程池实例
     * @param rejectedCount 统计得到的拒绝任务数
     */
    private void printMetrics(
        String stage,
        ThreadPoolExecutor executor,
        int rejectedCount
    ) {
        System.out.println();
        System.out.println("========== " + stage + " ==========");
        System.out.println("PoolSize             : " + executor.getPoolSize());
        System.out.println("ActiveCount          : " + executor.getActiveCount());
        System.out.println("CorePoolSize         : " + executor.getCorePoolSize());
        System.out.println("MaximumPoolSize      : " + executor.getMaximumPoolSize());
        System.out.println("LargestPoolSize      : " + executor.getLargestPoolSize());
        System.out.println("QueueSize            : " + executor.getQueue().size());
        System.out.println("QueueRemainingCapacity: " + executor.getQueue().remainingCapacity());
        System.out.println("TaskCount            : " + executor.getTaskCount());
        System.out.println("CompletedTaskCount   : " + executor.getCompletedTaskCount());
        System.out.println("RejectedCount        : " + rejectedCount);
    }

    /**
     * 打印任务等待时间和任务执行时间的汇总统计报告
     *
     * @param waitTimes      所有任务的队列等待时间集合
     * @param executionTimes 所有任务的实际业务执行时间集合
     */
    private void printTimingMetrics(
        ConcurrentLinkedQueue<Long> waitTimes,
        ConcurrentLinkedQueue<Long> executionTimes
    ) {
        printAverageAndMax("任务等待时间", waitTimes);
        printAverageAndMax("任务执行时间", executionTimes);
    }

    /**
     * 计算并打印指定指标的样本数、平均值和最大值
     * <p>
     * 生产环境用途：
     * - 平均等待时间可以反映系统当前的负载水平
     * - 最大等待时间可以反映系统在峰值压力下的最坏排队延迟
     *
     * @param name   指标显示名称
     * @param values 该指标的所有样本数据集合
     */
    private void printAverageAndMax(String name, ConcurrentLinkedQueue<Long> values) {
        // 线程安全地创建不可变副本，避免遍历过程中发生并发修改
        List<Long> list = List.copyOf(values);

        if (list.isEmpty()) {
            System.out.println(name + "：无数据");
            return;
        }

        // 计算所有样本的平均值
        double average = list.stream()
            .mapToLong(Long::longValue)
            .average()
            .orElse(0);

        // 找出所有样本中的最大值
        long max = list.stream()
            .mapToLong(Long::longValue)
            .max()
            .orElse(0);

        System.out.printf(
            "%s：样本数=%d，平均值=%.2fms，最大值=%dms%n",
            name, list.size(), average, max
        );
    }
}
