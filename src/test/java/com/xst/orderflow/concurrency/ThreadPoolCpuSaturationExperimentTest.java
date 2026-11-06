package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CPU 密集型线程池饱和度拐点探测实验测试类
 * <p>
 * 实验目的：
 * 通过逐步增加工作线程数量，精确探测纯 CPU 密集型任务场景下的系统吞吐量饱和拐点：
 * 1. 观察吞吐量随工作线程数增长的变化趋势
 * 2. 验证当工作线程数 ≤ CPU 核心数时，吞吐量近似线性增长
 * 3. 观察当工作线程数超过 CPU 核心数后，吞吐量增长停滞甚至开始下降的现象
 * 4. 同时统计任务平均等待时间、最大等待时间等队列指标，完整呈现系统从低负载到 CPU 完全饱和的全生命周期表现
 * <p>
 * 核心设计优势：
 * - 所有任务一次性批量提交，消除生产者提交速率对实验结果的干扰
 * - 纳秒级精度记录任务提交、开始执行、执行完成三个关键时间点
 * - 所有统计计算在线程池完全关闭后进行，避免任务执行过程中争用共享变量
 */
class ThreadPoolCpuSaturationExperimentTest {

    // 单次实验一次性批量提交的 CPU 密集型任务总数
    private static final int TASK_COUNT = 200;
    // 有界队列容量设置为与任务总数相等，保证所有任务都能被线程池接收，不会触发拒绝策略
    private static final int QUEUE_CAPACITY = TASK_COUNT;

    // 每个 CPU 任务内部执行的数学运算迭代次数，控制单任务的计算耗时
    private static final int CPU_ITERATIONS = 5_000_000;
    // JVM 预热阶段执行的空跑任务数量，触发 JIT 编译、类加载、代码优化
    private static final int WARMUP_TASKS = 20;

    // 黑洞变量：使用 volatile 修饰，防止 JVM 判定 cpuWork 方法为无副作用而进行死代码消除优化
    private static volatile long blackhole;

    /**
     * 主实验入口：探测 CPU 密集型线程池的吞吐量饱和拐点
     * <p>
     * 实验设计：
     * 1. 首先获取当前运行环境的 CPU 核心数
     * 2. 执行 JVM 预热，确保 JIT 编译完成后再开始正式性能测量
     * 3. 依次使用 1、2、4、8、CPU核心数、CPU核心数*2 这6个梯度的工作线程数运行实验
     * 4. 使用 LinkedHashSet 自动去重，避免 CPU 核心数恰好等于8时重复测试相同配置
     * 5. 随着工作线程数逐步增加，观察吞吐量从线性增长到饱和拐点的完整变化曲线
     */
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

    /**
     * JVM 预热方法
     * <p>
     * 作用原理：
     * 反复执行 cpuWork 方法，触发 JVM JIT 编译器将热点字节码编译为高度优化的本地机器码，
     * 同时完成类加载、方法内联、逃逸分析等一系列 JVM 优化动作，
     * 确保后续正式性能测量阶段不会因为解释执行而引入巨大的测量误差。
     * <p>
     * 通过将计算结果写入 volatile 类型的 blackhole，彻底阻止 JVM 将整个预热循环判定为无副作用而被完全消除。
     */
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

    /**
     * 纯 CPU 密集型计算任务
     * <p>
     * 使用线性同余生成器（LCG）风格的位运算和乘法操作，
     * 构造一个完全无副作用、无分支预测失败、无内存访问的纯 CPU 计算负载，
     * 确保任务的执行时间完全由 CPU 运算速度决定，不受内存带宽、缓存命中率等其他因素干扰。
     *
     * @param seed 任务种子值，用于生成不同的计算路径
     * @return 计算结果校验和，用于验证任务执行的完整性
     */
    private long cpuWork(long seed) {
        long value = seed + 1;

        for (int i = 0; i < CPU_ITERATIONS; i++) {
            value = (value ^ (value >>> 13))
                * 0x5DEECE66DL + i;
        }

        return value;
    }

    /**
     * 使用指定工作线程数运行一次完整的饱和度探测实验
     * <p>
     * 实验流程：
     * 1. 创建固定大小线程池，队列容量等于任务总数，AbortPolicy 拒绝策略
     * 2. 一次性批量提交全部 TASK_COUNT 个任务，记录每个任务的提交时刻
     * 3. 每个任务被工作线程取出执行时，记录开始执行时刻
     * 4. 任务 CPU 计算完成后，记录执行结束时刻
     * 5. 优雅关闭线程池，等待所有任务执行完成
     * 6. 所有任务结束后再统一汇总所有统计指标，完全避免任务执行过程中争用共享变量
     *
     * @param workerCount 本次实验使用的工作线程数量
     */
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

        // 生产者尝试提交的任务总数
        AtomicInteger submitted = new AtomicInteger();
        // 线程池成功接收（进入队列或立即执行）的任务数量
        AtomicInteger accepted = new AtomicInteger();
        // 被拒绝策略拒绝的任务数量
        AtomicInteger rejected = new AtomicInteger();
        // 真正完整执行完成的任务数量
        AtomicInteger executed = new AtomicInteger();

        // 保存每个 cpuWork 方法的返回结果，用于后续完整性校验
        long[] results = new long[TASK_COUNT];
        // 记录每个任务被提交给线程池的纳秒时间戳
        long[] submitTimes = new long[TASK_COUNT];
        // 记录每个任务被工作线程从队列中取出、开始执行的纳秒时间戳
        long[] startTimes = new long[TASK_COUNT];
        // 记录每个任务 CPU 计算完成的纳秒时间戳
        long[] endTimes = new long[TASK_COUNT];

        // 初始化时间戳数组为 -1，用于后续校验任务是否被完整执行
        Arrays.fill(submitTimes, -1L);
        Arrays.fill(startTimes, -1L);
        Arrays.fill(endTimes, -1L);

        // 整个实验开始的时刻（提交第一个任务之前）
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
                        // 即使计算过程抛出异常，也保证结束时间戳被记录
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
            // 所有任务提交完成后，优雅关闭线程池
            executor.shutdown();

            // 等待所有任务执行完成，超时后强制中断所有剩余任务
            if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
                executor.shutdownNow();

                assertTrue(
                    executor.awaitTermination(5, TimeUnit.SECONDS),
                    "线程池未能在规定时间内结束"
                );
            }
        }

        // 所有任务全部执行完成的时刻
        long experimentEnd = System.nanoTime();

        // 所有任务结束后再汇总结果，避免任务之间争用共享变量
        long checksum = 0;

        for (long result : results) {
            checksum ^= result;
        }

        blackhole = checksum;

        // 整个实验从第一个任务提交到最后一个任务完成的总耗时
        long totalDurationNs = experimentEnd - experimentStart;

        // 所有任务在队列中的总等待时间之和
        long totalWaitNs = 0;
        // 所有任务中最长的单次等待时间
        long maxWaitNs = 0;
        // 所有任务的 CPU 计算时间之和
        long totalExecutionNs = 0;
        // 所有任务中最长的单次 CPU 计算时间
        long maxExecutionNs = 0;
        // 有效完成的任务样本数，用于后续平均值计算
        int completedSamples = 0;

        for (int i = 0; i < TASK_COUNT; i++) {
            // 跳过未被完整执行的任务
            if (startTimes[i] < 0 || endTimes[i] < 0) {
                continue;
            }

            // 任务等待时间 = 开始执行时刻 - 提交时刻
            long waitNs = startTimes[i] - submitTimes[i];
            // 任务执行时间 = 执行完成时刻 - 开始执行时刻
            long executionNs = endTimes[i] - startTimes[i];

            totalWaitNs += waitNs;
            maxWaitNs = Math.max(maxWaitNs, waitNs);

            totalExecutionNs += executionNs;
            maxExecutionNs = Math.max(maxExecutionNs, executionNs);

            completedSamples++;
        }

        // 所有任务的平均队列等待时间（毫秒）
        double averageWaitMs = completedSamples == 0
            ? 0
            : totalWaitNs / 1_000_000.0 / completedSamples;

        // 所有任务的最大队列等待时间（毫秒）
        double maxWaitMs = maxWaitNs / 1_000_000.0;

        // 所有任务的平均 CPU 计算时间（毫秒）
        double averageExecutionMs = completedSamples == 0
            ? 0
            : totalExecutionNs / 1_000_000.0 / completedSamples;

        // 所有任务的最大 CPU 计算时间（毫秒）
        double maxExecutionMs = maxExecutionNs / 1_000_000.0;

        // 整个实验的总耗时（毫秒）
        double totalDurationMs = totalDurationNs / 1_000_000.0;

        // 系统整体吞吐量：单位时间内完成的任务总数（tasks/s）
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

        // 完整性校验：确保所有任务都被完整提交、接收、执行完成
        assertEquals(TASK_COUNT, submitted.get());
        assertEquals(TASK_COUNT, accepted.get() + rejected.get());
        assertEquals(accepted.get(), executed.get());
        assertEquals(executed.get(), completedSamples);
        assertEquals(TASK_COUNT, completedSamples);
    }
}
