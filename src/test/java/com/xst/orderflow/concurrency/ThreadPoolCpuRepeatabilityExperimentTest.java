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

/**
 * CPU 密集型线程池性能可重复性实验测试类
 * <p>
 * 实验目的：
 * 验证在纯 CPU 计算场景下，不同工作线程数配置的线程池的吞吐量表现是否具备统计可重复性，
 * 观察随着工作线程数从 4 → 8 → 16 → 32 增长，系统吞吐量的变化趋势，
 * 直观展示 CPU 密集型任务的最佳线程数通常与 CPU 核心数强相关的经典性能规律。
 * <p>
 * 实验设计要点：
 * 1. 所有任务都是纯 CPU 计算，无任何 IO 等待、锁竞争或外部依赖
 * 2. 执行多轮重复测试，通过中位数、极值范围来消除 JVM 即时编译、调度抖动带来的测量误差
 * 3. 预热机制确保 JIT 编译完成后再开始正式性能测量，避免解释执行阶段的数据失真
 */
class ThreadPoolCpuRepeatabilityExperimentTest {

    // 单次实验总共提交的 CPU 密集型任务总数
    private static final int TASK_COUNT = 200;
    // 每个 CPU 任务内部执行的数学运算迭代次数，用于控制单任务计算耗时
    private static final int CPU_ITERATIONS = 5_000_000;
    // JVM 预热阶段执行的空跑任务数量，触发 JIT 编译、类加载、代码优化
    private static final int WARMUP_TASKS = 20;
    // 每种工作线程数配置重复执行的轮次，用于统计可重复性
    private static final int REPEAT_COUNT = 5;

    // 黑洞变量：防止 JVM 进行死代码消除优化，确保 cpuWork 方法不会被判定为无副作用而被完全删掉
    private static volatile long blackhole;

    /**
     * 主实验入口：验证不同工作线程数下 CPU 密集型任务的吞吐量表现具备统计可重复性
     * <p>
     * 实验流程：
     * 1. 打印当前运行环境的 CPU 核心数等基础信息
     * 2. 执行全局预热，触发 JIT 编译优化
     * 3. 依次使用 4、8、16、32 四种不同的工作线程数配置运行实验
     * 4. 每种配置重复执行 REPEAT_COUNT 轮，记录每一轮的吞吐量
     * 5. 最后输出汇总统计报告，展示中位数、最小值、最大值、波动范围
     * <p>
     * 预期观察结果：
     * - 当工作线程数 ≤ CPU 核心数时，吞吐量随线程数增加近似线性增长
     * - 当工作线程数 > CPU 核心数后，吞吐量增长停滞甚至开始下降，出现 CPU 上下文切换开销
     */
    @Test
    void shouldVerifyCpuThreadPoolRepeatability()
        throws InterruptedException {

        // 待测试的工作线程数梯度序列
        int[] workerCounts = {4, 8, 16, 32};

        System.out.println();
        System.out.println("========== EXP-009 Environment ==========");
        System.out.println("AvailableProcessors : "
            + Runtime.getRuntime().availableProcessors());
        System.out.println("TaskCount           : " + TASK_COUNT);
        System.out.println("CpuIterations       : " + CPU_ITERATIONS);
        System.out.println("RepeatCount         : " + REPEAT_COUNT);

        // 全局预热：确保 JIT 编译完成，所有热点代码都被优化后再开始正式测量
        warmUp();

        // 使用 LinkedHashMap 保持插入顺序，按工作线程数从小到大保存吞吐量统计结果
        Map<Integer, double[]> throughputResults =
            new LinkedHashMap<>();

        for (int workerCount : workerCounts) {

            // 保存当前工作线程数配置下，REPEAT_COUNT 轮实验的吞吐量数据
            double[] throughputs = new double[REPEAT_COUNT];

            System.out.println();
            System.out.println("========== Workers: "
                + workerCount + " ==========");

            for (int round = 0; round < REPEAT_COUNT; round++) {

                // 每一轮正式实验前再次预热，消除上一轮实验可能带来的调度残留影响
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

                // 验证所有 TASK_COUNT 个任务都被完整执行完成
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

            // 使用中位数而不是平均值，避免极端异常值对统计结果的干扰
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
        long result = 0;

        for (int i = 0; i < WARMUP_TASKS; i++) {
            result ^= cpuWork(i);
        }

        blackhole = result;
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
     * 使用指定工作线程数运行一次完整的 CPU 密集型实验
     * <p>
     * 实验流程：
     * 1. 创建固定大小线程池，队列容量设置为 TASK_COUNT，确保所有任务都能被接收
     * 2. 记录每个任务的精确开始时间和结束时间（纳秒级精度）
     * 3. 提交所有 TASK_COUNT 个任务后关闭线程池，等待所有任务执行完成
     * 4. 汇总统计总执行时间、平均单任务执行时间、系统整体吞吐量
     *
     * @param workerCount 本次实验使用的工作线程数量
     * @return 本次实验的完整统计结果对象
     */
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

        // 记录每个任务开始执行的纳秒时间戳
        long[] startTimes = new long[TASK_COUNT];
        // 记录每个任务执行完成的纳秒时间戳
        long[] endTimes = new long[TASK_COUNT];
        // 保存每个 cpuWork 方法的返回结果，用于后续完整性校验
        long[] results = new long[TASK_COUNT];

        // 整个实验开始的时刻（提交第一个任务之前）
        long experimentStart = System.nanoTime();

        try {
            for (int i = 0; i < TASK_COUNT; i++) {

                final int taskId = i;

                executor.execute(() -> {
                    startTimes[taskId] = System.nanoTime();

                    try {
                        results[taskId] = cpuWork(taskId);
                    } finally {
                        // 即使计算过程抛出异常，也保证结束时间戳被记录
                        endTimes[taskId] = System.nanoTime();
                    }
                });
            }
        } finally {
            // 所有任务提交完成后，优雅关闭线程池
            executor.shutdown();

            assertTrue(
                executor.awaitTermination(120, TimeUnit.SECONDS),
                "线程池未能在规定时间内结束"
            );
        }

        // 所有任务全部执行完成的时刻
        long experimentEnd = System.nanoTime();

        long totalExecutionNs = 0;
        int completed = 0;
        long checksum = 0;

        for (int i = 0; i < TASK_COUNT; i++) {

            // 验证所有任务的时间戳都被正确记录
            assertTrue(startTimes[i] > 0);
            assertTrue(endTimes[i] > startTimes[i]);

            totalExecutionNs += endTimes[i] - startTimes[i];
            checksum ^= results[i];
            completed++;
        }

        // 将最终校验和写入黑洞变量，防止 JVM 消除整个计算逻辑
        blackhole = checksum;

        long durationNs = experimentEnd - experimentStart;

        double durationMs = durationNs / 1_000_000.0;

        // 所有任务的平均执行时间（毫秒）
        double averageExecutionMs =
            totalExecutionNs / 1_000_000.0 / completed;

        // 系统整体吞吐量：单位时间内完成的任务总数（tasks/s）
        double throughput =
            completed * 1_000_000_000.0 / durationNs;

        return new Result(
            completed,
            durationMs,
            averageExecutionMs,
            throughput
        );
    }

    /**
     * 实验结果不可变值类
     * 封装单次实验的所有统计指标，使用私有构造器保证不可变性
     *
     * @param executed           本次实验实际完成的任务总数
     * @param durationMs         从提交第一个任务到所有任务全部完成的总耗时（毫秒）
     * @param averageExecutionMs 所有任务的平均单次执行时间（毫秒）
     * @param throughput         本次实验测得的系统整体吞吐量（任务数/秒）
     */
    private record Result(int executed, double durationMs, double averageExecutionMs, double throughput) {

    }
}
