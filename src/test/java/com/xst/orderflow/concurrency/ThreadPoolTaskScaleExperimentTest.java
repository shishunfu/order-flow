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

/**
 * 线程池任务规模对比实验测试类
 * 用于测试不同工作线程数和不同任务总数场景下，线程池的各项性能指标表现
 * 通过多轮重复执行、随机打乱测试顺序、JIT预热和中位数统计等手段，
 * 确保性能测试结果的准确性、可复现性和低误差率。
 * <p>
 * 实验目标：
 * 1. 观察线程数与CPU核心数的关系对CPU密集型任务吞吐量的影响
 * 2. 测量不同任务规模下任务的平均等待时间变化趋势
 * 3. 分析线程池队列积压程度对整体性能的影响
 */
class ThreadPoolTaskScaleExperimentTest {

    // CPU密集型任务的迭代次数，用于模拟计算密集型工作负载
    // 数值越大，单个任务的CPU计算耗时越长，可根据机器性能调整
    private static final int CPU_ITERATIONS = 5_000_000;

    // 预热任务数量，用于JIT编译预热，避免测试结果受JVM启动影响
    // 预热阶段会触发类加载、解释执行、JIT编译和代码优化，确保正式测试运行在优化后的稳态
    private static final int WARMUP_TASKS = 20;

    // 每组测试用例的重复执行次数，用于统计中位数结果减少随机误差
    // 多次执行后取中位数可以有效消除单次测试的系统干扰和GC抖动影响
    private static final int REPEAT_COUNT = 5;

    // 待测试的工作线程数配置数组
    // 覆盖从小于CPU核心数到大于CPU核心数的多种场景，观察线程数伸缩对性能的影响
    private static final int[] WORKER_COUNTS = {8, 16, 32};

    // 待测试的总任务数配置数组
    // 覆盖轻负载、中等负载和高积压负载三种场景，观察任务队列长度变化带来的性能差异
    private static final int[] TASK_COUNTS = {200, 1000, 2000};

    // 用于防止JVM进行死代码消除优化的共享变量
    // 使用volatile修饰保证可见性，确保JVM不会将没有外部副作用的cpuWork方法判定为无效代码而消除
    private static volatile long blackhole;

    /**
     * 对比不同工作线程数和任务规模下的线程池性能表现
     * 测试过程会多轮重复执行并打乱测试顺序，保证结果的准确性和可复现性
     * <p>
     * 测试流程：
     * 1. 打印当前运行环境信息
     * 2. 生成所有工作线程数 × 任务数的组合测试用例
     * 3. 多轮重复执行所有测试用例，每轮内部再次打乱测试顺序
     * 4. 每轮测试前执行JVM预热，避免测试间的相互干扰
     * 5. 实时输出每个测试用例的性能指标
     * 6. 最后汇总所有轮次结果，输出中位数统计报告
     *
     * @throws InterruptedException 线程等待中断异常，线程池关闭等待过程中可能抛出
     */
    @Test
    void shouldCompareWorkerCountAndTaskScale()
        throws InterruptedException {

        System.out.println();
        System.out.println("========== EXP-010 Environment ==========");
        // 输出当前JVM可用的CPU核心数，作为后续性能数据分析的基准参考
        System.out.println("AvailableProcessors: "
            + Runtime.getRuntime().availableProcessors());
        System.out.println("CpuIterations      : " + CPU_ITERATIONS);
        System.out.println("RepeatCount        : " + REPEAT_COUNT);

        List<TestCase> testCases = new ArrayList<>();

        // 生成所有组合的测试用例：遍历任务数数组和线程数数组，生成完整笛卡尔积测试集合
        for (int taskCount : TASK_COUNTS) {
            for (int workerCount : WORKER_COUNTS) {
                testCases.add(new TestCase(workerCount, taskCount));
            }
        }

        // 固定随机种子，保证测试顺序可以复现
        // 相同种子生成相同的随机序列，确保每次运行测试用例的打乱顺序完全一致，便于问题排查和结果对比
        Collections.shuffle(testCases, new Random(20261104));

        List<Result> allResults = new ArrayList<>();

        // 多轮重复执行测试，每一轮完整执行一遍所有测试用例
        for (int round = 1; round <= REPEAT_COUNT; round++) {

            System.out.println();
            System.out.println("========== ROUND "
                + round + " ==========");

            List<TestCase> roundCases = new ArrayList<>(testCases);

            // 每轮改变测试顺序，减少固定顺序带来的系统误差影响
            // 避免"前面的测试用例总是处于JVM冷态，后面的测试用例总是处于JVM热态"的顺序偏差问题
            Collections.shuffle(
                roundCases,
                new Random(20261104L + round)
            );

            for (TestCase testCase : roundCases) {

                // 执行预热操作，确保JIT编译完成
                // 每个测试用例执行前都预热一次，消除上一个测试用例遗留的影响，保证每个用例都在相同的JVM稳态下运行
                warmUp();

                // 运行当前配置的性能实验，采集完整的性能指标数据
                Result result = runExperiment(
                    testCase.workerCount,
                    testCase.taskCount
                );

                result.round = round;
                allResults.add(result);

                // 实时打印当前测试结果，便于长时间测试过程中观察进度和初步数据
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

        // 打印所有测试结果的汇总统计信息，输出中位数聚合后的最终实验结论数据
        printSummary(allResults);
    }

    /**
     * JVM预热方法
     * 执行若干次CPU密集型任务，触发JIT编译优化，避免后续正式测试结果失真
     * <p>
     * 预热原理：
     * JVM启动初期处于解释执行模式，性能很低，只有当方法被调用足够多次后
     * 才会触发JIT即时编译，将字节码编译成本地机器码并进行深度优化。
     * 预热操作就是提前把测试中要用到的代码路径跑多遍，让JVM完成所有编译优化，
     * 确保正式测试时程序已经运行在最高性能的稳态。
     */
    private void warmUp() {

        long result = 0;

        for (int i = 0; i < WARMUP_TASKS; i++) {
            result ^= cpuWork(i);
        }

        blackhole = result;
    }

    /**
     * 模拟CPU密集型计算任务
     * 通过大量位运算和算术操作模拟真实的计算密集型工作负载
     * 使用类似线性同余生成器的算法，保证计算过程无法被JVM进行常量折叠或向量化优化消除
     *
     * @param seed 随机种子，用于生成不同的计算序列，确保每次调用的计算路径都不相同
     * @return 计算结果值，用于防止JVM优化消除计算逻辑，返回值必须被外部使用
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
     * 运行指定配置的线程池性能实验
     * 创建对应参数的线程池，提交指定数量的任务，采集完整的性能指标数据
     * <p>
     * 实验设计要点：
     * 1. 线程池核心线程数和最大线程数相等，创建固定大小线程池，避免动态扩容带来的性能干扰
     * 2. 队列容量等于总任务数，确保所有任务都能入队，不会触发拒绝策略
     * 3. 一次性批量提交所有任务，避免任务提交过程本身成为瓶颈
     * 4. 精确记录每个任务的提交、开始执行、结束执行三个时间点，计算等待时间和执行时间
     *
     * @param workerCount 线程池工作线程数量，即同时并发执行任务的最大线程数
     * @param taskCount   提交的总任务数量，所有任务都会被执行完成
     * @return 包含所有性能指标的结果对象，包含耗时、等待时间、执行时间、吞吐量等统计数据
     * @throws InterruptedException 线程等待中断异常，awaitTermination等待过程中可能抛出
     */
    private Result runExperiment(
        int workerCount,
        int taskCount) throws InterruptedException {

        // 创建固定大小的线程池，使用有界队列和Abort拒绝策略
        // 核心线程数=最大线程数=workerCount，空闲超时时间为0，线程创建后不会被回收
        // 队列容量设置为taskCount，保证所有任务都能入队，不会出现任务被拒绝的情况
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            workerCount,
            workerCount,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(taskCount),
            new ThreadPoolExecutor.AbortPolicy()
        );

        // 记录每个任务的提交时间戳（纳秒级精度），调用execute()方法之前的时刻
        long[] submitTimes = new long[taskCount];
        // 记录每个任务的实际开始执行时间戳，任务中的代码真正开始运行的时刻
        long[] startTimes = new long[taskCount];
        // 记录每个任务的执行结束时间戳，cpuWork计算完成后的时刻
        long[] endTimes = new long[taskCount];
        // 记录每个任务的计算结果，用于校验所有任务都被完整执行，同时防止JVM优化消除计算逻辑
        long[] results = new long[taskCount];

        // 实验整体开始时间，所有任务开始提交前的时刻
        long experimentStart = System.nanoTime();

        try {
            // 批量提交所有任务，在主线程中一次性把所有任务提交给线程池
            for (int i = 0; i < taskCount; i++) {

                final int taskId = i;

                submitTimes[taskId] = System.nanoTime();

                executor.execute(() -> {

                    // 任务开始执行时记录时间戳，这行代码是任务在线程池中真正被调度执行的第一行
                    startTimes[taskId] = System.nanoTime();

                    try {
                        results[taskId] = cpuWork(taskId);
                    } finally {
                        // 任务执行完成时记录时间戳，无论计算成功还是抛出异常都会记录结束时间
                        endTimes[taskId] = System.nanoTime();
                    }
                });
            }
        } finally {
            // 优雅关闭线程池并等待所有任务执行完成
            // shutdown()方法不再接受新任务，但会继续执行队列中已有的所有任务
            executor.shutdown();

            // 等待最长180秒，确保所有任务都执行完毕，超时则断言失败避免测试无限挂起
            assertTrue(
                executor.awaitTermination(180, TimeUnit.SECONDS),
                "线程池未能在规定时间内结束"
            );
        }

        // 实验整体结束时间，所有任务全部执行完成后的时刻
        long experimentEnd = System.nanoTime();

        // 所有任务的总等待时间（从提交到开始执行的时间差总和）
        // 等待时间 = 任务开始执行时刻 - 任务提交时刻，反映任务在队列中排队等待调度的总时长
        long totalWaitNs = 0;
        // 所有任务的总执行时间（实际CPU计算时间总和）
        // 执行时间 = 任务结束时刻 - 任务开始时刻，反映任务真正占用CPU进行计算的时长
        long totalExecutionNs = 0;

        // 最大等待时间，所有任务中排队等待时间最长的那个值
        long maxWaitNs = 0;
        // 最大执行时间，所有任务中CPU计算耗时最长的那个值
        long maxExecutionNs = 0;

        long checksum = 0;

        // 遍历所有任务计算各项统计指标
        for (int i = 0; i < taskCount; i++) {

            // 验证时间戳有效性，确保所有任务都被正确执行
            // 如果startTimes[i]为0，说明该任务从来没有被线程池调度执行过
            assertTrue(startTimes[i] > 0);
            // 确保结束时间一定大于开始时间，时间戳逻辑合法
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

            // 对所有任务的计算结果做异或校验，确保没有任何一个任务的计算逻辑被JVM优化消除
            checksum ^= results[i];
        }

        blackhole = checksum;

        // 整个实验从第一个任务提交到最后一个任务完成的总耗时
        long durationNs = experimentEnd - experimentStart;

        Result result = new Result();

        result.workerCount = workerCount;
        result.taskCount = taskCount;

        // 实验总耗时转换为毫秒，保留纳秒级精度后除以1e6得到毫秒值
        result.durationMs = durationNs / 1_000_000.0;

        // 平均等待时间（毫秒），所有任务的排队等待时间总和除以任务总数
        // 这个指标反映了线程池的调度延迟和队列积压程度
        result.averageWaitMs =
            totalWaitNs / 1_000_000.0 / taskCount;

        result.maxWaitMs = maxWaitNs / 1_000_000.0;

        // 平均执行时间（毫秒），单个CPU密集型任务的平均计算耗时
        // 在CPU没有饱和的情况下，这个值应该基本保持稳定
        result.averageExecutionMs =
            totalExecutionNs / 1_000_000.0 / taskCount;

        result.maxExecutionMs = maxExecutionNs / 1_000_000.0;

        // 系统吞吐量：每秒完成的任务数
        // 吞吐量 = 总任务数 / 总耗时（秒），是衡量线程池整体处理能力的最核心指标
        result.throughput =
            taskCount * 1_000_000_000.0 / durationNs;

        return result;
    }

    /**
     * 打印所有测试结果的汇总统计信息
     * 对每组相同配置的多次测试结果取中位数，减少异常值对统计结果的影响
     * <p>
     * 使用中位数而不是平均值的优势：
     * 性能测试过程中经常会遇到GC停顿、操作系统调度干扰、后台进程抢占CPU等偶发事件，
     * 这些事件会产生极端异常值，平均值对异常值非常敏感，而中位数具有很强的鲁棒性，
     * 不会被少数极端值带偏，能够更准确地反映真实的稳态性能水平。
     *
     * @param results 所有轮次的全部测试结果集合，包含每一轮每一个测试用例的原始数据
     */
    private void printSummary(List<Result> results) {
        System.out.println();
        System.out.println("========== SUMMARY ==========");

        // 按任务数分组，同一任务数下遍历所有不同的线程数配置
        for (int taskCount : TASK_COUNTS) {

            for (int workerCount : WORKER_COUNTS) {

                List<Double> throughputs = new ArrayList<>();
                List<Double> waits = new ArrayList<>();
                List<Double> durations = new ArrayList<>();

                // 筛选出相同配置下的所有测试结果，把多轮重复执行的同参数结果归集到一起
                for (Result result : results) {

                    if (result.taskCount == taskCount
                        && result.workerCount == workerCount) {

                        throughputs.add(result.throughput);
                        waits.add(result.averageWaitMs);
                        durations.add(result.durationMs);
                    }
                }

                // 排序后取中位数，避免极端值影响
                Collections.sort(throughputs);
                Collections.sort(waits);
                Collections.sort(durations);

                // 打印中位数统计结果
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

    /**
     * 计算有序Double列表的中位数
     * 中位数是将数据集合排序后位于中间位置的值，对极端异常值具有很强的鲁棒性
     * 在性能测试统计中使用中位数可以有效消除GC停顿、系统调度干扰等偶发因素导致的异常值偏差
     * <p>
     * 中位数计算规则：
     * 1. 空列表直接返回0作为默认值
     * 2. 元素个数为奇数时，直接取正中间位置的元素值
     * 3. 元素个数为偶数时，取中间两个相邻元素的平均值作为中位数结果
     * <p>
     * 注意：调用该方法前必须确保输入列表已经按升序排序完成
     *
     * @param values 已完成升序排序的Double值列表，不能为null
     * @return 列表元素的中位数，空列表返回0
     */
    private double median(List<Double> values) {

        int size = values.size();

        if (size == 0) {
            return 0;
        }

        if (size % 2 == 1) {
            return values.get(size / 2);
        }

        // 偶数个元素时取中间两个值的平均值
        return (
            values.get(size / 2 - 1)
                + values.get(size / 2)
        ) / 2.0;
    }

    /**
     * 测试用例内部类
     * 作为不可变值对象，完整封装一组性能实验的输入参数组合
     * 用于在测试框架中统一传递和标识不同的实验配置，支持随机打乱和多轮重复执行
     * <p>
     * 类设计特性：
     * - 所有字段使用final修饰，保证对象不可变，线程安全
     * - 仅包含两个核心参数：工作线程数和总任务数
     * - 用于生成笛卡尔积全量测试集合，覆盖所有待验证的配置组合
     */
    private static class TestCase {

        private final int workerCount;
        private final int taskCount;

        private TestCase(int workerCount, int taskCount) {
            this.workerCount = workerCount;
            this.taskCount = taskCount;
        }
    }

    /**
     * 测试结果内部类
     * 作为数据传输对象，完整封装单次性能实验运行后采集到的所有性能指标数据
     * 每个Result对象对应一次特定workerCount和taskCount配置下的完整实验执行结果
     * <p>
     * 包含的指标维度：
     * - 元数据信息：轮次编号、工作线程数配置、总任务数配置
     * - 时间维度指标：实验总耗时、平均等待时间、最大等待时间
     * - 执行维度指标：平均执行时间、最大执行时间
     * - 吞吐维度指标：系统每秒完成的任务数吞吐量
     * <p>
     * 所有指标后续可用于多轮结果聚合、中位数统计和最终实验报告生成
     */
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
