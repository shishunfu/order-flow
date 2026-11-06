package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 线程池队列容量实验测试类
 * 用于对比不同工作线程数和队列容量组合下，线程池的性能表现与过载保护行为
 * 通过控制变量实验，量化分析队列大小对任务等待时间、吞吐量、拒绝策略的影响
 */
class ThreadPoolQueueCapacityExperimentTest {

    // 单次实验提交的总任务数
    private static final int TASK_COUNT = 1000;
    // CPU密集型任务的迭代次数，用于模拟真实的CPU计算负载
    private static final int CPU_ITERATIONS = 5_000_000;
    // JVM预热任务数，避免JIT编译、类加载等因素影响实验结果准确性
    private static final int WARMUP_TASKS = 20;
    // 每组实验重复执行次数，用于取中位数消除随机波动
    private static final int REPEAT_COUNT = 3;

    // 待测试的工作线程数配置数组
    private static final int[] WORKER_COUNTS = {8, 16};
    // 待测试的阻塞队列容量配置数组
    private static final int[] QUEUE_CAPACITIES = {16, 64, 256, 1000};

    // 用于防止JVM进行死代码消除优化的共享变量
    private static volatile long blackhole;

    /**
     * 对比不同队列容量和过载保护策略的主测试方法
     * 执行多轮随机排序的对照实验，收集完整性能数据后输出统计摘要
     * <p>
     * 实验设计要点：
     * 1. 每轮实验随机打乱测试用例顺序，避免顺序带来的系统误差
     * 2. 每组配置重复多次，使用中位数而非平均值获得更稳健的统计结果
     * 3. 完整记录任务提交、开始执行、执行结束三个时间点，精确计算等待和执行耗时
     */
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

        // 生成所有工作线程数与队列容量的组合测试用例
        List<TestCase> testCases = new ArrayList<>();

        for (int workerCount : WORKER_COUNTS) {
            for (int queueCapacity : QUEUE_CAPACITIES) {
                testCases.add(
                    new TestCase(workerCount, queueCapacity)
                );
            }
        }

        // 存储所有轮次的实验结果
        List<Result> allResults = new ArrayList<>();

        for (int round = 1; round <= REPEAT_COUNT; round++) {

            // 每轮开始前打乱测试用例顺序，消除实验顺序带来的偏差
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

                // 执行预热操作，确保JIT编译完成后再开始正式实验
                warmUp();

                Result result = runExperiment(
                    testCase.workerCount,
                    testCase.queueCapacity
                );

                result.round = round;
                allResults.add(result);

                // 实时输出单条实验结果明细
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

        // 输出所有实验结果的中位数统计摘要
        printSummary(allResults);
    }

    /**
     * JVM预热方法
     * 提前执行若干次CPU密集型任务，触发JIT即时编译，避免首次执行的性能开销干扰实验数据
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
     * 使用线性同余生成器风格的位运算循环，避免JVM进行循环展开等优化，产生稳定的CPU负载
     *
     * @param seed 随机种子值
     * @return 计算结果校验和
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
     * 执行单组配置的完整线程池实验
     * 创建指定参数的线程池，批量提交任务，精确采集所有任务的时间指标
     *
     * @param workerCount   核心/最大工作线程数
     * @param queueCapacity 阻塞队列容量
     * @return 包含所有性能指标的实验结果对象
     * @throws InterruptedException 线程等待终止时被中断抛出
     */
    private Result runExperiment(
        int workerCount,
        int queueCapacity) throws InterruptedException {

        // 创建固定大小线程池，使用ArrayBlockingQueue作为工作队列，AbortPolicy作为拒绝策略
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            workerCount,
            workerCount,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(queueCapacity),
            new ThreadPoolExecutor.AbortPolicy()
        );

        // 记录每个任务的提交时间戳（纳秒级）
        long[] submitTimes = new long[TASK_COUNT];
        // 记录每个任务实际开始执行的时间戳（纳秒级）
        long[] startTimes = new long[TASK_COUNT];
        // 记录每个任务执行完成的时间戳（纳秒级）
        long[] endTimes = new long[TASK_COUNT];
        // 存储每个CPU任务的计算结果，用于校验完整性
        long[] results = new long[TASK_COUNT];

        // 初始化时间数组为-1，用于标记未成功执行的任务
        Arrays.fill(startTimes, -1L);
        Arrays.fill(endTimes, -1L);

        int accepted = 0;
        int rejected = 0;

        // 实验全局开始时间
        long experimentStart = System.nanoTime();

        try {
            // 批量提交所有任务
            for (int i = 0; i < TASK_COUNT; i++) {

                final int taskId = i;

                submitTimes[taskId] = System.nanoTime();

                Runnable task = () -> {

                    // 任务实际开始执行时记录时间戳
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
                    // 队列满且所有线程繁忙时触发拒绝策略
                    rejected++;
                }
            }
        } finally {
            // 停止接收新任务，等待已提交任务全部完成
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

        // 统计所有成功执行任务的累计等待时间和执行时间
        long totalWaitNs = 0;
        long totalExecutionNs = 0;

        long maxWaitNs = 0;
        long maxExecutionNs = 0;

        long checksum = 0;
        int executed = 0;

        // 遍历所有任务，计算各项聚合指标
        for (int i = 0; i < TASK_COUNT; i++) {

            // 跳过被拒绝未执行的任务
            if (startTimes[i] < 0 || endTimes[i] < 0) {
                continue;
            }

            // 等待时间 = 任务开始执行时间 - 任务提交时间
            long waitNs = startTimes[i] - submitTimes[i];
            // 执行时间 = 任务结束时间 - 任务开始时间
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

        // 校验提交任务总数等于接受+拒绝之和，且接受的任务全部成功执行
        assertEquals(TASK_COUNT, accepted + rejected);
        assertEquals(accepted, executed);

        return result;
    }

    /**
     * 打印所有实验结果的统计摘要
     * 对每组配置的多次重复实验结果取中位数，输出更具代表性的统计值
     * 使用中位数而非平均值可以有效避免极端异常值对结论的干扰
     *
     * @param results 所有轮次的完整实验结果集合
     */
    private void printSummary(List<Result> results) {

        System.out.println();
        System.out.println("========== SUMMARY ==========");

        for (int workerCount : WORKER_COUNTS) {

            for (int queueCapacity : QUEUE_CAPACITIES) {

                List<Double> throughputs = new ArrayList<>();
                List<Double> waits = new ArrayList<>();
                List<Double> durations = new ArrayList<>();
                List<Integer> rejectedCounts = new ArrayList<>();

                // 按配置分组收集所有轮次的实验数据
                for (Result result : results) {

                    if (result.workerCount == workerCount
                        && result.queueCapacity == queueCapacity) {

                        throughputs.add(result.throughput);
                        waits.add(result.averageWaitMs);
                        durations.add(result.durationMs);
                        rejectedCounts.add(result.rejected);
                    }
                }

                // 排序后取中位数
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

    /**
     * 计算Double类型列表的中位数
     * 奇数长度取中间元素，偶数长度取中间两个元素的平均值
     *
     * @param values 已排序的数值列表
     * @return 中位数结果
     */
    private double median(List<Double> values) {
        int size = values.size();
        if (size == 0) {
            return 0;
        }
        if (size % 2 == 1) {
            return values.get(size / 2);
        }
        return (values.get(size / 2 - 1) + values.get(size / 2)) / 2.0;
    }

    /**
     * 计算Integer类型列表的中位数
     * 奇数长度取中间元素，偶数长度取中间两个元素的整数平均值
     *
     * @param values 已排序的整数列表
     * @return 中位数结果
     */
    private int medianInt(List<Integer> values) {
        int size = values.size();
        if (size == 0) {
            return 0;
        }
        if (size % 2 == 1) {
            return values.get(size / 2);
        }
        return (values.get(size / 2 - 1) + values.get(size / 2)) / 2;
    }

    /**
         * 测试用例值对象
         * 封装一组待实验的线程池配置参数
         */
        private record TestCase(int workerCount, int queueCapacity) {

    }

    /**
     * 实验结果值对象
     * 存储单次实验的所有性能指标数据
     */
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
