package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ThreadPoolTaskTypeExperimentTest
 * 性能对比测试类，用于验证不同线程数配置下，CPU密集型任务与IO密集型任务的线程池表现差异
 * 帮助理解线程数选择对不同类型工作负载的吞吐量、等待时间等指标的影响
 * <p>
 * 实验设计原理：
 * - CPU密集型任务：性能上限受限于CPU核心数，超过核心数的线程会导致上下文切换开销增加
 * - IO密集型任务：线程大部分时间处于阻塞等待状态，更多线程可以提高整体吞吐量
 * - 通过量化不同线程数下的各项性能指标，直观展示两种任务类型的最佳线程配置差异
 */
class ThreadPoolTaskTypeExperimentTest {

    // 单轮实验提交的总任务数量：控制实验样本规模，保证统计结果具有参考意义
    private static final int TASK_COUNT = 40;
    // 线程池阻塞队列的容量大小：与任务总数保持一致，确保队列不会因为容量不足触发拒绝策略
    private static final int QUEUE_CAPACITY = 40;

    // CPU密集型任务的迭代次数：通过大量循环运算模拟真实的计算密集型工作负载
    // 数值经过调整，确保单任务执行时间在合理范围内，便于观察性能变化趋势
    private static final int CPU_ITERATIONS = 5_000_000;
    // IO模拟等待时长：模拟真实IO操作的阻塞耗时，200ms是典型的磁盘IO或网络请求耗时量级
    private static final long IO_WAIT_MS = 200;

    // 保存计算结果的volatile变量：用于避免CPU计算逻辑被JVM判定为无效代码进行死代码消除优化
    // 保证所有计算操作都会被实际执行，确保性能测试结果的真实性
    private static volatile long blackhole;

    /**
     * 对比CPU密集型任务与IO密集型任务在不同工作线程数下的性能表现
     * 分别在线程数为1、2、4、8的场景下运行两类任务，输出完整的性能指标报告
     * <p>
     * 实验分组设计：
     * A-CPU ~ D-CPU：CPU密集型任务，线程数从1递增到8
     * E-IO ~ H-IO：IO密集型任务，线程数从1递增到8
     * <p>
     * 预期观察结果：
     * - CPU密集型：吞吐量随线程数增长到CPU核心数附近达到峰值，之后增长停滞甚至下降
     * - IO密集型：吞吐量随线程数增加持续提升，直到系统资源达到瓶颈
     *
     * @throws InterruptedException 线程等待中断异常
     */
    @Test
    void shouldCompareCpuAndIoBoundTasks() throws InterruptedException {

        // CPU 预热：降低首次 JIT 编译对正式实验的影响，保证测试结果准确性
        // 预热阶段执行多次相同类型的计算，触发JVM的JIT编译优化，让代码进入稳定执行状态
        warmUpCpu();

        // CPU 密集型任务测试：依次使用1、2、4、8个工作线程运行实验
        // 观察随着线程数增加，CPU计算能力的利用效率变化
        runExperiment("A-CPU", "CPU", 1);
        runExperiment("B-CPU", "CPU", 2);
        runExperiment("C-CPU", "CPU", 4);
        runExperiment("D-CPU", "CPU", 8);

        // I/O 密集型任务测试：依次使用1、2、4、8个工作线程运行实验
        // 观察随着线程数增加，IO等待时间的重叠利用效率变化
        runExperiment("E-IO", "IO", 1);
        runExperiment("F-IO", "IO", 2);
        runExperiment("G-IO", "IO", 4);
        runExperiment("H-IO", "IO", 8);
    }

    /**
     * JVM预热方法
     * 执行多次CPU密集型任务，触发JIT编译优化，避免后续正式测试结果受JVM启动阶段优化影响
     * <p>
     * JVM在启动初期会以解释模式执行字节码，执行一定次数后才会将热点代码编译为本地机器码
     * 预热操作可以让测试代码提前进入完全优化状态，确保正式实验的性能数据准确反映稳定运行时的表现
     */
    private void warmUpCpu() {
        for (int i = 0; i < 10; i++) {
            blackhole = cpuWork(i);
        }
    }

    /**
     * 模拟CPU密集型计算任务
     * 通过大量位运算和算术操作模拟真实的计算密集型工作负载
     * 使用线性同余生成器(LGC)风格的运算模式，保证计算过程无法被JVM进行常量折叠等优化
     *
     * @param seed 随机种子，用于生成不同的计算序列，确保每次任务的计算路径不完全相同
     * @return 计算结果值，用于防止JVM优化消除计算逻辑，结果写入volatile变量保证计算实际执行
     */
    private long cpuWork(long seed) {
        long value = seed + 1;

        for (int i = 0; i < CPU_ITERATIONS; i++) {
            // 组合位移运算和乘法运算，形成典型的CPU密集型计算模式
            // 该运算序列来自Java随机数生成器的实现，具有良好的计算分布特性
            value = (value ^ (value >>> 13))
                * 0x5DEECE66DL + i;
        }

        return value;
    }

    /**
     * 运行指定配置的线程池性能实验
     * 创建对应参数的线程池，提交指定数量的任务，采集完整的性能指标数据并输出报告
     * <p>
     * 采集的核心指标包括：
     * - 任务提交/接受/拒绝/完成的数量统计
     * - 任务等待时间（从提交到开始执行的耗时）
     * - 任务执行时间（任务实际运行的耗时）
     * - 实验总耗时与系统整体吞吐量
     *
     * @param group       实验组别名，用于标识不同的测试组合，方便区分实验输出结果
     * @param taskType    任务类型，支持"CPU"表示CPU密集型任务，"IO"表示IO密集型任务
     * @param workerCount 线程池工作线程数量，同时设置核心线程数和最大线程数，创建固定大小线程池
     * @throws InterruptedException 线程等待中断异常
     */
    private void runExperiment(
        String group,
        String taskType,
        int workerCount
    ) throws InterruptedException {

        // 创建固定大小的线程池，使用有界队列和Abort拒绝策略
        // 核心线程数 = 最大线程数 = workerCount，保证线程池大小稳定
        // 空闲线程存活时间设置为0，线程执行完任务后立即回收
        // ArrayBlockingQueue使用有界队列，容量与任务总数一致
        // AbortPolicy策略：队列满时直接抛出异常，便于统计被拒绝的任务数量
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            workerCount,
            workerCount,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(QUEUE_CAPACITY),
            new ThreadPoolExecutor.AbortPolicy()
        );

        // 统计已提交的任务总数：记录所有尝试提交给线程池的任务数量
        AtomicInteger submitted = new AtomicInteger();
        // 统计被线程池成功接受执行的任务总数：包括正在队列中等待和正在执行的任务
        AtomicInteger accepted = new AtomicInteger();
        // 统计被拒绝策略拒绝的任务总数：队列满后无法提交的任务数量
        AtomicInteger rejected = new AtomicInteger();
        // 统计实际完成执行的任务总数：已经完整执行完毕的任务数量
        AtomicInteger executed = new AtomicInteger();

        // 记录每个任务的提交时间戳：使用System.nanoTime()获取高精度纳秒级时间戳
        long[] submitTimes = new long[TASK_COUNT];
        // 记录每个任务的实际开始执行时间戳：任务线程真正开始运行的时刻
        long[] startTimes = new long[TASK_COUNT];
        // 记录每个任务的执行结束时间戳：任务执行完成的时刻
        long[] endTimes = new long[TASK_COUNT];

        // 初始化时间戳数组为-1，用于后续判断任务是否被正常执行
        // 未被执行的任务对应的时间戳将保持-1值，在统计阶段会被跳过
        Arrays.fill(submitTimes, -1L);
        Arrays.fill(startTimes, -1L);
        Arrays.fill(endTimes, -1L);

        // 记录实验整体开始时间：从第一个任务提交前开始计时
        long experimentStart = System.nanoTime();

        try {
            // 批量提交所有任务：在极短时间内将所有任务一次性提交给线程池
            // 模拟真实系统中突发大量任务的场景，测试线程池在高负载下的表现
            for (int i = 0; i < TASK_COUNT; i++) {
                final int taskId = i;

                // 记录任务提交时刻，原子递增提交计数器
                submitTimes[taskId] = System.nanoTime();
                submitted.incrementAndGet();

                Runnable task = () -> {
                    // 任务开始执行时记录时间戳：任务分配到工作线程，开始运行的时刻
                    startTimes[taskId] = System.nanoTime();

                    try {
                        // 根据任务类型执行不同的工作负载
                        if ("CPU".equals(taskType)) {
                            // 执行CPU密集型计算任务，结果写入volatile变量防止优化
                            blackhole = cpuWork(taskId);
                        } else {
                            // 通过线程休眠模拟IO阻塞等待场景
                            // 线程休眠时不会占用CPU，模拟网络IO或磁盘IO的阻塞状态
                            Thread.sleep(IO_WAIT_MS);
                        }

                        // 任务正常完成，递增执行完成计数器
                        executed.incrementAndGet();
                    } catch (InterruptedException e) {
                        // 捕获中断异常并重置中断标记位
                        // 恢复线程的中断状态，保留中断信号以便上层逻辑感知
                        Thread.currentThread().interrupt();
                    } finally {
                        // 任务执行完成时记录时间戳：无论任务正常结束还是异常终止，都记录结束时间
                        endTimes[taskId] = System.nanoTime();
                    }
                };

                try {
                    // 向线程池提交任务，成功则递增接受计数器
                    executor.execute(task);
                    accepted.incrementAndGet();
                } catch (RejectedExecutionException e) {
                    // 捕获任务拒绝异常，统计被拒绝的任务数量
                    // 当队列已满且没有可用线程时，AbortPolicy会抛出此异常
                    rejected.incrementAndGet();
                }
            }
        } finally {
            // 优雅关闭线程池，等待已提交的任务执行完成
            // 调用shutdown()后不再接受新任务，但会继续执行队列中已有的任务
            executor.shutdown();

            // 等待任务执行完成，超时后强制关闭线程池
            // 设置120秒的超时时间，确保所有任务都有充足的时间完成
            if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
                // 超时后调用shutdownNow()尝试中断所有正在执行的任务
                executor.shutdownNow();

                // 再次等待线程池终止，确保所有资源都被正确释放
                assertTrue(
                    executor.awaitTermination(5, TimeUnit.SECONDS),
                    "线程池未能在规定时间内结束"
                );
            }
        }

        // 记录实验整体结束时间：所有任务执行完成，线程池终止的时刻
        long experimentEnd = System.nanoTime();

        // 计算实验总耗时，转换为毫秒单位
        // 从第一个任务提交开始，到最后一个任务执行完成的总时间
        long totalDurationMs = TimeUnit.NANOSECONDS.toMillis(
            experimentEnd - experimentStart
        );

        // 所有任务的总等待时间（从提交到开始执行的时间差总和）
        // 反映所有任务在队列中排队等待的总耗时
        long totalWaitNs = 0;
        // 所有任务中的最大等待时间：排队最久的任务的等待时长
        long maxWaitNs = 0;
        // 所有任务的总执行时间（实际工作负载耗时总和）
        // 所有任务实际运行时间的累加值
        long totalExecutionNs = 0;
        // 所有任务中的最大执行时间：执行最慢的任务的耗时
        long maxExecutionNs = 0;
        // 成功完成执行的有效任务样本数：用于后续计算平均值的分母
        int completedSamples = 0;

        // 遍历所有任务计算各项统计指标
        for (int i = 0; i < TASK_COUNT; i++) {
            // 跳过未被正常执行的无效任务样本
            // 被拒绝的任务不会有有效的startTime和endTime，不参与统计计算
            if (startTimes[i] < 0 || endTimes[i] < 0) {
                continue;
            }

            // 计算单个任务的等待时间：开始执行时刻 - 提交时刻
            long waitNs = startTimes[i] - submitTimes[i];
            // 计算单个任务的执行时间：结束时刻 - 开始执行时刻
            long executionNs = endTimes[i] - startTimes[i];

            // 累加总等待时间，更新最大等待时间
            totalWaitNs += waitNs;
            maxWaitNs = Math.max(maxWaitNs, waitNs);

            // 累加总执行时间，更新最大执行时间
            totalExecutionNs += executionNs;
            maxExecutionNs = Math.max(maxExecutionNs, executionNs);

            // 有效样本计数加1
            completedSamples++;
        }

        // 计算平均等待时间（毫秒）：所有任务等待时间的算术平均值
        // 反映任务在队列中排队的平均等待延迟
        double averageWaitMs = completedSamples == 0
            ? 0
            : totalWaitNs / 1_000_000.0 / completedSamples;

        // 最大等待时间转换为毫秒单位
        double maxWaitMs = maxWaitNs / 1_000_000.0;

        // 计算平均执行时间（毫秒）：所有任务实际执行时间的算术平均值
        // 反映单个任务本身的平均耗时
        double averageExecutionMs = completedSamples == 0
            ? 0
            : totalExecutionNs / 1_000_000.0 / completedSamples;

        // 最大执行时间转换为毫秒单位
        double maxExecutionMs = maxExecutionNs / 1_000_000.0;

        // 计算系统吞吐量：每秒完成的任务数
        // 吞吐量 = 完成任务总数 / 总耗时，是衡量系统整体处理能力的核心指标
        double throughput = totalDurationMs == 0
            ? 0
            : executed.get() * 1000.0 / totalDurationMs;

        // 格式化输出当前实验组的所有性能指标
        // 统一的输出格式便于对比不同实验组之间的性能差异
        System.out.println();
        System.out.println("========== Group " + group + " ==========");
        System.out.println("TaskType           : " + taskType);
        System.out.println("WorkerCount        : " + workerCount);
        System.out.println("QueueCapacity      : " + QUEUE_CAPACITY);
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
        System.out.println("TotalDuration      : " + totalDurationMs + " ms");
        System.out.println("ActualThroughput   : "
            + String.format("%.2f", throughput) + " tasks/s");

        // 校验实验结果的一致性，确保统计逻辑正确
        // 这些断言保证计数器之间的数学关系符合预期，排除代码逻辑错误
        assertEquals(TASK_COUNT, submitted.get());
        assertEquals(TASK_COUNT, accepted.get() + rejected.get());
        assertEquals(accepted.get(), executed.get());
        assertEquals(executed.get(), completedSamples);
    }
}
