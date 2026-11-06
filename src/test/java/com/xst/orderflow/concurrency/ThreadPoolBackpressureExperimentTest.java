package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 线程池背压策略对比实验测试类
 * <p>
 * 实验目的：
 * 对比 ThreadPoolExecutor 两种最常用的饱和策略（AbortPolicy 和 CallerRunsPolicy）
 * 在任务提交速度 > 线程池消费速度场景下的行为差异，直观展示背压机制的工作原理：
 * 1. AbortPolicy：队列满时直接抛出异常拒绝新任务，快速失败
 * 2. CallerRunsPolicy：队列满时由提交任务的生产者线程自己执行溢出的任务，
 * 天然降低生产者提交速度，实现无额外等待的流量控制
 */
class ThreadPoolBackpressureExperimentTest {

    // 总共要提交的任务数量
    private static final int TASK_COUNT = 40;
    // 线程池核心/最大工作线程数，固定为2个消费线程
    private static final int WORKER_COUNT = 2;
    // 有界阻塞队列的容量大小
    private static final int QUEUE_CAPACITY = 8;

    // 每个任务模拟执行业务逻辑的耗时（毫秒）
    private static final long TASK_EXECUTION_MS = 500;
    // 生产者提交两个任务之间的时间间隔（毫秒）
    private static final long SUBMIT_INTERVAL_MS = 50;

    /**
     * 对比 AbortPolicy 和 CallerRunsPolicy 两种饱和策略的完整表现
     * <p>
     * 实验设计：
     * 使用完全相同的线程池配置、任务数量、提交速率，分别运行两次实验，
     * 统计两种策略下的拒绝数量、执行数量、生产者耗时、总耗时、实际吞吐量等关键指标，
     * 直观展示 CallerRunsPolicy 如何通过让提交线程执行任务实现天然背压限流。
     */
    @Test
    void shouldCompareAbortPolicyAndCallerRunsPolicy() throws InterruptedException {

        runExperiment("AbortPolicy", new ThreadPoolExecutor.AbortPolicy());

        runExperiment("CallerRunsPolicy", new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * 使用指定的拒绝策略运行一次完整的背压实验
     * <p>
     * 实验流程：
     * 1. 创建固定大小线程池，使用指定容量的有界队列和目标饱和策略
     * 2. 生产者线程以固定间隔持续提交任务，直到提交完所有 TASK_COUNT 个任务
     * 3. 实时统计队列的最大长度，确保队列不会超过预设容量
     * 4. 等待所有任务执行完成后输出完整统计报告
     *
     * @param policyName 当前实验使用的饱和策略名称，用于输出报告
     * @param handler    目标拒绝策略处理器
     */
    private void runExperiment(String policyName, RejectedExecutionHandler handler) throws InterruptedException {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            WORKER_COUNT,
            WORKER_COUNT,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(QUEUE_CAPACITY),
            handler
        );

        // 生产者总共尝试提交的任务总数
        AtomicInteger submitted = new AtomicInteger();
        // 线程池成功接收（要么立即执行，要么进入队列）的任务数量
        AtomicInteger accepted = new AtomicInteger();
        // 被拒绝策略直接拒绝的任务数量
        AtomicInteger rejected = new AtomicInteger();
        // 最终真正被执行完成的任务总数
        AtomicInteger executed = new AtomicInteger();
        // 由生产者线程（调用线程）直接执行的任务数量，CallerRunsPolicy 场景下统计有效
        AtomicInteger callerRuns = new AtomicInteger();

        // 记录实验过程中队列曾经达到过的最大长度
        AtomicInteger maxQueueSize = new AtomicInteger();

        // 线程安全集合，记录所有被成功执行过的任务ID
        Set<Integer> executedTaskIds = ConcurrentHashMap.newKeySet();

        // 记录生产者线程（测试主线程）的名称，用于判断任务是否由调用线程直接执行
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

                    // 判断当前执行任务的线程是不是生产者线程
                    // 如果是，说明这个任务是被 CallerRunsPolicy 交给提交线程直接执行的
                    if (Thread.currentThread().getName().equals(producerThreadName)) {
                        callerRuns.incrementAndGet();
                    }

                    try {
                        // 模拟业务逻辑执行耗时
                        Thread.sleep(TASK_EXECUTION_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                };

                try {
                    // 提交任务到线程池，队列满时触发拒绝策略
                    executor.execute(task);
                    accepted.incrementAndGet();
                } catch (RejectedExecutionException e) {
                    // AbortPolicy 会抛出异常，在这里统计被拒绝的任务
                    rejected.incrementAndGet();
                }

                // 实时更新队列历史最大长度
                maxQueueSize.accumulateAndGet(executor.getQueue().size(), Math::max);

                // 模拟生产者以固定间隔提交任务。
                if (i < TASK_COUNT) {
                    Thread.sleep(SUBMIT_INTERVAL_MS);
                }
            }

        } finally {
            // 生产者提交完所有任务的时刻
            long producerEnd = System.nanoTime();

            // 优雅关闭线程池，等待所有已接收的任务执行完成
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

        // 验证所有任务都有明确的去向：要么被执行，要么被拒绝
        assertEquals(TASK_COUNT, submitted.get());
        assertEquals(TASK_COUNT, executed.get() + rejected.get());
//        assertEquals(TASK_COUNT, executedTaskIds.size());
        // 验证队列长度始终没有超过预设的有界队列容量
        assertTrue(maxQueueSize.get() <= QUEUE_CAPACITY);
    }

    /**
     * 格式化打印本次背压实验的完整统计结果报告
     * <p>
     * 输出指标说明：
     * - Submitted: 生产者尝试提交的总任务数
     * - Accepted: 线程池成功接收的任务数
     * - Rejected: 被饱和策略拒绝的任务数
     * - Executed: 最终被执行完成的任务总数
     * - CallerRuns: 由生产者线程直接执行的任务数（CallerRunsPolicy 核心指标）
     * - MaxQueueSize: 队列历史最大长度，验证队列没有溢出
     * - ProducerDuration: 生产者从开始提交到提交完所有任务的总耗时
     * - TotalDuration: 从实验开始到所有任务全部执行完成的总耗时
     * - ActualThroughput: 整个实验过程的实际平均吞吐量（任务数/秒）
     *
     * @param policyName      饱和策略名称
     * @param submitted       提交任务总数
     * @param accepted        接收任务总数
     * @param rejected        拒绝任务总数
     * @param executed        执行完成任务总数
     * @param callerRuns      调用线程直接执行的任务数
     * @param maxQueueSize    队列历史最大长度
     * @param executedTaskIds 所有被执行过的任务ID集合
     * @param executor        线程池实例，用于获取线程池内置统计指标
     * @param producerNanos   生产者提交所有任务花费的纳秒数
     * @param totalNanos      整个实验从开始到所有任务完成花费的纳秒数
     */
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
