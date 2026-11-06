package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程池队列容量与任务等待时间关系实验测试类
 * <p>
 * 实验目的：
 * 通过控制不同的队列容量，观察任务在队列中的等待时间分布，
 * 理解有界队列大小如何影响任务排队行为、拒绝策略触发时机，
 * 以及线程池任务调度的真实表现。
 */
class QueueWaitTimeExperimentTest {

    // 线程池核心线程数和最大线程数，固定为2个工作线程
    private static final int THREAD_COUNT = 2;
    // 总共要提交的任务数量
    private static final int TASK_COUNT = 10;
    // 每个任务模拟执行业务逻辑的持续时间（毫秒）
    private static final long TASK_DURATION_MS = 500;

    /**
     * 对比不同队列容量下的任务等待时间表现
     * <p>
     * 实验设计：
     * 依次使用 3、8、20 三种不同的队列容量运行实验，
     * 观察队列满时任务被拒绝的数量，以及不同队列容量下任务的平均等待时间差异。
     */
    @Test
    void compareQueueCapacityAndTaskWaitTime() throws InterruptedException {
        List<Integer> capacities = List.of(3, 8, 20);

        for (int capacity : capacities) {
            runExperiment(capacity);
        }
    }

    /**
     * 使用指定队列容量执行一次完整的实验
     * <p>
     * 实验流程设计：
     * 1. 创建固定大小线程池，使用 ArrayBlockingQueue 有界队列
     * 2. 先提交2个任务，让两个工作线程启动并阻塞在闸门处
     * 3. 确认两个工作线程都已就绪后，再提交剩余的所有任务
     * 4. 此时工作线程全部被占用，后续提交的任务会进入队列排队
     * 5. 释放闸门，让所有任务开始真正执行业务逻辑
     * 6. 统计每个任务的等待时间和执行时间
     *
     * @param queueCapacity 本次实验使用的有界队列容量
     */
    private void runExperiment(int queueCapacity)
        throws InterruptedException {

        // 创建固定大小线程池，使用指定容量的有界队列，拒绝策略为 AbortPolicy（抛出异常）
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            THREAD_COUNT,
            THREAD_COUNT,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(queueCapacity),
            new ThreadPoolExecutor.AbortPolicy()
        );

        // 同步闸门：等待前两个工作线程都启动并进入阻塞状态
        CountDownLatch firstWorkersStarted = new CountDownLatch(THREAD_COUNT);
        // 同步闸门：主线程通知所有阻塞的工作线程可以开始执行业务逻辑
        CountDownLatch releaseWorkers = new CountDownLatch(1);

        // 线程安全的指标收集列表，用于保存每个任务的时间统计数据
        List<TaskMetric> metrics = new CopyOnWriteArrayList<>();
        // 记录被线程池拒绝的任务数量
        AtomicInteger rejectedCount = new AtomicInteger();

        try {
            // 先提交两个任务，确保两个工作线程都已启动并被闸门阻塞
            for (int i = 1; i <= THREAD_COUNT; i++) {
                submitTask(
                    executor, i, firstWorkersStarted,
                    releaseWorkers, metrics, rejectedCount
                );
            }

            // 等待两个工作线程完全启动，最多等待5秒防止测试挂死
            boolean workersReady =
                firstWorkersStarted.await(5, TimeUnit.SECONDS);

            if (!workersReady) {
                throw new IllegalStateException("工作线程未能按时启动");
            }

            // 再提交剩余任务，此时两个工作线程都被占用，新任务会进入队列排队
            // 如果队列容量不足，超出的任务会直接触发拒绝策略
            for (int i = THREAD_COUNT + 1; i <= TASK_COUNT; i++) {
                submitTask(
                    executor, i, firstWorkersStarted,
                    releaseWorkers, metrics, rejectedCount
                );
            }

            // 打印当前实验的基础状态信息
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
            // 释放闸门，所有阻塞的任务开始执行业务逻辑
            releaseWorkers.countDown();
            // 优雅关闭线程池
            executor.shutdown();

            // 等待所有任务执行完成，最多等待15秒
            if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                throw new IllegalStateException("线程池未能正常结束");
            }
        }

        // 打印本次实验的详细指标统计
        printMetrics(metrics);
    }

    /**
     * 向线程池提交一个任务，并记录任务全生命周期的时间点
     * <p>
     * 时间点说明：
     * - submitTime: 任务被提交给线程池的时刻
     * - startTime: 任务被工作线程从队列中取出，开始执行的时刻
     * - executionStart: 任务被释放闸门放行，开始执行业务逻辑的时刻
     * - endTime: 任务执行业务逻辑完成的时刻
     *
     * @param executor            目标线程池
     * @param taskId              任务唯一标识ID
     * @param firstWorkersStarted 工作线程启动同步闸门
     * @param releaseWorkers      任务执行放行同步闸门
     * @param metrics             指标收集列表
     * @param rejectedCount       拒绝任务计数器
     */
    private void submitTask(
        ThreadPoolExecutor executor,
        int taskId,
        CountDownLatch firstWorkersStarted,
        CountDownLatch releaseWorkers,
        List<TaskMetric> metrics,
        AtomicInteger rejectedCount) {

        // 记录任务提交的时间点（使用纳秒级时间戳保证精度）
        long submitTime = System.nanoTime();

        try {
            executor.execute(() -> {
                // 任务真正被工作线程取出并开始运行的时刻
                long startTime = System.nanoTime();

                // 前两个任务到达这里后，通知主线程工作线程已经成功启动
                firstWorkersStarted.countDown();

                try {
                    // 在这里阻塞等待，保证所有后续提交的任务都能进入队列排队
                    // 这样可以精确控制所有任务的等待时间起点
                    releaseWorkers.await();

                    // 闸门释放后，任务正式开始执行业务逻辑
                    long executionStart = System.nanoTime();

                    // 模拟业务逻辑执行耗时
                    Thread.sleep(TASK_DURATION_MS);

                    // 业务逻辑执行完成的时刻
                    long endTime = System.nanoTime();

                    // 将该任务的所有时间指标收集起来
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
            // 捕获任务被拒绝的异常，统计拒绝数量
            rejectedCount.incrementAndGet();
        }
    }

    /**
     * 打印所有任务的详细指标和统计汇总数据
     * <p>
     * 输出内容包括：
     * 1. 按任务ID排序的任务明细表格，展示每个任务的等待时间和执行时间
     * 2. 汇总统计：实际完成任务数、平均等待时间、最大等待时间、平均执行时间
     *
     * @param metrics 所有完成任务的时间指标列表
     */
    private void printMetrics(List<TaskMetric> metrics) {
        List<TaskMetric> sorted = new ArrayList<>(metrics);
        // 按任务ID从小到大排序输出
        sorted.sort(Comparator.comparingInt(TaskMetric::taskId));

        // 打印表头
        System.out.println("\n任务明细：");
        System.out.printf(
            "%-8s %-16s %-16s%n",
            "任务ID", "等待时间(ms)", "执行时间(ms)"
        );

        // 逐行打印每个任务的指标数据
        for (TaskMetric metric : sorted) {
            System.out.printf(
                "%-8d %-16.2f %-16.2f%n",
                metric.taskId(),
                metric.waitMs(),
                metric.executionMs()
            );
        }

        // 计算汇总统计数据
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

        // 打印统计结果
        System.out.println("\n统计结果：");
        System.out.println("实际完成任务数：" + sorted.size());
        System.out.printf("平均等待时间：%.2f ms%n", averageWait);
        System.out.printf("最大等待时间：%.2f ms%n", maxWait);
        System.out.printf("平均执行时间：%.2f ms%n", averageExecution);
    }

    /**
     * 任务时间指标记录类
     * 使用 Java 16+ record 语法不可变封装任务全生命周期的关键时间点
     */
    private record TaskMetric(
        int taskId,
        long submitTime,
        long startTime,
        long executionStart,
        long endTime
    ) {
        /**
         * 计算任务在队列中的等待时间
         * 从任务提交给线程池，到被工作线程取出开始执行的时间间隔
         *
         * @return 等待时间（毫秒，保留小数精度）
         */
        double waitMs() {
            return (startTime - submitTime) / 1_000_000.0;
        }

        /**
         * 计算任务实际执行业务逻辑的耗时
         * 从闸门释放后开始执行业务，到业务执行完成的时间间隔
         *
         * @return 业务执行时间（毫秒，保留小数精度）
         */
        double executionMs() {
            return (endTime - executionStart) / 1_000_000.0;
        }
    }
}
