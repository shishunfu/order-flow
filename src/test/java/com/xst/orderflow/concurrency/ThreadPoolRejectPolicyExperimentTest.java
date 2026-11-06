package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 线程池拒绝策略对比实验测试类
 * 通过精心设计的同步控制机制，直观演示JDK内置4种拒绝策略在任务过载时的不同行为表现
 * 实验配置：2个工作线程 + 容量为2的阻塞队列，总共可同时容纳4个待处理任务
 * 当提交第5~8个任务时，将触发不同拒绝策略的处理逻辑
 */
class ThreadPoolRejectPolicyExperimentTest {

    // 本次实验总共提交的任务数量
    private static final int TASK_COUNT = 8;
    // 线程池核心/最大工作线程数，实验中固定为2
    private static final int WORKER_COUNT = 2;
    // 阻塞队列容量，实验中固定为2
    private static final int QUEUE_CAPACITY = 2;

    /**
     * 测试AbortPolicy中止策略
     * 该策略在任务被拒绝时直接抛出RejectedExecutionException异常，完全拒绝新任务
     */
    @Test
    void shouldCompareAbortPolicy() throws InterruptedException {
        runExperiment("AbortPolicy", new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * 测试CallerRunsPolicy调用者运行策略
     * 该策略不会丢弃任务，而是将被拒绝的任务回退到提交任务的主线程中同步执行
     */
    @Test
    void shouldCompareCallerRunsPolicy() throws InterruptedException {
        runExperiment("CallerRunsPolicy", new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * 测试DiscardPolicy静默丢弃策略
     * 该策略直接悄悄丢弃被拒绝的任务，不抛出任何异常，无任何感知
     */
    @Test
    void shouldCompareDiscardPolicy() throws InterruptedException {
        runExperiment("DiscardPolicy", new ThreadPoolExecutor.DiscardPolicy());
    }

    /**
     * 测试DiscardOldestPolicy丢弃最老任务策略
     * 该策略丢弃队列中存活时间最久的头部任务，然后重试提交当前新任务
     */
    @Test
    void shouldCompareDiscardOldestPolicy() throws InterruptedException {
        runExperiment("DiscardOldestPolicy", new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    /**
     * 执行单种拒绝策略的完整对照实验
     * <p>
     * 实验设计核心技巧：
     * 1. 使用双重CountDownLatch精确控制任务执行时序
     * 2. 前2个任务启动后立即阻塞，占满所有工作线程
     * 3. 此时队列恰好可以放入2个任务，达到饱和临界点
     * 4. 后续提交的任务将直接触发拒绝策略，行为差异被完全放大
     *
     * @param policyName 拒绝策略名称，用于输出标识
     * @param handler    待测试的拒绝策略实现
     * @throws InterruptedException 线程等待时被中断抛出
     */
    private void runExperiment(String policyName, RejectedExecutionHandler handler) throws InterruptedException {

        // 创建固定大小线程池，使用有界ArrayBlockingQueue，传入指定拒绝策略
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            WORKER_COUNT,
            WORKER_COUNT,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(QUEUE_CAPACITY),
            handler
        );

        // 等待所有工作线程成功启动并进入阻塞状态的同步门闩
        CountDownLatch workersStarted = new CountDownLatch(WORKER_COUNT);
        // 统一释放所有被阻塞工作线程的同步门闩
        CountDownLatch releaseWorkers = new CountDownLatch(1);

        // 记录所有实际被执行过的任务ID，使用有序并发Set保证线程安全且输出有序
        Set<Integer> executedTasks = new ConcurrentSkipListSet<>();
        // 记录所有抛出RejectedExecutionException异常的任务ID
        Set<Integer> rejectedExceptionTasks = new ConcurrentSkipListSet<>();

        // 记录每个任务最终是在哪个线程中被执行的
        ConcurrentHashMap<Integer, String> executionThreads = new ConcurrentHashMap<>();

        // 统计有多少个任务是由提交者主线程直接执行的（用于验证CallerRunsPolicy）
        AtomicInteger callerRunsCount = new AtomicInteger();

        // 记录提交任务的主线程名称，用于后续判断任务是否被回退到主线程执行
        String submitterThread = Thread.currentThread().getName();

        try {
            // 循环提交全部8个任务
            for (int i = 1; i <= TASK_COUNT; i++) {
                final int taskId = i;

                Runnable task = () -> {
                    String threadName = Thread.currentThread().getName();

                    executedTasks.add(taskId);
                    executionThreads.put(taskId, threadName);

                    // 如果当前执行线程就是提交任务的主线程，说明CallerRunsPolicy生效了
                    if (threadName.equals(submitterThread)) {
                        callerRunsCount.incrementAndGet();
                    }

                    // 前2个任务（工作线程数）启动后立即计数减一并阻塞
                    // 这样可以确保工作线程被完全占住，队列饱和
                    if (taskId <= WORKER_COUNT) {
                        workersStarted.countDown();

                        try {
                            releaseWorkers.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }

                    // 模拟任务执行耗时
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                };

                try {
                    executor.execute(task);
                } catch (RejectedExecutionException e) {
                    // 只有AbortPolicy策略会进入这个异常捕获分支
                    rejectedExceptionTasks.add(taskId);
                }

                // 当提交完第2个任务后，等待两个工作线程完全启动并进入阻塞状态
                // 此时工作线程已被占满，队列还可以放入2个任务，达到饱和临界点
                if (i == WORKER_COUNT) {
                    assertTrue(
                        workersStarted.await(5, TimeUnit.SECONDS),
                        "工作线程未能及时启动"
                    );
                }
            }

        } finally {
            // 统一释放所有被阻塞的工作线程，让实验可以正常结束
            releaseWorkers.countDown();
            executor.shutdown();

            assertTrue(
                executor.awaitTermination(15, TimeUnit.SECONDS),
                policyName + "线程池未能正常结束"
            );
        }

        // 计算所有从未被执行过的任务ID集合
        Set<Integer> notExecutedTasks = new TreeSet<>();

        for (int i = 1; i <= TASK_COUNT; i++) {
            if (!executedTasks.contains(i)) {
                notExecutedTasks.add(i);
            }
        }

        // 格式化输出该策略下的所有实验结果指标
        System.out.println();
        System.out.println("========== " + policyName + " ==========");
        System.out.println("ExecutedTasks      : " + executedTasks);
        System.out.println("RejectedTasks      : " + rejectedExceptionTasks);
        System.out.println("NotExecutedTasks   : " + notExecutedTasks);
        System.out.println("ExecutionThreads   : " + executionThreads);
        System.out.println("CallerRunsCount    : " + callerRunsCount.get());
        System.out.println("PoolCompletedCount : " + executor.getCompletedTaskCount());
        System.out.println("PoolTaskCount      : " + executor.getTaskCount());
    }
}
