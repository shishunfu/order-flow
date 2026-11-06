package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ThreadPoolExecutor 饱和状态行为实验测试类
 * <p>
 * 实验目的：
 * 精确演示 ThreadPoolExecutor 最经典的任务提交调度流程：
 * 1. 当工作线程数 < corePoolSize 时，直接创建新线程执行任务
 * 2. 当工作线程数 >= corePoolSize 时，任务优先加入阻塞队列排队
 * 3. 当队列已满且工作线程数 < maximumPoolSize 时，创建非核心线程立即执行任务
 * 4. 当队列已满且工作线程数 >= maximumPoolSize 时，触发拒绝策略
 * <p>
 * 本实验将 maximumPoolSize 与 corePoolSize 设置为相等，跳过第3步，
 * 直接观察队列填满后任务触发拒绝策略的完整过程。
 */
class ThreadPoolExperimentTest {

    /**
     * 验证线程池饱和状态下的运行表现
     * <p>
     * 测试场景参数：
     * - 核心线程数 corePoolSize = 2
     * - 最大线程数 maximumPoolSize = 2
     * - 有界队列容量 workQueue capacity = 3
     * - 饱和策略 handler = AbortPolicy
     * <p>
     * 预期行为推导：
     * 总提交任务数 = 2（第一批） + 8（第二批循环） = 10 个任务
     * - 前2个任务：直接创建核心线程执行，不进入队列
     * - 接下来3个任务：核心线程已满，全部进入容量为3的队列排队，队列被完全填满
     * - 剩余 10 - 2 - 3 = 5 个任务：队列已满，且 maximumPoolSize == corePoolSize，无法创建更多线程，全部触发 AbortPolicy 拒绝策略
     * <p>
     * 最终稳态预期：
     * - PoolSize = 2
     * - Queue.size() = 3
     * - Rejected count = 5
     * - Completed tasks = 2 + 3 = 5
     */
    @Test
    void shouldObserveThreadPoolSaturation() throws Exception {

        // 初始化线程池：核心线程数2，最大线程数2，空闲线程存活时间60秒
        // 有界阻塞队列容量3，拒绝策略为直接抛出RejectedExecutionException的AbortPolicy
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            2, 2, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(3),
            new ThreadPoolExecutor.AbortPolicy()
        );

        // 同步闸门1：等待2个工作线程完全启动并进入阻塞状态
        // 保证两个核心线程100%被占用，后续提交的任务只能进入队列
        CountDownLatch workersStarted = new CountDownLatch(2);
        // 同步闸门2：主线程统一释放所有阻塞的工作线程
        // 确保我们可以在所有任务排队完成后，统一放行让任务开始执行
        CountDownLatch releaseWorkers = new CountDownLatch(1);
        // 原子计数器：统计最终正常完成的任务数量
        AtomicInteger completed = new AtomicInteger();
        // 原子计数器：统计被线程池拒绝策略直接拒绝的任务数量
        AtomicInteger rejected = new AtomicInteger();

        // 自定义测试任务：启动时递减启动闭锁，然后阻塞等待释放信号，最终标记任务完成
        // 设计意图：任务执行后立即阻塞，占用工作线程不释放，模拟长耗时任务场景
        // 这样可以精确控制线程池的饱和状态，避免任务执行太快导致线程提前释放
        Runnable task = () -> {
            workersStarted.countDown();
            try {
                // 阻塞等待外部释放信号，模拟任务长时间执行占用线程
                releaseWorkers.await();
                completed.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        try {
            // 提交2个任务，启动两个核心工作线程并等待它们进入阻塞状态
            // 这一步执行完成后，线程池的两个核心线程已经100%被占用
            executor.execute(task);
            executor.execute(task);

            // 确保两个工作线程完全启动，最多等待5秒避免测试永久阻塞
            // 这是整个实验最关键的时序控制点：保证后续提交任务时，核心线程没有空闲
            assertTrue(workersStarted.await(5, TimeUnit.SECONDS));

            // 继续提交8个任务，此时核心线程已全部被占用，任务会先进入队列排队
            // 队列满后由于最大线程数等于核心线程数，不会创建额外非核心线程，后续任务直接触发拒绝策略
            // 提交顺序：第1个任务入队 → 第2个入队 → 第3个入队 → 队列已满 → 第4~8个任务全部被拒绝
            for (int i = 0; i < 8; i++) {
                try {
                    executor.execute(task);
                } catch (RejectedExecutionException e) {
                    // 捕获拒绝异常，统计被拒绝的任务数量
                    rejected.incrementAndGet();
                }
            }

            // 打印线程池当前运行状态指标
            System.out.println("线程池大小：" + executor.getPoolSize());
            System.out.println("活跃线程：" + executor.getActiveCount());
            System.out.println("队列长度：" + executor.getQueue().size());
            System.out.println("拒绝任务：" + rejected.get());

            // 验证线程池大小始终保持为核心线程数2，不会创建额外线程
            assertEquals(2, executor.getPoolSize());
            // 预期队列填满3个任务
            assertEquals(3, executor.getQueue().size());
            // 预期总提交任务数2+8=10，2个运行中+3个排队，剩余5个被拒绝
            assertEquals(5, rejected.get());

        } finally {
            // 释放所有阻塞的工作线程，让所有排队任务可以继续执行完成
            // 闸门打开后，两个工作线程会依次从队列中取出3个排队任务执行
            releaseWorkers.countDown();
            // 优雅关闭线程池
            executor.shutdown();

            // 等待所有任务执行完毕，最多等待5秒
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }

        // 打印最终所有成功完成的任务总数
        // 预期结果：2个初始任务 + 3个排队任务 = 5个任务全部执行完成
        System.out.println("完成任务：" + completed.get());
        assertEquals(5, completed.get());
    }
}
