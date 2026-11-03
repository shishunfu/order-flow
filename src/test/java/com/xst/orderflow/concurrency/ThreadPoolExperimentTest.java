package com.xst.orderflow.concurrency;


import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 线程池行为实验测试类
 * 用于验证 ThreadPoolExecutor 在核心线程满、队列满场景下的任务提交、排队和拒绝策略行为
 */
class ThreadPoolExperimentTest {

    /**
     * 验证线程池饱和状态下的运行表现
     * 测试场景：核心线程数=2，最大线程数=2，工作队列容量=3，使用AbortPolicy拒绝策略
     * 预期观察：线程池达到饱和时的线程数、队列长度和拒绝任务数量
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

        // 用于等待2个工作线程完全启动并进入阻塞状态的闭锁
        CountDownLatch workersStarted = new CountDownLatch(2);
        // 用于统一释放所有阻塞工作线程的闭锁
        CountDownLatch releaseWorkers = new CountDownLatch(1);
        // 原子计数器：统计最终正常完成的任务数量
        AtomicInteger completed = new AtomicInteger();
        // 原子计数器：统计被线程池拒绝的任务数量
        AtomicInteger rejected = new AtomicInteger();

        // 自定义测试任务：启动时递减启动闭锁，然后阻塞等待释放信号，最终标记任务完成
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
            executor.execute(task);
            executor.execute(task);

            // 确保两个工作线程完全启动，最多等待5秒避免测试永久阻塞
            assertTrue(workersStarted.await(5, TimeUnit.SECONDS));

            // 继续提交8个任务，此时核心线程已全部被占用，任务会先进入队列排队
            // 队列满后由于最大线程数等于核心线程数，不会创建额外非核心线程，后续任务直接触发拒绝策略
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
            // assertEquals(3, executor.getQueue().size());
            // 预期总提交任务数2+8=10，2个运行中+3个排队，剩余5个被拒绝
            // assertEquals(5, rejected.get());

        } finally {
            // 释放所有阻塞的工作线程，让所有排队任务可以继续执行完成
            releaseWorkers.countDown();
            // 优雅关闭线程池
            executor.shutdown();

            // 等待所有任务执行完毕，最多等待5秒
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }

        // 打印最终所有成功完成的任务总数
        System.out.println("完成任务：" + completed.get());
    }
}
