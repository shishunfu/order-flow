package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 线程池任务取消机制实验类
 * <p>
 * 核心目的：演示 Future.cancel(true) 在不同类型任务上的行为差异
 * 帮助理解 Java 线程中断机制的本质：中断是"协作式"的，不是强制终止
 */
class ThreadPoolTaskCancellationExperimentTest {

    // 不可中断任务的运行时长：2秒
    private static final long NON_INTERRUPTIBLE_DURATION_MS = 2_000;

    /**
     * 主测试入口：依次运行两个对比实验
     * 1. 可中断阻塞任务（Thread.sleep）
     * 2. 非可中断CPU循环任务
     */
    @Test
    void compareInterruptibleAndNonInterruptibleTasks() throws Exception {
        // 创建单线程线程池，保证两个实验的任务串行执行，互不干扰
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            runInterruptibleTaskExperiment(executor);
            runNonInterruptibleTaskExperiment(executor);
        } finally {
            // 无论测试结果如何，最终强制关闭线程池
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /**
     * 实验一：Thread.sleep() 是可中断阻塞操作。
     * <p>
     * 验证点：
     * 1. 调用 future.cancel(true) 会中断正在执行 sleep 的线程
     * 2. Thread.sleep() 会立即抛出 InterruptedException 退出阻塞状态
     * 3. 任务可以快速响应取消请求并正常结束
     */
    private void runInterruptibleTaskExperiment(ExecutorService executor)
        throws Exception {

        System.out.println("\n========== EXP-012-A Interruptible Task ==========");

        // 等待任务线程成功启动
        CountDownLatch started = new CountDownLatch(1);
        // 等待任务线程完全执行完毕
        CountDownLatch finished = new CountDownLatch(1);
        // 标记任务是否收到了中断异常
        AtomicBoolean interrupted = new AtomicBoolean(false);

        Future<?> future = executor.submit(() -> {
            started.countDown();

            try {
                System.out.println("Interruptible task started: "
                    + Thread.currentThread().getName());

                // 线程进入30秒的阻塞休眠状态
                // Thread.sleep() 是 JDK 标准的可中断阻塞方法
                Thread.sleep(30_000);

                // 如果没有被中断，才会执行到这里（正常情况下不会走到）
                System.out.println("Unexpected: sleep completed normally");
            } catch (InterruptedException e) {
                // 收到中断异常，说明 sleep 操作被成功打断
                interrupted.set(true);

                System.out.println("Task received InterruptedException");

                // 恢复中断标志位：这是并发编程的最佳实践
                // 避免上层代码感知不到线程曾经被中断过
                Thread.currentThread().interrupt();
            } finally {
                finished.countDown();
                System.out.println("Interruptible task finished");
            }
        });

        // 确保任务已经开始运行，再执行取消操作
        assertTrue(started.await(3, TimeUnit.SECONDS));

        System.out.println("Before cancel: isDone=" + future.isDone());
        // cancel(true) 表示：如果任务正在运行，尝试中断执行它的线程
        System.out.println("cancel(true) result=" + future.cancel(true));

        // 等待任务完全结束
        assertTrue(finished.await(3, TimeUnit.SECONDS));

        System.out.println("After cancel: isCancelled=" + future.isCancelled());
        System.out.println("After cancel: isDone=" + future.isDone());
        System.out.println("Task interrupted=" + interrupted.get());

        // 验证：任务被成功标记为已取消，且确实收到了中断信号
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
        assertTrue(interrupted.get());

        System.out.println("========== EXP-012-A Finished ==========");
    }

    /**
     * 实验二：CPU循环不主动响应中断。
     * <p>
     * cancel(true) 会尝试中断执行线程，但任务本身不检查中断标志并退出。
     * <p>
     * 核心结论：
     * Future.cancel(true) 只是设置线程的中断标志位，它不会强制杀死线程
     * 如果任务代码不主动检查 Thread.isInterrupted() 并退出，任务会一直运行到自然结束
     * 即使 Future 已经被标记为 isCancelled=true，底层的实际任务可能还在后台运行
     */
    private void runNonInterruptibleTaskExperiment(ExecutorService executor)
        throws Exception {

        System.out.println("\n========== EXP-012-B Non-interruptible Task ==========");

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);

        // 标记任务是否观察到了中断标志位被设置
        AtomicBoolean interruptObserved = new AtomicBoolean(false);
        // 记录任务实际完成的时间点
        AtomicLong actualFinishTime = new AtomicLong(0);

        Future<?> future = executor.submit(() -> {
            started.countDown();

            long start = System.nanoTime();
            // 任务将持续运行2秒，不主动退出
            long deadline = start
                + TimeUnit.MILLISECONDS.toNanos(
                NON_INTERRUPTIBLE_DURATION_MS);

            System.out.println("CPU task started: "
                + Thread.currentThread().getName());

            // 故意不根据 interrupt 状态退出
            // 这是一个纯CPU计算循环，没有调用任何可中断阻塞方法
            while (System.nanoTime() < deadline) {
                // 虽然这里检查了中断标志，但即使发现被中断也不会break退出循环
                if (Thread.currentThread().isInterrupted()) {
                    interruptObserved.set(true);
                }

                // 模拟CPU计算，防止循环完全没有业务操作
                Math.sqrt(System.nanoTime());
            }

            actualFinishTime.set(System.nanoTime());
            finished.countDown();

            System.out.println("CPU task actually finished");
        });

        // 确保任务已经开始运行
        assertTrue(started.await(3, TimeUnit.SECONDS));

        // 记录调用cancel的时间点
        long cancelTime = System.nanoTime();

        System.out.println("Before cancel: isDone=" + future.isDone());
        // 调用cancel(true)，此时JVM会设置工作线程的中断标志位
        System.out.println("cancel(true) result=" + future.cancel(true));
        // 注意：这里Future立刻就变成了isCancelled=true和isDone=true
        // 但这只是Future对象的状态，不代表底层任务已经停止
        System.out.println("Immediately after cancel: isCancelled="
            + future.isCancelled());
        System.out.println("Immediately after cancel: isDone="
            + future.isDone());

        // Future 被取消，不代表底层任务已经停止。
        // 提交一个探测任务，观察单线程池何时能执行它。
        // 因为是单线程池，探测任务必须等前面的CPU任务真正执行完才能开始
        CountDownLatch probeStarted = new CountDownLatch(1);
        AtomicLong probeStartTime = new AtomicLong(0);

        executor.submit(() -> {
            probeStartTime.set(System.nanoTime());
            probeStarted.countDown();
            System.out.println("Probe task started: "
                + Thread.currentThread().getName());
        });

        // 等待CPU任务自然运行结束
        assertTrue(finished.await(5, TimeUnit.SECONDS));
        // 等待探测任务开始执行
        assertTrue(probeStarted.await(3, TimeUnit.SECONDS));

        // 计算从调用cancel到任务实际结束的时间差
        // 这个值应该接近2000ms，证明任务并没有被立刻终止
        long actualTaskDurationMs = TimeUnit.NANOSECONDS.toMillis(
            actualFinishTime.get() - cancelTime);

        long probeDelayMs = TimeUnit.NANOSECONDS.toMillis(
            probeStartTime.get() - cancelTime);

        System.out.println("Interrupt observed by CPU task="
            + interruptObserved.get());
        System.out.println("Future isCancelled=" + future.isCancelled());
        System.out.println("Future isDone=" + future.isDone());
        System.out.println("Time from cancel to actual task finish="
            + actualTaskDurationMs + " ms");
        System.out.println("Time from cancel to probe start="
            + probeDelayMs + " ms");

        // 验证：虽然Future状态显示已取消，但任务确实观察到了中断标志，并且运行到自然结束
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
        assertTrue(interruptObserved.get());

        System.out.println("========== EXP-012-B Finished ==========");
    }
}
