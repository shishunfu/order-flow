package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 线程池异常处理机制对比实验测试类
 * <p>
 * 实验目的：
 * 系统演示 ThreadPoolExecutor 四种最核心的异常处理行为差异，
 * 解决生产环境中最常见的"任务异常消失"、"异常日志丢失"、"工作线程意外死亡"等疑难问题：
 * 1. execute() 提交任务时，异常会直接抛出到 UncaughtExceptionHandler，甚至导致工作线程终止
 * 2. submit() 提交任务时，异常会被静默捕获并封装到 Future 内部，不调用 get() 永远感知不到异常
 * 3. Future.get() 会将任务异常包装成 ExecutionException 重新抛出，业务代码可以获取原始异常栈
 * 4. 重写 afterExecute() 钩子方法可以统一全局捕获所有任务异常，是生产环境推荐的最佳实践
 */
class ThreadPoolExceptionHandlingExperimentTest {

    // 预定义模拟业务异常，用于所有实验场景的统一异常类型
    private static final RuntimeException BUSINESS_EXCEPTION =
        new IllegalStateException("Simulated business failure");

    @Test
    void compareExecuteSubmitAndAfterExecute() throws Exception {
        System.out.println("\n========== EXP-013 Environment ==========");
        System.out.println("AvailableProcessors: "
            + Runtime.getRuntime().availableProcessors());

        // 依次运行4个对比实验，完整覆盖线程池所有异常处理路径
        experimentAExecute();
        experimentBSubmitWithoutGet();
        experimentCSubmitWithGet();
        experimentDAfterExecute();
    }

    /**
     * A：execute() 提交任务，异常未捕获。
     * <p>
     * 核心行为验证：
     * 1. 使用 execute() 提交的任务如果抛出未检查异常，异常会直接穿透到工作线程
     * 2. 工作线程的 UncaughtExceptionHandler 会被触发，异常栈可以被正常记录
     * 3. 异常抛出后当前工作线程会直接终止，线程池后续会创建一个全新的工作线程来补充
     * 4. 这是生产环境中"线程池线程数莫名波动"的常见根因之一
     */
    private void experimentAExecute() throws Exception {
        System.out.println("\n========== EXP-013-A execute() ==========");

        // 同步闸门：等待未捕获异常处理器完成处理
        CountDownLatch exceptionHandled = new CountDownLatch(1);
        // 统计未捕获异常处理器被调用的次数
        AtomicInteger uncaughtCount = new AtomicInteger();

        // 自定义线程工厂，为每个工作线程注册 UncaughtExceptionHandler
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "exp013-execute-worker");

            // 注册全局未捕获异常回调，当线程抛出未捕获异常时会被调用
            thread.setUncaughtExceptionHandler((t, e) -> {
                uncaughtCount.incrementAndGet();

                System.out.println("UncaughtExceptionHandler thread="
                    + t.getName());
                System.out.println("Uncaught exception="
                    + e.getClass().getSimpleName() + ": "
                    + e.getMessage());

                // 通知主线程异常已经被完整处理
                exceptionHandled.countDown();
            });

            return thread;
        };

        ExecutorService executor = new ThreadPoolExecutor(
            1, 1,
            0, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            factory
        );

        try {
            // 使用 execute() 提交任务，任务内部直接抛出异常不做任何捕获
            executor.execute(() -> {
                System.out.println("execute task running");
                throw new IllegalStateException("execute failure");
            });

            // 等待异常处理器处理完成，最多等待3秒
            assertTrue(exceptionHandled.await(3, TimeUnit.SECONDS));

            System.out.println("Uncaught count=" + uncaughtCount.get());

            // 验证未捕获异常处理器确实被调用了1次
            assertEquals(1, uncaughtCount.get());
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }

        System.out.println("========== EXP-013-A Finished ==========");
    }

    /**
     * B：submit() 提交任务，不调用 Future.get()。
     * <p>
     * 核心行为验证：
     * 1. ExecutorService.submit() 方法内部会将 Runnable 包装成 FutureTask
     * 2. FutureTask 的 run() 方法会捕获所有 Throwable 异常，将异常对象保存到 outcome 字段
     * 3. 异常永远不会抛出到工作线程层面，UncaughtExceptionHandler 完全不会被触发
     * 4. 如果业务代码永远不调用 future.get()，这个异常会被永久静默吃掉，没有任何痕迹
     * 5. 这是生产环境中最经典的"任务无声失败"bug，日志中完全找不到任何异常线索
     */
    private void experimentBSubmitWithoutGet() throws Exception {
        System.out.println("\n========== EXP-013-B submit() without get() ==========");

        // 同步闸门：等待任务执行完成（无论成功失败）
        CountDownLatch taskFinished = new CountDownLatch(1);
        AtomicInteger uncaughtCount = new AtomicInteger();

        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "exp013-submit-no-get");

            thread.setUncaughtExceptionHandler((t, e) -> {
                uncaughtCount.incrementAndGet();
                System.out.println("Unexpected uncaught exception="
                    + e.getMessage());
            });

            return thread;
        };

        ExecutorService executor = new ThreadPoolExecutor(
            1, 1,
            0, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            factory
        );

        try {
            Future<?> future = executor.submit(() -> {
                try {
                    System.out.println("submit task running");
                    throw new IllegalStateException("submit failure");
                } finally {
                    // 任务抛出异常前先计数，通知主线程任务已经执行完成
                    taskFinished.countDown();
                }
            });

            // 等待任务执行完成
            assertTrue(taskFinished.await(3, TimeUnit.SECONDS));

            System.out.println("Future isDone=" + future.isDone());
            System.out.println("Future isCancelled=" + future.isCancelled());
            System.out.println("Uncaught count=" + uncaughtCount.get());

            // 验证：任务确实执行完成了，但异常完全被 FutureTask 内部吞掉了
            assertTrue(future.isDone());
            assertFalse(future.isCancelled());
            // 最关键的断言：UncaughtExceptionHandler 一次都没有被调用
            assertEquals(0, uncaughtCount.get());
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }

        System.out.println("========== EXP-013-B Finished ==========");
    }

    /**
     * C：submit() 提交任务，通过 Future.get() 获取异常。
     * <p>
     * 核心行为验证：
     * 1. 当调用 future.get() 时，如果任务执行过程中发生过异常，get() 方法不会直接返回原始异常
     * 2. 原始异常会被包装成 ExecutionException 重新抛出，原始异常作为 cause 被完整保留
     * 3. 业务代码可以通过 e.getCause() 获取到最原始的业务异常栈
     * 4. 这是 submit() 场景下唯一能感知到任务异常的正确方式
     */
    private void experimentCSubmitWithGet() throws Exception {
        System.out.println("\n========== EXP-013-C submit() with get() ==========");

        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            Future<?> future = executor.submit(() -> {
                System.out.println("submit task with get running");
                throw new IllegalArgumentException("get failure");
            });

            // 断言 future.get() 一定会抛出 ExecutionException
            ExecutionException executionException = assertThrows(
                ExecutionException.class,
                future::get
            );

            System.out.println("Future isDone=" + future.isDone());
            System.out.println("Exception from get="
                + executionException.getClass().getSimpleName());
            System.out.println("Root cause="
                + executionException.getCause().getClass().getSimpleName()
                + ": " + executionException.getCause().getMessage());

            // 验证 ExecutionException 的内部 cause 就是我们业务代码抛出的原始异常
            assertInstanceOf(
                IllegalArgumentException.class,
                executionException.getCause()
            );
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }

        System.out.println("========== EXP-013-C Finished ==========");
    }

    /**
     * D：通过 ThreadPoolExecutor.afterExecute() 观察异常。
     * <p>
     * 核心行为验证：
     * 1. afterExecute() 是 ThreadPoolExecutor 提供的扩展钩子方法，在每个任务执行完成后被回调
     * 2. 对于 execute() 提交的任务，异常会通过 throwable 参数直接传入 afterExecute
     * 3. 对于 submit() 提交的任务，传入的 throwable 永远是 null，因为异常已经被 FutureTask 内部吞掉
     * 4. 所以需要额外判断 runnable 是否是 Future 类型，主动调用 future.get() 才能取出隐藏的异常
     * 5. 这是生产环境中全局统一异常监控、异常日志埋点的最佳实践方案
     */
    private void experimentDAfterExecute() throws Exception {
        System.out.println("\n========== EXP-013-D afterExecute() ==========");

        // 同步闸门：等待 afterExecute 钩子方法执行完成
        CountDownLatch afterExecuteCalled = new CountDownLatch(1);

        // 通过匿名类继承 ThreadPoolExecutor，重写 afterExecute 扩展钩子
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            1, 1,
            0, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>()
        ) {
            @Override
            protected void afterExecute(Runnable runnable, Throwable throwable) {
                // 必须先调用父类实现，保证原有扩展逻辑不被破坏
                super.afterExecute(runnable, throwable);

                System.out.println("afterExecute runnable="
                    + runnable.getClass().getSimpleName());
                System.out.println("afterExecute throwable="
                    + (throwable == null
                    ? "null"
                    : throwable.getClass().getSimpleName()));

                // 专门处理 submit() 提交的 Future 类型任务，主动取出隐藏的异常
                if (runnable instanceof Future<?> future
                    && future.isDone()
                    && !future.isCancelled()) {
                    try {
                        // 这里调用 get() 不会阻塞，因为任务已经执行完成了
                        future.get();
                    } catch (ExecutionException e) {
                        // 成功取出 submit() 任务中被隐藏的原始业务异常
                        System.out.println("afterExecute root cause="
                            + e.getCause().getClass().getSimpleName()
                            + ": " + e.getCause().getMessage());
                    } catch (InterruptedException e) {
                        // 恢复线程中断标记，不破坏线程池的中断语义
                        Thread.currentThread().interrupt();
                    } catch (CancellationException e) {
                        // 处理任务被主动取消的场景
                        System.out.println("Task was cancelled");
                    }
                }

                afterExecuteCalled.countDown();
            }
        };

        try {
            // 使用 submit() 提交任务，验证 afterExecute 钩子可以捕获到 Future 内部隐藏的异常
            executor.submit(() -> {
                System.out.println("afterExecute test task running");
                throw new IllegalStateException("afterExecute failure");
            });

            assertTrue(afterExecuteCalled.await(3, TimeUnit.SECONDS));
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }

        System.out.println("========== EXP-013-D Finished ==========");
    }
}
