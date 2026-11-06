package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CompletableFuture 基础功能实验测试类
 * 用于演示 CompletableFuture 核心 API 的基本使用方式和行为特性
 */
class CompletableFutureBasicExperimentTest {

    /**
     * 场景 A：
     * supplyAsync 异步执行任务，并返回计算结果。
     * <p>
     * 验证点：
     * 1. supplyAsync 可以异步执行 Supplier 类型任务
     * 2. 异步任务能够正确返回计算结果
     * 3. 异步任务运行在独立的线程中，与调用线程不是同一个线程
     */
    @Test
    void supplyAsyncShouldReturnResult() throws Exception {

        // 记录调用线程（测试主线程）的名称
        String callerThread = Thread.currentThread().getName();

        // 使用原子引用保存异步任务执行线程的名称，因为 Lambda 表达式只能访问 effectively final 变量
        AtomicReference<String> workerThread = new AtomicReference<>();

        // supplyAsync: 异步执行一个带返回值的任务
        // 不指定自定义 Executor 时，默认使用 ForkJoinPool.commonPool() 中的线程
        CompletableFuture<Integer> future =
            CompletableFuture.supplyAsync(() -> {

                // 在异步工作线程中记录当前线程名称
                workerThread.set(Thread.currentThread().getName());

                System.out.println("supplyAsync thread = " + workerThread.get());

                // 返回异步计算结果
                return 100 + 200;
            });

        // 阻塞等待异步任务完成，设置3秒超时时间防止无限等待
        Integer result = future.get(3, TimeUnit.SECONDS);

        System.out.println("Caller thread = " + callerThread);
        System.out.println("Result = " + result);

        // 验证计算结果正确
        assertEquals(300, result);
        // 验证工作线程名称已被正确设置
        assertNotNull(workerThread.get());
        // 验证异步任务确实运行在与调用线程不同的线程中
        assertNotEquals(callerThread, workerThread.get());
    }

    /**
     * 场景 B：
     * runAsync 异步执行任务，但不返回业务计算结果。
     * <p>
     * 验证点：
     * 1. runAsync 可以异步执行 Runnable 类型任务
     * 2. 异步任务会被完整执行
     * 3. 无返回值的 CompletableFuture get() 方法返回 null
     */
    @Test
    void runAsyncShouldExecuteWithoutResult() throws Exception {

        // 使用原子布尔标记记录异步任务是否被执行完成
        AtomicBoolean executed = new AtomicBoolean(false);
        // 保存异步任务执行线程的名称
        AtomicReference<String> workerThread = new AtomicReference<>();

        // runAsync: 异步执行一个无返回值的 Runnable 任务
        CompletableFuture<Void> future =
            CompletableFuture.runAsync(() -> {

                workerThread.set(Thread.currentThread().getName());

                // 标记任务已执行
                executed.set(true);

                System.out.println("runAsync thread = " + workerThread.get());
            });

        // 阻塞等待异步任务完成，Void 类型的 Future 没有实际返回结果
        Void result = future.get(3, TimeUnit.SECONDS);

        System.out.println("runAsync result = " + result);
        System.out.println("executed = " + executed.get());

        // 验证无返回值任务的 get() 结果为 null
        assertNull(result);
        // 验证异步任务确实被执行完成
        assertTrue(executed.get());
        // 验证工作线程名称不为空
        assertNotNull(workerThread.get());
    }

    /**
     * 场景 C：
     * 使用自定义 Executor 执行 CompletableFuture 任务。
     * <p>
     * 验证点：
     * 1. CompletableFuture 支持传入自定义线程池来执行异步任务
     * 2. 异步任务会运行在自定义线程池创建的线程中，而不是默认的 ForkJoinPool
     * 3. 自定义线程可以按照业务需求进行命名，便于问题排查
     */
    @Test
    void shouldUseCustomExecutor() throws Exception {

        // 创建自定义固定大小线程池
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            2,
            2,
            0,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            // 自定义线程工厂，为线程指定有业务含义的名称
            runnable -> {
                Thread thread = new Thread(runnable);
                thread.setName("order-flow-worker");
                return thread;
            }
        );

        try {
            // 传入自定义 Executor 作为第二个参数，让 supplyAsync 使用我们自己的线程池执行任务
            CompletableFuture<String> future =
                CompletableFuture.supplyAsync(() -> {

                    String threadName =
                        Thread.currentThread().getName();

                    System.out.println("Custom executor thread = "
                        + threadName);

                    return threadName;
                }, executor);

            String result = future.get(3, TimeUnit.SECONDS);

            // 验证异步任务确实运行在我们自定义命名的线程中
            assertTrue(result.startsWith("order-flow-worker"));

        } finally {
            // 优雅关闭线程池，释放资源
            executor.shutdown();
            assertTrue(
                executor.awaitTermination(3, TimeUnit.SECONDS)
            );
        }
    }

    /**
     * 场景 D：
     * 验证异步任务提交后，调用线程不需要等待任务执行完成。
     * <p>
     * 验证点：
     * 1. 调用线程提交异步任务后可以继续执行后续逻辑，不会被阻塞
     * 2. 使用 CountDownLatch 可以精确控制任务的执行时序
     * 3. CompletableFuture.isDone() 可以判断异步任务是否已经完成
     * 4. 在任务未完成时调用 get() 方法会阻塞直到任务完成
     */
    @Test
    void asyncTaskShouldNotBlockCallerThread() throws Exception {

        // 用于同步：异步任务启动后通知调用线程
        CountDownLatch taskStarted = new CountDownLatch(1);
        // 用于控制时序：调用线程通知异步任务可以继续执行
        CountDownLatch releaseTask = new CountDownLatch(1);

        CompletableFuture<String> future =
            CompletableFuture.supplyAsync(() -> {

                // 标记异步任务已经开始执行，通知等待的调用线程
                taskStarted.countDown();

                try {
                    // 异步任务在这里阻塞，等待调用线程的释放信号
                    // 这样我们就可以在任务完成前观察 Future 的状态
                    releaseTask.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                        "Task interrupted", e
                    );
                }

                return "Order-1001";
            });

        try {
            // 等待异步任务启动，最多等待3秒
            assertTrue(taskStarted.await(3, TimeUnit.SECONDS));

            // 此时异步任务还在阻塞等待，Future 尚未完成
            assertFalse(future.isDone());

            System.out.println("Future isDone before release = " + future.isDone());

            // 释放异步任务，让它可以继续执行并返回结果
            releaseTask.countDown();

            // 现在调用 get() 会等待任务执行完成并获取结果
            String result = future.get(3, TimeUnit.SECONDS);

            System.out.println("Future result = " + result);
            System.out.println("Future isDone after release = " + future.isDone());

            assertEquals("Order-1001", result);
            // 任务完成后 isDone() 应该返回 true
            assertTrue(future.isDone());

        } finally {
            // 确保无论测试是否成功，都释放 latch 避免线程泄漏
            releaseTask.countDown();
        }
    }
}
