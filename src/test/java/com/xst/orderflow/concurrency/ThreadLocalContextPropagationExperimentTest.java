package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ThreadLocal 跨线程池上下文传播实验测试类
 * <p>
 * 实验目的：
 * 演示 ThreadLocal 的天然线程隔离特性，验证手动上下文包装方案的正确性，
 * 解决在线程池复用线程场景下，调用线程的上下文无法自动传递到工作线程的问题，
 * 同时保证多任务并发执行时上下文完全隔离、互不污染，避免内存泄漏和上下文串扰。
 */
class ThreadLocalContextPropagationExperimentTest {

    // 用户身份上下文：存储当前请求的登录用户标识
    private static final ThreadLocal<String> USER_CONTEXT =
        new ThreadLocal<>();

    // 链路追踪上下文：存储当前请求的全链路追踪ID
    private static final ThreadLocal<String> TRACE_CONTEXT =
        new ThreadLocal<>();

    /**
     * 安全设置 ThreadLocal 值的工具方法
     * <p>
     * 处理逻辑：
     * - 当 value 为 null 时，调用 remove() 彻底清除 ThreadLocal，避免内存泄漏
     * - 当 value 不为 null 时，调用 set() 设置具体的上下文值
     *
     * @param threadLocal 目标 ThreadLocal 对象
     * @param value       要设置的上下文值，可以为 null
     */
    private static void setOrRemove(
        ThreadLocal<String> threadLocal,
        String value
    ) {
        if (value == null) {
            threadLocal.remove();
        } else {
            threadLocal.set(value);
        }
    }

    /**
     * 场景 A：
     * 主线程设置 ThreadLocal，
     * Worker 线程不能自动读取主线程上下文。
     * <p>
     * 验证点：
     * 1. ThreadLocal 具有严格的线程隔离特性，不同线程之间无法互相访问对方的 ThreadLocal 数据
     * 2. 主线程设置的上下文，在线程池的工作线程中默认读取不到，返回 null
     * 3. 主线程自身的上下文数据在提交异步任务后仍然保持完整，不受影响
     */
    @Test
    void threadLocalShouldNotPropagateAutomatically()
        throws Exception {

        // 创建单线程线程池，保证所有异步任务都复用同一个工作线程
        ExecutorService executor =
            Executors.newSingleThreadExecutor();

        try {
            // 在调用线程（测试主线程）中设置用户上下文和追踪上下文
            USER_CONTEXT.set("user-A");
            TRACE_CONTEXT.set("trace-A");

            System.out.println("Caller thread = "
                + Thread.currentThread().getName());

            // 提交异步任务到线程池，工作线程尝试读取两个 ThreadLocal 的值
            Future<String> future = executor.submit(() -> {
                System.out.println("Worker thread = "
                    + Thread.currentThread().getName());

                String user = USER_CONTEXT.get();
                String trace = TRACE_CONTEXT.get();

                return user + ":" + trace;
            });

            // 阻塞等待异步任务执行完成，获取返回结果
            String result = future.get(3, TimeUnit.SECONDS);

            System.out.println("Worker read context = " + result);

            // 断言工作线程读取到的上下文都是 null，证明 ThreadLocal 不会自动跨线程传播
            assertEquals("null:null", result);

            // 调用线程自己的上下文仍然存在。
            assertEquals("user-A", USER_CONTEXT.get());
            assertEquals("trace-A", TRACE_CONTEXT.get());

        } finally {
            // 清理当前线程的 ThreadLocal，防止内存泄漏
            USER_CONTEXT.remove();
            TRACE_CONTEXT.remove();
            // 强制关闭线程池，中断所有正在执行的任务
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景 B：
     * 提交任务时捕获上下文，
     * Worker 执行时设置上下文，
     * 执行结束后恢复 Worker 原上下文。
     * <p>
     * 验证点：
     * 1. 通过任务包装器可以在任务提交时刻主动捕获调用线程的上下文快照
     * 2. 捕获的是任务提交瞬间的上下文，而不是工作线程实际执行时调用线程的最新上下文
     * 3. 任务执行完成后必须恢复工作线程原本的上下文，避免线程复用时发生上下文污染
     */
    @Test
    void contextShouldPropagateThroughTaskWrapper()
        throws Exception {

        // 创建单线程线程池，复用同一个工作线程验证上下文恢复效果
        ExecutorService executor =
            Executors.newSingleThreadExecutor();

        try {
            // 调用线程先设置初始上下文为 user-A / trace-A
            USER_CONTEXT.set("user-A");
            TRACE_CONTEXT.set("trace-A");

            // 在任务提交时刻捕获当前上下文，包装成支持上下文传播的 Runnable
            Runnable task = captureContext(() -> {
                // 工作线程执行时，应该能读取到提交时刻捕获的 user-A / trace-A
                assertEquals("user-A", USER_CONTEXT.get());
                assertEquals("trace-A", TRACE_CONTEXT.get());

                System.out.println("Worker user = "
                    + USER_CONTEXT.get());

                System.out.println("Worker trace = "
                    + TRACE_CONTEXT.get());
            });

            // 任务提交之后，修改调用线程上下文。
            // 这一步验证捕获的是提交时刻的快照，不会跟随调用线程后续的变化而变化
            USER_CONTEXT.set("user-B");
            TRACE_CONTEXT.set("trace-B");

            // 等待异步任务完整执行完成
            executor.submit(task).get(3, TimeUnit.SECONDS);

            // 证明捕获的是提交包装任务时的上下文，
            // 而不是 Worker 执行时或调用线程后续的上下文。
            assertEquals("user-B", USER_CONTEXT.get());
            assertEquals("trace-B", TRACE_CONTEXT.get());

            // Worker 执行完后，检查其上下文已恢复为空。
            // 验证线程复用时不会残留上一个任务的上下文数据
            Future<Boolean> checkWorker = executor.submit(() ->
                USER_CONTEXT.get() == null
                    && TRACE_CONTEXT.get() == null
            );

            assertTrue(checkWorker.get(3, TimeUnit.SECONDS));

        } finally {
            USER_CONTEXT.remove();
            TRACE_CONTEXT.remove();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景 C：
     * 多个任务使用不同上下文，
     * 在线程池中并发执行，
     * 验证任务上下文不会互相污染。
     * <p>
     * 验证点：
     * 1. 20个不同上下文的任务在线程池的4个线程中并发执行，每个任务都能正确读取自己提交时的上下文
     * 2. 即使线程被复用，不同任务的上下文也不会发生串扰
     * 3. 所有任务执行完成后，工作线程的上下文都会被正确恢复为空，没有残留
     */
    @Test
    void concurrentTasksShouldKeepTheirOwnContext()
        throws Exception {

        // 总任务数量：20个不同上下文的任务
        int taskCount = 20;

        // 创建固定大小4线程的线程池，模拟生产环境常用的线程池配置
        ExecutorService executor =
            Executors.newFixedThreadPool(4);

        // 同步闸门：等待4个工作线程都启动并进入阻塞状态
        CountDownLatch allStarted = new CountDownLatch(4);
        // 同步闸门：主线程通知所有阻塞的任务可以同时开始验证上下文
        CountDownLatch release = new CountDownLatch(1);

        try {
            List<Future<String>> futures = new ArrayList<>();

            // 循环提交20个任务，每个任务携带完全不同的用户和追踪上下文
            for (int i = 1; i <= taskCount; i++) {
                String user = "user-" + i;
                String trace = "trace-" + i;

                // 模拟每个调用线程提交前设置不同上下文。
                USER_CONTEXT.set(user);
                TRACE_CONTEXT.set(trace);

                // 捕获当前任务独有的上下文快照，包装成传播任务
                Runnable wrappedTask = captureContext(() -> {
                    // 工作线程启动后先计数，通知主线程该线程已就绪
                    allStarted.countDown();

                    try {
                        // 所有工作线程都就绪后，统一放行，制造最大并发场景
                        assertTrue(release.await(3, TimeUnit.SECONDS));

                        // 并发场景下验证当前任务读取到的上下文与提交时设置的完全一致
                        assertEquals(user, USER_CONTEXT.get());
                        assertEquals(trace, TRACE_CONTEXT.get());

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                            "Task interrupted", e
                        );
                    }
                });

                futures.add(executor.submit(() -> {
                    wrappedTask.run();

                    // 任务执行完成后，此时上下文已经被恢复，返回的应该是 null:null
                    return USER_CONTEXT.get()
                        + ":" + TRACE_CONTEXT.get();
                }));

                // 模拟下一次提交前调用线程上下文变化。
                // 清除当前线程的上下文，确保下一次循环设置的是全新的上下文
                USER_CONTEXT.remove();
                TRACE_CONTEXT.remove();
            }

            // 等待4个工作线程全部启动并阻塞，最多等待3秒
            assertTrue(allStarted.await(3, TimeUnit.SECONDS));

            // 统一放行所有任务，触发最大并发验证场景
            release.countDown();

            // 收集所有任务的执行结果，验证每个任务执行完成后工作线程上下文已被清空
            for (int i = 0; i < taskCount; i++) {
                String result = futures.get(i).get(
                    5, TimeUnit.SECONDS
                );

                // wrappedTask 执行后恢复 Worker 原上下文，
                // 本实验中原上下文为空。
                assertEquals("null:null", result);
            }

            System.out.println("Concurrent task count = " + taskCount);
            System.out.println("All task contexts isolated successfully");

        } finally {
            // 确保闸门被释放，避免测试线程挂死
            release.countDown();
            USER_CONTEXT.remove();
            TRACE_CONTEXT.remove();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    /**
     * 提交时捕获上下文。
     * <p>
     * 核心实现原理：
     * 1. 在任务提交的瞬间（调用线程中）立即读取当前两个 ThreadLocal 的值，保存到局部变量中
     * 2. 利用 Lambda 表达式的闭包特性，将捕获到的上下文快照绑定到返回的 Runnable 对象上
     * 3. 工作线程执行包装后的任务时，先保存工作线程当前已有的上下文
     * 4. 将捕获到的上下文快照设置到工作线程的 ThreadLocal 中
     * 5. 执行真正的业务任务逻辑
     * 6. 在 finally 块中严格恢复工作线程执行前的原有上下文，保证线程复用时没有上下文残留
     *
     * @param task 原始业务任务
     * @return 包装了上下文传播逻辑的 Runnable
     */
    private Runnable captureContext(Runnable task) {
        // 在调用线程中立即捕获上下文快照，这一步发生在任务提交时刻
        String capturedUser = USER_CONTEXT.get();
        String capturedTrace = TRACE_CONTEXT.get();

        return () -> {
            // 在工作线程中，先保存该线程当前已有的上下文值
            String previousUser = USER_CONTEXT.get();
            String previousTrace = TRACE_CONTEXT.get();

            try {
                // 将捕获到的调用线程上下文设置到当前工作线程
                setOrRemove(USER_CONTEXT, capturedUser);
                setOrRemove(TRACE_CONTEXT, capturedTrace);

                // 执行业务任务，此时业务代码可以正常从 ThreadLocal 读取到传播过来的上下文
                task.run();

            } finally {
                // 无论任务执行成功还是抛出异常，都必须恢复工作线程原本的上下文
                // 这是防止线程池上下文污染和内存泄漏的关键保障
                setOrRemove(USER_CONTEXT, previousUser);
                setOrRemove(TRACE_CONTEXT, previousTrace);
            }
        };
    }
}
