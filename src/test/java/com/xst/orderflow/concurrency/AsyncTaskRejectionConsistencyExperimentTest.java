package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * EXP-029：线程池拒绝与异步任务登记一致性实验
 *
 * 实验目标：
 * 1. 验证线程池拒绝后任务记录是否残留。
 * 2. 验证拒绝后清理记录是否有效。
 * 3. 验证清理后相同 taskId 是否可以重试。
 * 4. 验证并发提交相同 taskId 时是否存在悬空 Future。
 *
 * 线程池参数：
 * corePoolSize = 1
 * maximumPoolSize = 1
 * queueCapacity = 1
 * RejectedExecutionHandler = AbortPolicy
 */
class AsyncTaskRejectionConsistencyExperimentTest {

    private static final long TIMEOUT_SECONDS = 3;

    // =========================================================
    // A：旧实现——拒绝后残留 FAILED 记录
    // =========================================================

    @Test
    void experimentA_rejectionLeavesStaleRecord() throws Exception {

        ThreadPoolExecutor executor = newExecutor();

        try {
            Saturation saturation = saturate(executor);

            LegacyTaskRegistry registry = new LegacyTaskRegistry(executor);

            RejectedExecutionException exception = assertThrows(
                RejectedExecutionException.class,
                () -> registry.submit(
                    "ORDER-A",
                    () -> "DONE-ORDER-A"
                )
            );

            TaskRecord record = registry.get("ORDER-A");

            assertNotNull(record);
            assertEquals(TaskState.FAILED, record.state());
            assertFalse(record.future().isDone());

            System.out.println("========== EXP-029-A ==========");
            System.out.println("Rejected = " + exception.getClass().getSimpleName());
            System.out.println("A task state = " + record.state());
            System.out.println("A stale record exists = "
                + (registry.get("ORDER-A") != null));
            System.out.println("A future done = " + record.future().isDone());

            saturation.release();

            assertTrue(saturation.awaitQueueTask());

        } finally {
            shutdown(executor);
        }
    }

    // =========================================================
    // B：拒绝后清理记录
    // =========================================================

    @Test
    void experimentB_rejectionRemovesRecord() throws Exception {

        ThreadPoolExecutor executor = newExecutor();

        try {
            Saturation saturation = saturate(executor);

            CleanupTaskRegistry registry = new CleanupTaskRegistry(executor);

            assertThrows(
                RejectedExecutionException.class,
                () -> registry.submit(
                    "ORDER-B",
                    () -> "DONE-ORDER-B"
                )
            );

            System.out.println("========== EXP-029-B ==========");
            System.out.println("B record exists = "
                + (registry.get("ORDER-B") != null));
            System.out.println("B record count = " + registry.recordCount());

            assertNull(registry.get("ORDER-B"));
            assertEquals(0, registry.recordCount());

            saturation.release();
            assertTrue(saturation.awaitQueueTask());

        } finally {
            shutdown(executor);
        }
    }

    // =========================================================
    // C：拒绝后释放资源，使用相同 taskId 重试
    // =========================================================

    @Test
    void experimentC_retryAfterRejection() throws Exception {

        ThreadPoolExecutor executor = newExecutor();

        AtomicInteger executionCount = new AtomicInteger();

        try {
            Saturation saturation = saturate(executor);

            SafeTaskRegistry registry = new SafeTaskRegistry(executor);

            // 第一次提交：线程池饱和，预期拒绝
            assertThrows(
                RejectedExecutionException.class,
                () -> registry.submit(
                    "ORDER-C",
                    () -> {
                        executionCount.incrementAndGet();
                        return "DONE-ORDER-C";
                    }
                )
            );

            assertNull(registry.get("ORDER-C"));
            assertEquals(0, registry.recordCount());

            // 释放工作线程，让排队任务执行完成
            saturation.release();
            assertTrue(saturation.awaitQueueTask());

            // 第二次提交：使用相同 taskId 重试
            Future<String> future = registry.submit(
                "ORDER-C",
                () -> {
                    executionCount.incrementAndGet();
                    return "DONE-ORDER-C";
                }
            );

            String result = future.get(
                TIMEOUT_SECONDS,
                TimeUnit.SECONDS
            );

            assertEquals("DONE-ORDER-C", result);
            assertEquals(1, executionCount.get());
            assertEquals(1, registry.recordCount());

            TaskRecord record = registry.get("ORDER-C");

            assertNotNull(record);
            assertEquals(TaskState.SUCCESS, record.state());

            System.out.println("========== EXP-029-C ==========");
            System.out.println("C retry result = " + result);
            System.out.println("C execution count = " + executionCount.get());
            System.out.println("C task state = " + record.state());
            System.out.println("C record count = " + registry.recordCount());

        } finally {
            shutdown(executor);
        }
    }

    // =========================================================
    // D：20 个线程并发提交相同 taskId
    //
    // 修复点：
    // 使用 ConcurrentHashMap.computeIfAbsent。
    // 只有 executor.execute 成功后，记录才会发布到 Map。
    // =========================================================

    @Test
    void experimentD_concurrentSubmissionDoesNotExposeOrphanFuture()
        throws Exception {

        ThreadPoolExecutor executor = newExecutor();

        ThreadPoolExecutor callers = new ThreadPoolExecutor(
            20,
            20,
            0,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>()
        );

        AtomicInteger executionCount = new AtomicInteger();
        AtomicInteger rejectedCount = new AtomicInteger();
        AtomicInteger returnedFutureCount = new AtomicInteger();

        CountDownLatch ready = new CountDownLatch(20);
        CountDownLatch start = new CountDownLatch(1);

        try {
            Saturation saturation = saturate(executor);

            SafeTaskRegistry registry = new SafeTaskRegistry(executor);

            Future<?>[] callerResults = new Future<?>[20];

            for (int i = 0; i < 20; i++) {

                callerResults[i] = callers.submit(() -> {

                    ready.countDown();

                    if (!start.await(
                        TIMEOUT_SECONDS,
                        TimeUnit.SECONDS
                    )) {
                        throw new IllegalStateException(
                            "Concurrent start timeout"
                        );
                    }

                    try {
                        Future<String> future = registry.submit(
                            "ORDER-D",
                            () -> {
                                executionCount.incrementAndGet();
                                return "DONE-ORDER-D";
                            }
                        );

                        returnedFutureCount.incrementAndGet();

                        return future;

                    } catch (RejectedExecutionException e) {

                        rejectedCount.incrementAndGet();

                        return null;
                    }
                });
            }

            // 等待 20 个调用线程全部准备就绪
            assertTrue(ready.await(
                TIMEOUT_SECONDS,
                TimeUnit.SECONDS
            ));

            // 同时启动 20 个调用线程
            start.countDown();

            // 等待全部调用完成
            for (Future<?> callerResult : callerResults) {
                callerResult.get(
                    TIMEOUT_SECONDS,
                    TimeUnit.SECONDS
                );
            }

            // 此时工作线程仍被阻塞，队列仍然占满
            assertEquals(20, rejectedCount.get());
            assertEquals(0, returnedFutureCount.get());
            assertEquals(0, registry.recordCount());
            assertEquals(0, executionCount.get());

            System.out.println("========== EXP-029-D ==========");
            System.out.println("D submit count = 20");
            System.out.println("D rejected count = " + rejectedCount.get());
            System.out.println("D returned future count = "
                + returnedFutureCount.get());
            System.out.println("D final record count = "
                + registry.recordCount());
            System.out.println("D execution count = "
                + executionCount.get());

            // 释放线程池资源
            saturation.release();

            assertTrue(saturation.awaitQueueTask());

            // 使用相同 taskId 重试
            Future<String> retryFuture = registry.submit(
                "ORDER-D",
                () -> {
                    executionCount.incrementAndGet();
                    return "DONE-ORDER-D";
                }
            );

            String result = retryFuture.get(
                TIMEOUT_SECONDS,
                TimeUnit.SECONDS
            );

            assertEquals("DONE-ORDER-D", result);
            assertEquals(1, executionCount.get());
            assertEquals(1, registry.recordCount());

            assertEquals(
                TaskState.SUCCESS,
                registry.get("ORDER-D").state()
            );

            System.out.println("D retry result = " + result);
            System.out.println("D retry execution count = "
                + executionCount.get());
            System.out.println("D retry final record count = "
                + registry.recordCount());

        } finally {
            start.countDown();
            shutdown(callers);
            shutdown(executor);
        }
    }


    // =========================================================
    // E：20 个线程并发提交相同 taskId，线程池可用
    //
    // 验证：
    // 1. 只执行一次业务任务
    // 2. 只产生一条任务记录
    // 3. 所有调用者获取同一个 Future 对象
    // =========================================================

    @Test
    void experimentE_concurrentSuccessfulSubmissionReusesSameFuture()
        throws Exception {

        ThreadPoolExecutor executor = newExecutor();

        ThreadPoolExecutor callers = new ThreadPoolExecutor(
            20,
            20,
            0,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>()
        );

        SafeTaskRegistry registry = new SafeTaskRegistry(executor);

        AtomicInteger executionCount = new AtomicInteger();

        CountDownLatch ready = new CountDownLatch(20);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch businessStarted = new CountDownLatch(1);
        CountDownLatch releaseBusiness = new CountDownLatch(1);

        @SuppressWarnings("unchecked")
        Future<String>[] returnedFutures = new Future[20];

        Future<?>[] callerResults = new Future<?>[20];

        try {

            // 20 个线程同时提交相同 taskId
            for (int i = 0; i < 20; i++) {

                final int index = i;

                callerResults[i] = callers.submit(() -> {

                    ready.countDown();

                    if (!start.await(
                        TIMEOUT_SECONDS,
                        TimeUnit.SECONDS
                    )) {
                        throw new IllegalStateException(
                            "Concurrent start timeout"
                        );
                    }

                    Future<String> future = registry.submit(
                        "ORDER-E",
                        () -> {

                            executionCount.incrementAndGet();

                            businessStarted.countDown();

                            if (!releaseBusiness.await(
                                TIMEOUT_SECONDS * 3,
                                TimeUnit.SECONDS
                            )) {
                                throw new IllegalStateException(
                                    "Business release timeout"
                                );
                            }

                            return "DONE-ORDER-E";
                        }
                    );

                    returnedFutures[index] = future;

                    return future;
                });
            }

            // 等待所有调用线程准备就绪
            assertTrue(ready.await(
                TIMEOUT_SECONDS,
                TimeUnit.SECONDS
            ));

            // 同时开始提交
            start.countDown();

            // 等待所有调用者完成 submit
            for (Future<?> callerResult : callerResults) {
                callerResult.get(
                    TIMEOUT_SECONDS,
                    TimeUnit.SECONDS
                );
            }

            // 确认业务任务已经开始执行
            assertTrue(businessStarted.await(
                TIMEOUT_SECONDS,
                TimeUnit.SECONDS
            ));

            // 统计 Future 对象身份
            Future<String> firstFuture = returnedFutures[0];

            assertNotNull(firstFuture);

            int sameFutureCount = 0;

            for (Future<String> future : returnedFutures) {

                assertNotNull(future);

                if (future == firstFuture) {
                    sameFutureCount++;
                }
            }

            // 所有线程必须拿到同一个 Future 对象
            assertEquals(20, sameFutureCount);

            // 业务任务正在阻塞，尚未完成
            assertFalse(firstFuture.isDone());

            // Map 只登记一条记录
            assertEquals(1, registry.recordCount());

            // 业务只执行了一次
            assertEquals(1, executionCount.get());

            // 释放业务任务
            releaseBusiness.countDown();

            // 所有调用者持有的 Future 都应该得到相同结果
            for (Future<String> future : returnedFutures) {

                assertEquals(
                    "DONE-ORDER-E",
                    future.get(
                        TIMEOUT_SECONDS,
                        TimeUnit.SECONDS
                    )
                );
            }

            assertEquals(1, executionCount.get());
            assertEquals(1, registry.recordCount());

            assertEquals(
                TaskState.SUCCESS,
                registry.get("ORDER-E").state()
            );

            System.out.println("========== EXP-029-E ==========");
            System.out.println("E submit count = 20");
            System.out.println("E same Future count = " + sameFutureCount);
            System.out.println("E execution count = " + executionCount.get());
            System.out.println("E record count = " + registry.recordCount());
            System.out.println("E future done = " + firstFuture.isDone());
            System.out.println("E result = "
                + firstFuture.get(
                TIMEOUT_SECONDS,
                TimeUnit.SECONDS
            ));

        } finally {

            // 防止业务线程因测试异常而一直阻塞
            releaseBusiness.countDown();
            start.countDown();

            shutdown(callers);
            shutdown(executor);
        }
    }

    // =========================================================
    // 线程池及饱和工具
    // =========================================================

    private ThreadPoolExecutor newExecutor() {

        return new ThreadPoolExecutor(
            1,
            1,
            0,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }

    /**
     * 制造线程池饱和：
     *
     * 1. 一个任务占用工作线程。
     * 2. 一个任务占满等待队列。
     *
     * 此时再提交任务必然触发 AbortPolicy。
     */
    private Saturation saturate(ThreadPoolExecutor executor)
        throws InterruptedException {

        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        CountDownLatch queueTaskFinished = new CountDownLatch(1);

        executor.execute(() -> {
            workerStarted.countDown();

            try {
                if (!releaseWorker.await(
                    TIMEOUT_SECONDS * 3,
                    TimeUnit.SECONDS
                )) {
                    throw new IllegalStateException(
                        "Worker release timeout"
                    );
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        assertTrue(workerStarted.await(
            TIMEOUT_SECONDS,
            TimeUnit.SECONDS
        ));

        executor.execute(queueTaskFinished::countDown);

        assertEquals(1, executor.getActiveCount());
        assertEquals(1, executor.getQueue().size());

        return new Saturation(
            releaseWorker,
            queueTaskFinished
        );
    }

    private void shutdown(ThreadPoolExecutor executor) {

        executor.shutdownNow();

        try {
            if (!executor.awaitTermination(
                TIMEOUT_SECONDS,
                TimeUnit.SECONDS
            )) {
                throw new IllegalStateException(
                    "Executor did not terminate"
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static class Saturation {

        private final CountDownLatch releaseWorker;
        private final CountDownLatch queueTaskFinished;

        private Saturation(
            CountDownLatch releaseWorker,
            CountDownLatch queueTaskFinished
        ) {
            this.releaseWorker = releaseWorker;
            this.queueTaskFinished = queueTaskFinished;
        }

        void release() {
            releaseWorker.countDown();
        }

        boolean awaitQueueTask() throws InterruptedException {
            return queueTaskFinished.await(
                TIMEOUT_SECONDS,
                TimeUnit.SECONDS
            );
        }
    }

    // =========================================================
    // 任务状态
    // =========================================================

    enum TaskState {
        REGISTERED,
        SUBMITTED,
        RUNNING,
        SUCCESS,
        FAILED
    }

    /**
     * 任务记录。
     *
     * FutureTask 与状态记录绑定。
     */
    static class TaskRecord {

        private final String taskId;

        private volatile TaskState state = TaskState.REGISTERED;

        private final FutureTask<String> future;

        TaskRecord(
            String taskId,
            Callable<String> task
        ) {
            this.taskId = taskId;

            this.future = new FutureTask<>(() -> {

                state = TaskState.RUNNING;

                try {
                    String result = task.call();

                    state = TaskState.SUCCESS;

                    return result;

                } catch (Exception e) {

                    state = TaskState.FAILED;

                    throw e;

                } catch (Error e) {

                    state = TaskState.FAILED;

                    throw e;
                }
            });
        }

        String taskId() {
            return taskId;
        }

        FutureTask<String> future() {
            return future;
        }

        TaskState state() {
            return state;
        }

        void markSubmitted() {
            state = TaskState.SUBMITTED;
        }

        void markFailed() {
            state = TaskState.FAILED;
        }
    }

    // =========================================================
    // A：旧实现
    //
    // 问题：拒绝后没有清理 Map。
    // =========================================================

    static class LegacyTaskRegistry {

        private final ThreadPoolExecutor executor;

        private final Map<String, TaskRecord> records =
            new ConcurrentHashMap<>();

        LegacyTaskRegistry(ThreadPoolExecutor executor) {
            this.executor = executor;
        }

        Future<String> submit(
            String taskId,
            Callable<String> task
        ) {

            TaskRecord newRecord = new TaskRecord(taskId, task);

            TaskRecord existing = records.putIfAbsent(
                taskId,
                newRecord
            );

            if (existing != null) {
                return existing.future();
            }

            newRecord.markSubmitted();

            try {

                executor.execute(newRecord.future());

                return newRecord.future();

            } catch (RejectedExecutionException e) {

                newRecord.markFailed();

                // 故意不删除，模拟旧实现缺陷
                throw e;
            }
        }

        TaskRecord get(String taskId) {
            return records.get(taskId);
        }
    }

    // =========================================================
    // B：拒绝后清理实现
    // =========================================================

    static class CleanupTaskRegistry {

        private final ThreadPoolExecutor executor;

        private final Map<String, TaskRecord> records =
            new ConcurrentHashMap<>();

        CleanupTaskRegistry(ThreadPoolExecutor executor) {
            this.executor = executor;
        }

        Future<String> submit(
            String taskId,
            Callable<String> task
        ) {

            TaskRecord newRecord = new TaskRecord(taskId, task);

            TaskRecord existing = records.putIfAbsent(
                taskId,
                newRecord
            );

            if (existing != null) {
                return existing.future();
            }

            newRecord.markSubmitted();

            try {

                executor.execute(newRecord.future());

                return newRecord.future();

            } catch (RejectedExecutionException e) {

                newRecord.markFailed();

                records.remove(taskId, newRecord);

                throw e;
            }
        }

        TaskRecord get(String taskId) {
            return records.get(taskId);
        }

        int recordCount() {
            return records.size();
        }
    }

    // =========================================================
    // C、D：修正实现
    //
    // 核心：computeIfAbsent
    //
    // 只有提交成功，映射函数才返回 TaskRecord。
    // 提交失败抛异常，不会建立 Map 映射。
    // =========================================================

    static class SafeTaskRegistry {

        private final ThreadPoolExecutor executor;

        private final ConcurrentHashMap<String, TaskRecord> records =
            new ConcurrentHashMap<>();

        SafeTaskRegistry(ThreadPoolExecutor executor) {
            this.executor = executor;
        }

        Future<String> submit(
            String taskId,
            Callable<String> task
        ) {

            TaskRecord record = records.computeIfAbsent(
                taskId,
                key -> {

                    TaskRecord newRecord =
                        new TaskRecord(key, task);

                    newRecord.markSubmitted();

                    try {

                        executor.execute(newRecord.future());

                        // execute 成功后才发布记录
                        return newRecord;

                    } catch (RejectedExecutionException e) {

                        newRecord.markFailed();

                        // mapping function 抛异常：
                        // 不会创建该 key 的 Map 映射
                        throw e;
                    }
                }
            );

            return record.future();
        }

        TaskRecord get(String taskId) {
            return records.get(taskId);
        }

        int recordCount() {
            return records.size();
        }
    }
}
