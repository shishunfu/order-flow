
package com.xst.orderflow.concurrency;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class AsyncTaskStateMachineExperimentTest {

    enum State {
        CREATED,
        SUBMITTED,
        RUNNING,
        SUCCESS,
        FAILED,
        CANCELLING,
        CANCELLED
    }

    static class StateMachine {

        private static final Map<State, Set<State>> ALLOWED = Map.of(
            State.CREATED, Set.of(State.SUBMITTED, State.CANCELLED),
            State.SUBMITTED, Set.of(State.RUNNING, State.FAILED,
                State.CANCELLED),
            State.RUNNING, Set.of(State.SUCCESS, State.FAILED,
                State.CANCELLING),
            State.CANCELLING, Set.of(State.CANCELLED),
            State.SUCCESS, Set.of(),
            State.FAILED, Set.of(),
            State.CANCELLED, Set.of()
        );

        private final AtomicReference<State> state =
            new AtomicReference<>(State.CREATED);

        boolean transition(State next) {
            while (true) {
                State current = state.get();

                if (!ALLOWED.get(current).contains(next)) {
                    return false;
                }

                if (state.compareAndSet(current, next)) {
                    return true;
                }
            }
        }

        State state() {
            return state.get();
        }
    }

    static class ManagedTask {

        private final StateMachine machine = new StateMachine();

        private final AtomicInteger cleanupCount = new AtomicInteger();

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);

        private volatile Future<?> future;

        boolean submit(ExecutorService executor) {
            if (!machine.transition(State.SUBMITTED)) {
                return false;
            }

            try {
                future = executor.submit(this::execute);
                return true;
            } catch (RejectedExecutionException e) {
                machine.transition(State.FAILED);
                finished.countDown();
                throw e;
            }
        }

        private void execute() {
            if (!machine.transition(State.RUNNING)) {
                finished.countDown();
                return;
            }

            started.countDown();

            try {
                release.await();

                // 正常完成必须与取消竞争同一个 RUNNING 状态。
                machine.transition(State.SUCCESS);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                // 只有 CANCELLING 才允许进入 CANCELLED。
                machine.transition(State.CANCELLED);

            } finally {
                cleanupCount.incrementAndGet();
                finished.countDown();
            }
        }

        boolean cancel() {
            if (!machine.transition(State.CANCELLING)) {
                // CREATED / SUBMITTED 可以直接取消。
                if (machine.transition(State.CANCELLED)) {
                    finished.countDown();
                    return true;
                }
                return false;
            }

            Future<?> f = future;
            if (f != null) {
                f.cancel(true);
            }

            return true;
        }

        boolean complete() {
            return machine.transition(State.SUCCESS);
        }

        void release() {
            release.countDown();
        }

        boolean awaitStarted() throws InterruptedException {
            return started.await(2, TimeUnit.SECONDS);
        }

        boolean awaitFinished() throws InterruptedException {
            return finished.await(2, TimeUnit.SECONDS);
        }

        State state() {
            return machine.state();
        }

        int cleanupCount() {
            return cleanupCount.get();
        }

        Future<?> future() {
            return future;
        }
    }

    /**
     * A：正常执行
     */
    @Test
    void shouldCompleteNormally() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            ManagedTask task = new ManagedTask();

            assertTrue(task.submit(executor));
            assertTrue(task.awaitStarted());

            task.release();

            assertTrue(task.awaitFinished());
            assertEquals(State.SUCCESS, task.state());
            assertEquals(1, task.cleanupCount());

            System.out.println("A final state = " + task.state());
            System.out.println("A cleanup count = " + task.cleanupCount());

        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    /**
     * B：8 个线程同时取消
     */
    @Test
    void shouldAllowOnlyOneCancellationRequest() throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(8);

        try {
            ManagedTask task = new ManagedTask();

            assertTrue(task.submit(worker));
            assertTrue(task.awaitStarted());

            CountDownLatch ready = new CountDownLatch(8);
            CountDownLatch start = new CountDownLatch(1);

            ConcurrentLinkedQueue<Boolean> results =
                new ConcurrentLinkedQueue<>();

            for (int i = 0; i < 8; i++) {
                callers.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        results.add(task.cancel());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            assertTrue(ready.await(2, TimeUnit.SECONDS));
            start.countDown();

            callers.shutdown();
            assertTrue(callers.awaitTermination(2, TimeUnit.SECONDS));

            assertTrue(task.awaitFinished());

            long accepted = results.stream()
                .filter(Boolean::booleanValue)
                .count();

            assertEquals(1, accepted);
            assertEquals(State.CANCELLED, task.state());
            assertEquals(1, task.cleanupCount());

            System.out.println("B accepted cancellations = " + accepted);
            System.out.println("B final state = " + task.state());
            System.out.println("B cleanup count = " + task.cleanupCount());

        } finally {
            taskReleaseIfNeeded(worker);
            callers.shutdownNow();
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    /**
     * C：真正的完成与取消竞争
     *
     * 三个独立执行角色：
     * 1. worker：执行任务
     * 2. completer：直接尝试 RUNNING -> SUCCESS
     * 3. canceller：尝试 RUNNING -> CANCELLING
     */
    @Test
    void shouldRaceCompletionAgainstCancellation() throws Exception {
        int rounds = 100;

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger cancelledCount = new AtomicInteger();

        for (int i = 0; i < rounds; i++) {
            ExecutorService worker = Executors.newSingleThreadExecutor();
            ExecutorService racers = Executors.newFixedThreadPool(2);

            try {
                ManagedTask task = new ManagedTask();

                assertTrue(task.submit(worker));
                assertTrue(task.awaitStarted());

                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch start = new CountDownLatch(1);

                Future<Boolean> completion = racers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return task.complete();
                });

                Future<Boolean> cancellation = racers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return task.cancel();
                });

                assertTrue(ready.await(2, TimeUnit.SECONDS));
                start.countDown();

                boolean completed = completion.get(2, TimeUnit.SECONDS);
                boolean cancelled = cancellation.get(2, TimeUnit.SECONDS);

                assertNotEquals(completed, cancelled,
                    "Exactly one competing operation must win");

                // 如果取消获胜，确保任务收到中断。
                // 如果完成获胜，释放任务的等待闸门。
                task.release();

                assertTrue(task.awaitFinished());

                if (task.state() == State.SUCCESS) {
                    successCount.incrementAndGet();
                } else if (task.state() == State.CANCELLED) {
                    cancelledCount.incrementAndGet();
                } else {
                    fail("Unexpected terminal state: " + task.state());
                }

                assertEquals(1, task.cleanupCount());

            } finally {
                worker.shutdownNow();
                racers.shutdownNow();

                assertTrue(worker.awaitTermination(2, TimeUnit.SECONDS));
                assertTrue(racers.awaitTermination(2, TimeUnit.SECONDS));
            }
        }

        assertEquals(rounds, successCount.get() + cancelledCount.get());

        System.out.println("C rounds = " + rounds);
        System.out.println("C success count = " + successCount.get());
        System.out.println("C cancelled count = " + cancelledCount.get());
    }

    /**
     * D：合法状态转换和终态不可逆
     */
    @Test
    void shouldRejectIllegalTransitions() {
        StateMachine machine = new StateMachine();

        assertFalse(machine.transition(State.SUCCESS));
        assertEquals(State.CREATED, machine.state());

        assertTrue(machine.transition(State.SUBMITTED));
        assertTrue(machine.transition(State.RUNNING));
        assertTrue(machine.transition(State.SUCCESS));

        assertFalse(machine.transition(State.CANCELLED));
        assertFalse(machine.transition(State.RUNNING));
        assertFalse(machine.transition(State.FAILED));

        assertEquals(State.SUCCESS, machine.state());

        StateMachine cancelMachine = new StateMachine();

        assertTrue(cancelMachine.transition(State.CANCELLED));
        assertFalse(cancelMachine.transition(State.RUNNING));
        assertFalse(cancelMachine.transition(State.SUCCESS));

        System.out.println("D success terminal state = " + machine.state());
        System.out.println("D cancelled terminal state = "
            + cancelMachine.state());
        System.out.println("D illegal transitions rejected");
    }

    private static void taskReleaseIfNeeded(ExecutorService executor) {
        // 兜底关闭线程池，避免实验失败后遗留工作线程。
        executor.shutdownNow();
    }
}
