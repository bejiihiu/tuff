package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.LONG_WAIT;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.SHORT_WAIT;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.assertNoLiveThreads;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.await;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.awaitLatch;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.fixed;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.pool;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.scheduling;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
class DynamicWorkerPoolFaultTest {

    @Test
    void taskErrorDoesNotKillTheWorkerOrThePool() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("taskerror");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(1, 1, 1, prefix), scheduling(Duration.ofMillis(2)), 16, loop, prefix);
        final TestTask failing = new TestTask(() -> {
            throw new AssertionError("boom");
        });
        final TestTask following = new TestTask(() -> {
        });
        queue.submit(failing);
        queue.submit(following);

        pool.start();
        awaitLatch(failing.done, LONG_WAIT);
        awaitLatch(following.done, LONG_WAIT);
        assertInstanceOf(AssertionError.class, failing.failure);
        assertNull(following.failure);
        assertEquals(2, loop.executed.get());
        assertEquals(1, loop.workerStarts.get());
        assertEquals(0, loop.workerStops.get());
        assertEquals(1, loop.threadIds.size(), "the failed task must not rotate the worker thread");
        assertEquals(1, pool.workerCount());

        final TestTask after = new TestTask(() -> {
        });
        queue.submit(after);
        awaitLatch(after.done, LONG_WAIT);
        assertEquals(3, loop.executed.get());
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void escapedExecuteNextFailureIsContained() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("escape");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(2, 1, 2, prefix), scheduling(Duration.ofMillis(2)), 16, loop, prefix);
        loop.escapedFailuresRemaining.set(1);
        final TestTask first = new TestTask(() -> {
        });
        queue.submit(first);

        pool.start();
        awaitLatch(first.done, LONG_WAIT);
        assertEquals(2, pool.workerCount(), "a loop that escaped once must not lose a worker");

        for (int i = 0; i < 10; i++) {
            queue.submit(new TestTask(() -> {
            }));
        }
        await(() -> loop.executed.get() == 11, LONG_WAIT);
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void workerExitsAfterRepeatedEscapesButThePoolShutsDownCleanly() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("strikes");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(1, 1, 1, prefix), scheduling(Duration.ofMillis(2)), 16, loop, prefix);
        loop.escapedFailuresRemaining.set(3);

        pool.start();
        await(() -> loop.workerStops.get() == 1, LONG_WAIT);
        assertEquals(0, pool.workerCount());
        assertFalse(pool.isTerminated(), "the pool is not shut down just because a buggy loop died");
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void taskCancelledBeforeStartIsNotExecuted() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("precancel");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(1, 1, 1, prefix), scheduling(Duration.ofMillis(2)), 16, loop, prefix);
        final TestTask cancelled = new TestTask(() -> {
        });
        cancelled.cancelled.set(true);
        final TestTask normal = new TestTask(() -> {
        });
        queue.submit(cancelled);
        queue.submit(normal);

        pool.start();
        awaitLatch(cancelled.done, LONG_WAIT);
        awaitLatch(normal.done, LONG_WAIT);
        assertEquals(1, loop.cancelledBeforeStart.get());
        assertFalse(cancelled.started.get());
        assertEquals(1, loop.executed.get());
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void runningTaskCancellationIsCooperativeAndDoesNotInterruptTheWorker() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("runcancel");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(1, 1, 1, prefix), scheduling(Duration.ofMillis(2)), 16, loop, prefix);
        final AtomicBoolean cancel = new AtomicBoolean();
        final TestTask running = new TestTask(() -> {
            while (!cancel.get()) {
                Thread.onSpinWait();
            }
        });
        queue.submit(running);

        pool.start();
        await(() -> running.started.get(), LONG_WAIT);
        cancel.set(true);
        awaitLatch(running.done, LONG_WAIT);
        assertFalse(running.interrupted, "the pool must never interrupt a worker");
        assertEquals(1, loop.executed.get());

        final TestTask after = new TestTask(() -> {
        });
        queue.submit(after);
        awaitLatch(after.done, LONG_WAIT);
        assertEquals(running.threadId, after.threadId);
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void aStuckTaskDoesNotPreventShutdownNowFromReturningAfterItFinishes() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("stuck");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(2, 1, 2, prefix), scheduling(Duration.ofMillis(2)), 16, loop, prefix);
        final CountDownLatch release = new CountDownLatch(1);
        final TestTask stuck = new TestTask(() -> awaitLatch(release, LONG_WAIT));
        queue.submit(stuck);

        pool.start();
        await(() -> stuck.started.get(), LONG_WAIT);
        pool.shutdownNow();
        assertFalse(pool.awaitTermination(SHORT_WAIT));
        release.countDown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }
}
