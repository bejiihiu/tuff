package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.LONG_WAIT;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.assertNoLiveThreads;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.await;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.awaitLatch;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.fixed;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.pool;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.scheduling;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.uniquePrefix;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Submitting work must wake one idle worker, not the whole pool, and must not leave wake-up state on
 * a thread that then runs a task.
 *
 * <p>The idle wait is thirty seconds in every test, so a worker that was not signalled cannot wake
 * by timeout inside the test window. That is what makes the delivered wake-up count and the shutdown
 * timing meaningful instead of a race against a short poll.
 */
@Timeout(120)
class DynamicWorkerPoolWakeupTest {

    private static final Duration IDLE = Duration.ofSeconds(30L);

    @Test
    void oneSubmissionWakesExactlyOneParkedWorker() throws InterruptedException {
        final String prefix = uniquePrefix("wake-one");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(8, 8, 8, prefix), scheduling(IDLE), 16, loop, prefix);
        pool.start();
        try {
            await(() -> pool.parkedWorkers() == 8, LONG_WAIT);
            final int before = pool.deliveredWakeups();

            final TestTask task = new TestTask(() -> {
            });
            queue.submit(task);
            pool.wakeUp();

            awaitLatch(task.done, LONG_WAIT);
            assertEquals(before + 1, pool.deliveredWakeups(),
                "one submission woke " + (pool.deliveredWakeups() - before) + " workers");
            await(() -> pool.parkedWorkers() == 8, LONG_WAIT);
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(LONG_WAIT));
        }
        assertNoLiveThreads(prefix);
    }

    @Test
    void aBurstOfSubmissionsDoesNotPileUpWakeups() throws InterruptedException {
        final String prefix = uniquePrefix("wake-burst");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(2, 2, 2, prefix), scheduling(IDLE), 16, loop, prefix);
        pool.start();
        try {
            await(() -> pool.parkedWorkers() == 2, LONG_WAIT);
            final int before = pool.deliveredWakeups();
            final List<TestTask> tasks = new ArrayList<>();
            for (int index = 0; index < 200; index++) {
                final TestTask task = new TestTask(() -> {
                });
                tasks.add(task);
                queue.submit(task);
                pool.wakeUp();
            }
            // both workers are busy by now, so a credit counter that was not capped by the live
            // worker count would still hold almost every one of the 200 submissions here
            assertTrue(pool.pendingWakeups() <= 2,
                "wake-up credits piled up: " + pool.pendingWakeups());

            for (final TestTask task : tasks) {
                awaitLatch(task.done, LONG_WAIT);
            }
            assertEquals(200, loop.executed.get(), "a task was lost or duplicated");
            await(() -> pool.parkedWorkers() == 2, LONG_WAIT);
            assertEquals(0, pool.pendingWakeups(), "wake-up credits were left stuck in the pool");
            assertTrue(pool.deliveredWakeups() - before <= 200, "wake-ups multiplied");
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(LONG_WAIT));
        }
        assertNoLiveThreads(prefix);
    }

    @Test
    void aTimedParkInsideATaskBodyActuallyBlocks() throws InterruptedException {
        final String prefix = uniquePrefix("wake-park");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(1, 1, 1, prefix), scheduling(IDLE), 16, loop, prefix);
        pool.start();
        try {
            await(() -> pool.parkedWorkers() == 1, LONG_WAIT);
            final AtomicLong parked = new AtomicLong();
            final TestTask task = new TestTask(() -> {
                final long started = System.nanoTime();
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100L));
                parked.set(System.nanoTime() - started);
            });
            queue.submit(task);
            pool.wakeUp();
            awaitLatch(task.done, LONG_WAIT);

            assertTrue(parked.get() >= TimeUnit.MILLISECONDS.toNanos(50L),
                "a timed park returned after " + parked.get() + " ns: the wake path left an unpark permit");
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(LONG_WAIT));
        }
        assertNoLiveThreads(prefix);
    }

    @Test
    void aRetiringWorkerHandsItsWakeupToAnotherWorker() throws InterruptedException {
        final String prefix = uniquePrefix("wake-retire");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(2, 1, 2, prefix), scheduling(IDLE), 16, loop, prefix);
        pool.start();
        try {
            await(() -> pool.parkedWorkers() == 2, LONG_WAIT);
            // shrink while both workers are parked: only a wake-up can make them notice, and the one
            // that does notice must not swallow the credit when it retires
            pool.resize(1);

            final TestTask task = new TestTask(() -> {
            });
            queue.submit(task);
            pool.wakeUp();

            // the idle wait is thirty seconds, so this only passes if the credit was handed over
            awaitLatch(task.done, Duration.ofSeconds(5L));
            await(() -> pool.workerCount() == 1, LONG_WAIT);
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(LONG_WAIT));
        }
        assertNoLiveThreads(prefix);
    }

    @Test
    void shutdownWakesEveryParkedWorker() throws InterruptedException {
        final String prefix = uniquePrefix("wake-shutdown");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(8, 8, 8, prefix), scheduling(IDLE), 16, loop, prefix);
        pool.start();
        try {
            await(() -> pool.parkedWorkers() == 8, LONG_WAIT);
        } finally {
            pool.shutdown();
        }
        // a thirty second idle wait: without a wake-all this would time out instead of terminating
        assertTrue(pool.awaitTermination(Duration.ofSeconds(5L)),
            "shutdown did not wake every parked worker");
        assertNoLiveThreads(prefix);
    }

    @Test
    void growingThePoolWakesTheParkedWorkers() throws InterruptedException {
        final String prefix = uniquePrefix("wake-grow");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(2, 1, 6, prefix), scheduling(IDLE), 16, loop, prefix);
        pool.start();
        try {
            await(() -> pool.parkedWorkers() == 2, LONG_WAIT);
            final int before = pool.deliveredWakeups();

            pool.resize(6);

            await(() -> pool.parkedWorkers() == 6, LONG_WAIT);
            assertTrue(pool.deliveredWakeups() >= before + 2,
                "resize did not wake the workers that were already parked");
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(LONG_WAIT));
        }
        assertNoLiveThreads(prefix);
    }
}
