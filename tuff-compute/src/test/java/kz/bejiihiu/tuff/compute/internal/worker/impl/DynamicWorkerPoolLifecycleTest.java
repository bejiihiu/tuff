package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.compute.internal.util.ThreadFactories;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
class DynamicWorkerPoolLifecycleTest {

    @Test
    void startSpawnsTheConfiguredNumberOfWorkers() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("start");
        final TestWorkerLoop loop = new TestWorkerLoop(new TestTaskQueue(), false);
        final DynamicWorkerPool pool = pool(fixed(4, 1, 4, prefix), scheduling(Duration.ofMillis(5)), 16, loop, prefix);
        assertEquals(0, pool.workerCount());
        pool.start();
        assertEquals(4, pool.workerCount());
        assertEquals(0, pool.activeWorkers());
        assertEquals(4, pool.idleWorkers());
        assertEquals(4, pool.peakWorkers());
        assertEquals(4, pool.concurrencyLimit());
        assertEquals(0, loop.executed.get());
        await(() -> loop.workerStarts.get() == 4, LONG_WAIT);
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertTrue(pool.isTerminated());
        assertNoLiveThreads(prefix);
    }

    @Test
    void startIsIdempotent() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("idempotent");
        final TestWorkerLoop loop = new TestWorkerLoop(new TestTaskQueue(), false);
        final DynamicWorkerPool pool = pool(fixed(3, 1, 3, prefix), scheduling(Duration.ofMillis(5)), 16, loop, prefix);
        pool.start();
        pool.start();
        pool.start();
        assertEquals(3, pool.workerCount());
        assertEquals(3, pool.peakWorkers());
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void shutdownLetsTheRunningTaskFinishAndStopsClaiming() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("graceful");
        final TestTaskQueue queue = new TestTaskQueue();
        final CountDownLatch release = new CountDownLatch(1);
        final TestTask first = new TestTask(() -> awaitLatch(release, LONG_WAIT));
        final TestTask second = new TestTask(() -> {
        });
        queue.submit(first);
        queue.submit(second);
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(1, 1, 1, prefix), scheduling(Duration.ofMillis(5)), 16, loop, prefix);

        pool.start();
        await(() -> first.started.get(), LONG_WAIT);
        pool.shutdown();
        assertTrue(pool.isShutdown());
        assertFalse(second.started.get());
        assertFalse(first.interrupted);
        try {
            assertFalse(second.done.await(SHORT_WAIT.toMillis(), TimeUnit.MILLISECONDS));
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }

        release.countDown();
        awaitLatch(first.done, LONG_WAIT);
        assertNull(first.failure);
        assertFalse(second.started.get());
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void shutdownNowAlsoLetsTheRunningTaskFinishButClaimsNothingNew() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("now");
        final TestTaskQueue queue = new TestTaskQueue();
        final CountDownLatch release = new CountDownLatch(1);
        final TestTask first = new TestTask(() -> awaitLatch(release, LONG_WAIT));
        final TestTask second = new TestTask(() -> {
        });
        queue.submit(first);
        queue.submit(second);
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(1, 1, 1, prefix), scheduling(Duration.ofMillis(5)), 16, loop, prefix);

        pool.start();
        await(() -> first.started.get(), LONG_WAIT);
        pool.shutdownNow();
        release.countDown();
        awaitLatch(first.done, LONG_WAIT);
        assertFalse(second.started.get());
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void awaitTerminationTimesOutWhileATaskIsStuck() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("await");
        final TestTaskQueue queue = new TestTaskQueue();
        final CountDownLatch release = new CountDownLatch(1);
        final TestTask task = new TestTask(() -> awaitLatch(release, LONG_WAIT));
        queue.submit(task);
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(1, 1, 1, prefix), scheduling(Duration.ofMillis(5)), 16, loop, prefix);

        pool.start();
        await(() -> task.started.get(), LONG_WAIT);
        pool.shutdown();
        assertFalse(pool.awaitTermination(SHORT_WAIT));
        assertFalse(pool.isTerminated());
        release.countDown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void repeatedShutdownIsIdempotentAndStartAfterShutdownDoesNothing() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("repeat");
        final TestWorkerLoop loop = new TestWorkerLoop(new TestTaskQueue(), false);
        final DynamicWorkerPool pool = pool(fixed(2, 1, 2, prefix), scheduling(Duration.ofMillis(5)), 16, loop, prefix);
        pool.start();
        assertEquals(2, pool.workerCount());
        pool.shutdown();
        pool.shutdown();
        pool.shutdownNow();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        pool.start();
        assertEquals(0, pool.workerCount());
        assertTrue(pool.isShutdown());
        pool.shutdownNow();
        assertTrue(pool.awaitTermination(Duration.ofMillis(100)));
        assertNoLiveThreads(prefix);
    }

    @Test
    void shutdownBeforeStartTerminatesWithoutThreads() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("early");
        final TestWorkerLoop loop = new TestWorkerLoop(new TestTaskQueue(), false);
        final DynamicWorkerPool pool = pool(fixed(2, 1, 2, prefix), scheduling(Duration.ofMillis(5)), 16, loop, prefix);
        assertFalse(pool.isTerminated());
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertTrue(pool.isTerminated());
        assertEquals(0, pool.workerCount());
        assertNoLiveThreads(prefix);
    }

    @Test
    void virtualThreadFactoriesAreRejected() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("virtual");
        final TestWorkerLoop loop = new TestWorkerLoop(new TestTaskQueue(), false);
        final DynamicWorkerPool pool = new DynamicWorkerPool(
            fixed(1, 1, 1, prefix), scheduling(Duration.ofMillis(5)), 8, loop,
            ComputeClock.system(), ThreadFactories.virtual(prefix));
        assertThrows(IllegalArgumentException.class, pool::start);
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }
}
