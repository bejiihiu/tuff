package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.LONG_WAIT;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.assertNoLiveThreads;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.automatic;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.await;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.awaitLatch;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.fixed;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.pool;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.scheduling;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
class DynamicWorkerPoolReuseTest {

    @Test
    void oneThreadRunsManySequentialTasks() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("reuse");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(1, 1, 1, prefix), scheduling(Duration.ofMillis(2)), 16, loop, prefix);
        final List<TestTask> tasks = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            final TestTask task = new TestTask(() -> {
            });
            tasks.add(task);
            queue.submit(task);
        }

        pool.start();
        await(() -> loop.executed.get() == tasks.size(), LONG_WAIT);
        for (final TestTask task : tasks) {
            awaitLatch(task.done, LONG_WAIT);
        }
        assertEquals(1, loop.threadIds.size(), "the same worker must execute every task");
        assertEquals(1, loop.workerStarts.get());
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void keepAliveRetiresWorkersAboveTheMinimum() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("keepalive");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(
            automatic(1, 8, Duration.ofMillis(200), prefix),
            scheduling(Duration.ofMillis(5)), 16, loop, prefix);

        pool.start();
        assertEquals(8, pool.workerCount(), "automatic sizing is capped by maxThreads");
        await(() -> pool.workerCount() == 1, LONG_WAIT);
        assertEquals(1, pool.workerCount());
        assertTrue(pool.peakWorkers() >= 8);

        final TestTask afterShrink = new TestTask(() -> {
        });
        queue.submit(afterShrink);
        awaitLatch(afterShrink.done, LONG_WAIT);
        assertEquals(1, loop.executed.get(), "the remaining worker must still pick up work");

        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void resizeGrowsAndShrinksUnderLoadWithoutLosingTasks() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("resize");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(2, 1, 8, prefix), scheduling(Duration.ofMillis(2)), 16, loop, prefix);
        final CountDownLatch release = new CountDownLatch(1);
        final List<TestTask> tasks = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            final TestTask task = new TestTask(() -> awaitLatch(release, LONG_WAIT));
            tasks.add(task);
            queue.submit(task);
        }

        pool.start();
        await(() -> PoolTestSupport.started(tasks) >= 2, LONG_WAIT);
        pool.resize(6);
        await(() -> pool.workerCount() == 6, LONG_WAIT);
        await(() -> PoolTestSupport.started(tasks) >= 6, LONG_WAIT);

        pool.resize(2);
        release.countDown();
        for (final TestTask task : tasks) {
            awaitLatch(task.done, LONG_WAIT);
        }
        await(() -> pool.workerCount() == 2, LONG_WAIT);
        assertEquals(20, loop.executed.get());
        assertEquals(20, PoolTestSupport.started(tasks));
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void concurrencyLimitCapsSimultaneousTasks() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("limit");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(4, 1, 4, prefix), scheduling(Duration.ofMillis(2)), 16, loop, prefix);
        pool.setConcurrencyLimit(2);
        assertEquals(2, pool.concurrencyLimit());

        final CountDownLatch firstRelease = new CountDownLatch(1);
        final List<TestTask> firstBatch = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            final TestTask task = new TestTask(() -> awaitLatch(firstRelease, LONG_WAIT));
            firstBatch.add(task);
            queue.submit(task);
        }
        pool.start();
        await(() -> PoolTestSupport.started(firstBatch) == 2, LONG_WAIT);
        firstRelease.countDown();
        for (final TestTask task : firstBatch) {
            awaitLatch(task.done, LONG_WAIT);
        }
        assertEquals(2, loop.maxConcurrent.get());

        pool.setConcurrencyLimit(4);
        final CountDownLatch secondRelease = new CountDownLatch(1);
        final List<TestTask> secondBatch = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            final TestTask task = new TestTask(() -> awaitLatch(secondRelease, LONG_WAIT));
            secondBatch.add(task);
            queue.submit(task);
        }
        await(() -> PoolTestSupport.started(secondBatch) == 4, LONG_WAIT);
        secondRelease.countDown();
        for (final TestTask task : secondBatch) {
            awaitLatch(task.done, LONG_WAIT);
        }
        assertEquals(4, loop.maxConcurrent.get());

        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void peakWorkersTracksTheLargestPool() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("peak");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(2, 1, 6, prefix), scheduling(Duration.ofMillis(2)), 16, loop, prefix);
        pool.start();
        pool.resize(6);
        await(() -> pool.workerCount() == 6, LONG_WAIT);
        assertEquals(6, pool.peakWorkers());
        pool.resize(2);
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertEquals(6, pool.peakWorkers());
        assertNoLiveThreads(prefix);
    }

    @Test
    void activeAndIdleCountersFollowTheWork() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("counters");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(2, 1, 2, prefix), scheduling(Duration.ofMillis(2)), 16, loop, prefix);
        final CountDownLatch release = new CountDownLatch(1);
        final TestTask task = new TestTask(() -> awaitLatch(release, LONG_WAIT));
        queue.submit(task);
        pool.start();
        await(() -> task.started.get(), LONG_WAIT);
        await(() -> pool.activeWorkers() == 1, LONG_WAIT);
        assertEquals(1, pool.idleWorkers());
        release.countDown();
        awaitLatch(task.done, LONG_WAIT);
        final AtomicInteger idle = new AtomicInteger();
        await(() -> {
            idle.set(pool.idleWorkers());
            return pool.activeWorkers() == 0 && idle.get() == 2;
        }, LONG_WAIT);
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }
}
