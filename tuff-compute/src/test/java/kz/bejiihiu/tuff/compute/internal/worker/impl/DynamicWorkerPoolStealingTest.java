package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.LONG_WAIT;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.assertNoLiveThreads;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.await;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.awaitLatch;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.fixed;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.pool;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.scheduling;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
class DynamicWorkerPoolStealingTest {

    @Test
    void idleWorkerStealsWhenEnabled() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("steal");
        final TestTaskQueue queue = new TestTaskQueue(true);
        final TestWorkerLoop loop = new TestWorkerLoop(queue, true);
        final DynamicWorkerPool pool = pool(fixed(2, 1, 2, prefix), scheduling(Duration.ofMillis(2)), 16, loop, prefix);
        final CountDownLatch release = new CountDownLatch(1);
        final List<TestTask> tasks = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            final TestTask task = new TestTask(() -> awaitLatch(release, LONG_WAIT));
            tasks.add(task);
            queue.submit(task);
        }

        pool.start();
        await(() -> PoolTestSupport.started(tasks) >= 2, LONG_WAIT);
        assertTrue(loop.steals.get() >= 1, "the queue reported no steal although stealing is enabled");
        release.countDown();
        for (final TestTask task : tasks) {
            awaitLatch(task.done, LONG_WAIT);
        }
        assertEquals(4, loop.executed.get());
        assertTrue(loop.workerIndexes.contains(0));
        assertTrue(loop.workerIndexes.contains(1));
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void noStealingWhenDisabled() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("nosteal");
        final TestTaskQueue queue = new TestTaskQueue(false);
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(2, 1, 2, prefix), scheduling(Duration.ofMillis(2)), 16, loop, prefix);
        final List<TestTask> tasks = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            final TestTask task = new TestTask(() -> {
            });
            tasks.add(task);
            queue.submit(task);
        }

        pool.start();
        for (final TestTask task : tasks) {
            awaitLatch(task.done, LONG_WAIT);
        }
        assertEquals(4, loop.executed.get());
        assertEquals(0, loop.steals.get(), "work stealing must stay off when the setting is off");
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }
}
