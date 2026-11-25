package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.LONG_WAIT;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.assertNoLiveThreads;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.awaitLatch;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.fixed;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.pool;
import static kz.bejiihiu.tuff.compute.internal.worker.impl.PoolTestSupport.scheduling;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class DynamicWorkerPoolStressTest {

    private static final int TASKS = 50_000;

    @Test
    void everyShortTaskRunsExactlyOnce() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("stress");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(8, 1, 8, prefix), scheduling(Duration.ofMillis(1)), 16, loop, prefix);
        final AtomicIntegerArray runs = new AtomicIntegerArray(TASKS);
        final CountDownLatch allDone = new CountDownLatch(TASKS);
        for (int i = 0; i < TASKS; i++) {
            final int index = i;
            queue.submit(new TestTask(() -> {
                runs.incrementAndGet(index);
                allDone.countDown();
            }));
        }

        pool.start();
        awaitLatch(allDone, LONG_WAIT);
        for (int i = 0; i < TASKS; i++) {
            assertEquals(1, runs.get(i), "task " + i + " ran the wrong number of times");
        }
        assertEquals(TASKS, loop.executed.get());
        assertEquals(0, loop.cancelledBeforeStart.get());
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void resizeUnderLoadDoesNotLoseOrDuplicateTasks() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("resizestress");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(4, 1, 8, prefix), scheduling(Duration.ofMillis(1)), 16, loop, prefix);
        final int taskCount = 20_000;
        final AtomicIntegerArray runs = new AtomicIntegerArray(taskCount);
        final CountDownLatch allDone = new CountDownLatch(taskCount);
        for (int i = 0; i < taskCount; i++) {
            final int index = i;
            queue.submit(new TestTask(() -> {
                runs.incrementAndGet(index);
                allDone.countDown();
            }));
        }

        final int[] sizes = {1, 8, 2, 7, 3, 6, 4, 5};
        final Thread resizer = Thread.ofPlatform().name(prefix + "-resizer").daemon(true).start(() -> {
            int step = 0;
            final long deadline = System.nanoTime() + LONG_WAIT.toNanos();
            while (allDone.getCount() > 0L && System.nanoTime() < deadline) {
                pool.resize(sizes[step % sizes.length]);
                step++;
                LockSupport.parkNanos(1_000_000L);
            }
        });

        pool.start();
        awaitLatch(allDone, LONG_WAIT);
        try {
            resizer.join(LONG_WAIT.toMillis());
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
        for (int i = 0; i < taskCount; i++) {
            assertEquals(1, runs.get(i), "task " + i + " ran the wrong number of times");
        }
        assertEquals(taskCount, loop.executed.get());
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertNoLiveThreads(prefix);
    }

    @Test
    void shutdownUnderLoadLeavesNoWorkerThreads() throws InterruptedException {
        final String prefix = PoolTestSupport.uniquePrefix("shutdownstress");
        final TestTaskQueue queue = new TestTaskQueue();
        final TestWorkerLoop loop = new TestWorkerLoop(queue, false);
        final DynamicWorkerPool pool = pool(fixed(6, 1, 6, prefix), scheduling(Duration.ofMillis(1)), 16, loop, prefix);
        final CountDownLatch allDone = new CountDownLatch(10_000);
        for (int i = 0; i < 10_000; i++) {
            queue.submit(new TestTask(allDone::countDown));
        }

        pool.start();
        pool.shutdown();
        assertTrue(pool.awaitTermination(LONG_WAIT));
        assertTrue(allDone.getCount() > 0L, "shutdown must not wait for the queue to drain");
        assertNoLiveThreads(prefix);
    }
}
