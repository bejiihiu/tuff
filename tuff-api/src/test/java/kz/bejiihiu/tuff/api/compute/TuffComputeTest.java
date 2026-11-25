package kz.bejiihiu.tuff.api.compute;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TuffComputeTest {

    @AfterEach
    void uninstallEngine() {
        TuffCompute.uninstall();
    }

    @Test
    void exposesInstalledScheduler() {
        final TestScheduler scheduler = new TestScheduler();

        TuffCompute.install(scheduler);

        assertTrue(TuffCompute.isAvailable());
        assertEquals(Optional.of(scheduler), TuffCompute.scheduler());
        assertSame(scheduler, TuffCompute.require());
    }

    @Test
    void requireFailsWithActionableMessageWhenEngineIsAbsent() {
        final IllegalStateException exception = assertThrows(IllegalStateException.class, TuffCompute::require);

        assertTrue(exception.getMessage().contains("not available"));
        assertTrue(exception.getMessage().contains("config/tuff.yml"));
        assertTrue(exception.getMessage().contains("TuffCompute.scheduler()"));
        assertFalse(TuffCompute.isAvailable());
    }

    @Test
    void installingTheSameSchedulerTwiceIsIdempotent() {
        final TestScheduler scheduler = new TestScheduler();

        TuffCompute.install(scheduler);
        TuffCompute.install(scheduler);

        assertSame(scheduler, TuffCompute.require());
    }

    @Test
    void installingADifferentSchedulerIsRejectedAndKeepsTheFirstOne() {
        final TestScheduler first = new TestScheduler();
        final TestScheduler second = new TestScheduler();

        TuffCompute.install(first);
        assertThrows(IllegalStateException.class, () -> TuffCompute.install(second));
        assertSame(first, TuffCompute.require());

        assertSame(first, TuffCompute.uninstall().orElseThrow());
        assertTrue(TuffCompute.scheduler().isEmpty());
    }

    @Test
    void uninstallIsIdempotent() {
        final TestScheduler scheduler = new TestScheduler();
        TuffCompute.install(scheduler);

        assertSame(scheduler, TuffCompute.uninstall().orElseThrow());
        assertTrue(TuffCompute.uninstall().isEmpty());
        assertTrue(TuffCompute.scheduler().isEmpty());
        assertThrows(IllegalStateException.class, TuffCompute::require);
    }

    @Test
    void installRejectsNull() {
        assertThrows(NullPointerException.class, () -> TuffCompute.install(null));
        assertFalse(TuffCompute.isAvailable());
    }

    @Test
    void installRejectsTerminatedScheduler() {
        final TestScheduler scheduler = new TestScheduler();
        scheduler.shutdown();

        assertThrows(IllegalArgumentException.class, () -> TuffCompute.install(scheduler));
        assertFalse(TuffCompute.isAvailable());
    }

    @Test
    void executorFactoryUsesTheInstalledEngine() {
        assertThrows(IllegalStateException.class, () -> TuffCompute.executor("unit-test"));

        TuffCompute.install(new TestScheduler());
        final ComputeExecutor executor = TuffCompute.executor("unit-test");

        assertNotNull(executor);
        assertEquals("unit-test", executor.consumerId());
        assertEquals(ComputePriority.NORMAL, executor.priority());
        assertEquals(ComputeExecutor.RejectionPolicy.CALLER_RUNS, executor.rejectionPolicy());
    }

    @Test
    void concurrentInstallsOfTheSameInstanceAllSucceed() throws Exception {
        final TestScheduler scheduler = new TestScheduler();
        final int threads = 16;
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        final CountDownLatch start = new CountDownLatch(1);
        try {
            final List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    TuffCompute.install(scheduler);
                    return null;
                }));
            }
            start.countDown();
            for (final Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertSame(scheduler, TuffCompute.require());
    }

    @Test
    void concurrentInstallsOfDifferentInstancesHaveExactlyOneWinner() throws Exception {
        final int threads = 8;
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger accepted = new AtomicInteger();
        final AtomicInteger rejected = new AtomicInteger();
        final List<TestScheduler> candidates = new ArrayList<>();
        try {
            final List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                final TestScheduler candidate = new TestScheduler();
                candidates.add(candidate);
                futures.add(pool.submit(() -> {
                    start.await();
                    try {
                        TuffCompute.install(candidate);
                        accepted.incrementAndGet();
                    } catch (IllegalStateException _) {
                        rejected.incrementAndGet();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (final Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertEquals(1, accepted.get());
        assertEquals(threads - 1, rejected.get());
        assertTrue(candidates.contains(TuffCompute.require()));
    }

    @Test
    void concurrentReadersNeverSeeAForeignScheduler() throws Exception {
        final TestScheduler scheduler = new TestScheduler();
        final int threads = 8;
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        final CountDownLatch start = new CountDownLatch(1);
        try {
            final List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int read = 0; read < 500; read++) {
                        final AsyncComputeScheduler seen = TuffCompute.scheduler().orElse(null);
                        if (seen != null && seen != scheduler) {
                            throw new AssertionError("unexpected scheduler: " + seen);
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            TuffCompute.install(scheduler);
            for (final Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertSame(scheduler, TuffCompute.require());
    }
}
