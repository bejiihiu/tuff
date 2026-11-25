package kz.bejiihiu.tuff.compute.internal.worker.impl;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.compute.internal.util.ThreadFactories;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Shared fixtures: settings, pool construction and deadline based waiting.
 */
final class PoolTestSupport {

    static final Duration LONG_WAIT = Duration.ofSeconds(10);
    static final Duration SHORT_WAIT = Duration.ofMillis(200);

    private PoolTestSupport() {
    }

    static ComputeSettings.Workers fixed(final int threads, final int min, final int max, final String prefix) {
        return new ComputeSettings.Workers(threads, min, max, Duration.ofSeconds(30), prefix);
    }

    static ComputeSettings.Workers automatic(
        final int min,
        final int max,
        final Duration keepAlive,
        final String prefix
    ) {
        return new ComputeSettings.Workers(-1, min, max, keepAlive, prefix);
    }

    static ComputeSettings.Scheduling scheduling(final Duration idleWait) {
        return new ComputeSettings.Scheduling(
            ComputeSettings.Scheduling.Strategy.FIFO, false, false,
            Duration.ofSeconds(1), 1, idleWait);
    }

    static DynamicWorkerPool pool(
        final ComputeSettings.Workers workers,
        final ComputeSettings.Scheduling scheduling,
        final int processors,
        final TestWorkerLoop loop,
        final String prefix
    ) {
        return new DynamicWorkerPool(
            workers, scheduling, processors, loop, ComputeClock.system(),
            ThreadFactories.platform(prefix, true));
    }

    static String uniquePrefix(final String name) {
        return "tuff-test-" + name + "-" + Long.toHexString(System.nanoTime());
    }

    static int started(final List<TestTask> tasks) {
        int started = 0;
        for (final TestTask task : tasks) {
            if (task.started.get()) {
                started++;
            }
        }
        return started;
    }

    static void await(final BooleanSupplier condition, final Duration timeout) {
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                fail("condition was not satisfied within " + timeout);
            }
            LockSupport.parkNanos(1_000_000L);
        }
    }

    static void awaitLatch(final CountDownLatch latch, final Duration timeout) {
        try {
            if (!latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                fail("latch was not released within " + timeout);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            fail("interrupted while waiting", interrupted);
        }
    }

    static void assertNoLiveThreads(final String prefix) {
        await(
            () -> Thread.getAllStackTraces().keySet().stream()
                .noneMatch(thread -> thread.isAlive() && thread.getName().startsWith(prefix)),
            LONG_WAIT);
    }
}
