package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.cancellationOf;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.engine;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The pool size, its concurrency gate and the quota total limit are one number in three places.
 *
 * <p>Autoscaling used to grow the pool without moving the quota, so a pool of four workers with a
 * quota of three really ran three bodies; and a processor budget above the construction time budget
 * could not raise the ceiling of an automatic pool at all. These tests pin both down through the
 * public scheduler: they count bodies that are actually executing at the same time, they do not look
 * at internals that could agree with each other while the work still does not run.
 */
@Timeout(120)
class EngineSizingSyncTest {

    @Test
    void autoscaleKeepsTheQuotaLimitInStepWithThePool() throws Exception {
        final ComputeSettings built = settings()
            .autoWorkers(1, -1, Duration.ofSeconds(30))
            .metrics(new ComputeSettings.Metrics(true, false, Duration.ofSeconds(10), false, millis(20L)))
            // the test counts platform bodies, so the capacity must be the platform count
            .noVirtualLane()
            .grace(Duration.ofSeconds(1))
            .build();
        final ComputeEngineImpl engine = engine(built, new EngineTestSupport.FakeClock(), 4);
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch fourRunning = new CountDownLatch(4);
        final AtomicInteger running = new AtomicInteger();
        final AtomicInteger peak = new AtomicInteger();
        final List<CompletableFuture<Integer>> futures = new ArrayList<>();
        compute.start();
        try {
            for (int index = 0; index < 8; index++) {
                futures.add(compute.submit(() -> {
                    peak.accumulateAndGet(running.incrementAndGet(), Math::max);
                    fourRunning.countDown();
                    try {
                        release.await();
                    } finally {
                        running.decrementAndGet();
                    }
                    return 1;
                }));
            }
            assertTrue(fourRunning.await(10L, TimeUnit.SECONDS),
                "the pool grew to four workers but only " + peak.get() + " bodies could run");
            assertTrue(peak.get() <= 4, "the quota admitted more bodies than the pool has workers");
            assertEquals(4, engine.desiredWorkers(), "the pool must be at its budget ceiling");
            assertEquals(4, engine.totalLimit(), "the quota limit must follow the pool");
            assertEventually("the metrics gauge follows the resized pool", Duration.ofSeconds(5), () ->
                compute.metrics().concurrencyLimit() == 4 && compute.metrics().totalWorkers() <= 4);

            release.countDown();
            for (final CompletableFuture<Integer> future : futures) {
                assertEquals(1, future.get(10L, TimeUnit.SECONDS));
            }
            assertEventually("an idle pool shrinks and the limit shrinks with it", Duration.ofSeconds(10), () ->
                engine.desiredWorkers() == 1
                    && engine.totalLimit() == 1
                    && compute.metrics().concurrencyLimit() == 1);
            assertTrue(peak.get() <= 4, "the pool never ran more bodies than its ceiling");
        } finally {
            release.countDown();
            compute.shutdown();
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        }
    }

    @Test
    void budgetRaiseGrowsTheAutomaticCeilingAndShrinkKeepsEveryTask() throws Exception {
        final ComputeSettings built = settings()
            .autoWorkers(1, -1, Duration.ofSeconds(30))
            .metrics(new ComputeSettings.Metrics(true, false, Duration.ofSeconds(10), false, millis(20L)))
            .noVirtualLane()
            .grace(Duration.ofSeconds(1))
            .build();
        final ComputeEngineImpl engine = engine(built, new EngineTestSupport.FakeClock(), 2);
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch sixRunning = new CountDownLatch(6);
        final AtomicInteger running = new AtomicInteger();
        final AtomicInteger peak = new AtomicInteger();
        final List<CompletableFuture<Integer>> futures = new ArrayList<>();
        compute.start();
        try {
            for (int index = 0; index < 10; index++) {
                futures.add(compute.submit(() -> {
                    peak.accumulateAndGet(running.incrementAndGet(), Math::max);
                    sixRunning.countDown();
                    try {
                        release.await();
                    } finally {
                        running.decrementAndGet();
                    }
                    return 1;
                }));
            }
            assertEventually("the starting budget of two caps the pool", Duration.ofSeconds(10), () ->
                engine.desiredWorkers() == 2);

            engine.applyProcessorBudget(6);
            assertTrue(sixRunning.await(10L, TimeUnit.SECONDS),
                "a raised budget must let the pool reach six; peak was " + peak.get());
            assertTrue(peak.get() <= 6, "the quota outgrew the pool");
            assertEventually("pool and limit settle on the new budget", Duration.ofSeconds(5), () ->
                engine.desiredWorkers() == 6 && engine.totalLimit() == 6);

            engine.applyProcessorBudget(1);
            assertEventually("a smaller budget shrinks pool and limit together", Duration.ofSeconds(10), () ->
                engine.desiredWorkers() == 1 && engine.totalLimit() == 1);
            assertEquals(6, running.get(), "a shrink must not interrupt running bodies");

            release.countDown();
            for (final CompletableFuture<Integer> future : futures) {
                assertEquals(1, future.get(10L, TimeUnit.SECONDS));
            }
            assertEventually("no queued work was lost by the shrinks", Duration.ofSeconds(10), () ->
                compute.metrics().queuedTasks() == 0);
        } finally {
            release.countDown();
            compute.shutdown();
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        }
    }

    @Test
    void snapshotsExposeEvictionsAndCancellationReasons() throws Exception {
        final ConsumerPolicy dropOldest = new ConsumerPolicy(
            true,
            ConsumerPolicy.defaults().concurrencyLimit(),
            1,
            1.0d,
            OverflowPolicy.DROP_OLDEST,
            Duration.ZERO
        );
        final ComputeSettings built = settings()
            .fixedWorkers(1)
            .overflow(OverflowPolicy.DROP_OLDEST)
            .defaultPolicy(dropOldest)
            .grace(Duration.ofSeconds(1))
            .build();
        final ComputeEngineImpl engine = engine(built, new EngineTestSupport.FakeClock());
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        compute.start();
        try {
            final ComputeTaskHandle<Integer> blocking = compute.enqueue(ComputeRequest.of(() -> {
                started.countDown();
                release.await();
                return 1;
            }));
            assertTrue(started.await(10L, TimeUnit.SECONDS));

            final ComputeTaskHandle<Integer> evicted = compute.enqueue(ComputeRequest.of(() -> 2));
            final ComputeTaskHandle<Integer> survivor = compute.enqueue(ComputeRequest.of(() -> 3));
            assertEquals(ComputeCancelledException.Reason.EVICTED, cancellationOf(evicted.future()).reason());

            assertEventually("the eviction reaches the public snapshot", Duration.ofSeconds(5), () -> {
                final SchedulerMetrics metrics = compute.metrics();
                return metrics.evictedTasks() == 1L
                    && metrics.cancelledByReason().get(ComputeCancelledException.Reason.EVICTED) == 1L
                    && metrics.cancelledTasks() == 1L;
            });

            assertTrue(blocking.cancel(), "a running task must accept a cancellation request");
            release.countDown();
            assertEquals(ComputeCancelledException.Reason.CALLER, cancellationOf(blocking.future()).reason());
            assertEquals(3, survivor.future().get(10L, TimeUnit.SECONDS));

            assertEventually("the reason map covers every cancellation", Duration.ofSeconds(5), () -> {
                final SchedulerMetrics metrics = compute.metrics();
                final long byReason = metrics.cancelledByReason().values().stream()
                    .mapToLong(Long::longValue)
                    .sum();
                return metrics.evictedTasks() == 1L
                    && metrics.cancelledByReason().get(ComputeCancelledException.Reason.CALLER) == 1L
                    && metrics.cancelledByReason().get(ComputeCancelledException.Reason.EVICTED) == 1L
                    && metrics.cancelledTasks() == 2L
                    && byReason == metrics.cancelledTasks();
            });
        } finally {
            release.countDown();
            compute.shutdown();
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        }
    }
}
