package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.cancellationOf;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.engine;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Queue expiry and drop-oldest eviction, both driven by the real components.
 */
@Timeout(60)
class EngineExpiryEvictionTest {

    @Test
    void expiredQueuedTaskIsCancelledByTheSweepAndNeverRuns() throws Exception {
        final EngineTestSupport.FakeClock clock = new EngineTestSupport.FakeClock();
        final ComputeEngineImpl engine = engine(settings().fixedWorkers(1).build(), clock);
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean expiredRan = new AtomicBoolean();
        compute.start();
        try {
            compute.registerConsumer(ComputeConsumer.of("expiring", expiringPolicy(millis(100L))));
            final CompletableFuture<String> blocker = compute.submit("expiring", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final CompletableFuture<String> expired = compute.submit("expiring", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                expiredRan.set(true);
                return "expired";
            });

            clock.advance(millis(200L));
            engine.sweepNow();

            final ComputeCancelledException cancelled = cancellationOf(expired);
            assertEquals(ComputeCancelledException.Reason.QUEUE_EXPIRED, cancelled.reason());
            assertEquals(1L, compute.metrics().expiredTasks());
            release.countDown();
            assertEquals("blocked", blocker.get(10L, TimeUnit.SECONDS));
            assertFalse(expiredRan.get());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void janitorThreadExpiresQueuedWorkOnItsOwn() throws Exception {
        final ComputeSettings configuration = settings()
            .fixedWorkers(1)
            .metrics(new ComputeSettings.Metrics(true, false, Duration.ofSeconds(10), false, millis(20L)))
            .build();
        final ComputeEngineImpl engine = new ComputeEngineImpl(configuration, ComputeClock.system(), EngineTestSupport.PROCESSORS);
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        compute.start();
        try {
            compute.registerConsumer(ComputeConsumer.of("expiring", expiringPolicy(millis(50L))));
            final CompletableFuture<String> blocker = compute.submit("expiring", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final CompletableFuture<String> expired = compute.submit(
                "expiring",
                ComputeTaskType.GENERAL,
                ComputePriority.NORMAL,
                () -> "expired"
            );
            assertEventually("janitor cancels the expired entry", Duration.ofSeconds(5), expired::isDone);
            final ComputeCancelledException cancelled = cancellationOf(expired);
            assertEquals(ComputeCancelledException.Reason.QUEUE_EXPIRED, cancelled.reason());
            release.countDown();
            blocker.get(10L, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void dropOldestEvictsTheOldestQueuedTaskOfTheSameConsumer() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean victimRan = new AtomicBoolean();
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).capacity(100).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            compute.registerConsumer(ComputeConsumer.of("dropper", dropOldestPolicy()));
            final CompletableFuture<String> blocker = compute.submit("dropper", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final ComputeRequest<String> firstRequest = new ComputeRequest<>(
                "dropper",
                ComputeTaskType.GENERAL,
                ComputePriority.NORMAL,
                () -> {
                    victimRan.set(true);
                    return "victim";
                }
            );
            final CompletableFuture<String> victim = compute.enqueue(firstRequest).future();
            final CompletableFuture<String> survivor = compute.submit(
                "dropper",
                ComputeTaskType.GENERAL,
                ComputePriority.NORMAL,
                () -> "survivor"
            );

            final ComputeCancelledException cancelled = cancellationOf(victim);
            assertEquals(ComputeCancelledException.Reason.EVICTED, cancelled.reason());
            assertTrue(victim.isCancelled());

            release.countDown();
            assertEquals("blocked", blocker.get(10L, TimeUnit.SECONDS));
            assertEquals("survivor", survivor.get(10L, TimeUnit.SECONDS));
            assertFalse(victimRan.get());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    private static ConsumerPolicy expiringPolicy(final Duration maxQueueWait) {
        return new ConsumerPolicy(true, ConsumerPolicy.defaults().concurrencyLimit(), -1, 1.0d, OverflowPolicy.REJECT, maxQueueWait);
    }

    private static ConsumerPolicy dropOldestPolicy() {
        return new ConsumerPolicy(true, ConsumerPolicy.defaults().concurrencyLimit(), 1, 1.0d, OverflowPolicy.DROP_OLDEST, Duration.ZERO);
    }
}
