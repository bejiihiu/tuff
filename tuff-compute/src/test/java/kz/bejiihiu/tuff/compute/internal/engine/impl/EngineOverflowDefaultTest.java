package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.cancellationOf;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code queue.overflowPolicy} is the default for consumers the scheduler registers itself, and an
 * explicit consumer policy or override wins over it.
 */
@Timeout(60)
class EngineOverflowDefaultTest {

    @Test
    void dynamicConsumerInheritsDropOldestFromTheQueueSettings() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AsyncComputeScheduler compute = startedScheduler(
            settings()
                .fixedWorkers(1)
                .capacity(2).noVirtualLane()
                .overflow(OverflowPolicy.DROP_OLDEST)
                .defaultPolicy(ConsumerPolicy.defaults())
                .dynamicRegistration(true)
                .build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final CompletableFuture<String> blocker = compute.submit(() -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final CompletableFuture<String> victim = compute.submit("dyn", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "victim");
            final CompletableFuture<String> second = compute.submit("dyn", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "second");
            assertEquals(OverflowPolicy.DROP_OLDEST, compute.consumer("dyn").orElseThrow().policy().overflowPolicy());

            final CompletableFuture<String> survivor = compute.submit("dyn", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "survivor");
            final ComputeCancelledException evicted = cancellationOf(victim);
            assertEquals(ComputeCancelledException.Reason.EVICTED, evicted.reason());
            assertTrue(victim.isCancelled());

            release.countDown();
            blocker.get(10L, TimeUnit.SECONDS);
            assertEquals("second", second.get(10L, TimeUnit.SECONDS));
            assertEquals("survivor", survivor.get(10L, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void dynamicConsumerInheritsRejectFromTheQueueSettings() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AsyncComputeScheduler compute = startedScheduler(
            settings()
                .fixedWorkers(1)
                .capacity(2).noVirtualLane()
                .overflow(OverflowPolicy.REJECT)
                .defaultPolicy(dropOldestPolicy())
                .dynamicRegistration(true)
                .build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final CompletableFuture<String> blocker = compute.submit(() -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final CompletableFuture<String> first = compute.submit("dyn", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "first");
            compute.submit("dyn", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "second");
            assertEquals(OverflowPolicy.REJECT, compute.consumer("dyn").orElseThrow().policy().overflowPolicy());

            final ComputeRejectedException rejected = assertThrows(
                ComputeRejectedException.class,
                () -> compute.submit("dyn", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "overflow")
            );
            assertEquals(ComputeRejectedException.Reason.QUEUE_FULL, rejected.reason());
            assertFalse(first.isCancelled(), "REJECT must not evict queued work");

            release.countDown();
            blocker.get(10L, TimeUnit.SECONDS);
            assertEquals("first", first.get(10L, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void defaultConsumerInheritsTheQueueOverflowPolicy() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AsyncComputeScheduler compute = startedScheduler(
            settings()
                .fixedWorkers(1)
                .capacity(2).noVirtualLane()
                .overflow(OverflowPolicy.DROP_OLDEST)
                .build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            assertEquals(OverflowPolicy.DROP_OLDEST,
                compute.consumer(AsyncComputeScheduler.DEFAULT_CONSUMER_ID).orElseThrow().policy().overflowPolicy());
            final CompletableFuture<String> blocker = compute.submit(() -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final ComputeTaskHandle<String> victim = compute.enqueue(ComputeRequest.of(() -> "victim"));
            compute.submit(() -> "second");
            compute.submit(() -> "third");
            final ComputeCancelledException evicted = cancellationOf(victim.future());
            assertEquals(ComputeCancelledException.Reason.EVICTED, evicted.reason());
            release.countDown();
            blocker.get(10L, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void explicitConsumerPolicyWinsOverTheQueueDefault() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AsyncComputeScheduler compute = startedScheduler(
            settings()
                .fixedWorkers(1)
                .capacity(2).noVirtualLane()
                .overflow(OverflowPolicy.DROP_OLDEST)
                .dynamicRegistration(false)
                .build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            compute.registerConsumer(ComputeConsumer.of("explicit", ConsumerPolicy.defaults()));
            final CompletableFuture<String> blocker = compute.submit(() -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            compute.submit("explicit", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "first");
            compute.submit("explicit", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "second");
            final ComputeRejectedException rejected = assertThrows(
                ComputeRejectedException.class,
                () -> compute.submit("explicit", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "third")
            );
            assertEquals(ComputeRejectedException.Reason.QUEUE_FULL, rejected.reason());
            release.countDown();
            blocker.get(10L, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    private static ConsumerPolicy dropOldestPolicy() {
        return new ConsumerPolicy(
            true,
            ConsumerPolicy.defaults().concurrencyLimit(),
            -1,
            1.0d,
            OverflowPolicy.DROP_OLDEST,
            Duration.ZERO
        );
    }
}
