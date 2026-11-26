package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.cancellationOf;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.engine;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The result future of every cancellation path must look cancelled to the JDK: {@code isCancelled()},
 * exceptional completion and a {@link CancellationException} (our {@link ComputeCancelledException})
 * from {@code get()}/{@code join()}.
 */
@Timeout(60)
class EngineCancellationStateTest {

    @Test
    void queuedCancelIsVisibleOnTheFuture() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final CompletableFuture<String> blocker = compute.submit(() -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final ComputeTaskHandle<String> victim = compute.enqueue(ComputeRequest.of(() -> "victim"));
            assertFalse(victim.future().isCancelled());
            assertTrue(victim.cancel());
            assertCancelled(victim, ComputeCancelledException.Reason.CALLER);
            release.countDown();
            blocker.get(10L, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void futureCancelIsVisibleOnTheFuture() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final CompletableFuture<String> blocker = compute.submit(() -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final ComputeTaskHandle<String> victim = compute.enqueue(ComputeRequest.of(() -> "victim"));
            assertTrue(victim.future().cancel(true));
            assertCancelled(victim, ComputeCancelledException.Reason.CALLER);
            release.countDown();
            blocker.get(10L, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void runningCancelIsVisibleAfterTheBodyReturns() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean bodyFinished = new AtomicBoolean();
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final ComputeTaskHandle<String> handle = compute.enqueue(ComputeRequest.of(() -> {
                running.countDown();
                release.await();
                bodyFinished.set(true);
                return "discarded";
            }));
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            assertFalse(handle.future().isCancelled(), "a running future is not terminal yet");
            assertTrue(handle.cancel());
            assertTrue(handle.isCancellationRequested());
            release.countDown();
            assertThrows(CancellationException.class, handle.future()::join);
            assertCancelled(handle, ComputeCancelledException.Reason.CALLER);
            assertTrue(bodyFinished.get());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void expiredTaskIsVisibleAsCancelled() throws Exception {
        final EngineTestSupport.FakeClock clock = new EngineTestSupport.FakeClock();
        // a single platform worker and no lane: a NORMAL entry that waits stays queued and can expire
        final ComputeEngineImpl engine = engine(settings().fixedWorkers(1).noVirtualLane().build(), clock);
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        compute.start();
        try {
            compute.registerConsumer(ComputeConsumer.of("expiring", new ConsumerPolicy(
                true,
                ConsumerPolicy.defaults().concurrencyLimit(),
                -1,
                1.0d,
                OverflowPolicy.REJECT,
                millis(100L)
            )));
            final CompletableFuture<String> blocker = compute.submit("expiring", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final ComputeTaskHandle<String> victim = compute.enqueue(new ComputeRequest<>(
                "expiring",
                ComputeTaskType.GENERAL,
                ComputePriority.NORMAL,
                () -> "victim"
            ));
            clock.advance(millis(200L));
            engine.sweepNow();
            assertCancelled(victim, ComputeCancelledException.Reason.QUEUE_EXPIRED);
            release.countDown();
            blocker.get(10L, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void evictedTaskIsVisibleAsCancelled() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).capacity(100).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            compute.registerConsumer(ComputeConsumer.of("dropper", new ConsumerPolicy(
                true,
                ConsumerPolicy.defaults().concurrencyLimit(),
                1,
                1.0d,
                OverflowPolicy.DROP_OLDEST,
                Duration.ZERO
            )));
            final CompletableFuture<String> blocker = compute.submit("dropper", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final ComputeTaskHandle<String> victim = compute.enqueue(new ComputeRequest<>(
                "dropper",
                ComputeTaskType.GENERAL,
                ComputePriority.NORMAL,
                () -> "victim"
            ));
            compute.submit("dropper", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "survivor");
            assertCancelled(victim, ComputeCancelledException.Reason.EVICTED);
            release.countDown();
            blocker.get(10L, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void shutdownCancelledQueueIsVisibleAsCancelled() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final EngineTestSupport.SettingsBuilder builder = settings().fixedWorkers(1).grace(millis(10L));
        final AsyncComputeScheduler compute = startedScheduler(builder.build(), new EngineTestSupport.FakeClock());
        try {
            final CompletableFuture<String> blocker = compute.submit(() -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final ComputeTaskHandle<String> victim = compute.enqueue(ComputeRequest.of(() -> "victim"));
            compute.shutdown(millis(10L));
            assertCancelled(victim, ComputeCancelledException.Reason.SHUTDOWN);
            release.countDown();
            blocker.get(10L, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    private static void assertCancelled(
        final ComputeTaskHandle<?> handle,
        final ComputeCancelledException.Reason expected
    ) {
        // expiry and eviction may complete the future on the scheduler's completion path, so wait
        // for the completion before asserting on it
        final ComputeCancelledException cancelled = cancellationOf(handle.future());
        assertEquals(expected, cancelled.reason());
        assertTrue(handle.future().isCancelled(), "future must report isCancelled");
        assertTrue(handle.future().isCompletedExceptionally());
        assertEquals(ComputeTaskHandle.State.CANCELLED, handle.state());
        assertTrue(handle.isCancelled());
        assertTrue(handle.isDone());
        assertThrows(CancellationException.class, () -> handle.future().get());
        assertThrows(CancellationException.class, handle.future()::join);
    }
}
