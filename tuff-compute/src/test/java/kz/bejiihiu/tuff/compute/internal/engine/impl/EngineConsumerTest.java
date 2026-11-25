package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
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
 * Consumer registry wiring: register, update, unregister and the resulting cancellations.
 */
@Timeout(60)
class EngineConsumerTest {

    @Test
    void registrationIsIdempotentAndVisible() {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(2).dynamicRegistration(false).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final ComputeConsumer registered = compute.registerConsumer(ComputeConsumer.of("plugin", ConsumerPolicy.absolute(1)));
            assertEquals("plugin", registered.id());
            compute.registerConsumer(ComputeConsumer.of("plugin", ConsumerPolicy.absolute(2)));
            assertEquals(2, compute.consumer("plugin").orElseThrow().policy().resolveConcurrencyLimit(4));
            assertTrue(compute.consumerIds().contains("plugin"));
            assertFalse(compute.consumer("missing").isPresent());
            assertThrows(IllegalArgumentException.class, () ->
                compute.updateConsumerPolicy("missing", ConsumerPolicy.defaults()));
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void unregisteringCancelsQueuedWorkAndForgetsTheConsumer() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).dynamicRegistration(false).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            compute.registerConsumer(ComputeConsumer.of("doomed", ConsumerPolicy.defaults()));
            final CompletableFuture<String> blocker = compute.submit("doomed", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final CompletableFuture<String> queuedOne = compute.submit("doomed", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "one");
            final CompletableFuture<String> queuedTwo = compute.submit("doomed", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "two");

            assertTrue(compute.unregisterConsumer("doomed"));
            final ComputeCancelledException cancelledOne = cancellationOf(queuedOne);
            assertEquals(ComputeCancelledException.Reason.CONSUMER_DISABLED, cancelledOne.reason());
            assertEquals(
                ComputeCancelledException.Reason.CONSUMER_DISABLED,
                cancellationOf(queuedTwo).reason()
            );

            release.countDown();
            assertEquals("blocked", blocker.get(10L, TimeUnit.SECONDS));
            assertFalse(compute.unregisterConsumer("doomed"));

            final ComputeRejectedException unknown = assertThrows(
                ComputeRejectedException.class,
                () -> compute.submit("doomed", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "late")
            );
            assertEquals(ComputeRejectedException.Reason.CONSUMER_UNKNOWN, unknown.reason());
            assertFalse(compute.consumer("doomed").isPresent());
            assertTrue(compute.metrics().consumers().containsKey("doomed"), "the last snapshot is kept");
            assertFalse(compute.metrics().consumer("doomed").orElseThrow().enabled());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void unregisteringLetsRunningWorkFinishAndCancelsOnlyQueuedWork() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).dynamicRegistration(false).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            compute.registerConsumer(ComputeConsumer.of("running", ConsumerPolicy.defaults()));
            final CompletableFuture<String> handle = compute.submit("running", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                running.countDown();
                release.await();
                return "late result";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final CompletableFuture<String> queued = compute.submit(
                "running",
                ComputeTaskType.GENERAL,
                ComputePriority.NORMAL,
                () -> "queued"
            );

            assertTrue(compute.unregisterConsumer("running"));

            final ComputeCancelledException cancelled = cancellationOf(queued);
            assertEquals(ComputeCancelledException.Reason.CONSUMER_DISABLED, cancelled.reason());
            final ComputeRejectedException unknown = assertThrows(
                ComputeRejectedException.class,
                () -> compute.submit("running", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "late")
            );
            assertEquals(ComputeRejectedException.Reason.CONSUMER_UNKNOWN, unknown.reason());

            release.countDown();
            assertEquals("late result", handle.get(10L, TimeUnit.SECONDS));
            assertTrue(handle.isDone());
            assertFalse(handle.isCancelled());
            assertEquals(1L, compute.metrics().completedTasks());
            assertEquals(1L, compute.metrics().cancelledTasks());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void reRegistrationAfterUnregisterStartsFresh() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(2).dynamicRegistration(false).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            compute.registerConsumer(ComputeConsumer.of("cycled", ConsumerPolicy.defaults()));
            assertTrue(compute.unregisterConsumer("cycled"));
            compute.registerConsumer(ComputeConsumer.of(
                "cycled",
                Set.of(ComputeTaskType.GENERAL),
                ComputePriority.NORMAL,
                ConsumerPolicy.defaults()
            ));
            assertEquals("back", compute.submit("cycled", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "back")
                .get(10L, TimeUnit.SECONDS));
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void configuredOverridesAreAppliedOnStart() {
        final AsyncComputeScheduler compute = startedScheduler(
            settings()
                .fixedWorkers(2)
                .overrides(Map.of("configured", ConsumerPolicy.absolute(1)))
                .build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            assertTrue(compute.consumer("configured").isPresent());
            assertEquals(1, compute.consumer("configured").orElseThrow().policy().resolveConcurrencyLimit(4));
            assertTrue(compute.consumer(AsyncComputeScheduler.DEFAULT_CONSUMER_ID).isPresent());
        } finally {
            compute.shutdown();
        }
    }
}
