package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler.SchedulerState;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.engine;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Submission validation, ordering and rejection reasons.
 */
@Timeout(60)
class EngineSubmissionTest {

    @Test
    void basicSubmissionCompletesThroughTheRealFuture() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(2).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final CompletableFuture<Integer> future = compute.submit(ComputePriority.HIGH, () -> 20 + 22);
            final CompletableFuture<String> chained = future.thenApply(value -> "value=" + value);
            assertEquals("value=42", chained.get(10L, TimeUnit.SECONDS));
            assertTrue(future.isDone());
            compute.shutdown(Duration.ofSeconds(1));
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
            assertEquals(SchedulerState.TERMINATED, compute.state());
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void higherPriorityRunsFirstAndEqualPriorityKeepsFifo() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final ConcurrentLinkedQueue<String> order = new ConcurrentLinkedQueue<>();
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final CompletableFuture<String> blocker = compute.submit(() -> {
                running.countDown();
                release.await();
                order.add("blocker");
                return "blocker";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));

            final CompletableFuture<String> low = compute.submit(ComputePriority.LOW, () -> {
                order.add("low");
                return "low";
            });
            final CompletableFuture<String> high = compute.submit(ComputePriority.HIGH, () -> {
                order.add("high");
                return "high";
            });
            final CompletableFuture<String> first = compute.submit(ComputePriority.NORMAL, () -> {
                order.add("normal-a");
                return "normal-a";
            });
            final CompletableFuture<String> second = compute.submit(ComputePriority.NORMAL, () -> {
                order.add("normal-b");
                return "normal-b";
            });
            release.countDown();
            blocker.get(10L, TimeUnit.SECONDS);
            low.get(10L, TimeUnit.SECONDS);
            high.get(10L, TimeUnit.SECONDS);
            first.get(10L, TimeUnit.SECONDS);
            second.get(10L, TimeUnit.SECONDS);
            assertEquals(List.of("blocker", "high", "normal-a", "normal-b", "low"), List.copyOf(order));
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void rejectionReasonsMatchTheContract() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).capacity(2).dynamicRegistration(false).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            compute.registerConsumer(ComputeConsumer.of(
                "strict",
                Set.of(ComputeTaskType.GENERAL),
                ComputePriority.NORMAL,
                ConsumerPolicy.defaults()
            ));
            compute.registerConsumer(ComputeConsumer.of(
                "off",
                new ConsumerPolicy(false, ConsumerPolicy.defaults().concurrencyLimit(), -1, 1.0d, OverflowPolicy.REJECT, Duration.ZERO)
            ));

            final CompletableFuture<String> blocker = compute.submit("strict", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final CompletableFuture<String> queued = compute.submit("strict", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "queued");
            final CompletableFuture<String> filler = compute.submit("strict", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "filler");
            assertEquals(3L, compute.metrics().submittedTasks());

            assertReason(compute, ComputeRejectedException.Reason.CONSUMER_UNKNOWN, () ->
                compute.submit("ghost", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "ghost"));
            assertReason(compute, ComputeRejectedException.Reason.CONSUMER_DISABLED, () ->
                compute.submit("off", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "off"));
            assertReason(compute, ComputeRejectedException.Reason.TASK_TYPE_NOT_ALLOWED, () ->
                compute.submit("strict", ComputeTaskType.PATHFINDING, ComputePriority.NORMAL, () -> "type"));
            assertReason(compute, ComputeRejectedException.Reason.PRIORITY_NOT_ALLOWED, () ->
                compute.submit("strict", ComputeTaskType.GENERAL, ComputePriority.HIGH, () -> "priority"));
            assertReason(compute, ComputeRejectedException.Reason.QUEUE_FULL, () ->
                compute.submit("strict", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "overflow"));
            assertEquals(5L, compute.metrics().rejectedTasks());

            release.countDown();
            assertEquals("blocked", blocker.get(10L, TimeUnit.SECONDS));
            assertEquals("queued", queued.get(10L, TimeUnit.SECONDS));
            assertEquals("filler", filler.get(10L, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void consumerQueueBoundIsReportedSeparately() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final ConsumerPolicy bounded = new ConsumerPolicy(
            true,
            ConsumerPolicy.defaults().concurrencyLimit(),
            1,
            1.0d,
            OverflowPolicy.REJECT,
            Duration.ZERO
        );
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).capacity(100).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            compute.registerConsumer(ComputeConsumer.of("bounded", bounded));
            final CompletableFuture<String> blocker = compute.submit("bounded", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final CompletableFuture<String> queued = compute.submit("bounded", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "queued");
            assertReason(compute, ComputeRejectedException.Reason.CONSUMER_QUEUE_FULL, () ->
                compute.submit("bounded", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "extra"));
            release.countDown();
            blocker.get(10L, TimeUnit.SECONDS);
            queued.get(10L, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void unknownConsumerIsRegisteredOnTheFlyWhenAllowed() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(2).dynamicRegistration(true).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final ComputeTaskHandle<String> handle = compute.enqueue(new ComputeRequest<>(
                "plugin-dynamic",
                ComputeTaskType.CUSTOM,
                ComputePriority.NORMAL,
                () -> "dynamic"
            ));
            assertEquals("dynamic", handle.future().get(10L, TimeUnit.SECONDS));
            assertTrue(compute.consumer("plugin-dynamic").isPresent());
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void settingsAndClockAreExposedAndSurviveTermination() {
        final EngineTestSupport.FakeClock clock = new EngineTestSupport.FakeClock();
        final ComputeSettings configuration = settings().fixedWorkers(2).build();
        final ComputeEngineImpl compute = engine(configuration, clock);
        assertEquals(configuration, compute.settings());
        assertEquals(clock, compute.clock());
        compute.start();
        compute.shutdown(Duration.ofSeconds(1));
        try {
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
        assertEquals(configuration, compute.settings());
    }

    private static void assertReason(
        final AsyncComputeScheduler compute,
        final ComputeRejectedException.Reason expected,
        final Runnable submission
    ) {
        final ComputeRejectedException rejected = assertThrows(ComputeRejectedException.class, submission::run);
        assertEquals(expected, rejected.reason());
    }
}
