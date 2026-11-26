package kz.bejiihiu.tuff.compute.internal.lanes;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.NEVER;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.SettingsBuilder;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertCancelledWith;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.cancellationOf;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.start;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every cancellation path a virtual task can take: caller cancel while queued, caller cancel while
 * running, the future's own cancel, and eviction of the oldest queued entry.
 */
@Timeout(90)
class LaneCancellationTest {

    @Test
    void cancellingAQueuedLowTaskKeepsItOutOfTheLane() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("cancel-queued-low").fixedWorkers(1).virtualLane(true, true, 1, millis(50L), 64));
        final CountDownLatch blockerStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger executed = new AtomicInteger();
        try {
            compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                blockerStarted.countDown();
                release.await();
                return "blocker";
            });
            assertTrue(blockerStarted.await(10L, TimeUnit.SECONDS));

            final ComputeTaskHandle<String> queued = compute.enqueue(new ComputeRequest<>(
                "low",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> {
                    executed.incrementAndGet();
                    return "second";
                }
            ));
            assertTrue(queued.cancel(), "a queued task must accept cancellation");
            assertTrue(queued.isCancelled());
            assertEquals(ComputeCancelledException.Reason.CALLER, cancellationOf(queued.future()).reason());
            assertEquals(0, executed.get());
            assertEquals(1L, compute.metrics().lanes().virtual().submitted(),
                "the cancelled entry was dispatched to the lane");
            assertEventually("the cancelled entry was counted once", NEVER,
                () -> compute.metrics().cancelledTasks() == 1L && compute.metrics().lanes().virtual().cancelled() == 0L);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void cancellingARunningLowTaskDiscardsItsResultWithoutInterruptingIt() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("cancel-running-low").fixedWorkers(1).virtualLane(true, true, 2, millis(50L), 64));
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final LaneTestSupport.TaskProbe probe = new LaneTestSupport.TaskProbe();
        try {
            final ComputeTaskHandle<String> handle = compute.enqueue(new ComputeRequest<>(
                "low",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> {
                    probe.begin();
                    started.countDown();
                    release.await();
                    probe.end();
                    return "late";
                }
            ));
            assertTrue(started.await(10L, TimeUnit.SECONDS));

            assertTrue(handle.cancel(), "a running task must accept a cooperative cancellation");
            assertTrue(handle.isCancellationRequested());
            assertFalse(handle.isCancelled(), "a running task only ends when its body returns");

            release.countDown();
            assertCancelledWith(handle, ComputeCancelledException.Reason.CALLER);
            assertEquals(ComputeTaskHandle.State.CANCELLED, handle.state());
            assertFalse(probe.wasInterrupted(), "cancellation must never interrupt a task body");
            assertEventually("the lane counted exactly one cancellation", NEVER, () -> {
                final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
                return lane.cancelled() == 1L && lane.completed() == 0L;
            });
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void cancellingTheResultFutureRoutesThroughTheHandle() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("cancel-future").fixedWorkers(1).virtualLane(true, true, 1, millis(50L), 64));
        final CountDownLatch blockerStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger executed = new AtomicInteger();
        try {
            compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                blockerStarted.countDown();
                release.await();
                return "blocker";
            });
            assertTrue(blockerStarted.await(10L, TimeUnit.SECONDS));

            final ComputeTaskHandle<String> queued = compute.enqueue(new ComputeRequest<>(
                "low",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> {
                    executed.incrementAndGet();
                    return "second";
                }
            ));
            final CompletableFuture<String> future = queued.future();
            assertTrue(future.cancel(true), "future().cancel(true) must be routed to the handle");
            assertTrue(future.isCancelled());
            assertTrue(queued.isCancelled());
            assertEquals(ComputeCancelledException.Reason.CALLER, cancellationOf(future).reason());
            assertEquals(0, executed.get());
            assertEquals(1L, compute.metrics().lanes().virtual().submitted(),
                "only the running blocker may be in the lane");
            assertEquals(0L, compute.metrics().lanes().virtual().cancelled());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void evictionCancelsTheOldestQueuedLowTask() throws Exception {
        final ConsumerPolicy dropOldest = new ConsumerPolicy(
            true,
            ConsumerPolicy.ConcurrencyLimit.absolute(4),
            -1,
            1.0d,
            OverflowPolicy.DROP_OLDEST,
            Duration.ZERO
        );
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("cancel-evict")
                .fixedWorkers(1)
                .capacity(2)
                .overflow(OverflowPolicy.DROP_OLDEST)
                .virtualLane(true, true, 1, millis(50L), 64));
        compute.registerConsumer(ComputeConsumer.of("evict", dropOldest));
        final CountDownLatch blockerStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            compute.submit("evict", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                blockerStarted.countDown();
                release.await();
                return "blocker";
            });
            assertTrue(blockerStarted.await(10L, TimeUnit.SECONDS));

            final AtomicInteger counter = new AtomicInteger();
            final ComputeTaskHandle<String> oldest = compute.enqueue(new ComputeRequest<>(
                "evict",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> {
                    counter.incrementAndGet();
                    return "oldest";
                }
            ));
            final ComputeTaskHandle<String> second = compute.enqueue(new ComputeRequest<>(
                "evict",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> "second"
            ));

            // the third queued entry does not fit, so drop-oldest hands the slot to it by evicting
            // the oldest queued task of the same consumer
            final CompletableFuture<String> third = compute.submit("evict", ComputeTaskType.GENERAL,
                ComputePriority.LOW, () -> "third");

            assertCancelledWith(oldest, ComputeCancelledException.Reason.EVICTED);
            assertFalse(second.isCancelled());

            release.countDown();
            assertEquals("second", second.future().get(10L, TimeUnit.SECONDS));
            assertEquals("third", third.get(10L, TimeUnit.SECONDS));
            assertEquals(0, counter.get(), "an evicted task must never run");
            assertEventually("the eviction was counted once", NEVER, () -> compute.metrics().cancelledTasks() == 1L);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void cancellingAFinishedTaskChangesNothing() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("cancel-late").fixedWorkers(1).virtualLane(true, true, 2, millis(50L), 64));
        try {
            final ComputeTaskHandle<String> handle = compute.enqueue(new ComputeRequest<>(
                "low",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> "done"
            ));
            assertEquals("done", handle.future().get(10L, TimeUnit.SECONDS));
            assertFalse(handle.cancel(), "a terminal task must not accept a cancel request");
            assertEquals(ComputeTaskHandle.State.COMPLETED, handle.state());
            assertEventually("counters keep the single completion", NEVER, () ->
                compute.metrics().completedTasks() == 1L && compute.metrics().cancelledTasks() == 0L);
            assertEventually("the lane counted one completion", NEVER, () -> {
                final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
                return lane.completed() == 1L && lane.cancelled() == 0L;
            });
        } finally {
            compute.shutdown();
        }
    }
}
