package kz.bejiihiu.tuff.compute.internal.lanes;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.NEVER;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.SettingsBuilder;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.absolute;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertConsumerInvariant;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertLaneInvariant;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertSettled;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.start;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lane metrics are the operational contract of the feature: the lane invariant must hold with
 * non-zero values of every kind, gauges must match reality and queued work must not be counted as
 * dispatched.
 */
@Timeout(120)
class LaneMetricsTest {

    @Test
    void laneInvariantHoldsWithCompletedFailedSpilledAndCancelledWork() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("metrics-mixed")
                .fixedWorkers(1)
                .virtualLane(true, true, 2, Duration.ZERO, 1));
        // one low permit keeps the cancelled task below queued: with a free lane slot the pump could
        // claim it before cancel() runs, and "cancelled while queued" would be a coin flip
        compute.registerConsumer(ComputeConsumer.of("low", absolute(1)));
        compute.registerConsumer(ComputeConsumer.of("normal-block", absolute(1)));
        compute.registerConsumer(ComputeConsumer.of("normal-spill", absolute(1)));
        final CountDownLatch normalStarted = new CountDownLatch(1);
        final CountDownLatch lowStarted = new CountDownLatch(1);
        final CountDownLatch releaseNormal = new CountDownLatch(1);
        final CountDownLatch releaseLow = new CountDownLatch(1);
        final LaneTestSupport.TaskProbe spillProbe = new LaneTestSupport.TaskProbe();
        final LaneTestSupport.TaskProbe lowProbe = new LaneTestSupport.TaskProbe();
        try {
            // occupies the only platform worker and saturates the pool
            compute.submit("normal-block", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                normalStarted.countDown();
                releaseNormal.await();
                return "blocker";
            });
            assertTrue(normalStarted.await(10L, TimeUnit.SECONDS));
            // occupies one of the two virtual slots
            compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                lowProbe.begin();
                lowStarted.countDown();
                releaseLow.await();
                lowProbe.end();
                return "low-blocker";
            });
            assertTrue(lowStarted.await(10L, TimeUnit.SECONDS));

            final ComputeTaskHandle<String> cancelledQueued = compute.enqueue(new ComputeRequest<>(
                "low",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> "never"
            ));
            assertTrue(cancelledQueued.cancel());

            final CompletableFuture<String> spilled = compute.submit("normal-spill", ComputeTaskType.GENERAL,
                ComputePriority.NORMAL, () -> {
                    spillProbe.begin();
                    spillProbe.end();
                    return "spilled";
                });
            assertEquals("spilled", spilled.get(10L, TimeUnit.SECONDS),
                "a saturated pool did not spill NORMAL work while the low slot was taken");
            assertTrue(spillProbe.sawVirtual());

            final ComputeTaskHandle<String> failing = compute.enqueue(new ComputeRequest<>(
                "low",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> {
                    throw new IllegalStateException("planned failure");
                }
            ));
            releaseLow.countDown();
            final ExecutionException failure = assertThrows(ExecutionException.class,
                () -> failing.future().get(10L, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof IllegalStateException);

            releaseNormal.countDown();
            assertEventually("the mixed batch settles", NEVER, () -> {
                final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
                return lane.submitted() == 3L && lane.running() == 0 && lane.completed() == 2L
                    && lane.failed() == 1L;
            });

            final SchedulerMetrics metrics = compute.metrics();
            final SchedulerMetrics.VirtualLane lane = metrics.lanes().virtual();
            assertEquals(2, lane.limit());
            assertEquals(1L, lane.spilled(), "only the spilled NORMAL task counts as spilled");
            assertEquals(0L, lane.cancelled(), "a task cancelled while queued never entered the lane");
            assertLaneInvariant(metrics);
            assertConsumerInvariant(metrics);

            final SchedulerMetrics settled = assertSettled(compute);
            assertEquals(5L, settled.submittedTasks());
            assertEquals(3L, settled.completedTasks());
            assertEquals(1L, settled.failedTasks());
            assertEquals(1L, settled.cancelledTasks());
            assertEquals(0L, settled.rejectedTasks());
        } finally {
            releaseLow.countDown();
            releaseNormal.countDown();
            compute.shutdown();
        }
    }

    @Test
    void runningLimitAndQueuedGaugesMatchTheLaneReality() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("metrics-gauges").fixedWorkers(4).virtualLane(true, true, 3, millis(50L), 64));
        final CountDownLatch threeStarted = new CountDownLatch(3);
        final CountDownLatch release = new CountDownLatch(1);
        final LaneTestSupport.TaskProbe probe = new LaneTestSupport.TaskProbe();
        final List<CompletableFuture<String>> futures = new ArrayList<>();
        try {
            assertEventually("the platform pool is fully started", NEVER, () -> compute.metrics().totalWorkers() == 4);
            for (int i = 0; i < 5; i++) {
                futures.add(compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                    probe.begin();
                    threeStarted.countDown();
                    release.await();
                    probe.end();
                    return "low";
                }));
            }
            assertTrue(threeStarted.await(10L, TimeUnit.SECONDS));

            assertEventually("gauges expose the saturated lane", NEVER, () -> {
                final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
                return lane.running() == 3 && lane.limit() == 3 && lane.queued() == 2;
            });
            assertEquals(3, probe.active());
            final SchedulerMetrics busy = compute.metrics();
            assertEquals(0, busy.activeWorkers(), "virtual tasks are not platform workers");
            assertEquals(4, busy.totalWorkers(), "the platform pool size changed");
            assertLaneInvariant(busy);

            release.countDown();
            for (final CompletableFuture<String> future : futures) {
                assertEquals("low", future.get(10L, TimeUnit.SECONDS));
            }
            assertEventually("gauges drain after completion", NEVER, () -> {
                final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
                return lane.running() == 0 && lane.queued() == 0 && lane.completed() == 5L;
            });
            assertEquals(3, probe.peak());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void queuedWorkIsNotPartOfTheLaneInvariant() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("metrics-queued").fixedWorkers(8).virtualLane(true, true, 2, millis(50L), 64));
        final CountDownLatch twoStarted = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        final List<CompletableFuture<String>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < 4; i++) {
                futures.add(compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                    twoStarted.countDown();
                    release.await();
                    return "low";
                }));
            }
            assertTrue(twoStarted.await(10L, TimeUnit.SECONDS));
            assertEventually("the lane has a backlog", NEVER, () -> compute.metrics().lanes().virtual().queued() == 2);

            int queuedSamples = 0;
            for (int i = 0; i < 25; i++) {
                final SchedulerMetrics metrics = compute.metrics();
                assertLaneInvariant(metrics);
                assertEquals(2, metrics.lanes().virtual().running());
                if (metrics.lanes().virtual().queued() == 2) {
                    queuedSamples++;
                }
                assertConsumerInvariant(metrics);
                LaneTestSupport.sleep(2L);
            }
            assertTrue(queuedSamples >= 20, "queued gauge was unstable while work was blocked");
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void spilledCountsOnlyNormalTasksThatRanOnTheVirtualLane() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("metrics-spilled")
                .fixedWorkers(1)
                .virtualLane(true, true, 4, Duration.ZERO, 1));
        compute.registerConsumer(ComputeConsumer.of("occupy", absolute(1)));
        compute.registerConsumer(ComputeConsumer.of("normal", absolute(2)));
        compute.registerConsumer(ComputeConsumer.of("low", absolute(2)));
        final CountDownLatch occupierStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            compute.submit("occupy", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                occupierStarted.countDown();
                release.await();
                return "occupier";
            });
            assertTrue(occupierStarted.await(10L, TimeUnit.SECONDS));

            final List<CompletableFuture<String>> normal = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                normal.add(compute.submit("normal", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "normal"));
            }
            for (final CompletableFuture<String> future : normal) {
                assertEquals("normal", future.get(10L, TimeUnit.SECONDS));
            }

            final List<CompletableFuture<String>> low = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                low.add(compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> "low"));
            }
            for (final CompletableFuture<String> future : low) {
                assertEquals("low", future.get(10L, TimeUnit.SECONDS));
            }

            assertEventually("the lane counted every task", NEVER, () -> {
                final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
                return lane.submitted() == 5L && lane.completed() == 5L;
            });
            final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
            assertEquals(3L, lane.spilled(), "spilled must count only NORMAL work");
            assertEquals(2L, lane.submitted() - lane.spilled(), "LOW work was counted as spilled");
            assertLaneInvariant(compute.metrics());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }
}
