package kz.bejiihiu.tuff.compute.internal.lanes;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.NEVER;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.SettingsBuilder;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.absolute;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.start;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.weighted;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Ordering inside a lane: FIFO for equal work, aging that lifts a stuck LOW over newer NORMAL work
 * and weighted fairness between consumers.
 */
@Timeout(120)
class LaneOrderingTest {

    @Test
    void lowTasksStartInSubmissionOrderOnOneVirtualSlot() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("lane-fifo").fixedWorkers(2).virtualLane(true, true, 1, millis(50L), 64));
        final int total = 6;
        final CountDownLatch[] releases = new CountDownLatch[total];
        for (int i = 0; i < total; i++) {
            releases[i] = new CountDownLatch(1);
        }
        final BlockingQueue<Integer> startedOrder = new LinkedBlockingQueue<>();
        final List<CompletableFuture<Integer>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < total; i++) {
                final int index = i;
                futures.add(compute.submit("fifo", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                    startedOrder.add(index);
                    releases[index].await();
                    return index;
                }));
            }
            for (int expected = 0; expected < total; expected++) {
                final Integer started = startedOrder.poll(10L, TimeUnit.SECONDS);
                assertNotNull(started, "LOW task " + expected + " never started");
                assertEquals(expected, started.intValue(), "LOW work did not start in submission order");
                releases[expected].countDown();
            }
            for (final CompletableFuture<Integer> future : futures) {
                assertNotNull(future.get(10L, TimeUnit.SECONDS));
            }
        } finally {
            for (final CountDownLatch release : releases) {
                release.countDown();
            }
            compute.shutdown();
        }
    }

    @Test
    void agingLiftsAStuckLowAboveNewerNormalWork() throws Exception {
        final LaneTestSupport.FakeClock clock = new LaneTestSupport.FakeClock();
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("lane-aging")
                .fixedWorkers(1)
                .virtualLane(true, true, 1, Duration.ofDays(1L), 1),
            clock);
        compute.registerConsumer(ComputeConsumer.of("lane", absolute(2)));
        compute.registerConsumer(ComputeConsumer.of("spill", absolute(2)));
        compute.registerConsumer(ComputeConsumer.of("occupy", absolute(1)));

        final CountDownLatch platformStarted = new CountDownLatch(1);
        final CountDownLatch releasePlatform = new CountDownLatch(1);
        final CountDownLatch laneStarted = new CountDownLatch(1);
        final CountDownLatch releaseLane = new CountDownLatch(1);
        final BlockingQueue<Integer> startedOrder = new LinkedBlockingQueue<>();
        final List<CompletableFuture<Integer>> futures = new ArrayList<>();
        try {
            futures.add(compute.submit("occupy", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                platformStarted.countDown();
                releasePlatform.await();
                return -1;
            }));
            assertTrue(platformStarted.await(10L, TimeUnit.SECONDS));

            futures.add(compute.submit("lane", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                laneStarted.countDown();
                releaseLane.await();
                return 0;
            }));
            assertTrue(laneStarted.await(10L, TimeUnit.SECONDS));

            // the stuck LOW is submitted first, then newer NORMAL work opens the spill path; the
            // virtual slot is still occupied, so nothing can be claimed before the clock moves
            futures.add(compute.submit("lane", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                startedOrder.add(1);
                return 1;
            }));
            for (int i = 2; i <= 3; i++) {
                final int index = i;
                futures.add(compute.submit("spill", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                    startedOrder.add(index);
                    return index;
                }));
            }
            clock.advance(Duration.ofSeconds(5L));

            releaseLane.countDown();
            final Integer first = startedOrder.poll(10L, TimeUnit.SECONDS);
            assertNotNull(first, "nothing ran after the lane slot was released");
            // aging gives the LOW entry three levels, so it outranks a fresh NORMAL even though
            // NORMAL is normally preferred on the shared virtual lane
            assertEquals(1, first.intValue(), "an old LOW task was overtaken by newer NORMAL work");
            assertEventually("NORMAL work still spills behind the aged LOW", NEVER,
                () -> compute.metrics().lanes().virtual().spilled() >= 1L);

            releasePlatform.countDown();
            for (final CompletableFuture<Integer> future : futures) {
                assertNotNull(future.get(10L, TimeUnit.SECONDS));
            }
        } finally {
            releaseLane.countDown();
            releasePlatform.countDown();
            compute.shutdown();
        }
    }

    @Test
    void consumerSharesFollowTheirFairShareWeights() throws Exception {
        final AsyncComputeScheduler compute = start(SettingsBuilder.of("lane-weights").fixedWorkers(1));
        compute.registerConsumer(ComputeConsumer.of("light", weighted(1.0d)));
        compute.registerConsumer(ComputeConsumer.of("heavy", weighted(3.0d)));
        final int perConsumer = 40;
        // one worker plus a released-one-at-a-time gate: both queues stay full for the whole
        // measurement window, so every start is a real fairness decision
        final Semaphore gate = new Semaphore(0);
        final BlockingQueue<String> starts = new LinkedBlockingQueue<>();
        final List<CompletableFuture<String>> futures = new ArrayList<>(perConsumer * 2);
        try {
            for (int i = 0; i < perConsumer; i++) {
                futures.add(compute.submit("light", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                    starts.add("light");
                    gate.acquire();
                    return "light";
                }));
                futures.add(compute.submit("heavy", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                    starts.add("heavy");
                    gate.acquire();
                    return "heavy";
                }));
            }

            int window = 40;
            int heavyStarts = 0;
            int lightStarts = 0;
            for (int i = 0; i < window; i++) {
                final String started = starts.poll(10L, TimeUnit.SECONDS);
                assertNotNull(started, "start " + i + " never happened");
                if ("heavy".equals(started)) {
                    heavyStarts++;
                } else {
                    lightStarts++;
                }
                gate.release();
            }
            // a weight of three must get about three turns for every turn of a weight of one; a
            // queue that ignored the weight would split these forty starts evenly
            assertEquals(window, heavyStarts + lightStarts);
            assertTrue(heavyStarts >= 24 && heavyStarts <= 36,
                "weighted share is not roughly three to one: heavy=" + heavyStarts + " light=" + lightStarts
                    + " in the first " + window + " starts");
            assertTrue(lightStarts > 0, "the light consumer was starved");

            gate.release(perConsumer * 2);
            for (final CompletableFuture<String> future : futures) {
                assertNotNull(future.get(20L, TimeUnit.SECONDS));
            }
            final ConsumerMetrics light = compute.metrics().consumer("light").orElseThrow();
            final ConsumerMetrics heavy = compute.metrics().consumer("heavy").orElseThrow();
            assertEquals(perConsumer, light.completedTasks());
            assertEquals(perConsumer, heavy.completedTasks());
        } finally {
            gate.release(perConsumer * 2);
            compute.shutdown();
        }
    }
}
