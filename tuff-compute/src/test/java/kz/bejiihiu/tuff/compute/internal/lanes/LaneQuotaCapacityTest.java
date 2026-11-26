package kz.bejiihiu.tuff.compute.internal.lanes;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.SettingsBuilder;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.absolute;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.start;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Consumer limits are resolved against the total execution capacity, not just the platform pool.
 *
 * <p>These tests count bodies that are actually executing at the same time through the public
 * scheduler: a relative limit must let the virtual lane run more LOW work than the platform has
 * workers, an absolute limit must stay a hard cross lane bound, and a disabled lane must leave the
 * platform count as the whole capacity.
 */
@Timeout(120)
class LaneQuotaCapacityTest {

    @Test
    void relativeLimitUsesThePlatformAndTheVirtualCapacity() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("capacity-relative")
                .fixedWorkers(2)
                .virtualLane(true, true, 8, millis(50L), 64));
        final LaneTestSupport.TaskProbe probe = new LaneTestSupport.TaskProbe();
        final CountDownLatch eightRunning = new CountDownLatch(8);
        final CountDownLatch release = new CountDownLatch(1);
        final List<CompletableFuture<Integer>> futures = new ArrayList<>();
        try {
            for (int index = 0; index < 8; index++) {
                futures.add(compute.submit(ComputePriority.LOW, () -> {
                    probe.begin();
                    eightRunning.countDown();
                    release.await();
                    probe.end();
                    return 1;
                }));
            }
            // two platform workers alone could never run eight bodies; the quota has to resolve the
            // default relative limit against 2 + 8 lane permits for this to pass
            assertTrue(eightRunning.await(10L, TimeUnit.SECONDS),
                "only " + probe.peak() + " LOW bodies ran, the capacity is still the platform pool");
            assertEquals(8, probe.active());
            assertTrue(probe.sawVirtual(), "the extra bodies did not run on the virtual lane");

            release.countDown();
            for (final CompletableFuture<Integer> future : futures) {
                assertEquals(1, future.get(10L, TimeUnit.SECONDS));
            }
            assertEquals(8, probe.peak());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void absoluteLimitStaysHardAcrossBothLanes() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("capacity-absolute")
                .fixedWorkers(2)
                .virtualLane(true, true, 8, millis(50L), 64));
        compute.registerConsumer(ComputeConsumer.of("capped", absolute(3)));
        final LaneTestSupport.TaskProbe probe = new LaneTestSupport.TaskProbe();
        final CountDownLatch threeRunning = new CountDownLatch(3);
        final CountDownLatch allStarted = new CountDownLatch(8);
        final CountDownLatch release = new CountDownLatch(1);
        final List<CompletableFuture<Integer>> futures = new ArrayList<>();
        try {
            for (int index = 0; index < 8; index++) {
                futures.add(compute.submit("capped", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                    probe.begin();
                    threeRunning.countDown();
                    allStarted.countDown();
                    release.await();
                    probe.end();
                    return 1;
                }));
            }
            assertTrue(threeRunning.await(10L, TimeUnit.SECONDS));
            assertEventually("the absolute limit resolves to three", millis(500L), () -> probe.active() == 3);
            assertFalse(allStarted.await(300L, TimeUnit.MILLISECONDS),
                "an absolute limit of three admitted more bodies on the virtual lane");

            release.countDown();
            for (final CompletableFuture<Integer> future : futures) {
                assertEquals(1, future.get(10L, TimeUnit.SECONDS));
            }
            assertEquals(3, probe.peak());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void disabledLaneLeavesThePlatformCountAsCapacity() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("capacity-platform")
                .fixedWorkers(2)
                .noVirtualLane());
        final LaneTestSupport.TaskProbe probe = new LaneTestSupport.TaskProbe();
        final CountDownLatch twoRunning = new CountDownLatch(2);
        final CountDownLatch allStarted = new CountDownLatch(8);
        final CountDownLatch release = new CountDownLatch(1);
        final List<CompletableFuture<Integer>> futures = new ArrayList<>();
        try {
            for (int index = 0; index < 8; index++) {
                futures.add(compute.submit(ComputePriority.LOW, () -> {
                    probe.begin();
                    twoRunning.countDown();
                    allStarted.countDown();
                    release.await();
                    probe.end();
                    return 1;
                }));
            }
            assertTrue(twoRunning.await(10L, TimeUnit.SECONDS));
            assertEventually("the capacity is the two platform workers", millis(500L), () -> probe.active() == 2);
            assertFalse(allStarted.await(300L, TimeUnit.MILLISECONDS),
                "LOW work exceeded the platform capacity without a virtual lane");
            assertTrue(probe.sawPlatform(), "LOW work did not fall back to the platform lane");

            release.countDown();
            for (final CompletableFuture<Integer> future : futures) {
                assertEquals(1, future.get(10L, TimeUnit.SECONDS));
            }
            assertEquals(2, probe.peak());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }
}
