package kz.bejiihiu.tuff.compute.internal.lanes;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.NEVER;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.SettingsBuilder;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.absolute;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.start;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The virtual lane must stay invisible to the platform sizing machinery: it never grows the pool,
 * never blocks a budget change and never counts as adaptive queue depth.
 */
@Timeout(150)
class LaneAdaptiveBudgetTest {

    @Test
    void virtualBacklogNeverGrowsThePlatformPool() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("vt-no-grow")
                .autoWorkers(1, 4, Duration.ofSeconds(30L))
                .processorBudget(1)
                .capacity(1_024)
                .virtualLane(true, true, 2, millis(50L), 64));
        compute.registerConsumer(ComputeConsumer.of("low-a", absolute(2)));
        compute.registerConsumer(ComputeConsumer.of("low-b", absolute(2)));
        final CountDownLatch twoStarted = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        final List<CompletableFuture<String>> futures = new ArrayList<>();
        try {
            assertEventually("the pool starts at its budget", NEVER, () -> {
                final SchedulerMetrics metrics = compute.metrics();
                return metrics.totalWorkers() == 1 && metrics.concurrencyLimit() == 1;
            });
            for (final String consumer : List.of("low-a", "low-b")) {
                for (int i = 0; i < 10; i++) {
                    futures.add(compute.submit(consumer, ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                        twoStarted.countDown();
                        release.await();
                        return "low";
                    }));
                }
            }
            assertTrue(twoStarted.await(10L, TimeUnit.SECONDS));

            // a deep virtual backlog is not platform backlog: no new worker may appear
            for (int i = 0; i < 50; i++) {
                final SchedulerMetrics metrics = compute.metrics();
                assertEquals(1, metrics.totalWorkers(), "virtual backlog grew the platform pool");
                assertEquals(1, metrics.concurrencyLimit(), "virtual backlog changed the concurrency limit");
                assertEquals(1, metrics.peakWorkers(), "the platform pool peaked above its budget");
                LaneTestSupport.sleep(5L);
            }
            assertEquals(2, compute.metrics().lanes().virtual().running());
            assertEquals(18, compute.metrics().lanes().virtual().queued());

            release.countDown();
            for (final CompletableFuture<String> future : futures) {
                assertEquals("low", future.get(20L, TimeUnit.SECONDS));
            }
            assertEquals(1, compute.metrics().peakWorkers(), "the pool grew after the virtual batch");
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void budgetChangesStillApplyWhileVirtualWorkIsRunning() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("vt-budget")
                .autoWorkers(1, -1, Duration.ofSeconds(30L))
                .processorBudget(4)
                .capacity(1_024)
                .virtualLane(true, false, 2, millis(50L), 64));
        compute.registerConsumer(ComputeConsumer.of("low-a", absolute(2)));
        compute.registerConsumer(ComputeConsumer.of("low-b", absolute(2)));
        final CountDownLatch twoVirtual = new CountDownLatch(2);
        final CountDownLatch releaseVirtual = new CountDownLatch(1);
        final CountDownLatch platformStarted = new CountDownLatch(3);
        final CountDownLatch releasePlatform = new CountDownLatch(1);
        final List<CompletableFuture<String>> futures = new ArrayList<>();
        try {
            // virtual load that must not be touched by anything the budget does to the pool
            for (final String consumer : List.of("low-a", "low-b")) {
                for (int i = 0; i < 3; i++) {
                    futures.add(compute.submit(consumer, ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                        twoVirtual.countDown();
                        releaseVirtual.await();
                        return "low";
                    }));
                }
            }
            assertTrue(twoVirtual.await(10L, TimeUnit.SECONDS));

            // platform demand gives the autoscaler something to do, so a budget change is visible
            for (int i = 0; i < 10; i++) {
                futures.add(compute.submit("platform", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                    platformStarted.countDown();
                    releasePlatform.await();
                    return "platform";
                }));
            }
            final boolean fourStarted = platformStarted.await(10L, TimeUnit.SECONDS);
            assertTrue(fourStarted);
            assertEventually("the pool grows to the initial budget ceiling", NEVER,
                () -> compute.metrics().concurrencyLimit() >= 2);

            compute.applyProcessorBudget(1);
            assertEventually("the budget cut applies under virtual load", NEVER,
                () -> compute.metrics().concurrencyLimit() == 1);
            assertEquals(2, compute.metrics().lanes().virtual().running(),
                "a budget cut disturbed the virtual lane");

            compute.applyProcessorBudget(4);
            assertEventually("the budget raise applies under virtual load", NEVER,
                () -> compute.metrics().concurrencyLimit() >= 3);
            assertEquals(2, compute.metrics().lanes().virtual().running(),
                "a budget raise disturbed the virtual lane");

            releasePlatform.countDown();
            releaseVirtual.countDown();
            for (final CompletableFuture<String> future : futures) {
                assertNotNull(future.get(20L, TimeUnit.SECONDS));
            }
        } finally {
            releaseVirtual.countDown();
            releasePlatform.countDown();
            compute.shutdown();
        }
    }

    @Test
    void adaptiveShrinksAnIdlePlatformPoolDespiteVirtualBacklog() throws Exception {
        final LaneTestSupport.FakeClock clock = new LaneTestSupport.FakeClock();
        final ComputeSettings.Adaptive adaptive = new ComputeSettings.Adaptive(
            true,
            1,
            -1,
            Duration.ofSeconds(1L),
            5,
            0.1d,
            0.5d,
            0.9d
        );
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("vt-adaptive")
                .autoWorkers(1, 4, Duration.ofSeconds(30L))
                .processorBudget(4)
                .capacity(1_024)
                .adaptive(adaptive)
                .virtualLane(true, true, 2, millis(50L), 64),
            clock);
        compute.registerConsumer(ComputeConsumer.of("low-a", absolute(2)));
        compute.registerConsumer(ComputeConsumer.of("low-b", absolute(2)));
        final CountDownLatch twoStarted = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        final List<CompletableFuture<String>> futures = new ArrayList<>();
        try {
            assertEventually("the platform pool starts at the budget", NEVER,
                () -> compute.metrics().concurrencyLimit() >= 2);
            final int initialLimit = compute.metrics().concurrencyLimit();
            for (final String consumer : List.of("low-a", "low-b")) {
                for (int i = 0; i < 6; i++) {
                    futures.add(compute.submit(consumer, ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                        twoStarted.countDown();
                        release.await();
                        return "low";
                    }));
                }
            }
            assertTrue(twoStarted.await(10L, TimeUnit.SECONDS));

            // no platform task ever finishes, so every adaptive window is idle. The virtual backlog
            // must not be mistaken for platform backlog: a counted VT depth would keep the window
            // busy and the pool would never shrink.
            final long deadline = System.nanoTime() + Duration.ofSeconds(20L).toNanos();
            int observed = initialLimit;
            while (System.nanoTime() < deadline) {
                clock.advance(Duration.ofSeconds(1L));
                LaneTestSupport.sleep(200L);
                observed = compute.metrics().concurrencyLimit();
                if (observed < initialLimit) {
                    break;
                }
            }
            assertTrue(observed < initialLimit,
                "the adaptive controller did not shrink an idle platform pool; virtual backlog hid it, limit="
                    + observed);
            assertTrue(compute.metrics().lanes().virtual().queued() > 0,
                "the virtual backlog must still be waiting when the decision is taken");
            assertEquals(2, compute.metrics().lanes().virtual().running());

            release.countDown();
            for (final CompletableFuture<String> future : futures) {
                assertNotNull(future.get(20L, TimeUnit.SECONDS));
            }
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }
}
