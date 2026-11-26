package kz.bejiihiu.tuff.compute.internal.lanes;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.NEVER;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.SettingsBuilder;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.absolute;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertStateTerminal;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.expiring;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.start;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lane routing end to end: LOW ownership, the hard virtual limit, expiry of stuck LOW work, NORMAL
 * spill rules and the consumer quota that spans both lanes.
 */
@Timeout(120)
class LaneRoutingTest {

    @Test
    void lowPriorityAlwaysRunsOnVirtualThreads() throws Exception {
        final SettingsBuilder settings = SettingsBuilder.of("low-virtual")
            .fixedWorkers(4)
            .virtualLane(true, true, 8, millis(50L), 64);
        final AsyncComputeScheduler compute = start(settings);
        final LaneTestSupport.TaskProbe probe = new LaneTestSupport.TaskProbe();
        final CountDownLatch started = new CountDownLatch(4);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            assertEventually("the platform pool exists before the LOW batch", NEVER,
                () -> compute.metrics().totalWorkers() >= 4);

            final List<CompletableFuture<String>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                    probe.begin();
                    started.countDown();
                    release.await();
                    probe.end();
                    return "low";
                }));
            }
            assertTrue(started.await(10L, TimeUnit.SECONDS), "LOW tasks did not start");

            assertTrue(probe.sawVirtual(), "no LOW task ran on a virtual thread");
            assertFalse(probe.sawPlatform(), "a platform worker took a LOW task");
            for (final String name : probe.names()) {
                assertTrue(name.startsWith(settings.prefix() + "-virtual"), "unexpected LOW thread " + name);
            }

            release.countDown();
            for (final CompletableFuture<String> future : futures) {
                assertEquals("low", future.get(10L, TimeUnit.SECONDS));
            }
            assertEquals(0, probe.liveThreads(), "virtual task threads leaked");
            assertEventually("the lane accounted the whole batch", NEVER, () -> {
                final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
                return lane.submitted() == 8L && lane.completed() == 8L;
            });
            assertEventually("the platform pool stayed idle", NEVER, () -> compute.metrics().activeWorkers() == 0);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void lowFallsBackToPlatformWhenTheLaneIsDisabled() throws Exception {
        this.assertLowRunsOnPlatform(SettingsBuilder.of("no-lane").fixedWorkers(2).noVirtualLane(), false);
    }

    @Test
    void lowFallsBackToPlatformWhenLowRoutingIsOff() throws Exception {
        final SettingsBuilder settings = SettingsBuilder.of("low-off")
            .fixedWorkers(2)
            .virtualLane(false, true, 4, millis(50L), 64);
        this.assertLowRunsOnPlatform(settings, true);
    }

    @Test
    void virtualLimitIsHardAndQueuedLowWorkIsNotLost() throws Exception {
        final SettingsBuilder settings = SettingsBuilder.of("vt-limit")
            .fixedWorkers(8)
            .virtualLane(true, true, 3, millis(50L), 64);
        final AsyncComputeScheduler compute = start(settings);
        final LaneTestSupport.TaskProbe probe = new LaneTestSupport.TaskProbe();
        final CountDownLatch started = new CountDownLatch(3);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            final List<CompletableFuture<String>> futures = new ArrayList<>();
            for (int i = 0; i < 9; i++) {
                futures.add(compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                    probe.begin();
                    started.countDown();
                    release.await();
                    probe.end();
                    return "low";
                }));
            }
            assertTrue(started.await(10L, TimeUnit.SECONDS));

            assertEventually("the limit and the backlog are visible", NEVER, () -> {
                final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
                return lane.running() == 3 && lane.queued() == 6;
            });
            assertEquals(3, probe.active(), "more than maxConcurrent tasks are running");

            release.countDown();
            for (final CompletableFuture<String> future : futures) {
                assertEquals("low", future.get(10L, TimeUnit.SECONDS), "a queued LOW task was lost");
            }
            assertEquals(3, probe.peak(), "the virtual limit was exceeded");
            assertEventually("the lane accounted every queued task", NEVER, () -> {
                final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
                return lane.submitted() == 9L && lane.completed() == 9L && lane.cancelled() == 0L;
            });
            assertEquals(3, compute.metrics().lanes().virtual().limit());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void lowTaskExpiresWhileWaitingForAVirtualSlot() throws Exception {
        final LaneTestSupport.FakeClock clock = new LaneTestSupport.FakeClock();
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("vt-expiry").fixedWorkers(1).virtualLane(true, true, 1, millis(50L), 64),
            clock);
        compute.registerConsumer(ComputeConsumer.of("expiring", expiring(1, millis(100L))));
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger executed = new AtomicInteger();
        try {
            compute.submit("expiring", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                firstStarted.countDown();
                release.await();
                return "first";
            });
            assertTrue(firstStarted.await(10L, TimeUnit.SECONDS));

            final ComputeTaskHandle<String> queued = compute.enqueue(new ComputeRequest<>(
                "expiring",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> {
                    executed.incrementAndGet();
                    return "second";
                }
            ));
            clock.advance(millis(500L));

            final ComputeCancelledException cancellation = LaneTestSupport.cancellationOf(queued.future());
            assertEquals(ComputeCancelledException.Reason.QUEUE_EXPIRED, cancellation.reason());
            assertEquals(0, executed.get(), "an expired LOW task must never run");
            assertEquals(1L, compute.metrics().lanes().virtual().submitted(),
                "the expired task entered the lane");
            assertEventually("the journal counted the expiry", NEVER, () -> compute.metrics().expiredTasks() >= 1L);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void normalStaysOnPlatformWhileThereIsCapacity() throws Exception {
        final SettingsBuilder settings = SettingsBuilder.of("normal-capacity")
            .fixedWorkers(4)
            .virtualLane(true, true, 4, Duration.ZERO, 1);
        final AsyncComputeScheduler compute = start(settings);
        final LaneTestSupport.TaskProbe probe = new LaneTestSupport.TaskProbe();
        try {
            // one at a time: the pool is never saturated while a NORMAL task is claimable, so an
            // eager spill configuration (zero wait, depth one) must not move any of them
            for (int i = 0; i < 8; i++) {
                final String value = compute.submit("normal", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                    probe.begin();
                    probe.end();
                    return "normal";
                }).get(10L, TimeUnit.SECONDS);
                assertEquals("normal", value);
            }
            assertTrue(probe.sawPlatform(), "NORMAL work did not use the platform lane");
            assertFalse(probe.sawVirtual(), "NORMAL work spilled onto an unsaturated pool");
            assertEquals(0L, compute.metrics().lanes().virtual().submitted());
            assertEquals(0L, compute.metrics().lanes().virtual().spilled());
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void normalDoesNotSpillBeforeTheWaitThreshold() throws Exception {
        final LaneTestSupport.FakeClock clock = new LaneTestSupport.FakeClock();
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("spill-early").fixedWorkers(1).virtualLane(true, true, 2, millis(100L), 0),
            clock);
        compute.registerConsumer(ComputeConsumer.of("occupy", absolute(1)));
        compute.registerConsumer(ComputeConsumer.of("spill", absolute(1)));
        final CountDownLatch occupierStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final LaneTestSupport.TaskProbe probe = new LaneTestSupport.TaskProbe();
        try {
            compute.submit("occupy", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                occupierStarted.countDown();
                release.await();
                return "occupier";
            });
            assertTrue(occupierStarted.await(10L, TimeUnit.SECONDS));

            final CompletableFuture<String> candidate = compute.submit("spill", ComputeTaskType.GENERAL,
                ComputePriority.NORMAL, () -> {
                    probe.begin();
                    probe.end();
                    return "candidate";
                });
            // the pool is saturated but the entry has not waited long enough, so spilling must stay
            // closed: release the pool and let the platform lane take it
            clock.advance(millis(50L));
            release.countDown();

            assertEquals("candidate", candidate.get(10L, TimeUnit.SECONDS));
            assertTrue(probe.sawPlatform(), "a NORMAL task below the spill wait ran on the virtual lane");
            assertEquals(0L, compute.metrics().lanes().virtual().submitted());
            assertEquals(0L, compute.metrics().lanes().virtual().spilled());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void normalSpillsOnceTheWaitThresholdPasses() throws Exception {
        final LaneTestSupport.FakeClock clock = new LaneTestSupport.FakeClock();
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("spill-late").fixedWorkers(1).virtualLane(true, true, 2, millis(100L), 0),
            clock);
        compute.registerConsumer(ComputeConsumer.of("occupy", absolute(1)));
        compute.registerConsumer(ComputeConsumer.of("spill", absolute(1)));
        final CountDownLatch occupierStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final LaneTestSupport.TaskProbe probe = new LaneTestSupport.TaskProbe();
        try {
            compute.submit("occupy", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                occupierStarted.countDown();
                release.await();
                return "occupier";
            });
            assertTrue(occupierStarted.await(10L, TimeUnit.SECONDS));

            final CompletableFuture<String> candidate = compute.submit("spill", ComputeTaskType.GENERAL,
                ComputePriority.NORMAL, () -> {
                    probe.begin();
                    probe.end();
                    return "candidate";
                });
            clock.advance(millis(150L));

            // still holding the only platform worker: completing at all proves the task spilled
            assertEquals("candidate", candidate.get(10L, TimeUnit.SECONDS));
            assertTrue(probe.sawVirtual(), "a saturated pool with an old NORMAL task did not spill it");
            assertEquals(1L, compute.metrics().lanes().virtual().submitted());
            assertEquals(1L, compute.metrics().lanes().virtual().spilled());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void normalSpillsWhenTheNormalQueueDepthReachesTheTrigger() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("spill-depth")
                .fixedWorkers(1)
                .virtualLane(true, true, 2, Duration.ofDays(1L), 2));
        compute.registerConsumer(ComputeConsumer.of("occupy", absolute(1)));
        compute.registerConsumer(ComputeConsumer.of("spill", absolute(2)));
        final CountDownLatch occupierStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final LaneTestSupport.TaskProbe probe = new LaneTestSupport.TaskProbe();
        try {
            compute.submit("occupy", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                occupierStarted.countDown();
                release.await();
                return "occupier";
            });
            assertTrue(occupierStarted.await(10L, TimeUnit.SECONDS));

            final List<CompletableFuture<String>> candidates = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                candidates.add(compute.submit("spill", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                    probe.begin();
                    probe.end();
                    return "candidate";
                }));
            }
            // the trigger opens once the queue is two deep, so every candidate except the last one
            // becomes spill eligible; the last one keeps the queue below the trigger and must wait
            for (int i = 0; i < 3; i++) {
                assertEquals("candidate", candidates.get(i).get(10L, TimeUnit.SECONDS));
            }
            assertFalse(candidates.get(3).isDone(), "the last candidate fits below the depth trigger");
            assertTrue(probe.sawVirtual(), "the depth trigger did not spill NORMAL work");
            assertEquals(3L, compute.metrics().lanes().virtual().spilled());

            release.countDown();
            assertEquals("candidate", candidates.get(3).get(10L, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void highAndCriticalNeverSpillWhileNormalSpillIsOpen() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("no-urgent-spill")
                .fixedWorkers(1)
                .virtualLane(true, true, 4, Duration.ZERO, 1));
        compute.registerConsumer(ComputeConsumer.of("occupy", absolute(1)));
        compute.registerConsumer(ComputeConsumer.of("urgent", absolute(8)));
        compute.registerConsumer(ComputeConsumer.of("low", absolute(8)));
        final CountDownLatch occupierStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final LaneTestSupport.TaskProbe urgentProbe = new LaneTestSupport.TaskProbe();
        try {
            compute.submit("occupy", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                occupierStarted.countDown();
                release.await();
                return "occupier";
            });
            assertTrue(occupierStarted.await(10L, TimeUnit.SECONDS));

            final List<CompletableFuture<String>> urgent = new ArrayList<>();
            for (final ComputePriority priority : List.of(ComputePriority.HIGH, ComputePriority.CRITICAL)) {
                for (int i = 0; i < 3; i++) {
                    urgent.add(compute.submit("urgent", ComputeTaskType.GENERAL, priority, () -> {
                        urgentProbe.begin();
                        urgentProbe.end();
                        return priority.name();
                    }));
                }
            }
            // a LOW task proves the pump is awake and that the spill path is open; if HIGH or
            // CRITICAL were spill eligible they would have outranked it and been taken instead
            assertEquals("low", compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> "low")
                .get(10L, TimeUnit.SECONDS));

            assertEquals(1L, compute.metrics().lanes().virtual().submitted(),
                "only the LOW task may enter the virtual lane");
            assertEquals(0L, compute.metrics().lanes().virtual().spilled());
            assertFalse(urgentProbe.sawVirtual(), "urgent work entered the virtual lane");
            for (final CompletableFuture<String> future : urgent) {
                assertFalse(future.isDone(), "urgent work bypassed the saturated platform lane");
            }

            release.countDown();
            for (int i = 0; i < urgent.size(); i++) {
                final String expected = i < 3 ? ComputePriority.HIGH.name() : ComputePriority.CRITICAL.name();
                assertEquals(expected, urgent.get(i).get(10L, TimeUnit.SECONDS));
            }
            assertTrue(urgentProbe.sawPlatform(), "urgent work did not use the platform lane");
            assertFalse(urgentProbe.sawVirtual(), "urgent work used the virtual lane");
            assertEquals(0L, compute.metrics().lanes().virtual().spilled());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void consumerQuotaIsSharedAcrossBothLanes() throws Exception {
        final AsyncComputeScheduler compute = start(
            SettingsBuilder.of("cross-lane-quota")
                .fixedWorkers(2)
                .virtualLane(true, true, 4, millis(50L), 64));
        compute.registerConsumer(ComputeConsumer.of("shared", absolute(2)));
        compute.registerConsumer(ComputeConsumer.of("probe", absolute(1)));
        final LaneTestSupport.TaskProbe shared = new LaneTestSupport.TaskProbe();
        final CountDownLatch twoPlatform = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch sharedLowStarted = new CountDownLatch(1);
        try {
            for (int i = 0; i < 2; i++) {
                compute.submit("shared", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                    shared.begin();
                    twoPlatform.countDown();
                    release.await();
                    shared.end();
                    return "platform";
                });
            }
            assertTrue(twoPlatform.await(10L, TimeUnit.SECONDS));
            assertEquals(2, shared.active());

            // a different consumer proves the pump is claiming right now
            assertEquals("probe", compute.submit("probe", ComputeTaskType.GENERAL, ComputePriority.LOW,
                () -> "probe").get(10L, TimeUnit.SECONDS));
            final long laneBefore = compute.metrics().lanes().virtual().submitted();
            assertEquals(1L, laneBefore);

            final List<CompletableFuture<String>> low = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                low.add(compute.submit("shared", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                    shared.begin();
                    sharedLowStarted.countDown();
                    release.await();
                    shared.end();
                    return "low";
                }));
            }
            assertFalse(sharedLowStarted.await(300L, TimeUnit.MILLISECONDS),
                "the shared quota was bypassed through the virtual lane");
            assertEquals(laneBefore, compute.metrics().lanes().virtual().submitted());

            release.countDown();
            for (final CompletableFuture<String> future : low) {
                assertEquals("low", future.get(10L, TimeUnit.SECONDS));
            }
            assertTrue(shared.peak() <= 2, "quota exceeded across lanes: peak=" + shared.peak());
            assertEventually("the lane accounted the shared batch", NEVER, () -> {
                final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
                return lane.submitted() == laneBefore + 5L && lane.completed() == laneBefore + 5L;
            });
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    private void assertLowRunsOnPlatform(final SettingsBuilder settings, final boolean laneExists) throws Exception {
        final AsyncComputeScheduler compute = start(settings);
        final LaneTestSupport.TaskProbe probe = new LaneTestSupport.TaskProbe();
        try {
            assertEquals("low", compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                probe.begin();
                probe.end();
                return "low";
            }).get(10L, TimeUnit.SECONDS));

            assertTrue(probe.sawPlatform(), "LOW did not fall back to the platform lane");
            assertFalse(probe.sawVirtual());
            for (final String name : probe.names()) {
                assertTrue(name.startsWith(settings.prefix() + "-"), name);
                assertFalse(name.startsWith(settings.prefix() + "-virtual"), name);
            }
            final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
            assertEquals(laneExists, lane.enabled());
            assertEquals(0L, lane.submitted());
            assertEquals(0L, lane.spilled());
        } finally {
            compute.shutdown();
            assertStateTerminal(compute, Duration.ofSeconds(10L));
        }
    }
}
