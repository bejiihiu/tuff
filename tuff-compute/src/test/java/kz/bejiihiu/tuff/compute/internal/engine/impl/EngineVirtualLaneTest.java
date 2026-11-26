package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.awaitUntil;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.cancellationOf;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Virtual lane routing: LOW ownership, NORMAL spill and the hard virtual concurrency limit.
 */
@Timeout(90)
class EngineVirtualLaneTest {

    @Test
    void lowPriorityAlwaysRunsOnAVirtualThread() throws Exception {
        final EngineTestSupport.SettingsBuilder settings = settings()
            .fixedWorkers(2)
            .virtualLane(true, true, 8, millis(50L), 64);
        final AsyncComputeScheduler compute = startedScheduler(settings.build(), new EngineTestSupport.FakeClock());
        try {
            final AtomicBoolean virtual = new AtomicBoolean();
            final AtomicReference<String> threadName = new AtomicReference<>();
            final String value = compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                virtual.set(Thread.currentThread().isVirtual());
                threadName.set(Thread.currentThread().getName());
                return "low";
            }).get(10L, TimeUnit.SECONDS);

            assertEquals("low", value);
            assertTrue(virtual.get(), "LOW must never occupy a platform worker");
            assertTrue(threadName.get().startsWith(settings.prefix() + "-virtual"), threadName.get());
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void virtualLimitIsHardAndWaitersAreNotLost() throws Exception {
        final EngineTestSupport.SettingsBuilder settings = settings()
            .fixedWorkers(4)
            .virtualLane(true, true, 2, millis(50L), 64);
        final AsyncComputeScheduler compute = startedScheduler(settings.build(), new EngineTestSupport.FakeClock());
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch twoRunning = new CountDownLatch(2);
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger peak = new AtomicInteger();
        try {
            final List<CompletableFuture<String>> futures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                final int index = i;
                futures.add(compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                    final int now = active.incrementAndGet();
                    peak.accumulateAndGet(now, Math::max);
                    twoRunning.countDown();
                    release.await();
                    active.decrementAndGet();
                    return "task-" + index;
                }));
            }
            assertTrue(twoRunning.await(10L, TimeUnit.SECONDS), "exactly maxConcurrent LOW tasks should start");
            assertEquals(2, active.get());

            release.countDown();
            for (final CompletableFuture<String> future : futures) {
                assertNotNull(future.get(10L, TimeUnit.SECONDS));
            }
            assertTrue(peak.get() <= 2, "virtual lane exceeded maxConcurrent: " + peak.get());
            awaitUntil("the lane counted every completed task", Duration.ofSeconds(10), () -> {
                final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
                return lane.submitted() == 5L && lane.completed() == 5L;
            });
            assertEquals(5L, compute.metrics().lanes().virtual().submitted());
            assertEquals(5L, compute.metrics().lanes().virtual().completed());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void lowTaskExpiresWhileWaitingForALaneSlot() throws Exception {
        final EngineTestSupport.FakeClock clock = new EngineTestSupport.FakeClock();
        final ComputeSettingsHolder holder = new ComputeSettingsHolder(
            settings()
                .fixedWorkers(1)
                .virtualLane(true, true, 1, millis(50L), 64)
                .build(),
            clock
        );
        final ComputeEngineImpl engine = holder.engine;
        final AsyncComputeScheduler compute = holder.scheduler;
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger executed = new AtomicInteger();
        try {
            compute.registerConsumer(ComputeConsumer.of(
                "expiring",
                new ConsumerPolicy(
                    true,
                    ConsumerPolicy.ConcurrencyLimit.automatic(),
                    -1,
                    1.0d,
                    OverflowPolicy.REJECT,
                    millis(100L)
                )
            ));
            compute.submit("expiring", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                firstStarted.countDown();
                release.await();
                return "first";
            });
            assertTrue(firstStarted.await(10L, TimeUnit.SECONDS));

            final CompletableFuture<String> queued = compute.submit(
                "expiring",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> {
                    executed.incrementAndGet();
                    return "second";
                }
            );
            clock.advance(millis(200L));
            engine.sweepNow();

            final ComputeCancelledException cancellation = cancellationOf(queued);
            assertEquals(ComputeCancelledException.Reason.QUEUE_EXPIRED, cancellation.reason());
            assertEquals(0, executed.get(), "an expired LOW task must never run");
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void cancellingAQueuedLowTaskNeverRunsIt() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).virtualLane(true, true, 1, millis(50L), 64).build(),
            new EngineTestSupport.FakeClock()
        );
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger executed = new AtomicInteger();
        try {
            compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                firstStarted.countDown();
                release.await();
                return "blocker";
            });
            assertTrue(firstStarted.await(10L, TimeUnit.SECONDS));
            final ComputeTaskHandle<String> second = compute.enqueue(new ComputeRequest<>(
                "low",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> {
                    executed.incrementAndGet();
                    return "second";
                }
            ));
            assertTrue(second.cancel());
            final ComputeCancelledException cancellation = cancellationOf(second.future());
            assertEquals(ComputeCancelledException.Reason.CALLER, cancellation.reason());
            assertEquals(0, executed.get());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void cancellingARunningLowTaskDiscardsItsResult() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).virtualLane(true, true, 2, millis(50L), 64).build(),
            new EngineTestSupport.FakeClock()
        );
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            final ComputeTaskHandle<String> handle = compute.enqueue(new ComputeRequest<>(
                "low",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> {
                    started.countDown();
                    release.await();
                    return "late";
                }
            ));
            assertTrue(started.await(10L, TimeUnit.SECONDS));
            assertTrue(handle.cancel());
            release.countDown();
            final ComputeCancelledException cancellation = cancellationOf(handle.future());
            assertEquals(ComputeCancelledException.Reason.CALLER, cancellation.reason());
            assertEquals(ComputeTaskHandle.State.CANCELLED, handle.state());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void normalStaysOnPlatformWhileThereIsCapacity() throws Exception {
        final EngineTestSupport.SettingsBuilder settings = settings()
            .fixedWorkers(2)
            .virtualLane(true, true, 4, Duration.ofSeconds(5L), 0);
        final AsyncComputeScheduler compute = startedScheduler(settings.build(), new EngineTestSupport.FakeClock());
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            compute.submit("normal", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                firstStarted.countDown();
                release.await();
                return "first";
            });
            assertTrue(firstStarted.await(10L, TimeUnit.SECONDS));

            final AtomicBoolean virtual = new AtomicBoolean(true);
            compute.submit("normal", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                virtual.set(Thread.currentThread().isVirtual());
                return "second";
            }).get(10L, TimeUnit.SECONDS);

            assertFalse(virtual.get(), "NORMAL must stay on the platform lane while the pool has capacity");
            assertEquals(0L, compute.metrics().lanes().virtual().submitted());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void normalSpillsAfterTheWaitThresholdOnceThePoolIsSaturated() throws Exception {
        final EngineTestSupport.FakeClock clock = new EngineTestSupport.FakeClock();
        final EngineTestSupport.SettingsBuilder settings = settings()
            .fixedWorkers(2)
            .virtualLane(true, true, 2, millis(100L), 0);
        final AsyncComputeScheduler compute = startedScheduler(settings.build(), clock);
        compute.registerConsumer(ComputeConsumer.of("spill", ConsumerPolicy.absolute(2)));
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch secondStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean spilledTaskVirtual = new AtomicBoolean();
        try {
            compute.submit("occupier-a", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                firstStarted.countDown();
                release.await();
                return "occupier-a";
            });
            compute.submit("occupier-b", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                secondStarted.countDown();
                release.await();
                return "occupier-b";
            });
            assertTrue(firstStarted.await(10L, TimeUnit.SECONDS));
            assertTrue(secondStarted.await(10L, TimeUnit.SECONDS));

            final CompletableFuture<String> queued = compute.submit(
                "spill",
                ComputeTaskType.GENERAL,
                ComputePriority.NORMAL,
                () -> {
                    spilledTaskVirtual.set(Thread.currentThread().isVirtual());
                    return "spilled";
                }
            );
            clock.advance(millis(150L));
            assertEquals("spilled", queued.get(10L, TimeUnit.SECONDS));

            assertTrue(spilledTaskVirtual.get(), "a saturated pool with an old NORMAL task should spill it");
            assertEquals(1L, compute.metrics().lanes().virtual().spilled());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void normalSpillsWhenTheNormalQueueDepthReachesTheTrigger() throws Exception {
        final ComputeEngineImpl engine = EngineTestSupport.engine(
            settings()
                .fixedWorkers(2)
                .virtualLane(true, true, 2, Duration.ofDays(1L), 2)
                .build(),
            new EngineTestSupport.FakeClock()
        );
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        compute.start();
        compute.registerConsumer(ComputeConsumer.of("spill", ConsumerPolicy.absolute(2)));
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch secondStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            compute.submit("occupier-a", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                firstStarted.countDown();
                release.await();
                return "occupier-a";
            });
            compute.submit("occupier-b", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                secondStarted.countDown();
                release.await();
                return "occupier-b";
            });
            assertTrue(firstStarted.await(10L, TimeUnit.SECONDS));
            assertTrue(secondStarted.await(10L, TimeUnit.SECONDS));
            final List<CompletableFuture<String>> queued = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                queued.add(compute.submit("spill", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "ok"));
            }
            assertEventually("the NORMAL queue depth triggers a spill", Duration.ofSeconds(10), () ->
                compute.metrics().lanes().virtual().spilled() >= 1L);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void highAndCriticalNeverSpill() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).virtualLane(true, true, 4, Duration.ofSeconds(5L), 0).build(),
            new EngineTestSupport.FakeClock()
        );
        final CountDownLatch occupierStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            compute.submit("normal", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                occupierStarted.countDown();
                release.await();
                return "occupier";
            });
            assertTrue(occupierStarted.await(10L, TimeUnit.SECONDS));

            final CompletableFuture<String> high = compute.submit("urgent", ComputeTaskType.GENERAL, ComputePriority.HIGH, () -> "high");
            final CompletableFuture<String> critical = compute.submit("urgent", ComputeTaskType.GENERAL, ComputePriority.CRITICAL, () -> "critical");
            // a LOW task proves the pump is awake and actively claiming; if HIGH or CRITICAL were
            // spill eligible they would have outranked it and been taken instead
            assertEquals("low", compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> "low")
                .get(10L, TimeUnit.SECONDS));

            assertEquals(1L, compute.metrics().lanes().virtual().submitted(),
                "only the LOW task may enter the virtual lane");
            assertFalse(high.isDone(), "HIGH must wait for the platform lane");
            assertFalse(critical.isDone(), "CRITICAL must wait for the platform lane");

            release.countDown();
            assertEquals("high", high.get(10L, TimeUnit.SECONDS));
            assertEquals("critical", critical.get(10L, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void consumerQuotaIsSharedAcrossBothLanes() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(2).virtualLane(true, true, 4, Duration.ofSeconds(5L), 0).build(),
            new EngineTestSupport.FakeClock()
        );
        final CountDownLatch twoPlatform = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger peak = new AtomicInteger();
        try {
            compute.registerConsumer(ComputeConsumer.of("shared", ConsumerPolicy.absolute(2)));
            final List<CompletableFuture<String>> platform = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                platform.add(compute.submit("shared", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> {
                    final int now = active.incrementAndGet();
                    peak.accumulateAndGet(now, Math::max);
                    twoPlatform.countDown();
                    release.await();
                    active.decrementAndGet();
                    return "platform";
                }));
            }
            assertTrue(twoPlatform.await(10L, TimeUnit.SECONDS));

            final CompletableFuture<String> low = compute.submit("shared", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                final int now = active.incrementAndGet();
                peak.accumulateAndGet(now, Math::max);
                release.await();
                active.decrementAndGet();
                return "low";
            });
            assertEventually("the lane stays empty while the quota is exhausted", Duration.ofSeconds(1),
                () -> compute.metrics().lanes().virtual().submitted() == 0L);

            release.countDown();
            assertEquals("low", low.get(10L, TimeUnit.SECONDS));
            for (final CompletableFuture<String> future : platform) {
                assertEquals("platform", future.get(10L, TimeUnit.SECONDS));
            }
            assertTrue(peak.get() <= 2, "consumer quota was exceeded across lanes: " + peak.get());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void lowBacklogDoesNotGrowThePlatformPool() throws Exception {
        final ComputeEngineImpl engine = EngineTestSupport.engine(
            settings()
                .autoWorkers(1, 4, Duration.ofSeconds(30L))
                .capacity(1_024)
                .virtualLane(true, true, 2, Duration.ofSeconds(5L), 0)
                .build(),
            new EngineTestSupport.FakeClock(),
            1
        );
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        compute.start();
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch twoRunning = new CountDownLatch(2);
        try {
            final List<CompletableFuture<String>> futures = new ArrayList<>();
            // the default consumer limit is the platform pool size, so two consumers are needed to
            // fill both virtual slots of this one worker pool
            for (final String consumer : List.of("low-a", "low-b")) {
                for (int i = 0; i < 5; i++) {
                    futures.add(compute.submit(consumer, ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                        twoRunning.countDown();
                        release.await();
                        return "low";
                    }));
                }
            }
            assertTrue(twoRunning.await(10L, TimeUnit.SECONDS));
            assertEventually("the platform pool ignores the LOW backlog", Duration.ofSeconds(1),
                () -> engine.desiredWorkers() == 1);
            release.countDown();
            for (final CompletableFuture<String> future : futures) {
                assertEquals("low", future.get(10L, TimeUnit.SECONDS));
            }
            assertEquals(1, engine.desiredWorkers(), "LOW work must not grow the platform pool");
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void shutdownStopsTheLaneAndLeavesNoThreads() throws Exception {
        final EngineTestSupport.SettingsBuilder settings = settings()
            .fixedWorkers(1)
            // exactly one lane slot: the running task holds it and the second LOW task cannot be
            // claimed before shutdown, so "queued at shutdown" is deterministic instead of a race
            .virtualLane(true, true, 1, millis(50L), 64);
        final EngineTestSupport.FakeClock clock = new EngineTestSupport.FakeClock();
        final ComputeEngineImpl engine = EngineTestSupport.engine(settings.build(), clock);
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        compute.start();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                started.countDown();
                release.await();
                return "running";
            });
            assertTrue(started.await(10L, TimeUnit.SECONDS));
            final CompletableFuture<String> queued = compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> "queued");

            compute.shutdown(Duration.ofSeconds(5L));
            cancellationOf(queued);
            release.countDown();

            assertTrue(compute.awaitTermination(Duration.ofSeconds(10L)));
            assertTrue(engine.virtualLane().isTerminated());
            assertEquals(0, engine.virtualLane().liveVirtualThreads());
            assertTrue(engine.virtualLane().onlyVirtualThreads());
            EngineTestSupport.awaitNoEngineThreads(settings.prefix(), Duration.ofSeconds(10L));
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    /**
     * Holds an engine and its facade so a test can drive the janitor directly.
     */
    private static final class ComputeSettingsHolder {

        private final ComputeEngineImpl engine;
        private final AsyncComputeScheduler scheduler;

        private ComputeSettingsHolder(
            final ComputeSettings settings,
            final EngineTestSupport.FakeClock clock
        ) {
            this.engine = EngineTestSupport.engine(settings, clock);
            this.scheduler = new AsyncComputeSchedulerImpl(this.engine);
            this.scheduler.start();
        }
    }
}
