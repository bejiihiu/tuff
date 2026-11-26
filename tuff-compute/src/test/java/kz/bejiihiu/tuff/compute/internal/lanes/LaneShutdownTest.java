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
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.NEVER;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.SettingsBuilder;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertCancelledWith;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.assertStateTerminal;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.awaitNoThreads;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.lanes.LaneTestSupport.start;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shutdown with live virtual work: grace, cooperative completion, abandoned bodies that are never
 * interrupted, bounded termination and a continuation that blocks without wedging anything.
 */
@Timeout(150)
class LaneShutdownTest {

    @Test
    void gracefulShutdownCancelsQueuedLowAndLetsRunningLowFinish() throws Exception {
        final SettingsBuilder settings = SettingsBuilder.of("shutdown-grace")
            .fixedWorkers(2)
            .virtualLane(true, true, 4, millis(50L), 64);
        final AsyncComputeScheduler compute = start(settings);
        final LaneTestSupport.TaskProbe probe = new LaneTestSupport.TaskProbe();
        final CountDownLatch twoStarted = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger queuedRan = new AtomicInteger();
        final List<CompletableFuture<String>> running = new ArrayList<>();
        try {
            for (int i = 0; i < 2; i++) {
                running.add(compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                    probe.begin();
                    twoStarted.countDown();
                    release.await();
                    probe.end();
                    return "running";
                }));
            }
            assertTrue(twoStarted.await(10L, TimeUnit.SECONDS));

            final List<ComputeTaskHandle<String>> queued = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                queued.add(compute.enqueue(new ComputeRequest<>(
                    "low",
                    ComputeTaskType.GENERAL,
                    ComputePriority.LOW,
                    () -> {
                        queuedRan.incrementAndGet();
                        return "queued";
                    }
                )));
            }

            compute.shutdown(Duration.ofSeconds(10L));
            for (final ComputeTaskHandle<String> handle : queued) {
                assertCancelledWith(handle, ComputeCancelledException.Reason.SHUTDOWN);
            }

            release.countDown();
            for (final CompletableFuture<String> future : running) {
                assertEquals("running", future.get(10L, TimeUnit.SECONDS),
                    "a running LOW task must not be cancelled by a graceful shutdown");
            }
            assertFalse(probe.wasInterrupted(), "graceful shutdown interrupted a running LOW task");
            assertEquals(0, queuedRan.get(), "shutdown started a queued LOW task");

            assertStateTerminal(compute, Duration.ofSeconds(15L));
            assertEventually("the lane is empty after termination", NEVER, () -> {
                final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
                return lane.running() == 0 && lane.submitted() == 2L && lane.completed() == 2L
                    && lane.cancelled() == 0L;
            });
            final SchedulerMetrics.VirtualLane lane = compute.metrics().lanes().virtual();
            assertEquals(2L, lane.completed());
            assertEquals(0L, lane.cancelled());
            assertEventually("no virtual task thread stays alive", NEVER, () -> probe.liveThreads() == 0);
            awaitNoThreads(settings.prefix(), Duration.ofSeconds(15L));
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void graceExpiryTerminatesOnTimeAndNeverInterruptsTheBody() throws Exception {
        final SettingsBuilder settings = SettingsBuilder.of("shutdown-abandon")
            .fixedWorkers(1)
            .virtualLane(true, true, 1, millis(50L), 64);
        final AsyncComputeScheduler compute = start(settings);
        final LaneTestSupport.TaskProbe probe = new LaneTestSupport.TaskProbe();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
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

            compute.shutdown(millis(200L));
            assertStateTerminal(compute, Duration.ofSeconds(10L));
            assertEquals(1, probe.liveThreads(), "the abandoned body was interrupted or cancelled");

            release.countDown();
            // the contract says a body still running at the deadline completes normally afterwards
            assertEquals("late", handle.future().get(10L, TimeUnit.SECONDS));
            assertEquals(ComputeTaskHandle.State.COMPLETED, handle.state());
            assertFalse(probe.wasInterrupted(), "the abandoned body was interrupted");
            assertEventually("the abandoned body released its thread", NEVER, () -> probe.liveThreads() == 0);
            assertEventually("the lane accounted the late completion", NEVER,
                () -> compute.metrics().lanes().virtual().completed() == 1L);
            awaitNoThreads(settings.prefix(), Duration.ofSeconds(10L));
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void blockingContinuationCannotWedgeShutdownOfVirtualWork() throws Exception {
        final SettingsBuilder settings = SettingsBuilder.of("shutdown-continuation")
            .fixedWorkers(1)
            .virtualLane(true, true, 1, millis(50L), 64);
        final AsyncComputeScheduler compute = start(settings);
        final CountDownLatch blockerStarted = new CountDownLatch(1);
        final CountDownLatch releaseBlocker = new CountDownLatch(1);
        final CountDownLatch continuationEntered = new CountDownLatch(1);
        final CountDownLatch releaseContinuation = new CountDownLatch(1);
        try {
            compute.submit("low", ComputeTaskType.GENERAL, ComputePriority.LOW, () -> {
                blockerStarted.countDown();
                releaseBlocker.await();
                return "blocker";
            });
            assertTrue(blockerStarted.await(10L, TimeUnit.SECONDS));

            final ComputeTaskHandle<String> victim = compute.enqueue(new ComputeRequest<>(
                "low",
                ComputeTaskType.GENERAL,
                ComputePriority.LOW,
                () -> "victim"
            ));
            victim.future().whenComplete((value, failure) -> {
                continuationEntered.countDown();
                try {
                    releaseContinuation.await();
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            });

            compute.shutdown(Duration.ofSeconds(10L));
            // the user continuation is allowed to block; shutdown must not be waiting for it
            assertTrue(continuationEntered.await(10L, TimeUnit.SECONDS),
                "the cancellation completion never left the scheduler service thread");
            assertCancelledWith(victim, ComputeCancelledException.Reason.SHUTDOWN);

            releaseBlocker.countDown();
            assertStateTerminal(compute, Duration.ofSeconds(10L));
            awaitNoThreads(settings.prefix(), Duration.ofSeconds(10L));
        } finally {
            releaseContinuation.countDown();
            releaseBlocker.countDown();
            compute.shutdown();
        }
    }
}
