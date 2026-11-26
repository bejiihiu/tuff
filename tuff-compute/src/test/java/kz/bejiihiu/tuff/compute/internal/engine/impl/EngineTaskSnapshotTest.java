package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The live and recent task views exposed by {@link SchedulerMetrics}.
 */
@Timeout(90)
class EngineTaskSnapshotTest {

    @Test
    void aRunningTaskIsReportedUntilItFinishes() throws Exception {
        final EngineTestSupport.FakeClock clock = new EngineTestSupport.FakeClock();
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(2).noVirtualLane().build(), clock);
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            final ComputeTaskHandle<String> handle = compute.enqueue(ComputeRequest.of("blocked", () -> {
                started.countDown();
                release.await();
                return "done";
            }));
            assertTrue(started.await(10L, TimeUnit.SECONDS), "the task should have started");
            assertEventually("one running task", millis(5_000L),
                () -> compute.metrics().runningTasks().size() == 1);

            final SchedulerMetrics.RunningTaskInfo running = compute.metrics().runningTasks().get(0);

            assertEquals(handle.taskId(), running.taskId());
            assertEquals("default", running.consumerId());
            assertEquals(ComputeTaskType.GENERAL, running.taskType());
            assertEquals(ComputePriority.NORMAL, running.priority());
            assertEquals("platform", running.lane());
            assertEquals("blocked", running.label().orElseThrow());
            assertEquals(clock.instant(), running.startedAt());
            assertTrue(running.elapsedNanos() >= 0L, "elapsed must not be negative");

            release.countDown();
            assertEquals("done", handle.future().get(10L, TimeUnit.SECONDS));
            assertEventually("the completion ring entry", millis(5_000L),
                () -> compute.metrics().slowestRecentTasks(10).size() == 1);
            assertEventually("no running tasks", millis(5_000L),
                () -> compute.metrics().runningTasks().isEmpty());

            final SchedulerMetrics.CompletedTaskInfo completed = compute.metrics().slowestRecentTasks(10).get(0);

            assertEquals(handle.taskId(), completed.taskId());
            assertEquals("platform", completed.lane());
            assertEquals("blocked", completed.label().orElseThrow());
            assertEquals(ComputeTaskHandle.State.COMPLETED, completed.state());
            assertTrue(completed.executionNanos() >= 0L, "execution must not be negative");
            assertTrue(completed.queueWaitNanos() >= 0L, "queue wait must not be negative");
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void aLowTaskIsReportedOnTheVirtualLane() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(2).virtualLane(true, true, 4, millis(50L), 64).build(),
            new EngineTestSupport.FakeClock());
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            final ComputeTaskHandle<String> handle = compute.enqueue(
                ComputeRequest.of(ComputePriority.LOW, "io work", () -> {
                    started.countDown();
                    release.await();
                    return "low";
                }));
            assertTrue(started.await(10L, TimeUnit.SECONDS), "the low task should have started on the lane");
            assertEventually("the virtual task", millis(5_000L),
                () -> compute.metrics().runningTasks().size() == 1);

            final SchedulerMetrics.RunningTaskInfo running = compute.metrics().runningTasks().get(0);

            assertEquals(handle.taskId(), running.taskId());
            assertEquals("virtual", running.lane());
            assertEquals(ComputePriority.LOW, running.priority());
            assertEquals("io work", running.label().orElseThrow());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void theCompletionRingIsBoundedAndSortedByExecution() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).noVirtualLane().build(), new EngineTestSupport.FakeClock());
        try {
            for (int index = 0; index < 200; index++) {
                compute.submit(() -> "value").get(10L, TimeUnit.SECONDS);
            }
            assertEventually("a full ring", millis(10_000L),
                () -> compute.metrics().slowestRecentTasks(1_000).size() == 64);

            final List<SchedulerMetrics.CompletedTaskInfo> all = compute.metrics().slowestRecentTasks(1_000);

            assertEquals(64, all.size(), "the ring must stay bounded");
            for (int index = 1; index < all.size(); index++) {
                assertTrue(
                    all.get(index - 1).executionNanos() >= all.get(index).executionNanos(),
                    "entries must be sorted by execution time descending"
                );
            }
            assertTrue(all.stream().allMatch(entry -> entry.state() == ComputeTaskHandle.State.COMPLETED),
                "every retained entry finished successfully");
            assertTrue(compute.metrics().slowestRecentTasks(10).size() == 10);
            assertTrue(compute.metrics().slowestRecentTasks(0).isEmpty());
            assertTrue(compute.metrics().slowestRecentTasks(-3).isEmpty());
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void anIdleEngineReportsNoTasks() {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).noVirtualLane().build(), new EngineTestSupport.FakeClock());
        try {
            assertTrue(compute.metrics().runningTasks().isEmpty());
            assertTrue(compute.metrics().slowestRecentTasks(10).isEmpty());
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void aQueuedCancellationDoesNotEnterTheRing() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).noVirtualLane().build(), new EngineTestSupport.FakeClock());
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            compute.enqueue(ComputeRequest.of("busy", () -> {
                started.countDown();
                release.await();
                return "first";
            }));
            assertTrue(started.await(10L, TimeUnit.SECONDS), "the first task should have started");

            final ComputeTaskHandle<String> queued = compute.enqueue(ComputeRequest.of("never runs", () -> "second"));
            assertTrue(queued.cancel(), "a queued task can be cancelled");
            assertEventually("the cancelled task to finish", millis(5_000L), () -> queued.state() == ComputeTaskHandle.State.CANCELLED);

            release.countDown();
            assertEventually("the ring entry of the started task", millis(5_000L),
                () -> compute.metrics().slowestRecentTasks(10).size() == 1);

            final SchedulerMetrics.CompletedTaskInfo only = compute.metrics().slowestRecentTasks(10).get(0);

            assertFalse(only.taskId() == queued.taskId(), "a task that never started must not be in the ring");
            assertEquals(ComputeTaskHandle.State.COMPLETED, only.state());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void theRunningListIsSortedByElapsedTime() throws Exception {
        final EngineTestSupport.FakeClock clock = new EngineTestSupport.FakeClock();
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(4).noVirtualLane().build(), clock);
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch bothStarted = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            compute.enqueue(ComputeRequest.of("older", () -> {
                firstStarted.countDown();
                bothStarted.countDown();
                release.await();
                return "older";
            }));
            assertTrue(firstStarted.await(10L, TimeUnit.SECONDS), "the first task should have started");
            clock.advance(millis(50L));
            compute.enqueue(ComputeRequest.of("newer", () -> {
                bothStarted.countDown();
                release.await();
                return "newer";
            }));
            assertTrue(bothStarted.await(10L, TimeUnit.SECONDS), "both tasks should have started");
            clock.advance(millis(100L));
            assertEventually("two running tasks", millis(5_000L),
                () -> compute.metrics().runningTasks().size() == 2);

            final List<SchedulerMetrics.RunningTaskInfo> running = compute.metrics().runningTasks();

            assertEquals(2, running.size());
            assertEquals("older", running.get(0).label().orElseThrow());
            assertEquals("newer", running.get(1).label().orElseThrow());
            assertTrue(running.get(0).elapsedNanos() > running.get(1).elapsedNanos(),
                "the older task must report the larger elapsed time");
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }
}
