package kz.bejiihiu.tuff.compute.internal.metrics;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.RecentTaskRing;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The bounded completion ring behind the recent task view.
 */
@Timeout(30)
class RecentTaskRingTest {

    @Test
    void anEmptyRingReturnsNothing() {
        final RecentTaskRing ring = new RecentTaskRing();

        assertTrue(ring.snapshot().isEmpty());
        assertTrue(ring.slowest(10).isEmpty());
        assertTrue(ring.slowest(0).isEmpty());
    }

    @Test
    void theRingKeepsOnlyTheNewestCapacityEntries() {
        final RecentTaskRing ring = new RecentTaskRing();
        for (long taskId = 1L; taskId <= 200L; taskId++) {
            ring.record(taskId, "default", ComputeTaskType.GENERAL, ComputePriority.NORMAL, "platform",
                "task-" + taskId, taskId * 10L, taskId, ComputeTaskHandle.State.COMPLETED);
        }

        final List<SchedulerMetrics.CompletedTaskInfo> entries = ring.snapshot();

        assertEquals(RecentTaskRing.CAPACITY, entries.size());
        assertEquals(200L, entries.get(0).taskId());
        assertEquals(137L, entries.get(entries.size() - 1).taskId());
    }

    @Test
    void entriesAreSortedByExecutionTimeAndTheLimitIsClamped() {
        final RecentTaskRing ring = new RecentTaskRing();
        final long[] executions = {5L, 1L, 4L, 2L, 3L};
        for (int index = 0; index < executions.length; index++) {
            ring.record(index + 1L, "default", ComputeTaskType.GENERAL, ComputePriority.NORMAL, "platform", "",
                executions[index], 7L, ComputeTaskHandle.State.COMPLETED);
        }

        final List<SchedulerMetrics.CompletedTaskInfo> slowest = ring.slowest(3);

        assertEquals(3, slowest.size());
        assertEquals(5L, slowest.get(0).executionNanos());
        assertEquals(4L, slowest.get(1).executionNanos());
        assertEquals(3L, slowest.get(2).executionNanos());
        assertTrue(ring.slowest(0).isEmpty());
        assertTrue(ring.slowest(-1).isEmpty());
        assertEquals(5, ring.slowest(100).size());
        assertTrue(slowest.get(0).label().isEmpty());
    }

    @Test
    void aTaskThatNeverStartedIsNotRecorded() {
        final RecentTaskRing ring = new RecentTaskRing();

        ring.record(1L, "default", ComputeTaskType.GENERAL, ComputePriority.LOW, null, "queued", -1L, 5L,
            ComputeTaskHandle.State.CANCELLED);
        ring.record(2L, "default", ComputeTaskType.GENERAL, ComputePriority.LOW, "virtual", "started", -1L, 5L,
            ComputeTaskHandle.State.CANCELLED);

        assertTrue(ring.snapshot().isEmpty());
    }

    @Test
    void concurrentWritersDoNotCorruptEntries() throws Exception {
        final RecentTaskRing ring = new RecentTaskRing();
        final int writers = 8;
        final int perWriter = 200;
        final CountDownLatch start = new CountDownLatch(1);
        final List<Thread> threads = new ArrayList<>();
        for (int writer = 0; writer < writers; writer++) {
            final int index = writer;
            final Thread thread = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int task = 0; task < perWriter; task++) {
                    final long taskId = (long) index * perWriter + task + 1L;
                    ring.record(taskId, "c" + index, ComputeTaskType.GENERAL, ComputePriority.NORMAL, "platform",
                        "t" + taskId, taskId, index, ComputeTaskHandle.State.COMPLETED);
                }
            }, "ring-writer-" + writer);
            threads.add(thread);
            thread.start();
        }
        start.countDown();
        for (final Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(10L));
            assertFalse(thread.isAlive(), "writer did not finish");
        }

        final List<SchedulerMetrics.CompletedTaskInfo> entries = ring.snapshot();

        assertEquals(RecentTaskRing.CAPACITY, entries.size());
        final Set<Long> taskIds = new HashSet<>();
        for (final SchedulerMetrics.CompletedTaskInfo entry : entries) {
            assertTrue(taskIds.add(entry.taskId()), "duplicate task id in the ring");
            assertTrue(entry.consumerId().startsWith("c"), entry.consumerId());
            assertEquals("platform", entry.lane());
            assertEquals(ComputeTaskHandle.State.COMPLETED, entry.state());
            assertEquals(Long.parseLong(entry.consumerId().substring(1)), entry.queueWaitNanos());
        }
    }
}
