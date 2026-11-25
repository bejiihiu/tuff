package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Mixed lane traffic with cancellations and failures: every task runs at most once and the lane
 * counters stay consistent.
 */
@Timeout(120)
class EngineVirtualLaneStressTest {

    @Test
    void mixedTrafficRunsEachTaskAtMostOnce() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings()
                .fixedWorkers(4)
                .capacity(16_384)
                .virtualLane(true, true, 8, millis(20L), 1)
                .build(),
            new EngineTestSupport.FakeClock()
        );
        final int total = 500;
        final AtomicInteger[] runs = new AtomicInteger[total];
        for (int i = 0; i < total; i++) {
            runs[i] = new AtomicInteger();
        }
        final List<ComputeTaskHandle<?>> handles = new ArrayList<>(total);
        try {
            for (int i = 0; i < total; i++) {
                final int index = i;
                final ComputePriority priority = switch (i % 10) {
                    case 0, 1 -> ComputePriority.LOW;
                    case 2 -> ComputePriority.HIGH;
                    case 3 -> ComputePriority.CRITICAL;
                    default -> ComputePriority.NORMAL;
                };
                final boolean fails = i % 37 == 0;
                handles.add(compute.enqueue(new ComputeRequest<>(
                    "stress",
                    ComputeTaskType.GENERAL,
                    priority,
                    () -> {
                        final int attempt = runs[index].incrementAndGet();
                        if (attempt > 1) {
                            throw new IllegalStateException("task " + index + " ran twice");
                        }
                        if (fails) {
                            throw new IllegalArgumentException("planned failure " + index);
                        }
                        return Integer.valueOf(index);
                    },
                    "stress-" + index
                )));
            }
            // cancel a deterministic slice, some of which is still queued and some already running
            for (int i = 0; i < total; i += 13) {
                handles.get(i).cancel();
            }

            for (final ComputeTaskHandle<?> handle : handles) {
                try {
                    handle.future().get(60L, TimeUnit.SECONDS);
                } catch (Exception _) {
                    // cancelled and failed tasks are expected; the counters below are the assertion
                }
            }

            int ranAtMostOnce = 0;
            for (final AtomicInteger run : runs) {
                assertTrue(run.get() <= 1, "a task ran more than once");
                if (run.get() == 1) {
                    ranAtMostOnce++;
                }
            }
            assertTrue(ranAtMostOnce > 0);

            EngineTestSupport.assertEventually("lane counters settle", Duration.ofSeconds(10), () -> {
                final SchedulerMetrics metrics = compute.metrics();
                final SchedulerMetrics.VirtualLane lane = metrics.lanes().virtual();
                return metrics.submittedTasks()
                        == metrics.completedTasks() + metrics.failedTasks() + metrics.cancelledTasks()
                            + metrics.queuedTasks() + metrics.activeWorkers()
                    && lane.submitted()
                        == lane.completed() + lane.failed() + lane.cancelled() + lane.running();
            });
            final SchedulerMetrics metrics = compute.metrics();
            final SchedulerMetrics.VirtualLane lane = metrics.lanes().virtual();
            assertEquals(8, lane.limit());
            assertTrue(lane.spilled() <= lane.submitted());
        } finally {
            compute.shutdown();
        }
    }
}
