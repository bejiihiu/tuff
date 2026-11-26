package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler.SchedulerState;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeShutdownException;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Submissions racing a shutdown: every accepted handle must be terminal once the scheduler reached
 * {@code TERMINATED}, and refusals must be clean.
 */
@Timeout(120)
class EngineShutdownSubmitRaceTest {

    @Test
    void submissionsRacingShutdownAreAllTerminalAfterTermination() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(2).capacity(256).build(),
            new EngineTestSupport.FakeClock()
        );
        final List<ComputeTaskHandle<Integer>> accepted = new CopyOnWriteArrayList<>();
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicBoolean shuttingDown = new AtomicBoolean();
        final List<Thread> threads = new CopyOnWriteArrayList<>();

        for (int producer = 0; producer < 6; producer++) {
            final Thread thread = Thread.ofPlatform().daemon(true).unstarted(() -> {
                await(start);
                while (!shuttingDown.get()) {
                    try {
                        accepted.add(compute.enqueue(new ComputeRequest<>(
                            AsyncComputeScheduler.DEFAULT_CONSUMER_ID,
                            ComputeTaskType.GENERAL,
                            ComputePriority.NORMAL,
                            () -> 1
                        )));
                    } catch (ComputeRejectedException | ComputeShutdownException _) {
                        // expected once the queue is full or closed
                    }
                }
            });
            threads.add(thread);
            thread.start();
        }

        start.countDown();
        Thread.sleep(20L);
        compute.shutdown(Duration.ofMillis(100L));
        shuttingDown.set(true);
        for (final Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(30));
        }

        assertTrue(compute.awaitTermination(Duration.ofSeconds(10)), "shutdown must terminate");
        assertEquals(SchedulerState.TERMINATED, compute.state());
        for (final ComputeTaskHandle<Integer> handle : accepted) {
            assertTrue(handle.isDone(), "accepted task " + handle.taskId() + " is still pending after TERMINATED");
        }
        assertEquals(0, compute.metrics().consumer(AsyncComputeScheduler.DEFAULT_CONSUMER_ID)
            .orElseThrow()
            .queuedTasks());
        final var metrics = compute.metrics();
        assertEquals(
            metrics.submittedTasks(),
            metrics.completedTasks() + metrics.failedTasks() + metrics.cancelledTasks(),
            "submitted must equal terminal work after shutdown"
        );
    }

    private static void await(final CountDownLatch latch) {
        try {
            if (!latch.await(30L, TimeUnit.SECONDS)) {
                throw new IllegalStateException("start latch timed out");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", interrupted);
        }
    }
}
