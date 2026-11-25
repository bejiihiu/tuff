package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ConsumerMetrics;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The counter invariant {@code submitted == completed + failed + cancelled + queued + active} must
 * hold after a mixed workload, globally and per consumer.
 */
@Timeout(90)
class EngineMetricsConsistencyTest {

    @Test
    void mixedWorkloadKeepsEveryCounterConsistent() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).capacity(8).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            for (int index = 0; index < 20; index++) {
                assertEquals("ok", compute.submit(() -> "ok").get(10L, TimeUnit.SECONDS));
            }
            for (int index = 0; index < 10; index++) {
                final CompletableFuture<String> failed = compute.submit(() -> {
                    throw new IllegalStateException("expected failure");
                });
                assertThrows(CompletionException.class, failed::join);
            }

            final CountDownLatch running = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final CompletableFuture<String> blocker = compute.submit(() -> {
                running.countDown();
                release.await();
                return "blocker";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final List<ComputeTaskHandle<String>> queued = new ArrayList<>();
            for (int index = 0; index < 5; index++) {
                queued.add(compute.enqueue(ComputeRequest.of(() -> "queued")));
            }
            for (int index = 0; index < 3; index++) {
                assertTrue(queued.get(index).cancel());
            }
            release.countDown();
            assertEquals("blocker", blocker.get(10L, TimeUnit.SECONDS));
            for (int index = 3; index < 5; index++) {
                assertEquals("queued", queued.get(index).future().get(10L, TimeUnit.SECONDS));
            }

            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch runningRelease = new CountDownLatch(1);
            final ComputeTaskHandle<String> runningHandle = compute.enqueue(
                ComputeRequest.of(() -> {
                    started.countDown();
                    runningRelease.await();
                    return "discarded";
                })
            );
            assertTrue(started.await(10L, TimeUnit.SECONDS));
            assertTrue(runningHandle.cancel());
            runningRelease.countDown();
            assertEquals(ComputeCancelledException.Reason.CALLER, EngineTestSupport.cancellationOf(runningHandle.future()).reason());

            final CountDownLatch blocked = new CountDownLatch(1);
            final CountDownLatch blockedRelease = new CountDownLatch(1);
            final CompletableFuture<String> secondBlocker = compute.submit(() -> {
                blocked.countDown();
                blockedRelease.await();
                return "second-blocker";
            });
            assertTrue(blocked.await(10L, TimeUnit.SECONDS));
            final List<CompletableFuture<String>> fillers = new ArrayList<>();
            for (int index = 0; index < 8; index++) {
                fillers.add(compute.submit(() -> "filler"));
            }
            final ComputeRejectedException rejected = assertThrows(
                ComputeRejectedException.class,
                () -> compute.submit(() -> "overflow")
            );
            assertEquals(ComputeRejectedException.Reason.QUEUE_FULL, rejected.reason());
            blockedRelease.countDown();
            assertEquals("second-blocker", secondBlocker.get(10L, TimeUnit.SECONDS));
            for (final CompletableFuture<String> filler : fillers) {
                assertEquals("filler", filler.get(10L, TimeUnit.SECONDS));
            }

            final long settleDeadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            boolean settled = false;
            while (!settled && System.nanoTime() < settleDeadline) {
                final SchedulerMetrics sample = compute.metrics();
                settled = sample.submittedTasks() == sample.completedTasks() + sample.failedTasks()
                    + sample.cancelledTasks() + sample.queuedTasks() + sample.activeWorkers();
                if (!settled) {
                    Thread.sleep(1L);
                }
            }
            assertTrue(settled, "counters must settle to submitted == terminal + queued + active");
            final SchedulerMetrics metrics = compute.metrics();
            assertEquals(46L, metrics.submittedTasks());
            assertEquals(32L, metrics.completedTasks());
            assertEquals(10L, metrics.failedTasks());
            assertEquals(4L, metrics.cancelledTasks());
            assertEquals(1L, metrics.rejectedTasks());
            assertEquals(
                metrics.submittedTasks(),
                metrics.completedTasks() + metrics.failedTasks() + metrics.cancelledTasks()
                    + metrics.queuedTasks() + metrics.activeWorkers()
            );

            final ConsumerMetrics consumer = metrics.consumer(AsyncComputeScheduler.DEFAULT_CONSUMER_ID).orElseThrow();
            assertEquals(46L, consumer.submittedTasks());
            assertEquals(32L, consumer.completedTasks());
            assertEquals(10L, consumer.failedTasks());
            assertEquals(4L, consumer.cancelledTasks());
            assertEquals(0, consumer.queuedTasks());
            assertEquals(0, consumer.activeTasks());
        } finally {
            compute.shutdown();
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        }
    }
}
