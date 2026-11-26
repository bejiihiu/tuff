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

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.awaitUntil;
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
    void countersStayConsistentWithRunningAndQueuedTasks() throws Exception {
        final EngineTestSupport.SettingsBuilder builder = settings().fixedWorkers(2).capacity(64).noVirtualLane();
        final AsyncComputeScheduler compute = startedScheduler(builder.build(), new EngineTestSupport.FakeClock());
        final CountDownLatch running = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            for (int index = 0; index < 5; index++) {
                compute.submit(() -> {
                    running.countDown();
                    release.await();
                    return "held";
                });
            }
            assertTrue(running.await(10L, TimeUnit.SECONDS));

            // both workers are busy with a task and three more are queued: the invariant is checked
            // where every term is non zero instead of at rest
            final ConsumerMetrics busy = compute.metrics().consumer(AsyncComputeScheduler.DEFAULT_CONSUMER_ID).orElseThrow();
            assertEquals(5L, busy.submittedTasks());
            assertEquals(2, busy.activeTasks());
            assertEquals(3, busy.queuedTasks());

            awaitUntil("activeWorkers reports the two busy workers, not permits", Duration.ofSeconds(5), () -> {
                final SchedulerMetrics sample = compute.metrics();
                return sample.activeWorkers() == 2 && sample.idleWorkers() == 0;
            });
            final SchedulerMetrics live = compute.metrics();
            assertEquals(
                live.submittedTasks(),
                live.completedTasks() + live.failedTasks() + live.cancelledTasks()
                    + live.queuedTasks() + live.activeWorkers()
            );

            release.countDown();
            assertEventually("all tasks finish", Duration.ofSeconds(10), () -> {
                final SchedulerMetrics settled = compute.metrics();
                return settled.completedTasks() == 5L;
            });
        } finally {
            release.countDown();
            compute.shutdown();
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        }
    }

    @Test
    void rejectedSubmissionsAreNotCountedAsSubmitted() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).capacity(1).noVirtualLane().build(),
            new EngineTestSupport.FakeClock()
        );
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            compute.submit(() -> {
                running.countDown();
                release.await();
                return "blocker";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            compute.submit(() -> "queued");
            assertThrows(ComputeRejectedException.class, () -> compute.submit(() -> "overflow"));

            final SchedulerMetrics sample = compute.metrics();
            assertEquals(2L, sample.submittedTasks(), "the refused offer must not stay counted");
            assertEquals(1L, sample.rejectedTasks());
            assertEquals(1, sample.consumer(AsyncComputeScheduler.DEFAULT_CONSUMER_ID).orElseThrow().queuedTasks());

            release.countDown();
            assertEventually("accepted tasks finish", Duration.ofSeconds(10),
                () -> compute.metrics().completedTasks() == 2L);
        } finally {
            release.countDown();
            compute.shutdown();
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        }
    }

    @Test
    void mixedWorkloadKeepsEveryCounterConsistent() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).capacity(8).noVirtualLane().build(),
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

            // check the invariant while work is actually in flight, not only in the trivial 0/0 state
            assertEventually("the blocked body is visible as an active worker", Duration.ofSeconds(5), () -> {
                final SchedulerMetrics sample = compute.metrics();
                return sample.activeWorkers() == 1 && sample.queuedTasks() > 0;
            });
            final SchedulerMetrics inFlight = compute.metrics();
            assertEquals(1, inFlight.activeWorkers(), "one worker is inside the blocked body");
            assertTrue(inFlight.queuedTasks() > 0, "the fillers must still be queued");
            assertEquals(
                inFlight.submittedTasks(),
                inFlight.completedTasks() + inFlight.failedTasks() + inFlight.cancelledTasks()
                    + inFlight.queuedTasks() + inFlight.activeWorkers(),
                "the counters must add up while tasks are queued and running"
            );

            blockedRelease.countDown();
            assertEquals("second-blocker", secondBlocker.get(10L, TimeUnit.SECONDS));
            for (final CompletableFuture<String> filler : fillers) {
                assertEquals("filler", filler.get(10L, TimeUnit.SECONDS));
            }

            awaitUntil("the counters settle to submitted == terminal + queued + active",
                Duration.ofSeconds(10), () -> {
                    final SchedulerMetrics sample = compute.metrics();
                    return sample.submittedTasks() == sample.completedTasks() + sample.failedTasks()
                        + sample.cancelledTasks() + sample.queuedTasks() + sample.activeWorkers();
                });
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
