package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cooperative cancellation semantics, queued and running.
 */
@Timeout(60)
class EngineCancellationTest {

    @Test
    void cancellingAQueuedTaskRemovesItAndCompletesTheFuture() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean victimRan = new AtomicBoolean();
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final CompletableFuture<String> blocker = compute.submit(() -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final ComputeTaskHandle<String> victim = compute.enqueue(
                ComputeRequest.of(() -> {
                    victimRan.set(true);
                    return "victim";
                })
            );
            assertEquals(ComputeTaskHandle.State.QUEUED, victim.state());
            assertFalse(victim.isCancellationRequested());

            assertTrue(victim.cancel());
            assertEquals(ComputeTaskHandle.State.CANCELLED, victim.state());
            assertTrue(victim.isCancelled());
            assertTrue(victim.isCancellationRequested());
            assertTrue(victim.isDone());
            assertEquals(ComputeCancelledException.Reason.CALLER, cancellationOf(victim).reason());
            assertFalse(victim.cancel(), "a second cancel must report false");

            release.countDown();
            assertEquals("blocked", blocker.get(10L, TimeUnit.SECONDS));
            assertFalse(victimRan.get());
            assertEquals(1L, compute.metrics().cancelledTasks());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void cancellingARunningTaskDiscardsTheResultWithoutInterruptingIt() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean bodyFinished = new AtomicBoolean();
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final ComputeTaskHandle<String> handle = compute.enqueue(
                ComputeRequest.of(() -> {
                    running.countDown();
                    release.await();
                    bodyFinished.set(true);
                    return "late result";
                })
            );
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            assertEquals(ComputeTaskHandle.State.RUNNING, handle.state());

            assertTrue(handle.cancel());
            assertTrue(handle.isCancellationRequested());
            assertFalse(handle.future().isDone(), "a running task is only flagged, not terminated");

            release.countDown();
            assertEquals(ComputeCancelledException.Reason.CALLER, cancellationOf(handle).reason());
            assertTrue(bodyFinished.get());
            assertEquals(ComputeTaskHandle.State.CANCELLED, handle.state());
            assertTrue(handle.isCancelled());
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void futureCancelIsEquivalentToHandleCancel() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final CompletableFuture<String> blocker = compute.submit(() -> {
                running.countDown();
                release.await();
                return "blocked";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));
            final ComputeTaskHandle<String> victim = compute.enqueue(
                ComputeRequest.of(() -> "victim")
            );
            assertTrue(victim.future().cancel(true));
            assertEquals(ComputeTaskHandle.State.CANCELLED, victim.state());
            assertTrue(victim.future().isCancelled());
            assertEquals(ComputeCancelledException.Reason.CALLER, cancellationOf(victim).reason());
            release.countDown();
            blocker.get(10L, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void completedTaskCannotBeCancelled() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(2).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            final ComputeTaskHandle<String> handle = compute.enqueue(
                ComputeRequest.of(() -> "done")
            );
            assertEquals("done", handle.future().get(10L, TimeUnit.SECONDS));
            assertFalse(handle.cancel());
            assertFalse(handle.future().cancel(true));
            assertEquals(ComputeTaskHandle.State.COMPLETED, handle.state());
            assertFalse(handle.isCancellationRequested());
        } finally {
            compute.shutdown();
        }
    }

    private static ComputeCancelledException cancellationOf(final ComputeTaskHandle<?> handle) {
        return EngineTestSupport.cancellationOf(handle.future());
    }
}
