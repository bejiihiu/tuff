package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler.SchedulerState;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeShutdownException;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.awaitNoEngineThreads;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.awaitUntil;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.cancellationOf;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.engine;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.engineThreadNames;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.scheduler;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lifecycle contract of the facade and the engine.
 */
@Timeout(60)
class EngineLifecycleTest {

    @Test
    void newEngineAcceptsNoWorkAndOwnsNoThread() {
        final EngineTestSupport.SettingsBuilder builder = settings().noVirtualLane();
        final ComputeSettings configuration = builder.build();
        final AsyncComputeScheduler compute = scheduler(engine(configuration, new EngineTestSupport.FakeClock()));
        assertEquals(SchedulerState.NEW, compute.state());
        assertFalse(compute.isRunning());
        assertTrue(engineThreadNames(builder.prefix()).isEmpty(), "a NEW scheduler must not hold threads");
        assertThrows(ComputeShutdownException.class, () -> compute.submit(() -> 1));
        assertEquals(0L, compute.metrics().submittedTasks());
        compute.shutdown();
        assertEquals(SchedulerState.TERMINATED, compute.state());
        assertTrue(assertAwait(compute, Duration.ofSeconds(2)));
        assertTrue(engineThreadNames(builder.prefix()).isEmpty());
    }

    @Test
    void startIsIdempotentAndTerminationIsFinal() throws Exception {
        final EngineTestSupport.SettingsBuilder builder = settings().fixedWorkers(2).noVirtualLane();
        final AsyncComputeScheduler compute = startedScheduler(builder.build(), new EngineTestSupport.FakeClock());
        final String prefix = builder.prefix();
        compute.start();
        compute.start();
        assertEquals(SchedulerState.RUNNING, compute.state());
        assertTrue(compute.isRunning());

        final ComputeTaskHandle<Integer> handle = compute.enqueue(ComputeRequest.of(() -> 41 + 1));
        assertEquals(42, handle.future().get(10L, TimeUnit.SECONDS));
        assertEquals(ComputeTaskHandle.State.COMPLETED, handle.state());
        assertTrue(handle.isDone());
        assertTrue(handle.queueWaitNanos() >= 0L);
        assertTrue(handle.executionNanos() >= 0L);
        assertEquals(AsyncComputeScheduler.DEFAULT_CONSUMER_ID, handle.consumerId());
        assertTrue(compute.consumer(AsyncComputeScheduler.DEFAULT_CONSUMER_ID).isPresent());

        compute.shutdown(Duration.ofSeconds(2));
        assertTrue(assertAwait(compute, Duration.ofSeconds(10)));
        assertEquals(SchedulerState.TERMINATED, compute.state());
        assertThrows(ComputeShutdownException.class, () -> compute.submit(() -> 1));
        assertThrows(ComputeShutdownException.class, compute::start);
        assertThrows(ComputeShutdownException.class, () -> compute.registerConsumer(
            ComputeConsumer.of("late", ConsumerPolicy.defaults())
        ));
        awaitNoEngineThreads(prefix, Duration.ofSeconds(5));
    }

    @Test
    void gracefulShutdownLetsTheRunningTaskFinishAndCancelsTheQueue() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger executed = new AtomicInteger();
        final EngineTestSupport.SettingsBuilder builder = settings().fixedWorkers(1).grace(millis(100L)).noVirtualLane();
        final String prefix = builder.prefix();
        final AsyncComputeScheduler compute = startedScheduler(builder.build(), new EngineTestSupport.FakeClock());
        final CompletableFuture<Integer> blocker = compute.submit(() -> {
            running.countDown();
            release.await();
            executed.incrementAndGet();
            return 1;
        });
        assertTrue(running.await(10L, TimeUnit.SECONDS));
        final CompletableFuture<Integer> queued = compute.submit(() -> {
            executed.incrementAndGet();
            return 2;
        });

        compute.shutdown(millis(100L));

        final ComputeCancelledException cancelled = cancellationOf(queued);
        assertEquals(ComputeCancelledException.Reason.SHUTDOWN, cancelled.reason());
        release.countDown();
        assertEquals(1, blocker.get(10L, TimeUnit.SECONDS));
        assertTrue(assertAwait(compute, Duration.ofSeconds(10)));
        assertEquals(1, executed.get());
        awaitNoEngineThreads(prefix, Duration.ofSeconds(5));
    }

    @Test
    void shutdownNowCancelsTheQueueImmediatelyButNeverInterruptsRunningWork() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final EngineTestSupport.SettingsBuilder builder = settings().fixedWorkers(1).noVirtualLane();
        final String prefix = builder.prefix();
        final AsyncComputeSchedulerImpl compute = new AsyncComputeSchedulerImpl(engine(
            builder.build(),
            new EngineTestSupport.FakeClock()
        ));
        compute.start();
        final CompletableFuture<Integer> blocker = compute.submit(() -> {
            running.countDown();
            release.await();
            return 7;
        });
        assertTrue(running.await(10L, TimeUnit.SECONDS));
        final CompletableFuture<Integer> queued = compute.submit(() -> 8);

        compute.shutdownNow();

        final ComputeCancelledException cancelled = cancellationOf(queued);
        assertEquals(ComputeCancelledException.Reason.SHUTDOWN, cancelled.reason());
        release.countDown();
        assertEquals(7, blocker.get(10L, TimeUnit.SECONDS));
        assertTrue(assertAwait(compute, Duration.ofSeconds(10)));
        awaitNoEngineThreads(prefix, Duration.ofSeconds(5));
    }

    @Test
    void disabledSchedulerRunsWithoutThreadsAndRejectsWithItsOwnReason() {
        final EngineTestSupport.SettingsBuilder builder = settings().enabled(false).noVirtualLane();
        final String prefix = builder.prefix();
        final AsyncComputeScheduler compute = startedScheduler(builder.build(), new EngineTestSupport.FakeClock());
        assertEquals(SchedulerState.RUNNING, compute.state());
        assertTrue(engineThreadNames(prefix).isEmpty(), "a disabled scheduler must not create threads");
        final ComputeRejectedException rejected = assertThrows(
            ComputeRejectedException.class,
            () -> compute.submit(() -> 1)
        );
        assertEquals(ComputeRejectedException.Reason.SCHEDULER_DISABLED, rejected.reason());
        compute.shutdown();
        assertEquals(SchedulerState.TERMINATED, compute.state());
        assertTrue(engineThreadNames(prefix).isEmpty());
    }

    @Test
    void shutdownIsIdempotentInEveryOrder() {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(2).noVirtualLane().build(),
            new EngineTestSupport.FakeClock()
        );
        compute.shutdown();
        compute.shutdown();
        compute.shutdown(Duration.ofSeconds(1));
        assertTrue(assertAwait(compute, Duration.ofSeconds(10)));
    }

    @Test
    void graceExpiryTerminatesAndLetsTheRunningTaskFinishLater() throws Exception {
        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final EngineTestSupport.SettingsBuilder builder = settings().fixedWorkers(1).grace(millis(10L)).noVirtualLane();
        final String prefix = builder.prefix();
        final AsyncComputeScheduler compute = startedScheduler(builder.build(), new EngineTestSupport.FakeClock());
        try {
            final CompletableFuture<String> future = compute.submit(() -> {
                running.countDown();
                release.await();
                return "finished";
            });
            assertTrue(running.await(10L, TimeUnit.SECONDS));

            final long startNanos = System.nanoTime();
            compute.shutdown(millis(10L));
            assertTrue(assertAwait(compute, Duration.ofSeconds(5)), "termination must not wait for the body");
            assertTrue(System.nanoTime() - startNanos < TimeUnit.SECONDS.toNanos(5L));
            assertEquals(SchedulerState.TERMINATED, compute.state());
            assertFalse(future.isDone(), "an abandoned task must not be completed falsely");

            release.countDown();
            assertEquals("finished", future.get(10L, TimeUnit.SECONDS));
            awaitUntil("the abandoned task is accounted after it finishes", Duration.ofSeconds(10), () -> {
                final var metrics = compute.metrics();
                return metrics.submittedTasks() == 1L && metrics.completedTasks() == 1L;
            });
            assertEquals(1L, compute.metrics().submittedTasks());
            assertEquals(1L, compute.metrics().completedTasks());
            assertEquals(0L, compute.metrics().cancelledTasks());
            assertEquals(0L, compute.metrics().failedTasks());
            awaitNoEngineThreads(prefix, Duration.ofSeconds(5));
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    private static boolean assertAwait(final AsyncComputeScheduler compute, final Duration timeout) {
        try {
            return compute.awaitTermination(timeout);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", interrupted);
        }
    }
}
