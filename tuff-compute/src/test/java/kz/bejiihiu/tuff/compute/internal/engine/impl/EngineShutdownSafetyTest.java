package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler.SchedulerState;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.engineThreadNames;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * User continuations must never run on scheduler service threads, and shutdown must terminate even
 * when one of them blocks forever.
 */
@Timeout(120)
class EngineShutdownSafetyTest {

    @Test
    void blockingContinuationOfAnExpiredTaskCannotWedgeShutdown() throws Exception {
        final EngineTestSupport.SettingsBuilder builder = settings()
            .fixedWorkers(1)
            .metrics(new ComputeSettings.Metrics(true, false, Duration.ofSeconds(10), false, millis(20L)))
            .grace(millis(100L));
        final AsyncComputeScheduler compute = startedScheduler(builder.build(), ComputeClock.system());
        final CountDownLatch workerBusy = new CountDownLatch(1);
        final CountDownLatch releaseWorker = new CountDownLatch(1);
        final CountDownLatch continuationStarted = new CountDownLatch(1);
        final AtomicBoolean releaseContinuation = new AtomicBoolean();

        compute.registerConsumer(ComputeConsumer.of("expiring", new ConsumerPolicy(
            true,
            ConsumerPolicy.defaults().concurrencyLimit(),
            -1,
            1.0d,
            OverflowPolicy.REJECT,
            millis(500L)
        )));
        try {
            compute.submit(
                "expiring",
                ComputeTaskType.GENERAL,
                ComputePriority.NORMAL,
                () -> {
                    workerBusy.countDown();
                    releaseWorker.await();
                    return "blocked";
                }
            );
            assertTrue(workerBusy.await(10L, TimeUnit.SECONDS));
            final CompletableFuture<String> expired = compute.submit(
                "expiring",
                ComputeTaskType.GENERAL,
                ComputePriority.NORMAL,
                () -> "expired"
            );
            expired.whenComplete((value, failure) -> {
                continuationStarted.countDown();
                // typical plugin mistake: a dependent stage that waits uninterruptibly on something
                // that will never happen
                while (!releaseContinuation.get()) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
                }
            });

            assertTrue(continuationStarted.await(10L, TimeUnit.SECONDS), "the janitor must expire the queued task");

            final long startNanos = System.nanoTime();
            compute.shutdown(millis(100L));
            final boolean terminated = compute.awaitTermination(Duration.ofSeconds(5));
            assertTrue(terminated, "shutdown must be bounded even with a blocked continuation");
            assertTrue(System.nanoTime() - startNanos < TimeUnit.SECONDS.toNanos(5L));
            assertEquals(SchedulerState.TERMINATED, compute.state());
            assertTrue(engineThreadNames(builder.prefix() + "-janitor").isEmpty(), "the janitor must not stay alive");
            assertEventually("expiry is counted", Duration.ofSeconds(5), () -> compute.metrics().cancelledTasks() == 1L);
        } finally {
            releaseContinuation.set(true);
            releaseWorker.countDown();
            compute.shutdown();
        }
    }

    @Test
    void evictionCompletionDoesNotRunOnTheSubmittingThread() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(1).capacity(100).build(),
            new EngineTestSupport.FakeClock()
        );
        final CountDownLatch workerBusy = new CountDownLatch(1);
        final CountDownLatch releaseWorker = new CountDownLatch(1);
        final CountDownLatch continuation = new CountDownLatch(1);
        final AtomicReference<Thread> continuationThread = new AtomicReference<>();
        try {
            compute.registerConsumer(ComputeConsumer.of("dropper", new ConsumerPolicy(
                true,
                ConsumerPolicy.defaults().concurrencyLimit(),
                1,
                1.0d,
                OverflowPolicy.DROP_OLDEST,
                Duration.ZERO
            )));
            final CompletableFuture<String> blocker = compute.submit(
                "dropper",
                ComputeTaskType.GENERAL,
                ComputePriority.NORMAL,
                () -> {
                    workerBusy.countDown();
                    releaseWorker.await();
                    return "blocked";
                }
            );
            assertTrue(workerBusy.await(10L, TimeUnit.SECONDS));
            final CompletableFuture<String> victim = compute.submit(
                "dropper",
                ComputeTaskType.GENERAL,
                ComputePriority.NORMAL,
                () -> "victim"
            );
            victim.whenComplete((value, failure) -> {
                continuationThread.set(Thread.currentThread());
                continuation.countDown();
            });

            // this offer evicts the victim; the dependent stage must not run inside it
            compute.submit("dropper", ComputeTaskType.GENERAL, ComputePriority.NORMAL, () -> "survivor");

            assertTrue(continuation.await(10L, TimeUnit.SECONDS), "the evicted future must complete");
            assertTrue(
                continuationThread.get() != Thread.currentThread(),
                "user continuation ran on the submitting thread"
            );
            releaseWorker.countDown();
            blocker.get(10L, TimeUnit.SECONDS);
        } finally {
            releaseWorker.countDown();
            compute.shutdown();
        }
    }
}
