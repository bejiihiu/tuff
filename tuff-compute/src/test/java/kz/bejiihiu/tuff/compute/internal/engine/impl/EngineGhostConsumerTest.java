package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeShutdownException;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.startedScheduler;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Ghost consumer regression: a submission racing an unregister must either run, be cancelled or be
 * rejected, never hang in a stream the registry no longer knows.
 */
@Timeout(120)
class EngineGhostConsumerTest {

    @Test
    void submissionsRacingUnregisterAlwaysTerminalize() throws Exception {
        final AsyncComputeScheduler compute = startedScheduler(
            settings().fixedWorkers(2).dynamicRegistration(false).build(),
            new EngineTestSupport.FakeClock()
        );
        try {
            compute.registerConsumer(ComputeConsumer.of("cycled", ConsumerPolicy.defaults()));
            final List<ComputeTaskHandle<Integer>> accepted = new CopyOnWriteArrayList<>();
            final AtomicInteger rejected = new AtomicInteger();
            final AtomicBoolean stop = new AtomicBoolean();
            final int producers = 6;
            final CountDownLatch start = new CountDownLatch(1);
            final List<Thread> threads = new CopyOnWriteArrayList<>();

            for (int producer = 0; producer < producers; producer++) {
                final Thread thread = Thread.ofPlatform().daemon(true).unstarted(() -> {
                    await(start);
                    for (int index = 0; index < 400 && !stop.get(); index++) {
                        try {
                            accepted.add(compute.enqueue(new ComputeRequest<>(
                                "cycled",
                                ComputeTaskType.GENERAL,
                                ComputePriority.NORMAL,
                                () -> 1
                            )));
                        } catch (ComputeRejectedException | ComputeShutdownException _) {
                            rejected.incrementAndGet();
                        }
                    }
                });
                threads.add(thread);
                thread.start();
            }
            final Thread churn = Thread.ofPlatform().daemon(true).unstarted(() -> {
                await(start);
                for (int round = 0; round < 80 && !stop.get(); round++) {
                    compute.unregisterConsumer("cycled");
                    try {
                        compute.registerConsumer(ComputeConsumer.of("cycled", ConsumerPolicy.defaults()));
                    } catch (ComputeShutdownException _) {
                        return;
                    }
                }
            });
            threads.add(churn);
            churn.start();

            start.countDown();
            for (final Thread thread : threads) {
                thread.join(TimeUnit.SECONDS.toMillis(60));
            }
            stop.set(true);
            compute.unregisterConsumer("cycled");

            assertEventually("every accepted handle reaches a terminal state", Duration.ofSeconds(10), () -> {
                for (final ComputeTaskHandle<Integer> handle : accepted) {
                    if (!handle.isDone()) {
                        return false;
                    }
                }
                return true;
            });
            assertEventually("no queued work is left behind", Duration.ofSeconds(10),
                () -> compute.metrics().queuedTasks() == 0);
            assertEventually("counters balance", Duration.ofSeconds(10), () -> {
                final var metrics = compute.metrics();
                return metrics.submittedTasks() == metrics.completedTasks() + metrics.failedTasks()
                    + metrics.cancelledTasks();
            });
            assertTrue(rejected.get() >= 0);
            assertEventually("counters balance", Duration.ofSeconds(10), () -> {
                final var metrics = compute.metrics();
                return metrics.submittedTasks() == metrics.completedTasks() + metrics.failedTasks()
                    + metrics.cancelledTasks();
            });
            assertEventually("no busy workers remain", Duration.ofSeconds(10),
                () -> compute.metrics().activeWorkers() == 0);
        } finally {
            compute.shutdown();
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        }
    }

    private static void await(final CountDownLatch latch) {
        try {
            if (!latch.await(30L, TimeUnit.SECONDS)) {
                throw new IllegalStateException("test start latch timed out");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", interrupted);
        }
    }
}
