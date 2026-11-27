package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import kz.bejiihiu.tuff.compute.RegionRelief;
import kz.bejiihiu.tuff.compute.RegionReliefSettings;
import kz.bejiihiu.tuff.compute.internal.spi.ComputeThreadFactoryPort;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Startup sizing and pool growth rules of the engine.
 *
 * <p>Both tests drive the real pool with a fixed budget and a frozen clock, so the worker counts are
 * the same on a two core runner and on a sixty four core box.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class EngineSizingRulesTest {

    @Test
    void aDeferredStartSpawnsAtTheFirstBudgetAndNeverReportsTheTransient() throws Exception {
        final ComputeSettings settings = EngineTestSupport.settings()
            .autoWorkers(2, -1, Duration.ofSeconds(30L))
            .build();
        final ComputeEngineImpl engine = new ComputeEngineImpl(
            settings,
            new EngineTestSupport.FakeClock(),
            12,
            ComputeThreadFactoryPort.legacy(),
            RegionReliefSettings.defaults(),
            true
        );
        final AsyncComputeScheduler scheduler = EngineTestSupport.scheduler(engine);
        try {
            scheduler.start();
            assertEquals(0, scheduler.metrics().totalWorkers(), "a deferred pool starts without workers");
            assertEquals(0, scheduler.metrics().peakWorkers(), "the provisional budget must not be recorded as a peak");

            final CompletableFuture<Integer> queued = scheduler.submit(() -> 42);
            assertFalse(queued.isDone(), "work waits for the pool instead of being refused");

            scheduler.applyProcessorBudget(2);

            assertEquals(2, engine.desiredWorkers(), "the pool spawns once, at the budgeted size");
            EngineTestSupport.awaitUntil("the budgeted workers exist", Duration.ofSeconds(10L),
                () -> scheduler.metrics().totalWorkers() == 2);
            assertEquals(42, queued.get(10L, TimeUnit.SECONDS));
            assertEquals(2, scheduler.metrics().peakWorkers(),
                "the peak is the budgeted size, never the provisional one");
        } finally {
            scheduler.shutdown(Duration.ZERO);
            scheduler.awaitTermination(Duration.ofSeconds(5L));
        }
    }

    @Test
    void reliefBacklogDoesNotGrowThePool() throws Exception {
        final CountDownLatch release = new CountDownLatch(1);
        final ComputeEngineImpl engine = this.engine();
        final AsyncComputeScheduler scheduler = EngineTestSupport.scheduler(engine);
        scheduler.start();
        try {
            // one worker is busy, so the next submission is what a normal consumer would grow on
            final CompletableFuture<Boolean> running = scheduler.submit(ComputePriority.NORMAL, blocking(release));
            EngineTestSupport.awaitUntil("the first task runs", Duration.ofSeconds(10L),
                () -> scheduler.metrics().activeWorkers() == 1);

            scheduler.submit(RegionRelief.CONSUMER_ID, ComputeTaskType.CUSTOM, ComputePriority.NORMAL, blocking(release));
            assertEquals(1, engine.desiredWorkers(),
                "relief work is shed work: it must not justify a bigger pool");

            // the control: ordinary backlog of the same shape does grow the pool
            scheduler.submit(ComputePriority.NORMAL, blocking(release));
            assertEquals(2, engine.desiredWorkers(),
                "ordinary backlog still grows the pool, so the exclusion is specific to relief");

            release.countDown();
            running.get(10L, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            scheduler.shutdown(Duration.ZERO);
            scheduler.awaitTermination(Duration.ofSeconds(5L));
        }
    }

    private ComputeEngineImpl engine() {
        final ComputeSettings settings = EngineTestSupport.settings()
            .autoWorkers(1, 4, Duration.ofSeconds(30L))
            .build();
        return new ComputeEngineImpl(settings, new EngineTestSupport.FakeClock(), 2);
    }

    private static ComputeTask<Boolean> blocking(final CountDownLatch release) {
        return () -> {
            if (!release.await(10L, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the test never released the task");
            }
            return Boolean.TRUE;
        };
    }
}
