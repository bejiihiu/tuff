package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.engine;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millisNanos;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runtime processor budget: the third factory argument and {@code applyProcessorBudget} are budgets,
 * not raw core counts, and a budget change reshapes an automatically sized pool without losing work.
 */
@Timeout(90)
class EngineProcessorBudgetTest {

    @Test
    void budgetChangesTheAutomaticPoolAndItsLimits() throws Exception {
        final EngineTestSupport.SettingsBuilder builder = settings()
            .autoWorkers(1, -1, Duration.ofSeconds(30))
            .metrics(new ComputeSettings.Metrics(true, false, Duration.ofSeconds(10), false, millis(20L)))
            .grace(Duration.ofSeconds(1));
        final ComputeEngineImpl engine = engine(builder.build(), new EngineTestSupport.FakeClock(), 8);
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        final CountDownLatch release = new CountDownLatch(1);
        compute.start();
        try {
            final List<CompletableFuture<Integer>> futures = new ArrayList<>();
            for (int index = 0; index < 6; index++) {
                futures.add(compute.submit(() -> {
                    release.await();
                    return 1;
                }));
            }
            assertEventually("the pool grows under the backlog", Duration.ofSeconds(5), () -> engine.desiredWorkers() >= 4);

            engine.applyProcessorBudget(3);
            assertEventually("a smaller budget caps the pool", Duration.ofSeconds(5), () ->
                engine.desiredWorkers() <= 3 && engine.totalLimit() <= 3 && engine.totalLimit() >= 1);

            engine.applyProcessorBudget(8);
            assertEventually("a bigger budget allows growth again", Duration.ofSeconds(5), () ->
                engine.desiredWorkers() >= 4 && engine.totalLimit() >= 4);

            release.countDown();
            for (final CompletableFuture<Integer> future : futures) {
                assertEquals(1, future.get(10L, TimeUnit.SECONDS));
            }
            assertEventually("no queued work was lost", Duration.ofSeconds(5), () -> compute.metrics().queuedTasks() == 0);
        } finally {
            release.countDown();
            compute.shutdown();
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        }
    }

    @Test
    void budgetIsClampedByTheContractAndTheConstructionCeiling() throws Exception {
        final EngineTestSupport.FakeClock clock = new EngineTestSupport.FakeClock();
        final ComputeEngineImpl engine = engine(
            settings()
                .autoWorkers(1, -1, Duration.ofSeconds(30))
                .metrics(new ComputeSettings.Metrics(true, false, Duration.ofSeconds(10), false, millis(20L)))
                .build(),
            clock,
            4
        );
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        final CountDownLatch release = new CountDownLatch(1);
        compute.start();
        try {
            for (int index = 0; index < 6; index++) {
                compute.submit(() -> {
                    release.await();
                    return 1;
                });
            }
            assertEventually("the backlog fills the initial pool", Duration.ofSeconds(5), () -> engine.desiredWorkers() >= 3);

            engine.applyProcessorBudget(100);
            assertEventually("the pool never grows above its construction ceiling", Duration.ofSeconds(5), () ->
                engine.desiredWorkers() == 4 && engine.totalLimit() == 4);

            engine.applyProcessorBudget(1);
            assertEventually("a tiny budget shrinks to the contract floor", Duration.ofSeconds(5), () ->
                engine.desiredWorkers() == 1 && engine.totalLimit() == 1);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void explicitMaximumStillWinsOverTheBudget() throws Exception {
        final ComputeEngineImpl engine = engine(
            settings()
                .autoWorkers(1, 5, Duration.ofSeconds(30))
                .metrics(new ComputeSettings.Metrics(true, false, Duration.ofSeconds(10), false, millis(20L)))
                .build(),
            new EngineTestSupport.FakeClock(),
            8
        );
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        final CountDownLatch release = new CountDownLatch(1);
        compute.start();
        try {
            for (int index = 0; index < 6; index++) {
                compute.submit(() -> {
                    release.await();
                    return 1;
                });
            }
            engine.applyProcessorBudget(100);
            assertEventually("explicit maxThreads is the ceiling", Duration.ofSeconds(5), () ->
                engine.desiredWorkers() == 5 && engine.totalLimit() == 5);
        } finally {
            release.countDown();
            compute.shutdown();
        }
    }

    @Test
    void minimumWinsOverASmallBudget() {
        final ComputeEngineImpl engine = engine(
            settings().autoWorkers(4, -1, Duration.ofSeconds(30)).build(),
            new EngineTestSupport.FakeClock(),
            8
        );
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        compute.start();
        try {
            engine.applyProcessorBudget(2);
            assertTrue(engine.desiredWorkers() >= 4, "configured minimum must be kept");
            assertTrue(engine.totalLimit() >= 4);
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void fixedPoolIgnoresTheBudget() {
        final ComputeEngineImpl engine = engine(
            settings().fixedWorkers(3).build(),
            new EngineTestSupport.FakeClock(),
            8
        );
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        compute.start();
        try {
            engine.applyProcessorBudget(1);
            engine.applyProcessorBudget(100);
            assertEquals(3, engine.desiredWorkers());
            assertEquals(3, engine.totalLimit());
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void budgetOutsideRunningStateIsANoOp() {
        final ComputeEngineImpl engine = engine(
            settings().autoWorkers(1, -1, Duration.ofSeconds(30)).build(),
            new EngineTestSupport.FakeClock(),
            8
        );
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        engine.applyProcessorBudget(2);
        assertEquals(0, engine.desiredWorkers(), "before start there is no pool to resize");
        compute.start();
        assertTrue(engine.desiredWorkers() >= 1);
        compute.shutdown();
        try {
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
        engine.applyProcessorBudget(1);
        assertEquals(AsyncComputeScheduler.SchedulerState.TERMINATED, compute.state());
    }

    @Test
    void adaptiveDecisionsNeverExceedTheBudget() {
        final EngineTestSupport.FakeClock clock = new EngineTestSupport.FakeClock();
        final ComputeEngineImpl engine = engine(
            settings()
                .autoWorkers(1, 8, Duration.ofSeconds(30))
                .adaptive(new ComputeSettings.Adaptive(true, 1, 8, millis(100L), 5, 0.5d, 0.5d, 0.9d))
                .build(),
            clock,
            8
        );
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        compute.start();
        try {
            engine.applyProcessorBudget(3);
            assertTrue(engine.totalLimit() <= 2, "the budget target caps the applied limit");
            assertTrue(engine.desiredWorkers() <= 2, "the pool follows the applied limit");

            for (int sample = 0; sample < 1024; sample++) {
                engine.adaptiveController().recordSample(0L, millisNanos(1L), false);
            }
            clock.advance(Duration.ofSeconds(2));
            engine.evaluateAdaptiveNow();
            engine.evaluateAdaptiveNow();
            assertTrue(engine.totalLimit() <= 2, "the controller must not fight the budget ceiling");
            assertTrue(engine.desiredWorkers() <= 2);
        } finally {
            compute.shutdown();
        }
    }

    @Test
    void nonPositiveBudgetIsRejected() {
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine(
            settings().autoWorkers(1, -1, Duration.ofSeconds(30)).build(),
            new EngineTestSupport.FakeClock(),
            8
        ));
        assertThrows(IllegalArgumentException.class, () -> compute.applyProcessorBudget(0));
        assertThrows(IllegalArgumentException.class, () -> compute.applyProcessorBudget(-3));
    }
}
