package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.engine;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The quota capacity is the platform pool plus the virtual lane permits, and it is recomputed on
 * every pool change.
 *
 * <p>The tests read the engine's capacity hook instead of the pool size: a bug that kept the quota
 * at the platform count while the lane was enabled would still pass a pool size assertion.
 */
@Timeout(120)
class EngineQuotaCapacityTest {

    private static final int LANE_PERMITS = 4;

    @Test
    void capacityAddsTheVirtualLanePermitsAndFollowsThePool() throws Exception {
        final ComputeSettings built = settings()
            .autoWorkers(1, -1, Duration.ofSeconds(30))
            .virtualLane(true, true, LANE_PERMITS, millis(50L), 64)
            .metrics(new ComputeSettings.Metrics(true, false, Duration.ofSeconds(10), false, millis(20L)))
            .grace(Duration.ofSeconds(1))
            .build();
        final ComputeEngineImpl engine = engine(built, new EngineTestSupport.FakeClock(), 2);
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        final CountDownLatch twoRunning = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        final List<CompletableFuture<Integer>> futures = new ArrayList<>();
        compute.start();
        try {
            for (int index = 0; index < 6; index++) {
                futures.add(compute.submit(() -> {
                    twoRunning.countDown();
                    release.await();
                    return 1;
                }));
            }
            assertTrue(twoRunning.await(10L, TimeUnit.SECONDS));
            assertEventually("the grown pool and its capacity move together", Duration.ofSeconds(5), () ->
                engine.desiredWorkers() == 2 && engine.totalLimit() == engine.desiredWorkers() + LANE_PERMITS);

            release.countDown();
            for (final CompletableFuture<Integer> future : futures) {
                assertEquals(1, future.get(10L, TimeUnit.SECONDS));
            }
        } finally {
            release.countDown();
            compute.shutdown();
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        }
    }

    @Test
    void capacityIsRecomputedWhenThePlatformPoolShrinks() throws Exception {
        final ComputeSettings built = settings()
            .autoWorkers(1, -1, Duration.ofSeconds(30))
            .virtualLane(true, true, LANE_PERMITS, millis(50L), 64)
            .metrics(new ComputeSettings.Metrics(true, false, Duration.ofSeconds(10), false, millis(20L)))
            .grace(Duration.ofSeconds(1))
            .build();
        final ComputeEngineImpl engine = engine(built, new EngineTestSupport.FakeClock(), 4);
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        final CountDownLatch threeRunning = new CountDownLatch(3);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger running = new AtomicInteger();
        final List<CompletableFuture<Integer>> futures = new ArrayList<>();
        compute.start();
        try {
            for (int index = 0; index < 6; index++) {
                futures.add(compute.submit(() -> {
                    running.incrementAndGet();
                    threeRunning.countDown();
                    try {
                        release.await();
                    } finally {
                        running.decrementAndGet();
                    }
                    return 1;
                }));
            }
            assertTrue(threeRunning.await(10L, TimeUnit.SECONDS));
            assertEventually("the starting budget resolves the capacity", Duration.ofSeconds(5), () ->
                engine.desiredWorkers() >= 3
                    && engine.totalLimit() == engine.desiredWorkers() + LANE_PERMITS);

            engine.applyProcessorBudget(2);
            assertEventually("a smaller pool resolves a smaller capacity", Duration.ofSeconds(5), () ->
                engine.desiredWorkers() <= 2
                    && engine.totalLimit() == engine.desiredWorkers() + LANE_PERMITS);
            assertTrue(running.get() >= 3, "the shrink interrupted running bodies");

            release.countDown();
            for (final CompletableFuture<Integer> future : futures) {
                assertEquals(1, future.get(10L, TimeUnit.SECONDS));
            }
        } finally {
            release.countDown();
            compute.shutdown();
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        }
    }

    @Test
    void disabledLaneCapacityIsThePlatformOnly() {
        final ComputeSettings built = settings()
            .fixedWorkers(2)
            .noVirtualLane()
            .grace(Duration.ofSeconds(1))
            .build();
        final ComputeEngineImpl engine = engine(built, new EngineTestSupport.FakeClock(), 8);
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        compute.start();
        try {
            assertEquals(2, engine.desiredWorkers());
            assertEquals(2, engine.totalLimit(), "a disabled lane must not add capacity");
        } finally {
            compute.shutdown();
        }
    }
}
