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
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Static autoscaling with {@code workers.threads == -1}: grow on backlog, shrink on idle.
 */
@Timeout(90)
class EngineAutoScaleTest {

    @Test
    void automaticPoolGrowsUnderLoadAndShrinksBackOnIdle() throws Exception {
        final ComputeSettings configuration = settings()
            .autoWorkers(1, 12, millis(50L))
            .metrics(new ComputeSettings.Metrics(true, false, Duration.ofSeconds(10), false, millis(20L)))
            .grace(Duration.ofSeconds(1))
            .noVirtualLane()
            .build();
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine(configuration, new EngineTestSupport.FakeClock()));
        final CountDownLatch release = new CountDownLatch(1);
        compute.start();
        try {
            assertEventually("initial workers are visible", Duration.ofSeconds(5), () -> compute.metrics().totalWorkers() > 0);
            final int initialWorkers = compute.metrics().totalWorkers();

            final List<CompletableFuture<Integer>> futures = new ArrayList<>();
            for (int index = 0; index < 12; index++) {
                futures.add(compute.submit(() -> {
                    release.await();
                    return 1;
                }));
            }
            assertEventually("pool grows under a blocked backlog", Duration.ofSeconds(10), () ->
                compute.metrics().totalWorkers() > initialWorkers && compute.metrics().totalWorkers() >= 8);

            release.countDown();
            for (final CompletableFuture<Integer> future : futures) {
                assertEquals(1, future.get(10L, TimeUnit.SECONDS));
            }
            assertEventually("pool shrinks back to the minimum", Duration.ofSeconds(10), () ->
                compute.metrics().totalWorkers() <= 2);
        } finally {
            release.countDown();
            compute.shutdown();
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        }
    }
}
