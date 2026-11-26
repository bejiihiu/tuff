package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.assertEventually;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.engine;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The limit a consumer reports must be the limit the quota enforces, whatever the pool currently is.
 *
 * <p>This is the report side of {@link EngineQuotaCapacityTest}: the quota already resolves relative
 * limits lazily, so the only way the two can disagree is a stale snapshot. The tests register while
 * the pool is still at its startup size and then shrink the budget, which is exactly the smoke
 * scenario (limits recorded from 7 workers, then the pool drops to 1 or 2).
 */
@Timeout(120)
class EngineConsumerLimitReportTest {

    private static final int LANE_PERMITS = 4;

    @Test
    void reportedLimitFollowsACapacityShrink() throws InterruptedException {
        final ComputeEngineImpl engine = this.startedEngine();
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        compute.start();
        try {
            compute.registerConsumer(ComputeConsumer.of("whole-pool", ConsumerPolicy.defaults()));
            engine.applyProcessorBudget(1);
            assertEventually("the report follows a shrink", Duration.ofSeconds(5), () -> {
                final int capacity = engine.totalLimit();
                // the automatic pool bottom is one worker, so this state is stable: no keep-alive
                // retirement can move it again while the assertion is running
                return engine.desiredWorkers() == 1
                    && capacity == 1 + LANE_PERMITS
                    && reportedLimit(compute, "whole-pool") == capacity;
            });
        } finally {
            compute.shutdown();
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        }
    }

    @Test
    void policyUpdatesResolveAgainstTheCurrentCapacity() throws InterruptedException {
        final ComputeEngineImpl engine = this.startedEngine();
        final AsyncComputeScheduler compute = new AsyncComputeSchedulerImpl(engine);
        compute.start();
        try {
            compute.registerConsumer(ComputeConsumer.of("half", ConsumerPolicy.relative(0.5d)));
            engine.applyProcessorBudget(1);
            final int capacity = 1 + LANE_PERMITS;
            assertEventually("the half share follows the shrink", Duration.ofSeconds(5), () ->
                engine.totalLimit() == capacity
                    && reportedLimit(compute, "half") == ConsumerPolicy.relative(0.5d).resolveConcurrencyLimit(capacity));

            compute.updateConsumerPolicy("half", ConsumerPolicy.relative(1.0d));
            assertEventually("a policy update resolves against the current capacity", Duration.ofSeconds(5), () ->
                reportedLimit(compute, "half") == capacity);

            // absolute limits are not a share, so they stay as configured even above the capacity;
            // the quota reports the same number and the lanes are the ones that cannot fill it
            compute.updateConsumerPolicy("half", ConsumerPolicy.absolute(capacity + LANE_PERMITS));
            assertEventually("an absolute limit is reported as configured", Duration.ofSeconds(5), () ->
                reportedLimit(compute, "half") == capacity + LANE_PERMITS);
        } finally {
            compute.shutdown();
            assertTrue(compute.awaitTermination(Duration.ofSeconds(10)));
        }
    }

    private ComputeEngineImpl startedEngine() {
        final ComputeSettings built = settings()
            .autoWorkers(1, -1, Duration.ofSeconds(30))
            .virtualLane(true, true, LANE_PERMITS, millis(50L), 64)
            .grace(Duration.ofSeconds(1))
            .build();
        return engine(built, new EngineTestSupport.FakeClock(), 8);
    }

    private static int reportedLimit(final AsyncComputeScheduler compute, final String consumerId) {
        return compute.metrics().consumer(consumerId).orElseThrow().concurrencyLimit();
    }
}
