package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler.SchedulerState;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Absurd duration settings must not break start, expiry or shutdown: they saturate instead of
 * overflowing.
 */
@Timeout(60)
class EngineOversizedDurationsTest {

    @Test
    void enormousDurationsDoNotBreakStartRunOrShutdown() throws Exception {
        final Duration huge = Duration.ofSeconds(Long.MAX_VALUE);
        final ConsumerPolicy policy = new ConsumerPolicy(
            true,
            ConsumerPolicy.defaults().concurrencyLimit(),
            -1,
            1.0d,
            OverflowPolicy.REJECT,
            huge
        );
        final ComputeSettings configuration = new ComputeSettings(
            true,
            new ComputeSettings.Workers(-1, 1, 4, huge, "tuff-oversized-durations"),
            new ComputeSettings.QueueSettings(64, OverflowPolicy.REJECT),
            new ComputeSettings.Scheduling(
                ComputeSettings.Scheduling.Strategy.WEIGHTED_FAIR,
                false,
                true,
                huge,
                3,
                huge
            ),
            new ComputeSettings.Consumers(policy, Map.of(), false),
            new ComputeSettings.Metrics(true, false, huge, false, huge),
            new ComputeSettings.Adaptive(false, 1, -1, huge, 20, 0.1d, 0.5d, 0.9d),
            ComputeSettings.Lanes.defaults(),
            huge
        );
        final AsyncComputeScheduler compute = EngineTestSupport.startedScheduler(configuration, ComputeClock.system());
        try {
            assertEquals(42, compute.submit(() -> 42).get(10L, TimeUnit.SECONDS));
            assertTrue(compute.metrics().submittedTasks() >= 1L);
        } finally {
            compute.shutdown(huge);
        }
        assertTrue(compute.awaitTermination(Duration.ofSeconds(5)), "shutdown must not hang on a saturated grace period");
        assertEquals(SchedulerState.TERMINATED, compute.state());
    }
}
