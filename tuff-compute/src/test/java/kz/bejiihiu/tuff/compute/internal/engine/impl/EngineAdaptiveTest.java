package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveConcurrencyController;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.engine;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millis;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.millisNanos;
import static kz.bejiihiu.tuff.compute.internal.engine.impl.EngineTestSupport.settings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The adaptive layer must move the real pool limit, and must stay out of the way when the worker
 * count is fixed.
 */
@Timeout(60)
class EngineAdaptiveTest {

    @Test
    void latencyPressureShrinksTheAppliedLimit() {
        final EngineTestSupport.FakeClock clock = new EngineTestSupport.FakeClock();
        final ComputeSettings configuration = settings()
            .autoWorkers(1, 8, Duration.ofSeconds(30))
            .adaptive(new ComputeSettings.Adaptive(true, 1, 8, millis(100L), 5, 0.5d, 0.5d, 0.9d))
            .build();
        final ComputeEngineImpl compute = engine(configuration, clock);
        compute.start();
        try {
            final AdaptiveConcurrencyController controller = compute.adaptiveController();
            assertNotNull(controller);
            assertTrue(controller.enabled());
            final int initial = compute.totalLimit();
            assertEquals(initial, controller.currentLimit());

            for (int sample = 0; sample < 10; sample++) {
                controller.recordSample(millisNanos(100L), millisNanos(1L), false);
            }
            clock.advance(Duration.ofSeconds(2));
            compute.evaluateAdaptiveNow();

            final int shrunk = compute.totalLimit();
            assertTrue(shrunk < initial, "latency pressure must shrink the limit, was " + initial + " now " + shrunk);
            assertEquals(controller.currentLimit(), shrunk);
            assertEquals(Math.max(1, initial - (int) Math.ceil(initial * 0.5d)), shrunk);
        } finally {
            compute.shutdown(Duration.ofSeconds(1));
        }
    }

    @Test
    void fixedWorkerCountKeepsAdaptiveOutOfTheWay() {
        final EngineTestSupport.FakeClock clock = new EngineTestSupport.FakeClock();
        final ComputeSettings configuration = settings()
            .fixedWorkers(4)
            .adaptive(new ComputeSettings.Adaptive(true, 1, 8, millis(100L), 5, 0.5d, 0.5d, 0.9d))
            .build();
        final ComputeEngineImpl compute = engine(configuration, clock);
        compute.start();
        try {
            final AdaptiveConcurrencyController controller = compute.adaptiveController();
            assertNotNull(controller);
            final int initial = compute.totalLimit();
            assertEquals(4, initial);
            for (int sample = 0; sample < 50; sample++) {
                controller.recordSample(millisNanos(500L), millisNanos(1L), false);
            }
            clock.advance(Duration.ofSeconds(5));
            compute.evaluateAdaptiveNow();
            assertEquals(initial, compute.totalLimit());
        } finally {
            compute.shutdown(Duration.ofSeconds(1));
        }
    }
}
