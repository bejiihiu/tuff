package kz.bejiihiu.tuff.compute.internal.adaptive;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import kz.bejiihiu.tuff.compute.internal.adaptive.impl.AimdAdaptiveConcurrencyController;
import kz.bejiihiu.tuff.compute.internal.adaptive.impl.AimdAdaptiveConcurrencyController.DecisionReason;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveTestSupport.automatic;
import static kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveTestSupport.controller;
import static kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveTestSupport.feed;
import static kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveTestSupport.settings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
class AdaptiveConcurrencyScenarioTest {

    private static final Duration WINDOW = Duration.ofSeconds(1);

    @Test
    void slowTasksWithGrowingQueueGrowThePool() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 8), true, 1, 8, 2, 0.5d, 0.5d, 0.8d), clock, 2);

        for (int window = 0; window < 5; window++) {
            clock.advance(WINDOW);
            // two hundred and fifty millisecond tasks: one slot finishes four per second
            feed(controller, controller.currentLimit() * 4, 10L, 250L);
            controller.onQueueState(10 + window, 8192);
            controller.evaluate();
        }

        assertTrue(controller.currentLimit() > 2, "a sustained backlog must grow the pool");
        assertTrue(controller.lastDecisionReason() == DecisionReason.GROW_BACKLOG
            || controller.lastDecisionReason() == DecisionReason.AT_MAXIMUM);
    }

    @Test
    void fastTasksWithEmptyQueueShrinkThePool() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 8), true, 1, 8, 1, 0.5d, 0.5d, 0.8d), clock, 8);

        for (int window = 0; window < 6; window++) {
            clock.advance(WINDOW);
            feed(controller, 3, 0L, 1L);
            controller.onQueueState(0, 8192);
            controller.evaluate();
        }

        assertTrue(controller.currentLimit() < 8, "an idle pool must shrink");
        assertEquals(1, controller.currentLimit());
    }

    @Test
    void lowCpuBudgetBlocksGrowth() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller = new AimdAdaptiveConcurrencyController(
            settings(automatic(1, 8), true, 1, 8, 2, 0.5d, 0.5d, 0.8d),
            clock,
            2,
            AdaptiveTestSupport.PROCESSORS,
            null,
            () -> OptionalDouble.of(0.97d));
        controller.start();

        for (int window = 0; window < 6; window++) {
            clock.advance(WINDOW);
            feed(controller, 20, 5L, 100L);
            controller.onQueueState(10, 8192);
            assertEquals(2, controller.evaluate());
            assertEquals(DecisionReason.HOLD_CPU_BUDGET, controller.lastDecisionReason());
        }

        assertEquals(2, controller.currentLimit());
    }

    @Test
    void sawtoothLoadDoesNotOscillate() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(2, 16), true, 2, 16, 1, 0.25d, 0.5d, 0.8d), clock, 6);

        final List<Integer> limits = new ArrayList<>();
        for (int cycle = 0; cycle < 10; cycle++) {
            for (int burst = 0; burst < 2; burst++) {
                clock.advance(WINDOW);
                feed(controller, controller.currentLimit() * 4, 10L, 250L);
                controller.onQueueState(5, 8192);
                limits.add(controller.evaluate());
            }
            for (int quiet = 0; quiet < 2; quiet++) {
                clock.advance(WINDOW);
                feed(controller, 2, 0L, 1L);
                controller.onQueueState(0, 8192);
                limits.add(controller.evaluate());
            }
        }

        int previous = limits.getFirst();
        for (final int limit : limits) {
            assertTrue(limit >= previous, "rapid sawtooth must not push the limit down");
            assertTrue(limit >= controller.minLimit());
            assertTrue(limit <= controller.maxLimit());
            previous = limit;
        }
        assertEquals(controller.maxLimit(), controller.currentLimit());
    }

    @Test
    void noisyLoadAroundTheBoundaryHoldsTheLimit() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 8), true, 1, 8, 4, 0.5d, 0.5d, 0.8d), clock, 4);

        // wait is between the growth ratio and the shrink ratio: the dead band must absorb it
        for (int window = 0; window < 10; window++) {
            clock.advance(WINDOW);
            feed(controller, 40, 150L, 100L);
            controller.onQueueState(1, 8192);
            assertEquals(4, controller.evaluate());
        }

        assertEquals(DecisionReason.HOLD_STABLE, controller.lastDecisionReason());
    }

    @Test
    void latencyPressureShrinksWithinOneWindow() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 16), true, 1, 16, 2, 0.5d, 0.5d, 0.8d), clock, 16);

        clock.advance(WINDOW);
        feed(controller, 160, 900L, 100L);
        controller.onQueueState(500, 8192);

        assertTrue(controller.evaluate() < 16);
        assertEquals(DecisionReason.SHRINK_WAIT, controller.lastDecisionReason());
    }

    @Test
    void recoveryAfterIdleIsGradual() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 8), true, 1, 8, 1, 0.5d, 0.5d, 0.8d), clock, 8);

        for (int window = 0; window < 5; window++) {
            clock.advance(WINDOW);
            feed(controller, 2, 0L, 1L);
            controller.onQueueState(0, 8192);
            controller.evaluate();
        }
        final int minimum = controller.currentLimit();
        assertEquals(1, minimum);

        clock.advance(WINDOW);
        feed(controller, 10, 5L, 100L);
        controller.onQueueState(10, 8192);
        controller.evaluate();
        assertEquals(DecisionReason.HOLD_CAUTIOUS, controller.lastDecisionReason());
        assertEquals(minimum, controller.currentLimit());

        clock.advance(WINDOW);
        feed(controller, 10, 5L, 100L);
        controller.onQueueState(10, 8192);
        assertEquals(minimum + 1, controller.evaluate());
        assertEquals(DecisionReason.GROW_BACKLOG, controller.lastDecisionReason());
    }
}
