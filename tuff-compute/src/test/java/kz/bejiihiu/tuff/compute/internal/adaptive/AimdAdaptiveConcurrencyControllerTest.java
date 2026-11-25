package kz.bejiihiu.tuff.compute.internal.adaptive;

import java.time.Duration;
import java.util.OptionalDouble;
import java.util.concurrent.atomic.AtomicInteger;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.internal.adaptive.impl.AimdAdaptiveConcurrencyController;
import kz.bejiihiu.tuff.compute.internal.adaptive.impl.AimdAdaptiveConcurrencyController.DecisionReason;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveTestSupport.automatic;
import static kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveTestSupport.controller;
import static kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveTestSupport.feed;
import static kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveTestSupport.fixed;
import static kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveTestSupport.settings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
class AimdAdaptiveConcurrencyControllerTest {

    private static final Duration WINDOW = Duration.ofSeconds(1);

    @Test
    void newControllerDoesNothingUntilStarted() {
        final TestComputeClock clock = new TestComputeClock();
        final ComputeSettings settings = settings(automatic(1, 8), true, 1, 8, 2, 0.5d, 0.5d, 0.8d);
        final AimdAdaptiveConcurrencyController controller = new AimdAdaptiveConcurrencyController(
            settings, clock, 3, AdaptiveTestSupport.PROCESSORS, null, OptionalDouble::empty);

        feed(controller, 100, 0L, 1L);
        controller.onQueueState(10, 8192);
        clock.advance(WINDOW);

        assertEquals(3, controller.evaluate());
        assertEquals(DecisionReason.NOT_STARTED, controller.lastDecisionReason());
        assertEquals(3, controller.currentLimit());
        assertEquals(3, controller.desiredLimit());
        assertTrue(controller.enabled());
        assertEquals(0, controller.snapshot().samples());
        assertEquals(Duration.ZERO, controller.snapshot().averageQueueWait());
        assertEquals(clock.instant(), controller.snapshot().capturedAt());
    }

    @Test
    void startIsIdempotentAndResetsTheWindow() {
        final TestComputeClock clock = new TestComputeClock();
        final ComputeSettings settings = settings(automatic(1, 8), true, 1, 8, 2, 0.5d, 0.5d, 0.8d);
        final AimdAdaptiveConcurrencyController controller = new AimdAdaptiveConcurrencyController(
            settings, clock, 2, AdaptiveTestSupport.PROCESSORS, null, OptionalDouble::empty);

        controller.start();
        controller.start();
        clock.advance(WINDOW);
        assertEquals(2, controller.evaluate());
        assertEquals(DecisionReason.INSUFFICIENT_SAMPLES, controller.lastDecisionReason());
    }

    @Test
    void growsUnderSustainedBacklog() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 8), true, 1, 8, 2, 0.5d, 0.5d, 0.8d), clock, 2);

        int previous = controller.currentLimit();
        for (int window = 0; window < 6; window++) {
            clock.advance(WINDOW);
            feed(controller, controller.currentLimit() * 10, 5L, 100L);
            controller.onQueueState(4, 8192);
            final int limit = controller.evaluate();
            assertTrue(limit >= previous, "limit must be monotone under sustained backlog");
            assertTrue(limit <= controller.maxLimit());
            previous = limit;
        }

        assertEquals(8, controller.currentLimit());
        assertEquals(DecisionReason.AT_MAXIMUM, controller.lastDecisionReason());
        assertTrue(controller.lastThroughputPerSecond() > 0.0d);
    }

    @Test
    void shrinksWhenThePoolStaysIdle() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 8), true, 1, 8, 1, 0.5d, 0.5d, 0.8d), clock, 8);

        int previous = controller.currentLimit();
        for (int window = 0; window < 6; window++) {
            clock.advance(WINDOW);
            feed(controller, 2, 0L, 1L);
            controller.onQueueState(0, 8192);
            final int limit = controller.evaluate();
            assertTrue(limit <= previous, "limit must not grow while the pool is idle");
            previous = limit;
        }

        assertEquals(1, controller.currentLimit());
        assertEquals(DecisionReason.AT_MINIMUM, controller.lastDecisionReason());
    }

    @Test
    void respectsTheConfiguredCeiling() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 8), true, 1, 4, 1, 0.5d, 0.5d, 0.8d), clock, 2);

        for (int window = 0; window < 8; window++) {
            clock.advance(WINDOW);
            feed(controller, controller.currentLimit() * 10, 5L, 100L);
            controller.onQueueState(4, 8192);
            controller.evaluate();
        }

        assertEquals(4, controller.currentLimit());
        assertEquals(4, controller.maxLimit());
    }

    @Test
    void respectsTheConfiguredFloor() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(3, 8), true, 3, 8, 1, 0.5d, 0.5d, 0.8d), clock, 6);

        for (int window = 0; window < 6; window++) {
            clock.advance(WINDOW);
            feed(controller, 2, 0L, 1L);
            controller.onQueueState(0, 8192);
            controller.evaluate();
        }

        assertEquals(3, controller.currentLimit());
        assertEquals(3, controller.minLimit());
    }

    @Test
    void cooldownPreventsRapidResizes() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 8), true, 1, 8, 2, 0.5d, 0.5d, 0.8d), clock, 2);

        clock.advance(WINDOW);
        feed(controller, 20, 5L, 100L);
        controller.onQueueState(4, 8192);
        final int first = controller.evaluate();
        assertTrue(first > 2);

        feed(controller, 20, 5L, 100L);
        controller.onQueueState(4, 8192);
        assertEquals(first, controller.evaluate());
        assertEquals(DecisionReason.COOLDOWN, controller.lastDecisionReason());

        clock.advance(Duration.ofMillis(500));
        assertEquals(first, controller.evaluate());
        assertEquals(DecisionReason.COOLDOWN, controller.lastDecisionReason());

        clock.advance(Duration.ofMillis(500));
        feed(controller, 20, 5L, 100L);
        controller.onQueueState(4, 8192);
        assertTrue(controller.evaluate() > first);
    }

    @Test
    void disabledControllerNeverMoves() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 8), false, 1, 8, 1, 0.5d, 0.5d, 0.8d), clock, 4);

        for (int window = 0; window < 5; window++) {
            clock.advance(WINDOW);
            feed(controller, 100, 1L, 50L);
            controller.onQueueState(50, 8192);
            assertEquals(4, controller.evaluate());
            assertEquals(DecisionReason.DISABLED, controller.lastDecisionReason());
        }

        assertFalse(controller.enabled());
        assertEquals(4, controller.currentLimit());
    }

    @Test
    void explicitWorkerCountWinsOverAdaptiveSizing() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(fixed(4), true, 1, -1, 1, 0.5d, 0.5d, 0.8d), clock, 4);

        for (int window = 0; window < 5; window++) {
            clock.advance(WINDOW);
            feed(controller, 100, 1L, 50L);
            controller.onQueueState(50, 8192);
            assertEquals(4, controller.evaluate());
            assertEquals(DecisionReason.EXPLICIT_WORKER_COUNT, controller.lastDecisionReason());
        }
    }

    @Test
    void extremeMetricsStayInsideTheBounds() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 8), true, 1, 8, 1, 1.0d, 0.5d, 0.8d), clock, 2);

        for (int window = 0; window < 6; window++) {
            clock.advance(WINDOW);
            controller.recordSample(Long.MAX_VALUE, Long.MAX_VALUE, false);
            controller.recordSample(-5L, -5L, false);
            controller.recordSample(1L, 0L, false);
            controller.onQueueState(Integer.MAX_VALUE, Integer.MIN_VALUE);
            final int limit = controller.evaluate();
            assertTrue(limit >= controller.minLimit());
            assertTrue(limit <= controller.maxLimit());
        }

        assertEquals(0, controller.lastQueueCapacity());
    }

    @Test
    void failedSamplesAreDropped() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 8), true, 1, 8, 2, 0.5d, 0.5d, 0.8d), clock, 4);

        for (int i = 0; i < 50; i++) {
            controller.recordSample(1_000_000L, 2_000_000L, true);
        }
        clock.advance(WINDOW);

        assertEquals(4, controller.evaluate());
        assertEquals(DecisionReason.INSUFFICIENT_SAMPLES, controller.lastDecisionReason());
        assertEquals(50L, controller.failedSampleCount());
        assertEquals(0, controller.snapshot().samples());
    }

    @Test
    void zeroExecutionTimeDoesNotDivideByZero() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 8), true, 1, 8, 1, 0.5d, 0.5d, 0.8d), clock, 4);

        clock.advance(WINDOW);
        feed(controller, 5, 0L, 0L);
        controller.onQueueState(2, 8192);

        final int limit = controller.evaluate();
        assertEquals(4, limit);
        assertEquals(DecisionReason.HOLD_STABLE, controller.lastDecisionReason());
        assertEquals(0.0d, controller.snapshot().estimatedUtilization());
    }

    @Test
    void snapshotReportsTheLastWindow() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 8), true, 1, 8, 2, 0.5d, 0.5d, 0.8d), clock, 2);

        clock.advance(WINDOW);
        feed(controller, 20, 5L, 100L);
        controller.onQueueState(4, 8192);
        controller.evaluate();

        final var snapshot = controller.snapshot();
        assertEquals(20, snapshot.samples());
        assertEquals(Duration.ofMillis(5), snapshot.averageQueueWait());
        assertEquals(Duration.ofMillis(100), snapshot.averageExecution());
        assertEquals(1.0d, snapshot.estimatedUtilization());
        assertEquals(controller.currentLimit(), snapshot.currentLimit());
        assertEquals(controller.desiredLimit(), snapshot.desiredLimit());
        assertEquals(controller.minLimit(), snapshot.minLimit());
        assertEquals(controller.maxLimit(), snapshot.maxLimit());
        assertTrue(snapshot.enabled());
    }

    @Test
    void stopFreezesTheLimit() {
        final TestComputeClock clock = new TestComputeClock();
        final AimdAdaptiveConcurrencyController controller =
            controller(settings(automatic(1, 8), true, 1, 8, 1, 0.5d, 0.5d, 0.8d), clock, 3);

        controller.stop();
        clock.advance(WINDOW);
        feed(controller, 100, 0L, 1L);
        controller.onQueueState(50, 8192);

        assertEquals(3, controller.evaluate());
        assertEquals(DecisionReason.NOT_STARTED, controller.lastDecisionReason());
        assertEquals(3, controller.currentLimit());
    }

    @Test
    void idleShrinkUsesBusyWorkersWhenTheyAreKnown() {
        final TestComputeClock clock = new TestComputeClock();
        final ComputeSettings settings = settings(automatic(1, 8), true, 1, 8, 1, 0.5d, 0.5d, 0.8d);
        final AtomicInteger busy = new AtomicInteger(8);
        final AimdAdaptiveConcurrencyController controller = new AimdAdaptiveConcurrencyController(
            settings, clock, 8, AdaptiveTestSupport.PROCESSORS, busy::get, OptionalDouble::empty);
        controller.start();

        for (int window = 0; window < 4; window++) {
            clock.advance(WINDOW);
            controller.onQueueState(0, 8192);
            controller.evaluate();
        }
        assertEquals(8, controller.currentLimit(), "busy workers must block an idle shrink");

        busy.set(0);
        for (int window = 0; window < 5; window++) {
            clock.advance(WINDOW);
            controller.onQueueState(0, 8192);
            controller.evaluate();
        }
        assertEquals(1, controller.currentLimit());
    }
}
