package kz.bejiihiu.tuff.threads.pressure;

import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The controller is the whole policy: hysteresis, the deadband, the cooldown, the ramp and the ceiling.
 *
 * <p>Everything here runs on a hand-written clock, so the tests say exactly which second they mean and
 * no test sleeps. A decision that moves the reserve has to name its reason; a decision that does not
 * must leave the number alone.
 */
@Timeout(120)
class TickPressureControllerTest {

    private static final long SECOND = TimeUnit.SECONDS.toNanos(1L);

    @Test
    void laggingRegionsTakeOneCoreAndTheReasonSaysWhy() {
        final TickPressureController controller = new TickPressureController(policy(true));
        final TickPressureController.Decision decision = controller.evaluate(lagging(), 2, 0L);

        assertTrue(decision.changed());
        assertEquals(1, decision.reserve());
        assertEquals(TickPressureController.Reason.LAG_ENTER, decision.reason());
        assertEquals(TickPressureController.State.PRESSURE, decision.state());
    }

    @Test
    void aDisabledPolicyNeverMovesTheReserve() {
        final TickPressureController controller = new TickPressureController(policy(false));

        for (int second = 0; second < 30; second++) {
            final TickPressureController.Decision decision = controller.evaluate(lagging(), 2, second * SECOND);
            assertFalse(decision.changed());
            assertEquals(TickPressureController.Reason.DISABLED, decision.reason());
        }
        assertEquals(0, controller.reserve());
    }

    @Test
    void anUnmeasuredSignalKeepsTheCurrentReserve() {
        final TickPressureController controller = new TickPressureController(policy(true));
        controller.evaluate(lagging(), 2, 0L);

        final TickPressureController.Decision decision = controller.evaluate(TickSignal.unknown(), 2, 10 * SECOND);

        assertFalse(decision.changed());
        assertEquals(1, decision.reserve(), "a failed region read must not release the reserve");
        assertEquals(TickPressureController.Reason.SIGNAL_UNAVAILABLE, decision.reason());
    }

    @Test
    void theCooldownBlocksASecondStep() {
        final TickPressureController controller = new TickPressureController(policy(true));
        controller.evaluate(lagging(), 2, 0L);

        final TickPressureController.Decision early = controller.evaluate(lagging(), 2, 2 * SECOND);
        assertFalse(early.changed(), "two seconds after entering is inside the five second cooldown");
        assertEquals(1, early.reserve());

        final TickPressureController.Decision grown = controller.evaluate(lagging(), 2, 5 * SECOND);
        assertTrue(grown.changed());
        assertEquals(2, grown.reserve());
        assertEquals(TickPressureController.Reason.LAG_STEP_UP, grown.reason());
    }

    @Test
    void theReserveStopsAtTheOwnersCeiling() {
        final TickPressureController controller = new TickPressureController(policy(true));
        controller.evaluate(lagging(), 1, 0L);

        final TickPressureController.Decision pinned =
            controller.evaluate(lagging(), 1, 30 * SECOND);

        assertFalse(pinned.changed(), "a pool of one core has nothing left to give");
        assertEquals(1, pinned.reserve());
        assertEquals(TickPressureController.Reason.CEILING_REACHED, pinned.reason());
    }

    @Test
    void aCeilingOfZeroNeverEntersPressure() {
        final TickPressureController controller = new TickPressureController(policy(true));

        final TickPressureController.Decision decision = controller.evaluate(lagging(), 0, 0L);

        assertFalse(decision.changed());
        assertEquals(0, decision.reserve());
        assertEquals(TickPressureController.State.NORMAL, decision.state());
        assertEquals(TickPressureController.Reason.CEILING_REACHED, decision.reason());
    }

    @Test
    void theHysteresisBandMovesNothing() {
        final TickPressureController controller = new TickPressureController(policy(true));
        controller.evaluate(lagging(), 2, 0L);

        // lag 0 % but the slowest region is 19.2: not lagging (>= 19.0), not calm (< 19.5)
        final TickPressureController.Decision decision = controller.evaluate(signal(0.0d, 19.2d), 2, 60 * SECOND);

        assertFalse(decision.changed(), "the band between 19.0 and 19.5 must not resize the pool");
        assertEquals(1, decision.reserve());
    }

    @Test
    void calmRegionsReleaseTheReserveAfterTheWindowsAndTheCooldown() {
        final TickPressureController controller = new TickPressureController(policy(true));
        controller.evaluate(lagging(), 2, 0L);

        for (int second = 1; second <= 4; second++) {
            assertFalse(controller.evaluate(calm(), 2, second * SECOND).changed(),
                "before the cooldown and the windows nothing may move");
        }

        final TickPressureController.Decision released = controller.evaluate(calm(), 2, 5 * SECOND);
        assertTrue(released.changed());
        assertEquals(0, released.reserve());
        assertEquals(TickPressureController.State.NORMAL, released.state());
        assertEquals(TickPressureController.Reason.CALM_EXIT, released.reason());
    }

    @Test
    void oneLaggingSampleResetsTheCalmWindows() {
        final TickPressureController controller = new TickPressureController(policy(true));
        controller.evaluate(lagging(), 2, 0L);

        controller.evaluate(calm(), 2, 1 * SECOND);
        controller.evaluate(calm(), 2, 2 * SECOND);
        // a single lagging sample in the middle must restart the count, not pause it
        controller.evaluate(lagging(), 2, 3 * SECOND);
        controller.evaluate(calm(), 2, 4 * SECOND);

        final TickPressureController.Decision stillHeld = controller.evaluate(calm(), 2, 5 * SECOND);
        assertFalse(stillHeld.changed(), "two calm samples after the reset are not three");
        assertEquals(1, stillHeld.reserve());
    }

    @Test
    void aSawtoothSignalCannotResizeThePoolEverySecond() {
        final TickPressureController controller = new TickPressureController(policy(true));

        long changed = 0L;
        for (int second = 0; second < 40; second++) {
            // one second late, one second calm, forever: the classic "pila" that makes naive controllers
            // fight themselves
            final TickSignal signal = second % 2 == 0 ? lagging() : calm();
            if (controller.evaluate(signal, 2, second * SECOND).changed()) {
                changed++;
            }
        }

        final long changes = changed;
        assertTrue(changes <= 8L, () -> "40 sawtooth seconds caused " + changes + " resizes; the guards are gone");
        assertTrue(controller.reserve() <= 2, "the reserve escaped its ceiling");
    }

    @Test
    void aShrinkingCeilingIsHonouredImmediately() {
        final TickPressureController controller = new TickPressureController(policy(true));
        controller.evaluate(lagging(), 2, 0L);
        controller.evaluate(lagging(), 2, 5 * SECOND);
        assertEquals(2, controller.reserve());

        final TickPressureController.Decision clamped = controller.evaluate(lagging(), 1, 6 * SECOND);

        assertTrue(clamped.changed());
        assertEquals(1, clamped.reserve());
        assertEquals(TickPressureController.Reason.CEILING_REACHED, clamped.reason());
    }

    private static TickSignal lagging() {
        return signal(0.5d, 16.0d);
    }

    private static TickSignal calm() {
        return signal(0.0d, 20.0d);
    }

    private static TickSignal signal(final double lagFraction, final double minimumTps) {
        return new TickSignal(true, 4, lagFraction, minimumTps, 12.0d);
    }

    private static ThreadManagementSettings.AsyncCompute.TickPressure policy(final boolean enabled) {
        return new ThreadManagementSettings.AsyncCompute.TickPressure(enabled, 0.25d, 19.0d, 19.5d, 3, 5, 2, 1);
    }
}
