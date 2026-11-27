package kz.bejiihiu.tuff.threads.observe;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * The observer fan-out: one volatile read for the tick path, any number of receivers behind it.
 *
 * <p>The base observer belongs to the configuration that owns per-tick recording; listeners are added
 * by running subsystems. Both have to see the same body, in a defined order, and neither may take the
 * other down.
 */
@Timeout(30)
class TickObserverFanOutTest {

    @BeforeEach
    void resetFeed() {
        TickObservers.uninstall();
    }

    @AfterEach
    void cleanFeed() {
        TickObservers.uninstall();
    }

    @Test
    void nothingInstalledLeavesTheTickPathEmpty() {
        assertNull(TickObservers.observer());
    }

    @Test
    void theBaseObserverRunsFirstThenTheListenersInRegistrationOrder() {
        final List<String> seen = new ArrayList<>();
        TickObservers.install(recording("base", seen));
        TickObservers.addListener(recording("first", seen));
        TickObservers.addListener(recording("second", seen));

        final TickObserver effective = TickObservers.observer();
        assertNotNull(effective);
        fire(effective);

        assertEquals(List.of("base", "first", "second"), seen);
    }

    @Test
    void installingTheBaseKeepsEveryListener() {
        final List<String> seen = new ArrayList<>();
        TickObservers.addListener(recording("listener", seen));
        TickObservers.install(recording("base", seen));

        final TickObserver effective = TickObservers.observer();
        assertNotNull(effective);
        fire(effective);

        assertEquals(List.of("base", "listener"), seen);
    }

    @Test
    void aThrowingListenerIsIsolatedFromTheBaseAndTheOtherListeners() {
        final List<String> seen = new ArrayList<>();
        TickObservers.install(recording("base", seen));
        TickObservers.addListener(throwing());
        TickObservers.addListener(recording("after", seen));

        final TickObserver effective = TickObservers.observer();
        assertNotNull(effective);
        fire(effective);
        fire(effective);

        assertEquals(List.of("base", "after", "base", "after"), seen,
            "the broken listener must not stop the base or the listeners after it");
    }

    @Test
    void aThrowingBaseStillReachesTheListeners() {
        final List<String> seen = new ArrayList<>();
        TickObservers.install(throwing());
        TickObservers.addListener(recording("listener", seen));

        final TickObserver effective = TickObservers.observer();
        assertNotNull(effective);
        fire(effective);

        assertEquals(List.of("listener"), seen);
    }

    @Test
    void uninstallClearsTheBaseAndEveryListener() {
        final List<String> seen = new ArrayList<>();
        TickObservers.install(recording("base", seen));
        TickObservers.addListener(recording("listener", seen));

        TickObservers.uninstall();
        assertNull(TickObservers.observer(), "a stopped server must leave no observer behind");

        TickObservers.install(recording("next", seen));
        final TickObserver next = TickObservers.observer();
        assertNotNull(next);
        fire(next);

        assertEquals(List.of("next"), seen, "the listener was cleared by uninstall");
    }

    @Test
    void theSameListenerInstanceIsRegisteredOnce() {
        final List<String> seen = new ArrayList<>();
        final TickObserver listener = recording("listener", seen);
        TickObservers.addListener(listener);
        TickObservers.addListener(listener);

        final TickObserver effective = TickObservers.observer();
        assertNotNull(effective);
        fire(effective);

        assertEquals(List.of("listener"), seen);
    }

    @Test
    void removeListenerKeepsTheBaseRunning() {
        final List<String> seen = new ArrayList<>();
        final TickObserver base = recording("base", seen);
        final TickObserver listener = recording("listener", seen);
        TickObservers.install(base);
        TickObservers.addListener(listener);
        TickObservers.removeListener(listener);

        assertSame(base, TickObservers.observer(), "with no listeners the effective observer is the base itself");
        final TickObserver effective = TickObservers.observer();
        assertNotNull(effective);
        fire(effective);
        assertEquals(List.of("base"), seen);
    }

    private static void fire(final TickObserver observer) {
        observer.onTick(TickOrigin.EDF, 3L, 7L, 1_000L, 1_100L, 1_200L, 1L, 1L);
    }

    private static TickObserver recording(final String name, final List<String> seen) {
        return (origin, regionId, tickId, scheduledStartNanos, startNanos, endNanos, periodsAhead, tickCount) ->
            seen.add(name);
    }

    private static TickObserver throwing() {
        return (origin, regionId, tickId, scheduledStartNanos, startNanos, endNanos, periodsAhead, tickCount) -> {
            throw new IllegalStateException("this observer is broken on purpose");
        };
    }
}
