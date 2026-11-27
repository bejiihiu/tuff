package kz.bejiihiu.tuff.threads.relief;

import java.util.List;

/**
 * An immutable snapshot of the overrun trigger.
 *
 * <p>The counters are monotonic since the trigger was created; the gauges describe the moment the
 * snapshot was taken. Everything here is read from the command thread and never blocks a region: the
 * per-region state is only locked for the few field reads of one region at a time.
 *
 * @param enabled              whether the trigger is acting; {@code false} means every tick is ignored
 * @param registeredUnits      number of work units currently registered
 * @param unitIds              ids of those units, in registration order
 * @param regions              regions with retained trigger state
 * @param openWindows          windows currently open
 * @param windowsEntered       windows opened since start
 * @param windowsExited        windows closed because the region became calm
 * @param windowsAbandoned     open windows forgotten because the region stopped ticking
 * @param overrunBodies        tick bodies that counted as an overrun
 * @param calmBodies           tick bodies that counted as calm
 * @param suppressedByCooldown entries that hysteresis allowed but the cooldown blocked
 * @param suppressedByRate     entries that hysteresis allowed but the per-region rate blocked
 * @param unitFailures         unit callbacks that threw and were isolated
 * @param lastWindowAgeNanos   age of the most recent window opening, or {@code -1} when none opened yet
 * @param topOffenders         regions with the most overrun bodies, at most five
 */
public record ReliefTriggerSnapshot(
    boolean enabled,
    int registeredUnits,
    List<String> unitIds,
    int regions,
    int openWindows,
    long windowsEntered,
    long windowsExited,
    long windowsAbandoned,
    long overrunBodies,
    long calmBodies,
    long suppressedByCooldown,
    long suppressedByRate,
    long unitFailures,
    long lastWindowAgeNanos,
    List<RegionOffence> topOffenders
) {

    /**
     * Validates the snapshot.
     *
     * @throws NullPointerException     if a list is {@code null}
     * @throws IllegalArgumentException if a counter or gauge is negative
     */
    public ReliefTriggerSnapshot {
        unitIds = List.copyOf(unitIds);
        topOffenders = List.copyOf(topOffenders);
        if (registeredUnits < 0 || regions < 0 || openWindows < 0 || windowsEntered < 0 || windowsExited < 0
            || windowsAbandoned < 0 || overrunBodies < 0 || calmBodies < 0 || suppressedByCooldown < 0
            || suppressedByRate < 0 || unitFailures < 0) {
            throw new IllegalArgumentException("trigger counters must not be negative");
        }
    }

    /**
     * Returns an empty snapshot of a trigger that never ran.
     *
     * @return the empty snapshot
     */
    public static ReliefTriggerSnapshot empty() {
        return new ReliefTriggerSnapshot(false, 0, List.of(), 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, -1L, List.of());
    }

    /**
     * One region in the offender list.
     *
     * @param regionId      id of the region
     * @param overrunBodies overrun bodies since the region's state was created
     * @param windows       windows the region opened in that time
     * @param open          whether a window of that region is open right now
     */
    public record RegionOffence(long regionId, long overrunBodies, long windows, boolean open) {
    }
}
