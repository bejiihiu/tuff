package kz.bejiihiu.tuff.threads.pressure;

import java.util.Objects;

/**
 * What the pressure controller decided on the last pass, as one immutable value.
 *
 * <p>The command reads this instead of the controller so a report never mixes two passes. The
 * {@code signal} is the reading the decision was made from, which is what makes the printed line
 * explain itself: "the reserve is two because two of four regions were below 18 TPS".
 *
 * @param enabled      whether the feature was on when the pass ran
 * @param state        whether cores are yielded right now
 * @param reserve      cores the compute pool is yielding
 * @param maxReserve   most cores it could yield on this machine
 * @param budget       processor budget the pool should be running at
 * @param usableCores  budget ledger's usable cores at the time of the pass
 * @param reason       why the last decision moved or refused to move
 * @param signal       the region reading behind it
 */
public record TickPressureSnapshot(
    boolean enabled,
    TickPressureController.State state,
    int reserve,
    int maxReserve,
    int budget,
    int usableCores,
    TickPressureController.Reason reason,
    TickSignal signal
) {

    /**
     * Validates a snapshot.
     *
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if a core count is out of range
     */
    public TickPressureSnapshot {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(signal, "signal");
        if (reserve < 0 || maxReserve < 0) {
            throw new IllegalArgumentException("reserve counts must not be negative");
        }
        if (reserve > maxReserve) {
            throw new IllegalArgumentException("reserve " + reserve + " exceeds its ceiling " + maxReserve);
        }
        if (budget < 1 || usableCores < 1) {
            throw new IllegalArgumentException("budget and usable cores must be at least 1");
        }
    }
}
