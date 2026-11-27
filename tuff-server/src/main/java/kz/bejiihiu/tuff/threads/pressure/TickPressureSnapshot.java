package kz.bejiihiu.tuff.threads.pressure;

import java.util.Objects;

/**
 * What the pressure controller decided on the last pass, as one immutable value.
 *
 * <p>The command reads this instead of the controller so a report never mixes two passes. The
 * {@code signal} is the reading the decision was made from, which is what makes the printed line
 * explain itself: "the reserve is two because two of four regions were below 18 TPS".
 *
 * <p>Three budget numbers, and only one of them is what the engine runs at:
 * <ul>
 *     <li>{@link #budget()} is the ledger level estimate {@code usable - reserve}. It ignores the pool's
 *     own cap and load shedding, so it explains the arithmetic but must not be printed as the applied
 *     size.</li>
 *     <li>{@link #poolBudget()} is the compute pool's effective capacity before the reserve: its spec
 *     cap, its {@code max-threads} and the shed already applied.</li>
 *     <li>{@link #appliedBudget()} is {@code poolBudget - reserve}, floored at {@code min-threads}: the
 *     exact argument the adapter hands to {@code applyProcessorBudget}.</li>
 * </ul>
 *
 * @param enabled       whether the feature was on when the pass ran
 * @param state         whether cores are yielded right now
 * @param reserve       cores the compute pool is yielding
 * @param maxReserve    most cores it could yield on this machine
 * @param budget        ledger level budget estimate {@code usable - reserve}, at least one
 * @param poolBudget    compute pool capacity before the reserve, at least one
 * @param appliedBudget budget actually handed to the engine, at least one
 * @param usableCores   budget ledger's usable cores at the time of the pass
 * @param reason        why the last decision moved or refused to move
 * @param signal        the region reading behind it
 */
public record TickPressureSnapshot(
    boolean enabled,
    TickPressureController.State state,
    int reserve,
    int maxReserve,
    int budget,
    int poolBudget,
    int appliedBudget,
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
        if (budget < 1 || poolBudget < 1 || appliedBudget < 1 || usableCores < 1) {
            throw new IllegalArgumentException("budget, pool budget, applied budget and usable cores must be at least 1");
        }
    }
}
