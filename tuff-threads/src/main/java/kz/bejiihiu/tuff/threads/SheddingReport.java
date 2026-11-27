package kz.bejiihiu.tuff.threads;

import java.util.List;
import java.util.Objects;

/**
 * How much of the budget is held for the critical lanes, and how much of it is withheld from the rest.
 *
 * <p>Two numbers that look similar and are not:
 * <ul>
 *     <li>{@link #criticalReserveCores()} is permanent for the life of the ledger. It is the part of the
 *     budget the non critical pools may never take, even when they oversubscribe their own caps, because
 *     region ticks and network event loops are what the whole server depends on.</li>
 *     <li>{@link #shedCores()} is temporary. The policy takes it out of the non critical pools one step at
 *     a time while a critical pool's lease queue grows, and gives it back one step at a time after the
 *     queue has been calm for a few samples. It never goes below a pool's declared minimum.</li>
 * </ul>
 *
 * <p>{@link #state()} is the honest answer to "can this policy ever fire here". The trigger needs a
 * critical pool that can lease, which means {@link CoverageLevel#L3} or deeper. In the shipped wiring
 * region execution and network stop at L2, so an enabled policy is {@link State#DORMANT}: it costs
 * nothing and it does not pretend to protect anything. {@link #summary()} is the one printable line for
 * an operator, so "we are degraded" never appears without "by how much and from whom".
 *
 * @param criticalReserveCores cores the non critical pools may not take, at least zero
 * @param enabled              whether the load shedding policy is configured at all
 * @param state                whether the policy can fire, is waiting, or is withholding cores right now
 * @param shedCores            cores currently withheld from non critical pools, at least zero
 * @param pools                pools whose capacity is reduced right now, in id order
 * @param summary              one line with the numbers, never blank
 */
public record SheddingReport(
    int criticalReserveCores,
    boolean enabled,
    State state,
    int shedCores,
    List<PoolId> pools,
    String summary
) {

    /**
     * Validates a shedding report.
     *
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if a count is negative, a pool repeats, the summary is blank or
     *                                  the state disagrees with the switch
     */
    public SheddingReport {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(pools, "pools");
        Objects.requireNonNull(summary, "summary");
        if (criticalReserveCores < 0) {
            throw new IllegalArgumentException(
                "criticalReserveCores must not be negative, got " + criticalReserveCores
            );
        }
        if (shedCores < 0) {
            throw new IllegalArgumentException("shedCores must not be negative, got " + shedCores);
        }
        if ((state == State.OFF) == enabled) {
            throw new IllegalArgumentException(
                "state " + state + " disagrees with enabled=" + enabled
            );
        }
        if (summary.isBlank()) {
            throw new IllegalArgumentException("summary must not be blank");
        }
        pools = List.copyOf(pools);
    }

    /**
     * Creates the report of a controller that reserves nothing and sheds nothing.
     *
     * <p>Used by hand-built snapshots and by implementations that do not do capacity sharing. It is a
     * real answer, not a placeholder: the numbers are zero because nothing is held.
     *
     * @return the inactive report
     */
    public static SheddingReport none() {
        return new SheddingReport(0, false, State.OFF, 0, List.of(), "no critical reserve, load shedding off");
    }

    /**
     * Checks whether some capacity is withheld right now.
     *
     * @return {@code true} when {@link #shedCores()} is positive
     */
    public boolean active() {
        return this.shedCores > 0;
    }

    /**
     * What the load shedding policy is doing in this configuration.
     */
    public enum State {

        /**
         * The policy is disabled by configuration.
         */
        OFF,

        /**
         * The policy is enabled but no critical pool can lease capacity, so the trigger cannot fire.
         *
         * <p>This is the shipped state: region execution and network are at most L2 and never call
         * {@code acquire}. The reserve still protects their arithmetic; only the dynamic half waits for
         * an L3 critical lane.
         */
        DORMANT,

        /**
         * The policy is enabled, at least one critical pool can lease, and nothing is withheld.
         */
        ARMED,

        /**
         * Cores are withheld from non critical pools right now.
         */
        SHEDDING
    }
}
