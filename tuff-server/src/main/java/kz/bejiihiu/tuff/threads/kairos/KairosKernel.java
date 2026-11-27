package kz.bejiihiu.tuff.threads.kairos;

/**
 * The knobs that act on a single tick or a single permit rather than on the size of the fund.
 *
 * <p>{@link KairosTuning} answers "how large may the fund be"; this record answers "what may the
 * fund do to a tick that is late, to a tick that belongs to another runner, and to the permit that
 * was handed out too slowly". They are separate because they have different blast radii: a fund
 * limit changes how many threads exist, while every field here changes what a tick body does, and
 * each of them has its own load test in the rollout plan.
 *
 * <p>All of them default to the stock behaviour of the ported earliest-deadline-first queue: one tick
 * body per acquisition with the whole backlog written off, no local backlogs, no stealing, no lease
 * warning and no per-tick recording. A server that opts into Kairos therefore keeps Folia's semantics
 * unless the operator asks for more.
 *
 * <p>The values are read once when the scheduler starts. Changing any of them needs a restart,
 * exactly like the rest of the section.
 *
 * @param catchUpCapTicks      tick bodies the fund may run back to back for one late region, at least 1
 * @param catchUpBudgetMillis  wall clock a single catch-up burst may spend, or 0 for no time limit
 * @param affinityQueueSize    ticks a runner may keep for itself, or 0 to keep the single queue
 * @param stealThresholdMillis how late a kept tick must be before another runner may take it, at least 0
 * @param leaseTimeoutMillis   permit wait above which the supervisor warns, or 0 for accounting only
 * @param jfr                  whether the per-tick and per-pass recording is armed
 */
public record KairosKernel(
    int catchUpCapTicks,
    int catchUpBudgetMillis,
    int affinityQueueSize,
    int stealThresholdMillis,
    int leaseTimeoutMillis,
    boolean jfr
) {

    /**
     * Validates the knobs.
     *
     * @throws IllegalArgumentException if a value is outside its documented range
     */
    public KairosKernel {
        if (catchUpCapTicks < 1) {
            throw new IllegalArgumentException("catchUpCapTicks must be at least 1, got " + catchUpCapTicks);
        }
        if (catchUpBudgetMillis < 0) {
            throw new IllegalArgumentException(
                "catchUpBudgetMillis must not be negative, got " + catchUpBudgetMillis
            );
        }
        if (affinityQueueSize < 0) {
            throw new IllegalArgumentException(
                "affinityQueueSize must not be negative, got " + affinityQueueSize
            );
        }
        if (stealThresholdMillis < 0) {
            throw new IllegalArgumentException(
                "stealThresholdMillis must not be negative, got " + stealThresholdMillis
            );
        }
        if (leaseTimeoutMillis < 0) {
            throw new IllegalArgumentException(
                "leaseTimeoutMillis must not be negative, got " + leaseTimeoutMillis
            );
        }
    }

    /**
     * Creates the knobs of everything but the recording, which is off.
     *
     * @param catchUpCapTicks      tick bodies one late region may run back to back
     * @param catchUpBudgetMillis  wall clock a single catch-up burst may spend
     * @param affinityQueueSize    ticks a runner may keep for itself
     * @param stealThresholdMillis how late a kept tick must be before another runner may take it
     * @param leaseTimeoutMillis   permit wait above which the supervisor warns
     */
    public KairosKernel(
        final int catchUpCapTicks,
        final int catchUpBudgetMillis,
        final int affinityQueueSize,
        final int stealThresholdMillis,
        final int leaseTimeoutMillis
    ) {
        this(catchUpCapTicks, catchUpBudgetMillis, affinityQueueSize, stealThresholdMillis, leaseTimeoutMillis, false);
    }

    /**
     * Returns the stock behaviour: Folia's single queue, one tick body per acquisition, no recording.
     *
     * <p>The budget is the documented default of {@code catch-up.budget-millis} even though nothing can
     * use it while the cap is one; keeping the two in step means a file that only raises the cap also
     * gets the documented budget instead of a surprise zero.
     *
     * @return the default knobs, never {@code null}
     */
    public static KairosKernel defaults() {
        return new KairosKernel(1, 50, 0, 3, 0, false);
    }

    /**
     * Returns whether the fund may run more than one tick body for a late region.
     *
     * @return {@code true} when catch-up is enabled
     */
    public boolean catchUp() {
        return this.catchUpCapTicks > 1 && this.catchUpBudgetMillis > 0;
    }

    /**
     * Returns whether a runner may keep ticks of its own.
     *
     * @return {@code true} when soft affinity is enabled
     */
    public boolean affinity() {
        return this.affinityQueueSize > 0;
    }

    /**
     * Returns the same knobs with the recording switch replaced.
     *
     * @param recording whether the per-tick and per-pass JFR events are armed
     * @return a kernel with the same limits, never {@code null}
     */
    public KairosKernel withJfr(final boolean recording) {
        return new KairosKernel(
            this.catchUpCapTicks,
            this.catchUpBudgetMillis,
            this.affinityQueueSize,
            this.stealThresholdMillis,
            this.leaseTimeoutMillis,
            recording
        );
    }
}
