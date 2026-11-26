package kz.bejiihiu.tuff.threads;

/**
 * How deeply a pool is integrated with the unified thread manager.
 *
 * <p>The levels are cumulative: a pool at {@link #L3} also satisfies everything {@link #L2} promises.
 * The level is declared in {@link PoolSpec} and shown in every snapshot, so an operator can tell
 * "visible but untouched" apart from "fully executed through the manager" without reading code.
 */
public enum CoverageLevel {

    /**
     * Outside the manager on purpose.
     *
     * <p>Documented exceptions such as JDK owned virtual thread carriers, JFR and logger threads. Such
     * a pool has no registry entry; the runtime scan reports it as unregistered and that is expected.
     */
    L0,

    /**
     * Registered and observed only.
     *
     * <p>The manager sees the pool's threads, counters and saturation, but does not create threads and
     * does not influence capacity. This is the safe default for every pool whose semantics are not
     * fully understood yet.
     */
    L1,

    /**
     * Threads are created through {@link ThreadFactoryProvider}.
     *
     * <p>Names, daemon status, priority and the uncaught exception handler come from a
     * {@link ThreadSpec}, and creation and termination are accounted. Scheduling and capacity stay with
     * the pool owner.
     */
    L2,

    /**
     * The resource controller owns the pool's capacity.
     *
     * <p>Minimum and maximum thread counts, reserves and budget pressure are decided centrally. The
     * owner still creates and runs its own work; only how many threads may exist is influenced.
     */
    L3,

    /**
     * Execution itself goes through a manager owned lane.
     *
     * <p>Full control: the manager owns the queue or lane, the threads and the budget. Only the compute
     * engine is expected to reach this level, because moving region ticks or chunk ordering into a
     * shared lane would break Folia semantics.
     */
    L4;

    /**
     * Checks whether this level is at least the given one.
     *
     * @param other level to compare with
     * @return {@code true} if this level is the same or deeper
     */
    public boolean atLeast(final CoverageLevel other) {
        return this.ordinal() >= other.ordinal();
    }

    /**
     * Checks whether the manager creates the pool's threads.
     *
     * @return {@code true} for {@link #L2} and deeper
     */
    public boolean managesThreads() {
        return this.atLeast(L2);
    }

    /**
     * Checks whether the manager influences how many threads the pool may have.
     *
     * @return {@code true} for {@link #L3} and deeper
     */
    public boolean managesCapacity() {
        return this.atLeast(L3);
    }

    /**
     * Checks whether work is executed through a manager owned lane.
     *
     * @return {@code true} only for {@link #L4}
     */
    public boolean executesThroughManager() {
        return this == L4;
    }
}
