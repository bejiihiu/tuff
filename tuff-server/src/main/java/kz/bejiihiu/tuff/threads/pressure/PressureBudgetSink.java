package kz.bejiihiu.tuff.threads.pressure;

/**
 * The pool that can actually give cores away.
 *
 * <p>The pressure controller decides; this is the one writer that turns a decision into a pool size.
 * In the server that writer is the compute adapter, which calls the engine's
 * {@code applyProcessorBudget} - the same single entry point the startup budget uses. Without a managed
 * compute pool nothing is bound here, and the monitor only logs and publishes.
 *
 * <p>Both methods are called from the sampler thread. They must be quick and must not throw: a refused
 * budget is logged by the implementation, it never kills the sampler.
 */
public interface PressureBudgetSink {

    /**
     * Returns the pool's effective capacity before the tick pressure reserve.
     *
     * <p>This is the base both numbers in a decision are computed from: the reserve ceiling is
     * {@code poolBudget - minBudget()} and the applied budget is {@code poolBudget - reserve}. The
     * monitor asks here instead of using the ledger's {@code usable}, because a pool can be capped by
     * its own budget, its {@code max-threads} or load shedding - and a report that printed
     * {@code usable - reserve} would name a budget the engine never sees.
     *
     * <p>The default answers {@code usable}, which is what a pool that owns the whole remainder would
     * get. An implementation that knows its own cap should override it.
     *
     * @param usableCores cores the budget ledger says are available, at least one
     * @return the pool's budget in cores, at least one
     */
    default int poolBudget(final int usableCores) {
        return Math.max(1, usableCores);
    }

    /**
     * Returns the floor the pool never shrinks below, whatever the reserve says.
     *
     * <p>For the compute pool that is {@code workers.min-threads}: the engine spawns that many workers
     * even on a budget of one, so a report that printed a smaller applied budget would name a size the
     * engine never runs at. The default is one, the engine's own lower bound.
     *
     * @return the minimum applied budget, at least one
     */
    default int minBudget() {
        return 1;
    }

    /**
     * Returns the most cores this pool may yield on the given machine.
     *
     * <p>This is where the compute pool's {@code min-threads} floor lives: a pool of two workers on a
     * budget of two has nothing to give, and the answer is zero rather than a negative number.
     *
     * @param usableCores cores the budget ledger says are available
     * @return the reserve ceiling, at least zero
     */
    int maxReserve(int usableCores);

    /**
     * Re-applies the pool's processor budget after the reserve changed.
     *
     * @param reserve cores the pool is yielding right now
     */
    void reserveChanged(int reserve);
}
