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
