package kz.bejiihiu.tuff.compute;

/**
 * The region side of a relief request: who owns the work, how to run back on it, and whether a result
 * is still wanted.
 *
 * <p>This is the seam that keeps the compute module free of any server type. A region implements it
 * over its own scheduler and keeps the object for as long as the region lives; the relief service
 * stores it only while requests of that owner are in flight.
 *
 * <p>Ownership is absolute. {@link #runOnOwner(Runnable)} is the only way a computed result reaches
 * world state, and the service refuses to call an applicator from any other thread. An implementation
 * that runs the task anywhere else breaks the contract and the result is dropped, never applied.
 *
 * <p>All methods are called from more than one thread: {@link #thread()} and the two policy checks
 * from compute workers, {@link #runOnOwner(Runnable)} from a compute worker, and the runnable itself
 * from the owner thread.
 */
public interface RegionReliefOwner {

    /**
     * Returns the thread that owns the region.
     *
     * <p>Relief work may only be submitted from this thread, and the applicator may only run on it.
     * The identity comparison is intentional: a region is tied to one live thread for as long as it
     * exists, so a different thread object is a different owner even when names collide.
     *
     * @return the owning region thread, never {@code null}
     */
    Thread thread();

    /**
     * Checks whether a snapshot epoch is still the current one.
     *
     * <p>Called on the owning thread right before the applicator runs. A snapshot whose epoch moved on
     * is stale: applying it would write a result computed from state that no longer exists, so the
     * relief service drops it and counts it instead.
     *
     * @param epoch epoch of the snapshot that produced the result
     * @return {@code true} if the result may still be applied
     */
    boolean isCurrent(long epoch);

    /**
     * Checks whether the region is gone.
     *
     * <p>Called on a compute worker when a result arrives and again on the owner thread before the
     * applicator runs. A stopped region never gets a result applied and its pending requests are
     * cancelled.
     *
     * @return {@code true} if the region no longer accepts work
     */
    boolean isStopped();

    /**
     * Runs a task on the owning region thread.
     *
     * <p>Called from a compute worker, so the implementation must schedule the task onto the region
     * rather than run it in the calling thread. The task itself re-checks the owner thread and drops
     * the result instead of applying it off-region, so a scheduling mistake costs a result, never
     * world integrity.
     *
     * @param task task to run on the owner thread, never {@code null}
     */
    void runOnOwner(Runnable task);
}
