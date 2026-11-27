package kz.bejiihiu.tuff.compute;

/**
 * One unit of work a region may hand to the compute pool while it is overloaded.
 *
 * <p>A relief work item is three values plus one version:
 * <ul>
 *     <li>a {@link #snapshot() snapshot} taken on the region thread and treated as immutable from
 *     then on - nothing in it may reference mutable world state, entities or chunk handles;</li>
 *     <li>a {@link #compute(Object) pure computation} over that snapshot, which runs on a compute
 *     worker and must not read, write or lazily resolve anything belonging to the server;</li>
 *     <li>an {@link #apply(Object) applicator} that runs back on the owning region thread and is the
 *     only place allowed to touch world state;</li>
 *     <li>an {@link #epoch() epoch} identifying the world state the snapshot was taken from, so the
 *     region can drop a result that arrived after the state moved on.</li>
 * </ul>
 *
 * <p>The contract is deliberately narrow: the pool schedules, it never decides what is legal to
 * offload. A subsystem that cannot express its work as this shape must run it inline instead.
 *
 * <p>Instances are created per request and are not reused. They may be read from a compute worker
 * thread and from the owning region thread, so every accessor must be safe to call from both; making
 * the implementation a record over immutable fields is the simplest way to guarantee that.
 *
 * @param <S> type of the snapshot, immutable by contract
 * @param <R> type of the computed result
 */
public interface RegionReliefWork<S, R> {

    /**
     * Returns the immutable snapshot captured on the owning region thread.
     *
     * <p>Called on the region thread when the work is accepted, and possibly again if the offload is
     * refused and the work falls back to running inline. It must not read live world state on the
     * second call: the snapshot is part of the work item, not of the request.
     *
     * @return the snapshot, never {@code null}
     */
    S snapshot();

    /**
     * Returns the version of the world state the snapshot was taken from.
     *
     * <p>The region decides what an epoch means (a tick counter, a block change counter, a structure
     * version); the compute pool only carries it back and asks the owner whether it is still current
     * before applying the result.
     *
     * @return the epoch of the snapshot
     */
    long epoch();

    /**
     * Computes the result from the snapshot on a compute worker.
     *
     * <p>The body must be pure with respect to the server: no world access, no entity access, no
     * blocking IO, no locks held by a region and no calls into any Bukkit or server API. A thrown
     * exception fails the request; it is not an inline fallback, because running the same body again
     * would fail the same way.
     *
     * @param snapshot snapshot returned by {@link #snapshot()}
     * @return the computed result, never {@code null}
     * @throws Exception if the computation fails
     */
    R compute(S snapshot) throws Exception;

    /**
     * Applies the computed result on the owning region thread.
     *
     * <p>Called at most once, only on the owning region thread, and only while {@link #epoch()} is
     * still current according to {@link RegionReliefOwner#isCurrent(long)}. The body may mutate world
     * state that belongs to that region. It must not block and must not schedule the same work again.
     *
     * @param result value returned by {@link #compute(Object)}
     */
    void apply(R result);
}
