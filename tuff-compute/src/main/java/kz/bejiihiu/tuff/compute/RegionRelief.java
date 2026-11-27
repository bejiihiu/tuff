package kz.bejiihiu.tuff.compute;

/**
 * Offloads work from an overloaded region without giving up ownership of world state.
 *
 * <p>A call to {@link #submit(RegionReliefOwner, RegionReliefWork)} is made on the owning region
 * thread and returns immediately: either the request is accepted and will be computed on a pool
 * worker, or the work already ran inline on the calling thread. Both outcomes are correct; offloading
 * is an optimisation, never a requirement.
 *
 * <p>The whole path preserves three guarantees:
 * <ul>
 *     <li><b>ownership</b> - the compute body runs off-region and must be pure; the applicator runs
 *     only on the owning region thread and only while the snapshot epoch is current;</li>
 *     <li><b>boundedness</b> - one region never has more than
 *     {@link RegionReliefSettings#maxInFlightPerRegion()} accepted requests in flight, the consumer
 *     queue is bounded and a request that waited too long expires instead of arriving late;</li>
 *     <li><b>no obligation</b> - disabled compute, a full queue, a refusal, an expired request or a
 *     stopped region all end in "ran inline" or "dropped", never in lost work or a failed tick.</li>
 * </ul>
 *
 * <p>The service is owned by the scheduler. It exists even while {@code async-compute.enabled} is
 * {@code false}; submissions then run inline immediately, so callers do not need a special case for
 * a disabled engine.
 *
 * <p>All methods are thread safe. {@link #submit(RegionReliefOwner, RegionReliefWork)} is the only
 * one restricted to a thread, and that restriction is checked.
 */
public interface RegionRelief {

    /**
     * Id of the compute consumer that receives relief work.
     */
    String CONSUMER_ID = RegionReliefSettings.CONSUMER_ID;

    /**
     * Checks whether relief offloads work at all.
     *
     * <p>{@code false} means every submission runs inline. The consumer stays registered and its
     * metrics keep counting, so the state is visible; nothing is queued.
     *
     * @return {@code true} when requests may be handed to the compute pool
     */
    boolean enabled();

    /**
     * Submits one work item from the owning region thread.
     *
     * <p>Must be called on {@link RegionReliefOwner#thread()}. When the work is accepted, the compute
     * body runs on a pool worker and the applicator is scheduled back onto the owner thread. When it
     * is not - relief switched off, pool disabled, queue full, per-region cap reached, request
     * expired - the same work runs inline on the calling thread before this method returns.
     *
     * <p>The method never blocks waiting for a worker, never throws for a pool condition and never
     * runs the applicator twice.
     *
     * @param owner region that owns the work
     * @param work  work to compute and apply
     * @param <S>   type of the snapshot
     * @param <R>   type of the computed result
     * @return {@code true} if the work was handed to the pool, {@code false} if it already ran inline
     * @throws NullPointerException  if an argument is {@code null}
     * @throws IllegalStateException if called from a thread other than the owner's
     */
    <S, R> boolean submit(RegionReliefOwner owner, RegionReliefWork<S, R> work);

    /**
     * Cancels every outstanding request of one region and forgets its in-flight counter.
     *
     * <p>Queued requests are cancelled cooperatively, running requests are flagged so their result is
     * dropped instead of applied, and later results of that region are dropped as well. Called when a
     * region stops; it is idempotent and never blocks.
     *
     * @param owner region that is going away
     * @return how many requests were cancelled or dropped
     * @throws NullPointerException if {@code owner} is {@code null}
     */
    int cancel(RegionReliefOwner owner);

    /**
     * Returns the counters and gauges of the relief path.
     *
     * @return an immutable snapshot, never {@code null}
     */
    RegionReliefMetrics metrics();
}
