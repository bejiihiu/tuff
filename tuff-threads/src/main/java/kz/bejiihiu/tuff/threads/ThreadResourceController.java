package kz.bejiihiu.tuff.threads;

import java.util.Optional;

/**
 * Owner of the shared CPU budget and the lease ledger.
 *
 * <p>The controller is the one place that adds up reserves and minimums, decides what is actually
 * usable after the critical lanes took their share, and grants leases without letting background work
 * starve region ticks or network loops. Pool owners ask it for capacity; they never compute a budget
 * themselves, because two independent budgets are exactly how overcommit appears.
 *
 * <p>Leases are advisory for work that is already running and mandatory for work that is about to
 * start. The controller never interrupts a running task, never moves a task between pools and never
 * changes a pool's queue. Its only levers are admission (grant or refuse a lease), pool sizing for
 * pools whose owner exposes resize, and degradation reports.
 *
 * <p>Threading: every method is thread safe. {@link #acquire(LeaseRequest)} may block only when the
 * request asks for a positive wait, and only outside the region execution pool. All other methods are
 * non blocking and cheap enough for a sampler; none of them may be called from a region tick thread in
 * a loop.
 */
public interface ThreadResourceController {

    /**
     * Returns the number of cores available to managed pools right now.
     *
     * <p>This is the resolved value after the configured reserve and the container aware processor
     * count, not the raw machine size. It is the denominator every pool budget is resolved against,
     * and it may change between calls when the platform reports a different topology.
     *
     * @return usable cores, at least one
     */
    int usableCores();

    /**
     * Returns the total budget the controller may divide.
     *
     * <p>An elastic budget with share {@code 1.0} and no reserve, resolved against
     * {@link #usableCores()}. It exists so callers can print one coherent ledger instead of
     * recomputing a formula.
     *
     * @return the total managed budget
     */
    ResourceBudget totalBudget();

    /**
     * Returns the effective budget of one pool.
     *
     * <p>For a registered pool this is its declared {@link PoolSpec#budget()} resolved against the
     * current capacity, with the reserve floor applied. Empty means the pool is not registered or has
     * been unregistered.
     *
     * @param pool pool id
     * @return the resolved budget, or an empty optional if the pool is unknown
     * @throws NullPointerException if {@code pool} is {@code null}
     */
    Optional<ResourceBudget> budget(PoolId pool);

    /**
     * Tries to acquire one unit of capacity from a pool.
     *
     * <p>The returned lease is granted immediately or the call fails; there is no state where the
     * caller holds a lease it did not receive. A request with zero wait never blocks and fails fast
     * when no capacity is free. A request with a positive wait may park up to the smaller of the wait
     * and the deadline, then fails with {@link LeaseTimeoutException}.
     *
     * <p>The caller must release the lease when the unit of work ends. The controller does not track
     * ownership of the work and cannot release it for the caller; a lease that is never released
     * reduces the pool's available capacity until the pool is shut down and is reported as a leak.
     *
     * <p>Calling this from a thread accounted to the {@code region-execution} pool is rejected: that
     * thread is the resource a lease would wait for, so blocking there is a self deadlock.
     *
     * @param request lease request
     * @return the granted lease, never {@code null}
     * @throws NullPointerException  if {@code request} is {@code null}
     * @throws LeaseTimeoutException if the capacity was not granted within the request's wait
     * @throws IllegalStateException if the manager is not running, the pool is unknown, the pool is not
     *                               managed, or the caller is a region execution thread asking for a
     *                               blocking wait
     */
    Lease acquire(LeaseRequest request);

    /**
     * Releases a lease.
     *
     * <p>Equivalent to {@link Lease#release()}. Idempotent and safe to call from any thread, including
     * after shutdown. Releasing a lease that was never granted by this controller is rejected with
     * {@link IllegalArgumentException}, because it usually means two pools or two managers share a
     * handle by mistake.
     *
     * @param lease lease to release
     * @throws NullPointerException     if {@code lease} is {@code null}
     * @throws IllegalArgumentException if the lease was not granted by this controller
     */
    void release(Lease lease);

    /**
     * Returns the current saturation of one pool.
     *
     * <p>For a pool without a probe this is {@link SaturationReport#unknown(PoolId)}; it never throws
     * for an unknown pool, because commands and alerts must be able to print something useful about an
     * id that just disappeared during reload.
     *
     * @param pool pool id
     * @return the saturation report, never {@code null}
     * @throws NullPointerException if {@code pool} is {@code null}
     */
    SaturationReport saturation(PoolId pool);
}
