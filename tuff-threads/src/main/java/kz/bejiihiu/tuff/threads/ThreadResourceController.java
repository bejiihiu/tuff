package kz.bejiihiu.tuff.threads;

import java.util.List;
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
     * <p>This is the ledger's {@code usable}: the thread ceiling ({@code floor(processors * 0.8)}) minus
     * the lanes that were already deducted (region, chunk, io) and the effective reserve, floored at
     * one. It is <b>the</b> usable number of the module - the snapshot's usable core count is this same
     * value, never the raw {@code processors - reserve} headroom. The raw subtraction before the floor,
     * which may be zero or negative, is printed by {@link #budgetLedger()}.
     *
     * <p>It may change between calls when the registry or the platform topology changed; it is never
     * recomputed by a caller.
     *
     * @return usable cores, at least one
     */
    int usableCores();

    /**
     * Returns the whole budget ledger as one printable line.
     *
     * <p>The line spells out every term of {@link #usableCores()} - cores, ceiling, region, chunk, io,
     * reserve, the raw remainder and the floor when one was applied - so "why does my pool have one
     * core" no longer needs a debugger. It is the same text as the internal plan's ledger, produced from
     * the plan the controller is actually using.
     *
     * @return a single line such as
     *     {@code cores 16: ceiling 12 = region 2 + chunk 0 + io 0 + reserve 1 + usable 9}
     */
    String budgetLedger();

    /**
     * Returns the cores kept out of the shared budget for the critical lanes.
     *
     * <p>This is the guaranteed part of {@code region-execution} and {@code network}: the cores they may
     * always lease, whatever the non critical pools declare, and the part load shedding never touches.
     * It is already subtracted from the base the shared pools resolve their budgets against, so a compute
     * burst cannot take it even when the compute caps add up to more than the machine has.
     *
     * <p>Zero is a normal answer: region threads are usually deducted from the headroom as threads rather
     * than cores, and an unmanaged network pool guarantees nothing. The number is a count of cores of the
     * current ledger, not of the machine.
     *
     * @return reserved cores, at least zero
     */
    int criticalReserve();

    /**
     * Returns how much of the budget is currently withheld from non critical pools.
     *
     * <p>Empty of shed cores in the normal case. When load shedding is configured and a critical pool's
     * queue grows while the shared capacity is exhausted, the controller reduces the capacity of the non
     * critical pools - never below their declared minimum - and reports the pools and the core count here
     * until the critical queue has been calm long enough to give the cores back. Held permits are never
     * revoked; only new leases are refused.
     *
     * @return the report, never {@code null}
     */
    SheddingReport sheddingReport();

    /**
     * Returns the reasons the controller reports for one pool.
     *
     * <p>Complements the reasons a measurement adds: the controller knows about budget exhaustion, a
     * critical pool whose queue is growing ({@link DegradedReason#CRITICAL_POOL_PRESSURE}) and a pool
     * whose capacity is temporarily reduced ({@link DegradedReason#LOAD_SHEDDING}). An unknown pool
     * answers with an empty list rather than throwing, because a command may print an id that was
     * unregistered a moment ago.
     *
     * @param pool pool id
     * @return an immutable list of reasons, possibly empty
     * @throws NullPointerException if {@code pool} is {@code null}
     */
    List<DegradedReason> degradedReasons(PoolId pool);

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
     *                               managed, the caller is a region execution thread asking for a
     *                               blocking wait, or the caller already holds a lease and asks for
     *                               another blocking acquire
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
