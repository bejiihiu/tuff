package kz.bejiihiu.tuff.threads.internal.impl.controller;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import kz.bejiihiu.tuff.threads.ExecutionPoolRegistry;
import kz.bejiihiu.tuff.threads.Lease;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.LeaseTimeoutException;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.ThreadResourceController;
import kz.bejiihiu.tuff.threads.internal.impl.budget.BudgetDeductions;
import kz.bejiihiu.tuff.threads.internal.impl.budget.BudgetPlan;
import kz.bejiihiu.tuff.threads.internal.impl.budget.BudgetPlanner;
import kz.bejiihiu.tuff.threads.internal.impl.budget.PoolBudget;
import kz.bejiihiu.tuff.threads.internal.spi.MonotonicClock;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadEventSink;
import kz.bejiihiu.tuff.threads.internal.spi.TopologyView;

/**
 * Default resource controller: one permit ledger, one budget plan, one lock.
 *
 * <p>The budget is computed exactly once, in the constructor, from the pools that are registered at that
 * moment. That is not laziness: a live server cannot change its processor count, and a second ledger
 * computed later from a different topology would silently disagree with the leases already handed out.
 * A pool registered after the plan exists has no budget and no leases, which surfaces the mistake
 * instead of hiding it.
 *
 * <p>All shared state - in use counters, waiters, the global admission counter - lives under one lock.
 * The lock is only held while deciding and while parking a waiter; the event sink is called outside it.
 * A parked request holds no permit: the permit is taken at grant time, not at request time, so a lease
 * never waits while holding another lease. That is a structural guarantee for a flat request, and the
 * one thing it cannot see is a caller that nests leases - acquire, then block again while still holding
 * the first. Nesting is a caller error and documented as such rather than guessed at, because a lease
 * may legally be released by a different thread than the one that acquired it and ownership tracking
 * would produce false rejections instead of safety.
 *
 * <p>Admission has two layers. Per pool permits protect a pool's own capacity, so a compute burst can
 * never take the region pool's slots. The optional global cap
 * ({@link ThreadManagementSettings.Resources#maxConcurrentTasks()}) limits background work as a whole;
 * region and network pools are exempt from it, because a cap meant to keep background work away from the
 * server must not be able to starve the server itself.
 */
public final class ResourceControllerImpl implements ThreadResourceController {

    private final MonotonicClock clock;
    private final ExecutionPoolRegistry registry;
    private final RegionThreadDetector regionThreads;
    private final ThreadEventSink events;
    private final BudgetPlan plan;
    private final int globalLimit;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition permitFreed = this.lock.newCondition();
    private final Map<PoolId, PoolState> pools;
    private final List<Waiter> waiters = new ArrayList<>();
    private final AtomicLong uids = new AtomicLong();
    private long sequence;
    private int globalInUse;
    private volatile boolean closed;

    /**
     * Builds the controller and its single budget plan.
     *
     * @param topology      processor topology to size against
     * @param registry      pool registry, read once to build the ledger
     * @param resources     resource settings, providing the reserve and the global cap
     * @param deductions    threads already claimed outside the managed budget
     * @param clock         monotonic clock for grant and wait timestamps
     * @param regionThreads detector for region tick threads
     * @param events        event sink for lease timeouts
     * @throws NullPointerException if a reference is {@code null}
     */
    public ResourceControllerImpl(
        final TopologyView topology,
        final ExecutionPoolRegistry registry,
        final ThreadManagementSettings.Resources resources,
        final BudgetDeductions deductions,
        final MonotonicClock clock,
        final RegionThreadDetector regionThreads,
        final ThreadEventSink events
    ) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.regionThreads = Objects.requireNonNull(regionThreads, "regionThreads");
        this.events = Objects.requireNonNull(events, "events");
        this.globalLimit = Objects.requireNonNull(resources, "resources").maxConcurrentTasks();
        final List<PoolSpec> specs = registry.specs();
        this.plan = new BudgetPlanner(topology, resources, deductions).plan(specs);
        final Map<PoolId, PoolState> states = new HashMap<>();
        for (final PoolSpec spec : specs) {
            final Optional<PoolBudget> planned = this.plan.pool(spec.id());
            if (planned.isPresent()) {
                states.put(spec.id(), new PoolState(planned.get(), spec));
            }
        }
        this.pools = Map.copyOf(states);
    }

    @Override
    public int usableCores() {
        return this.plan.usableCores();
    }

    @Override
    public ResourceBudget totalBudget() {
        return ResourceBudget.defaults();
    }

    @Override
    public Optional<ResourceBudget> budget(final PoolId pool) {
        Objects.requireNonNull(pool, "pool");
        if (!this.registry.contains(pool)) {
            return Optional.empty();
        }
        final Optional<PoolBudget> planned = this.plan.pool(pool);
        if (planned.isEmpty()) {
            return Optional.empty();
        }
        final int cores = planned.get().capacity();
        return Optional.of(ResourceBudget.fixed(cores, Math.min(planned.get().reservedCores(), cores)));
    }

    @Override
    public Lease acquire(final LeaseRequest request) {
        Objects.requireNonNull(request, "request");
        final PoolId poolId = request.pool();
        final PoolState state = this.pools.get(poolId);
        if (this.closed) {
            throw new IllegalStateException(
                "the thread manager is not running; pool '" + poolId + "' cannot lease capacity"
            );
        }
        if (state == null || !this.registry.contains(poolId)) {
            throw new IllegalStateException("pool '" + poolId + "' is not registered with this controller");
        }
        if (!state.managed) {
            throw new IllegalStateException(
                "pool '" + poolId + "' is registered below capacity coverage; raise it to L3 before leasing from it"
            );
        }
        if (!request.immediate() && this.regionThreads.isRegionThread(Thread.currentThread())) {
            throw new IllegalStateException(
                "a region execution thread cannot wait for a lease from pool '" + poolId
                    + "': it is the resource the wait would block on; send a non blocking request instead"
            );
        }
        final long startedAt = this.clock.nanoTime();
        final Optional<LeaseImpl> granted = this.attempt(state, request);
        if (granted.isPresent()) {
            return granted.get();
        }
        this.emitTimedOut(request);
        throw new LeaseTimeoutException(
            poolId,
            Duration.ofNanos(Math.max(0L, this.clock.nanoTime() - startedAt))
        );
    }

    @Override
    public void release(final Lease lease) {
        Objects.requireNonNull(lease, "lease");
        if (!(lease instanceof LeaseImpl impl) || impl.owner() != this) {
            throw new IllegalArgumentException("lease was not granted by this controller");
        }
        impl.release();
    }

    @Override
    public SaturationReport saturation(final PoolId pool) {
        Objects.requireNonNull(pool, "pool");
        final PoolState state = this.pools.get(pool);
        if (state == null || !this.registry.contains(pool)) {
            return SaturationReport.unknown(pool);
        }
        final long now = this.clock.nanoTime();
        final int inUse;
        final int waiting;
        final long oldest;
        this.lock.lock();
        try {
            inUse = state.inUse;
            int count = 0;
            long oldestWait = Long.MAX_VALUE;
            for (final Waiter waiter : this.waiters) {
                if (waiter.state == state) {
                    count++;
                    oldestWait = Math.min(oldestWait, waiter.sinceNanos);
                }
            }
            waiting = count;
            oldest = count == 0 ? 0L : oldestWait;
        } finally {
            this.lock.unlock();
        }
        final int capacity = state.capacity;
        final double utilisation = capacity <= 0
            ? (inUse > 0 ? 1.0d : 0.0d)
            : Math.min(1.0d, inUse / (double) capacity);
        final double queuePressure = capacity <= 0 ? 0.0d : Math.min(1.0d, waiting / (double) capacity);
        final long longestWait = waiting == 0 ? 0L : Math.max(0L, now - oldest);
        return new SaturationReport(
            pool,
            utilisation,
            queuePressure,
            waiting,
            longestWait,
            levelOf(utilisation, waiting, capacity)
        );
    }

    /**
     * Returns the single budget plan of this controller.
     *
     * <p>Read only. It exists so the runtime and commands can print the ledger without recomputing it.
     *
     * @return the plan
     */
    public BudgetPlan plan() {
        return this.plan;
    }

    /**
     * Returns how many permits of a pool are currently held.
     *
     * @param pool pool id
     * @return held permits, zero for an unknown pool
     */
    public int inUse(final PoolId pool) {
        this.lock.lock();
        try {
            final PoolState state = this.pools.get(pool);
            return state == null ? 0 : state.inUse;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Returns how many threads are parked waiting for a pool.
     *
     * @param pool pool id
     * @return waiter count, zero for an unknown pool
     */
    public int waiting(final PoolId pool) {
        this.lock.lock();
        try {
            final PoolState state = this.pools.get(pool);
            if (state == null) {
                return 0;
            }
            int count = 0;
            for (final Waiter waiter : this.waiters) {
                if (waiter.state == state) {
                    count++;
                }
            }
            return count;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Closes admission and wakes every waiter.
     *
     * <p>Held permits stay valid: work that is already running must finish, and releasing it later still
     * works. Parked waiters fail with {@link IllegalStateException} instead of waiting out their timeout,
     * which is what keeps shutdown bounded. Idempotent.
     */
    public void close() {
        this.lock.lock();
        try {
            if (this.closed) {
                return;
            }
            this.closed = true;
            this.permitFreed.signalAll();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Checks whether admission is closed.
     *
     * @return {@code true} after {@link #close()}
     */
    public boolean isClosed() {
        return this.closed;
    }

    /**
     * Returns a permit to its pool and hands it to the best waiter, if any.
     *
     * <p>Called by {@link LeaseImpl}; package private so the lease stays the only public handle.
     *
     * @param pool   pool the permit belongs to
     * @param global whether the permit also held shared background admission
     */
    void releasePermit(final PoolId pool, final boolean global) {
        this.lock.lock();
        try {
            final PoolState state = this.pools.get(pool);
            if (state != null && state.inUse > 0) {
                state.inUse--;
            }
            if (global && this.globalInUse > 0) {
                this.globalInUse--;
            }
            this.grantWaitersLocked();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Tries to grant a permit, parking the caller when it asked for a wait.
     *
     * @param state   pool state
     * @param request lease request
     * @return the granted lease, or empty on timeout
     * @throws IllegalStateException if admission was closed while the request was in flight
     */
    private Optional<LeaseImpl> attempt(final PoolState state, final LeaseRequest request) {
        final long now = this.clock.nanoTime();
        final Duration budget = waitBudget(request, untilDeadlineNanos(request, now));
        final long budgetNanos = nanos(budget);
        this.lock.lock();
        try {
            if (this.closed) {
                throw new IllegalStateException(
                    "the thread manager stopped while pool '" + state.id + "' was requesting a lease"
                );
            }
            final LeaseImpl immediate = this.tryGrantLocked(state, request, now);
            if (immediate != null) {
                return Optional.of(immediate);
            }
            if (budgetNanos <= 0L) {
                return Optional.empty();
            }
            final Waiter waiter = new Waiter(
                state, request, this.sequence++, now, saturatingAdd(now, budgetNanos)
            );
            this.waiters.add(waiter);
            long remaining = budgetNanos;
            try {
                while (true) {
                    if (waiter.lease != null) {
                        return Optional.of(waiter.lease);
                    }
                    if (this.closed) {
                        throw new IllegalStateException(
                            "the thread manager stopped while pool '" + state.id + "' was waiting for a lease"
                        );
                    }
                    if (remaining <= 0L) {
                        return Optional.empty();
                    }
                    remaining = this.permitFreed.awaitNanos(remaining);
                }
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                if (waiter.lease != null) {
                    return Optional.of(waiter.lease);
                }
                return Optional.empty();
            } finally {
                this.waiters.remove(waiter);
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Hands free permits to waiters, highest weight first and FIFO inside a weight.
     *
     * <p>Called under the lock, from release and from admission changes. Never grants after
     * {@link #close()}: a waiter woken by shutdown must fail, not receive a permit it cannot use.
     */
    private void grantWaitersLocked() {
        if (this.closed || this.waiters.isEmpty()) {
            return;
        }
        boolean granted = false;
        while (true) {
            final long now = this.clock.nanoTime();
            final Waiter best = this.selectWaiterLocked(now);
            if (best == null) {
                break;
            }
            final LeaseImpl lease = this.tryGrantLocked(best.state, best.request, now);
            if (lease == null) {
                break;
            }
            best.lease = lease;
            granted = true;
        }
        if (granted) {
            this.permitFreed.signalAll();
        }
    }

    /**
     * Picks the next waiter to serve.
     *
     * @param now current monotonic time
     * @return the best waiter, or {@code null} when none can be served
     */
    private Waiter selectWaiterLocked(final long now) {
        Waiter best = null;
        for (final Waiter waiter : this.waiters) {
            if (waiter.lease != null || now >= waiter.waitDeadlineNanos || !this.canGrantLocked(waiter.state)) {
                continue;
            }
            if (best == null
                || waiter.state.weight > best.state.weight
                || (waiter.state.weight == best.state.weight && waiter.sequence < best.sequence)) {
                best = waiter;
            }
        }
        return best;
    }

    /**
     * Checks whether a pool could be granted one more permit right now.
     *
     * @param state pool state
     * @return {@code true} if the pool and the global cap both allow it
     */
    private boolean canGrantLocked(final PoolState state) {
        if (state.inUse >= state.capacity) {
            return false;
        }
        return state.critical || this.globalLimit < 0 || this.globalInUse < this.globalLimit;
    }

    /**
     * Grants one permit without waiting.
     *
     * @param state   pool state
     * @param request lease request
     * @param now     grant moment
     * @return the lease, or {@code null} when no permit could be taken
     */
    private LeaseImpl tryGrantLocked(final PoolState state, final LeaseRequest request, final long now) {
        if (!this.canGrantLocked(state)) {
            return null;
        }
        state.inUse++;
        final boolean global = !state.critical;
        if (global) {
            this.globalInUse++;
        }
        return new LeaseImpl(this, this.uids.incrementAndGet(), state.id, now, request.deadlineNanos(), global);
    }

    /**
     * Reports a timed out request without letting a broken sink break the caller.
     *
     * @param request the request that failed
     */
    private void emitTimedOut(final LeaseRequest request) {
        try {
            this.events.leaseTimedOut(request);
        } catch (RuntimeException _) {
            // a sink is not allowed to turn a timeout into a crash; it is traced elsewhere
        }
    }

    /**
     * Maps a pool kind to its admission weight.
     *
     * <p>Weights only order waiters for the shared background cap. They never change a pool's own
     * permits, so a low weight can be slow but can never be starved by arithmetic.
     *
     * @param kind pool kind
     * @return the weight, higher is served first
     */
    private static int weightOf(final PoolKind kind) {
        return switch (kind) {
            case REGION_EXECUTION -> 100;
            case NETWORK -> 90;
            case CHUNK, IO -> 50;
            case COMPUTE -> 30;
            case VIRTUAL -> 20;
            case SERVICE -> 10;
        };
    }

    /**
     * Summarises utilisation and queue pressure into one level.
     *
     * @param utilisation share of permits in use
     * @param waiting     parked waiters
     * @param capacity    permits of the pool
     * @return the level
     */
    private static SaturationReport.Level levelOf(final double utilisation, final int waiting, final int capacity) {
        if (waiting > 0) {
            return capacity <= 0 || utilisation >= 1.0d
                ? SaturationReport.Level.SATURATED
                : SaturationReport.Level.BUSY;
        }
        if (capacity <= 0 || utilisation <= 0.5d) {
            return SaturationReport.Level.IDLE;
        }
        return utilisation < 0.9d ? SaturationReport.Level.NORMAL : SaturationReport.Level.BUSY;
    }

    /**
     * Computes how long a request may still wait.
     *
     * <p>The smaller of the caller's patience and the remaining time to the deadline. A request whose
     * deadline already passed gets a single non blocking attempt.
     *
     * @param request            lease request
     * @param untilDeadlineNanos nanoseconds left until the deadline
     * @return the wait budget, never negative
     */
    private static Duration waitBudget(final LeaseRequest request, final long untilDeadlineNanos) {
        if (request.immediate() || untilDeadlineNanos <= 0L) {
            return Duration.ZERO;
        }
        final Duration untilDeadline = Duration.ofNanos(untilDeadlineNanos);
        return request.maxWait().compareTo(untilDeadline) < 0 ? request.maxWait() : untilDeadline;
    }

    /**
     * Computes the distance to a deadline without overflowing.
     *
     * @param request lease request
     * @param now     current monotonic time
     * @return nanoseconds left, or {@link Long#MAX_VALUE} when the subtraction would overflow
     */
    private static long untilDeadlineNanos(final LeaseRequest request, final long now) {
        try {
            return Math.subtractExact(request.deadlineNanos(), now);
        } catch (ArithmeticException _) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Mutable per pool ledger entry.
     *
     * <p>All fields except {@link #inUse} are fixed at construction; {@code inUse} is only touched under
     * the controller lock.
     */
    private static final class PoolState {

        private final PoolId id;
        private final int capacity;
        private final int weight;
        private final boolean critical;
        private final boolean managed;
        private int inUse;

        private PoolState(final PoolBudget budget, final PoolSpec spec) {
            this.id = budget.pool();
            this.capacity = budget.capacity();
            this.weight = weightOf(budget.kind());
            this.critical = budget.critical();
            this.managed = spec.managesCapacity();
        }
    }

    /**
     * A parked lease request.
     *
     * <p>{@link #lease} is {@code null} until a releasing thread hands one over; both the field and the
     * waiters list are guarded by the controller lock. The grant-before-wake handoff is what makes
     * "released permit goes to a specific waiter" atomic, so no permit is ever lost in a race between a
     * release and a timeout.
     */
    private static final class Waiter {

        private final PoolState state;
        private final LeaseRequest request;
        private final long sequence;
        private final long sinceNanos;
        private final long waitDeadlineNanos;
        private LeaseImpl lease;

        private Waiter(
            final PoolState state,
            final LeaseRequest request,
            final long sequence,
            final long sinceNanos,
            final long waitDeadlineNanos
        ) {
            this.state = state;
            this.request = request;
            this.sequence = sequence;
            this.sinceNanos = sinceNanos;
            this.waitDeadlineNanos = waitDeadlineNanos;
        }
    }

    /**
     * Converts a duration to nanoseconds, saturating instead of throwing.
     *
     * @param duration duration to convert
     * @return nanoseconds, or {@link Long#MAX_VALUE} when the value does not fit
     */
    private static long nanos(final Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException _) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Adds two non negative nanosecond values without wrapping.
     *
     * @param base  starting value
     * @param delta value to add
     * @return the sum, saturated at {@link Long#MAX_VALUE}
     */
    private static long saturatingAdd(final long base, final long delta) {
        final long sum = base + delta;
        if (((base ^ sum) & (delta ^ sum)) < 0L) {
            return Long.MAX_VALUE;
        }
        return sum;
    }
}
