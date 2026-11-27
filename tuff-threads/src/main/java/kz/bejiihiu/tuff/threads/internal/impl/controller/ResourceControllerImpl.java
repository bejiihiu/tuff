package kz.bejiihiu.tuff.threads.internal.impl.controller;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.ExecutionPoolRegistry;
import kz.bejiihiu.tuff.threads.Lease;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.LeaseTimeoutException;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.SheddingReport;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.ThreadResourceController;
import kz.bejiihiu.tuff.threads.internal.impl.budget.BudgetDeductions;
import kz.bejiihiu.tuff.threads.internal.impl.budget.BudgetPlan;
import kz.bejiihiu.tuff.threads.internal.impl.budget.BudgetPlanner;
import kz.bejiihiu.tuff.threads.internal.impl.budget.PoolBudget;
import kz.bejiihiu.tuff.threads.internal.spi.MonotonicClock;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadEventSink;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadRegistry;
import kz.bejiihiu.tuff.threads.internal.spi.TopologyView;
import org.jspecify.annotations.Nullable;

/**
 * Default resource controller: one permit ledger, one lock, live against the pool registry.
 *
 * <p>The ledger is not frozen at construction. A pool registered after the controller, or a spec
 * replaced under the same id - which is the documented way coverage moves from L1 to L3 and a budget
 * changes - is picked up the next time the controller is asked about that pool. The refresh re-reads
 * every spec, rebuilds the budget plan and keeps the existing ledger entries and their held permits, so
 * the plan, {@link #budget(PoolId)}, {@link #saturation(PoolId)} and the metrics layer never describe
 * two different worlds. The refresh is O(pools) and only happens when a spec the controller already
 * knows about differs from the registry; the steady state cost is one map read plus one spec comparison.
 *
 * <p>All shared state - in use counters, waiters, the per thread permit count, the global admission
 * counter - lives under one lock. The lock is only held while deciding and while parking a waiter; the
 * event sink is called outside it. A parked request holds no permit: the permit is taken at grant time,
 * not at request time.
 *
 * <p>Nested leases: acquiring immediately while holding a permit is fine, waiting while holding one is
 * not. A thread that already holds a permit and asks for a <em>blocking</em> lease is rejected with a
 * clear message, because that is the caller-side hold-and-wait that can deadlock two workers as easily
 * as one. The count is kept per requesting thread and is decremented by whoever releases the lease, so
 * a lease released on another thread does not leave a phantom holder behind.
 *
 * <p>Region threads are recognised through the thread registry first; the name check is only a fallback
 * for threads nobody registered yet. A retorted tick thread is still a tick thread, and a plugin thread
 * that merely borrowed Folia's name is not.
 *
 * <p>Admission has two layers. Per pool permits protect a pool's own capacity, so a compute burst can
 * never take the region pool's slots. The optional global cap
 * ({@link ThreadManagementSettings.Resources#maxConcurrentTasks()}) limits background work as a whole;
 * region and network pools are exempt from it, because a cap meant to keep background work away from the
 * server must not be able to starve the server itself.
 *
 * <p>The critical lanes have a second guarantee on top of their own permits: the budget planner subtracts
 * their minimum from the remainder the shared pools divide, so no combination of elastic compute caps can
 * claim the cores a starving network loop or tick thread needs. A critical pool that is at its capacity
 * with requests parked reports {@link DegradedReason#CRITICAL_POOL_PRESSURE}; the numbers are in
 * {@link #sheddingReport()}.
 *
 * <p>Load shedding is the temporary half of that protection, and it is opt in
 * ({@link ThreadManagementSettings.Resources#loadShedding()}). While a critical pool has a live parked
 * request and every non critical managed pool is at its capacity, the controller withholds one core more
 * from the non critical pools per cooldown period, lowest admission weight first, never below a pool's
 * declared minimum. When the critical queue has been quiet for a few evaluations it gives one core back
 * per cooldown, so a sawtooth cannot move the pools every second. Held permits are never revoked; only new
 * leases are refused. The policy is restart only, like the rest of the section.
 */
public final class ResourceControllerImpl implements ThreadResourceController {

    /**
     * Cores withheld per shedding decision.
     *
     * <p>One means a ramp, not a jump: an operator sees the reduction grow instead of losing half of the
     * background capacity between two samples.
     */
    private static final int SHED_STEP_CORES = 1;

    /**
     * Consecutive calm evaluations needed before a shed core is given back.
     *
     * <p>One evaluation is one admission decision; three keeps a bursty critical queue from releasing and
     * re-taking the core on every other request.
     */
    private static final int SHED_CALM_EVALUATIONS = 3;

    /**
     * Minimum time between two shedding changes.
     *
     * <p>Five seconds, so a ramp up or down costs at most one core per five seconds and a two core reserve
     * is fully withheld in ten. This is the cooldown half of the hysteresis: without it, the calm counter
     * alone would let a request storm move the pools several times per second.
     */
    private static final long SHED_COOLDOWN_NANOS = 5_000_000_000L;

    private final TopologyView topology;
    private final ExecutionPoolRegistry registry;
    private final ThreadManagementSettings.Resources resources;
    private final BudgetDeductions deductions;
    private final RegionThreadDetector regionThreads;
    private final ThreadEventSink events;
    private final MonotonicClock clock;
    private final int globalLimit;
    private final boolean loadShedding;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition permitFreed = this.lock.newCondition();
    private final List<Waiter> waiters = new ArrayList<>();
    private final Map<Thread, Integer> heldPermits = new HashMap<>();
    private final List<Runnable> capacityListeners = new CopyOnWriteArrayList<>();
    private final AtomicLong uids = new AtomicLong();
    private volatile Ledger ledger;
    private volatile Map<PoolId, Integer> shedShares = Map.of();
    private long sequence;
    private long shedVersion;
    private int globalInUse;
    private int shedCores;
    private int calmEvaluations;
    private long lastShedChangeNanos = Long.MIN_VALUE;
    private volatile boolean closed;

    /**
     * Builds the controller and its first budget ledger.
     *
     * @param topology   processor topology to size against
     * @param registry   pool registry, re-read whenever a spec changes
     * @param threads    thread registry, used to recognise region tick threads
     * @param resources  resource settings, providing the reserve and the global cap
     * @param deductions threads already claimed outside the managed budget
     * @param clock      monotonic clock for grant and wait timestamps
     * @param events     event sink for lease timeouts
     * @throws NullPointerException if a reference is {@code null}
     */
    public ResourceControllerImpl(
        final TopologyView topology,
        final ExecutionPoolRegistry registry,
        final ThreadRegistry threads,
        final ThreadManagementSettings.Resources resources,
        final BudgetDeductions deductions,
        final MonotonicClock clock,
        final ThreadEventSink events
    ) {
        this.topology = Objects.requireNonNull(topology, "topology");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.resources = Objects.requireNonNull(resources, "resources");
        this.deductions = Objects.requireNonNull(deductions, "deductions");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.events = Objects.requireNonNull(events, "events");
        this.globalLimit = resources.maxConcurrentTasks();
        this.loadShedding = resources.loadShedding();
        this.regionThreads = RegionThreadDetector.registered(
            Objects.requireNonNull(threads, "threads"),
            RegionThreadDetector.byThreadName()
        );
        this.installLedger(this.buildLedger(Map.of()));
    }

    @Override
    public int usableCores() {
        return this.ledger.plan().usableCores();
    }

    @Override
    public String budgetLedger() {
        return this.ledger.plan().ledger();
    }

    @Override
    public int criticalReserve() {
        return this.ledger.plan().criticalReserveCores();
    }

    @Override
    public SheddingReport sheddingReport() {
        this.lock.lock();
        try {
            final Map<PoolId, Integer> shares = this.shedShares;
            final List<PoolId> pools = shares.entrySet().stream()
                .filter(entry -> entry.getValue() > 0)
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
            final SheddingReport.State state;
            if (!this.loadShedding) {
                state = SheddingReport.State.OFF;
            } else if (!this.criticalLanesManagedLocked()) {
                state = SheddingReport.State.DORMANT;
            } else if (this.shedCores > 0) {
                state = SheddingReport.State.SHEDDING;
            } else {
                state = SheddingReport.State.ARMED;
            }
            return new SheddingReport(
                this.ledger.plan().criticalReserveCores(),
                this.loadShedding,
                state,
                this.shedCores,
                pools,
                this.summaryLocked(pools, state)
            );
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void addCapacityListener(final Runnable listener) {
        Objects.requireNonNull(listener, "listener");
        this.capacityListeners.add(listener);
    }

    @Override
    public void removeCapacityListener(final Runnable listener) {
        Objects.requireNonNull(listener, "listener");
        this.capacityListeners.remove(listener);
    }

    @Override
    public List<DegradedReason> degradedReasons(final PoolId pool) {
        Objects.requireNonNull(pool, "pool");
        // resolve first: a pool registered after the controller (the compute adapter's L4 spec, the
        // sampler's service spec) is not in the ledger until something asks about it, and a reason list
        // that silently skipped it would disagree with the snapshot
        this.state(pool);
        this.lock.lock();
        try {
            final @Nullable PoolState state = this.ledger.states().get(pool);
            if (state == null) {
                return List.of();
            }
            final List<DegradedReason> reasons = new ArrayList<>(2);
            final Optional<PoolBudget> budget = this.ledger.plan().pool(pool);
            if (state.critical) {
                if (this.hasLiveWaiterLocked(state, this.clock.nanoTime())
                    || !budget.map(PoolBudget::satisfied).orElse(true)) {
                    reasons.add(DegradedReason.CRITICAL_POOL_PRESSURE);
                }
            } else {
                if (this.shedShares.getOrDefault(pool, 0) > 0) {
                    reasons.add(DegradedReason.LOAD_SHEDDING);
                }
                if (budget.isPresent() && !budget.get().satisfied()) {
                    reasons.add(DegradedReason.BUDGET_EXHAUSTED);
                }
            }
            return List.copyOf(reasons);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public ResourceBudget totalBudget() {
        return ResourceBudget.defaults();
    }

    @Override
    public Optional<ResourceBudget> budget(final PoolId pool) {
        Objects.requireNonNull(pool, "pool");
        final @Nullable PoolState state = this.state(pool);
        if (state == null) {
            return Optional.empty();
        }
        // the effective capacity, so a caller that asks "how much may I use right now" gets the number
        // the ledger would grant; while load shedding is active that is below the declared budget, and
        // the declared one stays readable through plan().pool(id)
        final int capacity = this.capacityOf(state);
        return Optional.of(ResourceBudget.fixed(capacity, Math.min(state.reservedCores, capacity)));
    }

    @Override
    public Lease acquire(final LeaseRequest request) {
        Objects.requireNonNull(request, "request");
        final PoolId poolId = request.pool();
        if (this.closed) {
            throw new IllegalStateException(
                "the thread manager is not running; pool '" + poolId + "' cannot lease capacity"
            );
        }
        final @Nullable PoolState state = this.state(poolId);
        if (state == null) {
            throw new IllegalStateException("pool '" + poolId + "' is not registered with the thread manager");
        }
        if (!state.managed) {
            throw new IllegalStateException(
                "pool '" + poolId + "' is registered below capacity coverage; raise it to L3 before leasing from it"
            );
        }
        final Thread caller = Thread.currentThread();
        if (!request.immediate() && this.regionThreads.isRegionThread(caller)) {
            throw new IllegalStateException(
                "a region execution thread cannot wait for a lease from pool '" + poolId
                    + "': it is the resource the wait would block on; send a non blocking request instead"
            );
        }
        final long startedAt = this.clock.nanoTime();
        final Optional<LeaseImpl> granted = this.attempt(state, request, caller);
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
        final @Nullable PoolState state = this.state(pool);
        if (state == null) {
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
        final int capacity = this.capacityOf(state);
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
     * Returns the budget plan currently backing the ledger.
     *
     * <p>Read only. It is replaced, never mutated, when the registry changes, so a caller either sees the
     * old plan or the new one, never a half rebuilt ledger. It exists so the runtime and commands can
     * print the numbers the controller is actually using.
     *
     * @return the current plan
     */
    public BudgetPlan plan() {
        return this.ledger.plan();
    }

    /**
     * Rebuilds the budget ledger from the registry as it is now.
     *
     * <p>The ledger already refreshes itself when a spec changes, so this is not needed on the request
     * path. It exists for bootstrap: the controller is built before any pool is registered, and the
     * runtime wants {@link #plan()} to describe the registered set before the first lease or snapshot
     * asks for it. Held permits and parked waiters are preserved.
     */
    public void refresh() {
        this.lock.lock();
        try {
            this.installLedger(this.buildLedger(this.ledger.states()));
        } finally {
            this.lock.unlock();
        }
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
            final @Nullable PoolState state = this.ledger.states().get(pool);
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
            final @Nullable PoolState state = this.ledger.states().get(pool);
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
     * <p>Called by {@link LeaseImpl}; package private so the lease stays the only public handle. The
     * holder is the thread that requested the lease, not the one releasing it, so the nested acquire
     * bookkeeping stays correct when release happens on another thread.
     *
     * @param pool   pool the permit belongs to
     * @param global whether the permit also held shared background admission
     * @param holder thread that asked for the lease
     */
    void releasePermit(final PoolId pool, final boolean global, final Thread holder) {
        final boolean capacityChanged;
        this.lock.lock();
        try {
            final @Nullable PoolState state = this.ledger.states().get(pool);
            if (state != null && state.inUse > 0) {
                state.inUse--;
            }
            if (global && this.globalInUse > 0) {
                this.globalInUse--;
            }
            this.heldPermits.computeIfPresent(holder, (ignored, count) -> count <= 1 ? null : count - 1);
            this.grantWaitersLocked();
            // after the hand off, so a critical request that was just served does not count as pressure
            capacityChanged = this.evaluateSheddingLocked(this.clock.nanoTime());
        } finally {
            this.lock.unlock();
        }
        if (capacityChanged) {
            this.fireCapacityChanged();
        }
    }

    /**
     * Resolves a pool state against the registry, refreshing the ledger when the spec changed.
     *
     * <p>The fast path is one volatile read, one map read and one spec comparison. A pool that appeared
     * after construction, or whose spec was replaced under the same id, takes the slow path once and is
     * then served from the rebuilt ledger like any other pool.
     *
     * @param pool pool id
     * @return the current state, or {@code null} when the pool is not registered
     */
    private @Nullable PoolState state(final PoolId pool) {
        final Ledger current = this.ledger;
        final @Nullable PoolState state = current.states().get(pool);
        final Optional<PoolSpec> spec = this.registry.spec(pool);
        if (sameSpec(state, spec)) {
            return state;
        }
        this.lock.lock();
        try {
            final Ledger locked = this.ledger;
            final @Nullable PoolState lockedState = locked.states().get(pool);
            final Optional<PoolSpec> lockedSpec = this.registry.spec(pool);
            if (sameSpec(lockedState, lockedSpec)) {
                // somebody else rebuilt the ledger while this thread waited for the lock
                return lockedState;
            }
            this.installLedger(this.buildLedger(locked.states()));
            return this.ledger.states().get(pool);
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Checks whether a ledger entry already matches the registry.
     *
     * @param state ledger entry, may be {@code null}
     * @param spec  current registry spec
     * @return {@code true} when the entry can be used as is
     */
    private static boolean sameSpec(final @Nullable PoolState state, final Optional<PoolSpec> spec) {
        return spec.map(current -> state != null && state.spec.equals(current)).orElse(state == null);
    }

    /**
     * Rebuilds the plan and reuses the ledger entries that are still registered.
     *
     * <p>Entry identity is preserved on purpose: a parked waiter points at its {@link PoolState}, and
     * replacing the object would make the waiter invisible to saturation and unreleasable. Only the
     * numbers derived from the spec change, and only while the controller lock is held.
     *
     * @param previous entries of the ledger being replaced
     * @return the new ledger
     */
    private Ledger buildLedger(final Map<PoolId, PoolState> previous) {
        final List<PoolSpec> specs = this.registry.specs();
        final BudgetPlan plan = new BudgetPlanner(this.topology, this.resources, this.deductions).plan(specs);
        final Map<PoolId, PoolState> states = new HashMap<>(Math.max(16, specs.size() * 2));
        for (final PoolSpec spec : specs) {
            final Optional<PoolBudget> budget = plan.pool(spec.id());
            if (budget.isEmpty()) {
                continue;
            }
            final @Nullable PoolState existing = previous.get(spec.id());
            if (existing == null) {
                states.put(spec.id(), new PoolState(budget.get(), spec));
            } else {
                existing.rebase(budget.get(), spec);
                states.put(spec.id(), existing);
            }
        }
        return new Ledger(plan, Map.copyOf(states));
    }

    /**
     * Publishes a rebuilt ledger and the withheld shares that belong to it.
     *
     * <p>The ledger is the only description of capacity, so the shares have to follow a rebuild: a pool
     * that lost capacity or disappeared cannot keep a share that no longer exists. Called under the lock
     * (or from the constructor before anything else can read the controller).
     *
     * @param rebuilt new ledger
     */
    private void installLedger(final Ledger rebuilt) {
        this.ledger = rebuilt;
        this.publishShedSharesLocked();
    }

    /**
     * Tries to grant a permit, parking the caller when it asked for a wait.
     *
     * @param state   pool state
     * @param request lease request
     * @param caller  requesting thread
     * @return the granted lease, or empty on timeout
     * @throws IllegalStateException if admission was closed while the request was in flight, or the
     *                               caller already holds a permit and asked for a blocking acquire
     */
    private Optional<LeaseImpl> attempt(
        final PoolState state,
        final LeaseRequest request,
        final Thread caller
    ) {
        final long now = this.clock.nanoTime();
        final Duration budget = waitBudget(request, untilDeadlineNanos(request, now));
        final long budgetNanos = nanos(budget);
        this.lock.lock();
        boolean capacityChanged = false;
        try {
            if (this.closed) {
                throw new IllegalStateException(
                    "the thread manager stopped while pool '" + state.id + "' was requesting a lease"
                );
            }
            // every admission decision is a chance to notice a starving critical lane and to give the
            // withheld cores back once it has been quiet; the cooldown keeps this from moving on
            // every request
            capacityChanged = this.evaluateSheddingLocked(now);
            final @Nullable LeaseImpl immediate = this.tryGrantLocked(state, request, now, caller);
            if (immediate != null) {
                return Optional.of(immediate);
            }
            if (budgetNanos <= 0L) {
                return Optional.empty();
            }
            if (this.heldPermits.getOrDefault(caller, 0) > 0) {
                throw new IllegalStateException(
                    "thread '" + caller.getName() + "' already holds a lease and cannot wait for pool '"
                        + state.id + "': release the held lease first, or send a non blocking request"
                );
            }
            final Waiter waiter = new Waiter(
                state, request, caller, this.sequence++, now, saturatingAdd(now, budgetNanos)
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
            if (capacityChanged) {
                // outside the lock: the listener re-reads the ledger and may take the lock again
                this.fireCapacityChanged();
            }
        }
    }

    /**
     * Hands free permits to waiters, highest weight first and FIFO inside a weight.
     *
     * <p>Called under the lock, from release and from admission changes. Never grants after
     * {@link #close()}: a waiter woken by shutdown must fail, not receive a permit it cannot use. A
     * waiter whose pool was unregistered while it parked is skipped and left to its timeout.
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
            final @Nullable LeaseImpl lease = this.tryGrantLocked(best.state, best.request, now, best.caller);
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
    private @Nullable Waiter selectWaiterLocked(final long now) {
        Waiter best = null;
        for (final Waiter waiter : this.waiters) {
            if (waiter.lease != null
                || now >= waiter.waitDeadlineNanos
                || this.ledger.states().get(waiter.state.id) != waiter.state
                || !this.canGrantLocked(waiter.state)) {
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
        if (state.inUse >= this.capacityOf(state)) {
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
     * @param caller  thread that asked for the permit
     * @return the lease, or {@code null} when no permit could be taken
     */
    private @Nullable LeaseImpl tryGrantLocked(
        final PoolState state,
        final LeaseRequest request,
        final long now,
        final Thread caller
    ) {
        if (!this.canGrantLocked(state)) {
            return null;
        }
        state.inUse++;
        final boolean global = !state.critical;
        if (global) {
            this.globalInUse++;
        }
        this.heldPermits.merge(caller, 1, Integer::sum);
        return new LeaseImpl(this, this.uids.incrementAndGet(), state.id, now, request.deadlineNanos(), global, caller);
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
     * Returns what a pool can actually grant right now.
     *
     * <p>The declared capacity minus the share load shedding is withholding from it. The share map is an
     * immutable snapshot replaced under the lock, so this is a volatile read plus a map lookup on the
     * grant path and never a walk over the ledger.
     *
     * @param state pool entry
     * @return effective permits, never negative
     */
    private int capacityOf(final PoolState state) {
        final int shed = this.shedShares.getOrDefault(state.id, 0);
        return shed <= 0 ? state.capacity : Math.max(0, state.capacity - shed);
    }

    /**
     * Runs one load shedding decision.
     *
     * <p>Called under the lock from admission and release, which is why the policy needs no thread of its
     * own: with no traffic there is nothing to shed for. The state machine is deliberately tiny - pressure
     * takes one core per cooldown, calm gives one back after {@link #SHED_CALM_EVALUATIONS} evaluations
     * and a cooldown - because a policy that reacts instantly on both edges is a sawtooth, and the whole
     * point is to keep the non critical pools from oscillating.
     *
     * @param now current monotonic time
     * @return {@code true} when the effective capacity of some pool changed and owners must re-read it
     */
    private boolean evaluateSheddingLocked(final long now) {
        final long before = this.shedVersion;
        if (!this.loadShedding) {
            if (this.shedCores != 0 || !this.shedShares.isEmpty()) {
                this.shedCores = 0;
                this.calmEvaluations = 0;
                this.publishShedSharesLocked();
            }
            return before != this.shedVersion;
        }
        if (!this.criticalPressureLocked(now)) {
            if (this.shedCores == 0) {
                this.calmEvaluations = 0;
                return false;
            }
            this.calmEvaluations++;
            if (this.calmEvaluations < SHED_CALM_EVALUATIONS || !this.cooldownElapsed(now)) {
                return false;
            }
            this.shedCores = Math.max(0, this.shedCores - SHED_STEP_CORES);
            this.calmEvaluations = 0;
            this.lastShedChangeNanos = now;
            this.publishShedSharesLocked();
            return before != this.shedVersion;
        }
        this.calmEvaluations = 0;
        if (!this.cooldownElapsed(now)) {
            return false;
        }
        final int headroom = this.shedHeadroomLocked();
        if (headroom <= 0) {
            return false;
        }
        this.shedCores += Math.min(SHED_STEP_CORES, headroom);
        this.lastShedChangeNanos = now;
        this.publishShedSharesLocked();
        return before != this.shedVersion;
    }

    /**
     * Checks whether a critical lane is waiting while the background lanes are full.
     *
     * <p>Both halves are needed. A parked critical request alone is normal back pressure inside its own
     * pool; it only becomes pressure on the machine when the non critical pools have taken every permit
     * they have and the critical work is what has to run. Without the second half, shedding would take
     * background capacity for a critical pool that is simply saturated on its own accounting.
     *
     * @param now current monotonic time
     * @return {@code true} when a core should be withheld
     */
    private boolean criticalPressureLocked(final long now) {
        boolean criticalWaits = false;
        for (final Waiter waiter : this.waiters) {
            if (waiter.lease == null && waiter.state.critical && now < waiter.waitDeadlineNanos) {
                criticalWaits = true;
                break;
            }
        }
        if (!criticalWaits) {
            return false;
        }
        boolean anyShared = false;
        for (final PoolState state : this.ledger.states().values()) {
            if (state.critical || !state.managed) {
                continue;
            }
            anyShared = true;
            if (state.inUse < this.capacityOf(state)) {
                return false;
            }
        }
        return anyShared;
    }

    /**
     * Returns how many more cores the non critical pools could still give up.
     *
     * <p>The room of a pool is its capacity above the higher of its declared minimum and its declared
     * reserve, minus what is already withheld from it. Zero means the shed already reaches the floor and
     * the policy must stop rather than eat into a guarantee.
     *
     * @return cores that may still be withheld, never negative
     */
    private int shedHeadroomLocked() {
        int headroom = -this.shedCores;
        for (final PoolState state : this.ledger.states().values()) {
            if (state.critical || !state.managed) {
                continue;
            }
            headroom += roomOf(state);
        }
        return Math.max(0, headroom);
    }

    /**
     * Returns how many cores may be withheld from a pool without breaking its floor.
     *
     * @param state pool entry
     * @return cores above the pool's floor, never negative
     */
    private static int roomOf(final PoolState state) {
        final int floor = Math.max(state.minCores, state.reservedCores);
        return Math.max(0, state.capacity - floor);
    }

    /**
     * Spreads the withheld cores over the non critical pools and publishes the result.
     *
     * <p>Lowest admission weight first (service, then virtual, then compute, then chunk and io), one core
     * each per round, so the cheapest work is reduced before the work closest to the critical lanes. The
     * map is replaced as a whole under the lock, which is what makes the shed state visible to readers
     * without a second ledger.
     *
     * <p>When a refresh shrank the pools since the last decision, the requested amount can exceed the
     * room left; the count is then clamped to what was actually applied, so the report never claims to
     * withhold a core that nobody gave up.
     */
    private void publishShedSharesLocked() {
        if (this.shedCores <= 0) {
            this.shedShares = Map.of();
            return;
        }
        final List<PoolState> candidates = new ArrayList<>();
        for (final PoolState state : this.ledger.states().values()) {
            if (!state.critical && state.managed && roomOf(state) > 0) {
                candidates.add(state);
            }
        }
        candidates.sort(Comparator.comparingInt((PoolState state) -> state.weight)
            .thenComparing(state -> state.id));
        final Map<PoolId, Integer> shares = new HashMap<>();
        int remaining = this.shedCores;
        boolean progress = true;
        while (remaining > 0 && progress) {
            progress = false;
            for (final PoolState state : candidates) {
                if (remaining <= 0) {
                    break;
                }
                final int taken = shares.getOrDefault(state.id, 0);
                if (taken >= roomOf(state)) {
                    continue;
                }
                shares.put(state.id, taken + 1);
                remaining--;
                progress = true;
            }
        }
        this.shedCores -= remaining;
        final Map<PoolId, Integer> published = Map.copyOf(shares);
        if (!published.equals(this.shedShares)) {
            this.shedShares = published;
            this.shedVersion++;
        }
    }

    /**
     * Checks whether any critical pool could lease capacity in this wiring.
     *
     * <p>The shedding trigger is a parked critical request, and a request can only be parked by a pool
     * that got past the L3 gate in {@link #acquire(LeaseRequest)}. When region execution and network are
     * at L2, as they are in the shipped wiring, the trigger is unreachable and the report says
     * {@link SheddingReport.State#DORMANT} instead of pretending the protection is active.
     *
     * @return {@code true} when at least one critical pool is registered at capacity coverage
     */
    private boolean criticalLanesManagedLocked() {
        for (final PoolState state : this.ledger.states().values()) {
            if (state.critical && state.managed) {
                return true;
            }
        }
        return false;
    }

    /**
     * Calls every capacity listener after a change that no lease caused.
     *
     * <p>Called outside the controller lock: a listener typically re-reads {@link #budget(PoolId)} and
     * resizes a pool, which may ask the controller again. A listener that throws is ignored - the ledger
     * already moved and an owner's failure must not roll it back.
     */
    private void fireCapacityChanged() {
        for (final Runnable listener : this.capacityListeners) {
            try {
                listener.run();
            } catch (RuntimeException _) {
                // an owner that cannot handle the change must not break admission
            }
        }
    }

    /**
     * Checks whether enough time has passed for another shedding change.
     *
     * @param now current monotonic time
     * @return {@code true} when the cooldown is over
     */
    private boolean cooldownElapsed(final long now) {
        return this.lastShedChangeNanos == Long.MIN_VALUE
            || now - this.lastShedChangeNanos >= SHED_COOLDOWN_NANOS;
    }

    /**
     * Checks whether a pool has a parked request that has not timed out.
     *
     * @param state pool entry
     * @param now   current monotonic time
     * @return {@code true} when at least one live waiter belongs to the pool
     */
    private boolean hasLiveWaiterLocked(final PoolState state, final long now) {
        for (final Waiter waiter : this.waiters) {
            if (waiter.state == state && waiter.lease == null && now < waiter.waitDeadlineNanos) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds the one printable line of the shedding report.
     *
     * @param pools pools currently reduced, in id order
     * @param state what the policy is doing in this wiring
     * @return the summary, never blank
     */
    private String summaryLocked(final List<PoolId> pools, final SheddingReport.State state) {
        final int reserve = this.ledger.plan().criticalReserveCores();
        final String held = reserve > 0
            ? reserve + " core(s) held for the critical lanes"
            : "no core held for the critical lanes";
        return switch (state) {
            case OFF -> held + "; load shedding off";
            case DORMANT -> held + "; load shedding on but dormant: no critical pool can lease capacity "
                + "(region and network are at most L2), so the trigger cannot fire";
            case ARMED -> held + "; load shedding on, nothing withheld";
            case SHEDDING -> {
                final StringBuilder names = new StringBuilder();
                for (final PoolId pool : pools) {
                    if (!names.isEmpty()) {
                        names.append(", ");
                    }
                    names.append(pool.value());
                }
                yield "load shedding withholding " + this.shedCores + " core(s) from " + names
                    + " while a critical pool waits; " + held;
            }
        };
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
     * One immutable view of the ledger: the plan and the entries it was built from.
     *
     * <p>The entries themselves are mutable because permits are counted in them, but the map that holds
     * them is replaced as a whole, so a reader either sees the old ledger or the new one.
     *
     * @param plan   budget plan backing this ledger
     * @param states per pool entries, immutable map of mutable counters
     */
    private record Ledger(BudgetPlan plan, Map<PoolId, PoolState> states) {
    }

    /**
     * Mutable per pool ledger entry.
     *
     * <p>{@link #inUse} is only touched under the controller lock. Everything else is derived from the
     * current spec and is rewritten on a refresh, again under the same lock.
     */
    private static final class PoolState {

        private final PoolId id;
        private volatile PoolSpec spec;
        private volatile int capacity;
        private volatile int minCores;
        private volatile int reservedCores;
        private int weight;
        private boolean critical;
        private volatile boolean managed;
        private int inUse;

        private PoolState(final PoolBudget budget, final PoolSpec spec) {
            this.id = budget.pool();
            this.rebase(budget, spec);
        }

        private void rebase(final PoolBudget budget, final PoolSpec spec) {
            // spec is published last: a reader that sees the new spec through the volatile field also
            // sees the capacity and coverage that belong to it, so a mixed view is impossible
            this.capacity = budget.capacity();
            this.minCores = budget.minCores();
            this.reservedCores = budget.reservedCores();
            this.weight = weightOf(budget.kind());
            this.critical = budget.critical();
            this.managed = spec.managesCapacity();
            this.spec = spec;
        }
    }

    /**
     * A parked lease request.
     *
     * <p>{@link #lease} is {@code null} until a releasing thread hands one over; both the field and the
     * waiters list are guarded by the controller lock. The grant-before-wake handoff is what makes
     * "released permit goes to a specific waiter" atomic, so no permit is ever lost in a race between a
     * release and a timeout. {@link #caller} is the thread the permit is accounted to for nested acquire
     * detection.
     */
    private static final class Waiter {

        private final PoolState state;
        private final LeaseRequest request;
        private final Thread caller;
        private final long sequence;
        private final long sinceNanos;
        private final long waitDeadlineNanos;
        private LeaseImpl lease;

        private Waiter(
            final PoolState state,
            final LeaseRequest request,
            final Thread caller,
            final long sequence,
            final long sinceNanos,
            final long waitDeadlineNanos
        ) {
            this.state = state;
            this.request = request;
            this.caller = caller;
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
