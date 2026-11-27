package kz.bejiihiu.tuff.threads.internal.impl.metrics;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.ExecutionPoolRegistry;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.RegionPoolMetrics;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.SheddingReport;
import kz.bejiihiu.tuff.threads.ThreadResourceController;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.internal.impl.events.MetricsEventSink;
import kz.bejiihiu.tuff.threads.internal.impl.registry.UnregisteredThreads;
import kz.bejiihiu.tuff.threads.internal.spi.MonotonicClock;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.internal.spi.PoolProbe;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadRegistry;
import org.jspecify.annotations.Nullable;

/**
 * Builds one snapshot of the thread layer at a time.
 *
 * <p>There is no sampling thread. A pass runs when the snapshot is read and the configured interval has
 * elapsed, so an idle server costs nothing and a busy one pays for exactly the reads that were asked for.
 * A driver that wants periodic data (a JFR pool state heartbeat, for example) calls
 * {@link #sampleIfDue()} from whatever timer it already owns.
 *
 * <p>One pass is: one {@link ThreadRegistry#snapshot()} (a single walk over registered threads), one
 * {@code observe()} per attached probe, one CPU read per thread and a handful of JVM counters. Pool state
 * is never read from a pool lock, regions are never scanned, and no stack dump happens here - the
 * unaccounted thread report is a separate, throttled scan that belongs to the registry's report class.
 *
 * <p>The budget numbers are not measured here: {@code usableCores} and the printable ledger line come
 * from {@link ThreadResourceController}, the single owner of the plan, for both the sampled and the
 * structural snapshot.
 *
 * <p>Concurrency: single flight. A caller that finds the pass already running does not wait, it reads the
 * previous snapshot; that keeps a command handler from ever queueing behind a slow probe. Snapshots,
 * counters and rates are published through a volatile field, so readers never observe a half built object.
 */
public final class ResourceSampler {

    /**
     * Marker pool id used only inside {@link ThreadSnapshot} entries of the unaccounted thread report.
     *
     * <p>An unaccounted thread has no pool by definition, but the snapshot record requires one; the id is
     * deliberately not a canonical pool and never appears in {@code pools()}.
     */
    public static final PoolId UNREGISTERED_POOL = PoolId.of("unregistered");

    private static final double SATURATED_UTILISATION = 0.95d;
    private static final double SATURATED_PRESSURE = 0.90d;
    private static final double BUSY_UTILISATION = 0.75d;
    private static final double BUSY_PRESSURE = 0.50d;

    /**
     * One allow list for the whole module.
     *
     * <p>The sampler used to carry its own copy with slightly different entries, so a thread could be
     * "expected outside the manager" in one report and a finding in the other. Sharing the predicate
     * keeps the two answers identical; the enumeration stays separate because a sampling pass must not
     * take a stack dump.
     */
    private static final Predicate<Thread> OUTSIDE_ALLOW_LIST = UnregisteredThreads.defaultIgnore();

    private final ExecutionPoolRegistry pools;
    private final ThreadRegistry registry;
    private final PoolProbeSource probes;
    private final ThreadCounters counters;
    private final MonotonicClock clock;
    private final MetricsEventSink sink;
    private final int reservedCores;
    private final ThreadResourceController budget;

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<PoolId, SaturationReport.Level> lastSaturation = new HashMap<>();
    private final Map<PoolId, ResourceBudget> lastBudget = new HashMap<>();

    private volatile MetricsConfig config;
    private volatile @Nullable ResourceSnapshot latest;
    private volatile List<ThreadSnapshot> unregistered = List.of();

    // guarded by the lock, except the two timestamps that readers peek at without it
    private volatile long lastSampleNanos = Long.MIN_VALUE;
    private volatile long lastScanNanos = Long.MIN_VALUE;
    private int peakThreads;
    private Map<Long, Long> cpuBaseline = Map.of();
    private long lastStarted = -1L;
    private long lastFinished = -1L;
    private double creationRate;
    private double terminationRate;
    private boolean closed;

    /**
     * Creates a sampler over the controller that owns the ledger.
     *
     * <p>Pool probes can say how deep a queue is, but not how long the oldest task has been waiting.
     * The controller can: its waiters know when they parked. Reading it here keeps one number out of two
     * sources - the sampler owns the pool view, the controller owns the lease wait - and turns the old
     * "zero nanoseconds while the queue is not empty" into an honest value. The same controller is the
     * only source of {@code usableCores} and of the printable ledger, so a snapshot can never describe a
     * budget the ledger does not use.
     *
     * @param pools         registered pool descriptions
     * @param registry      live registered threads and per pool creation and termination totals
     * @param probes        attached pool probes, needed because the frozen probe SPI has no read side
     * @param counters      JVM thread and CPU counters
     * @param clock         time source for windows and timestamps
     * @param sink          event sink; JFR in production, a recorder in tests
     * @param config        sampling policy
     * @param reservedCores cores kept out of the managed budget, reported but never sized here
     * @param budget        controller that owns the ledger and the lease waits
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if the reserve is negative
     */
    public ResourceSampler(
        final ExecutionPoolRegistry pools,
        final ThreadRegistry registry,
        final PoolProbeSource probes,
        final ThreadCounters counters,
        final MonotonicClock clock,
        final MetricsEventSink sink,
        final MetricsConfig config,
        final int reservedCores,
        final ThreadResourceController budget
    ) {
        this.pools = Objects.requireNonNull(pools, "pools");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.probes = Objects.requireNonNull(probes, "probes");
        this.counters = Objects.requireNonNull(counters, "counters");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.config = Objects.requireNonNull(config, "config");
        if (reservedCores < 0) {
            throw new IllegalArgumentException("reservedCores must not be negative, got " + reservedCores);
        }
        this.reservedCores = reservedCores;
        this.budget = Objects.requireNonNull(budget, "budget");
    }

    /**
     * Checks whether sampling is enabled right now.
     *
     * @return {@code true} while the current policy allows sampling
     */
    public boolean enabled() {
        return !this.closed && this.config.enabled();
    }

    /**
     * Returns the sampling policy in force.
     *
     * @return the policy
     */
    public MetricsConfig config() {
        return this.config;
    }

    /**
     * Replaces the sampling policy, which is how a live reload takes effect.
     *
     * <p>Everything measured so far stays valid; the new interval only changes when the next pass is due.
     *
     * @param newConfig new policy
     * @throws NullPointerException if {@code newConfig} is {@code null}
     */
    public void reconfigure(final MetricsConfig newConfig) {
        this.config = Objects.requireNonNull(newConfig, "newConfig");
    }

    /**
     * Returns the current snapshot, taking a new one when the interval has elapsed.
     *
     * <p>With collection disabled the answer is a structural snapshot: the registered pools and the
     * threads the registry accounts for, without probes, CPU or rates. That is what makes a disabled
     * manager still report "7 pools registered" instead of a fake "0", while
     * {@link DegradedReason#METRICS_UNAVAILABLE} and {@link #enabled()} say that nothing was measured.
     *
     * @return the latest snapshot, never {@code null}
     */
    public ResourceSnapshot current() {
        if (this.closed) {
            return this.lastOrEmpty();
        }
        if (!this.enabled()) {
            return this.structuralSnapshot();
        }
        if (this.latest == null) {
            return this.sampleNow();
        }
        return this.sampleIfDue();
    }

    /**
     * Takes a snapshot when the sampling interval has elapsed since the last pass.
     *
     * <p>Cheap enough to call from any ticker: one clock read and one volatile read while the interval has
     * not passed.
     *
     * @return the latest snapshot, never {@code null}
     */
    public ResourceSnapshot sampleIfDue() {
        if (this.closed) {
            return this.lastOrEmpty();
        }
        if (!this.enabled()) {
            return this.structuralSnapshot();
        }
        final ResourceSnapshot existing = this.latest;
        if (existing != null && this.clock.nanoTime() - this.lastSampleNanos < this.config.intervalNanos()) {
            return existing;
        }
        return this.sampleNow();
    }

    /**
     * Forces a sampling pass, ignoring the interval.
     *
     * <p>Used by commands that must show the present moment, and by tests. When another pass is running
     * this method does not wait: it returns the previous snapshot, because a metrics read must never block
     * an operator's thread. With collection disabled it returns the structural snapshot instead of
     * measuring anything.
     *
     * @return the snapshot taken by this call, or the previous one when a pass was already running
     */
    public ResourceSnapshot sampleNow() {
        if (this.closed) {
            return this.lastOrEmpty();
        }
        if (!this.enabled()) {
            return this.structuralSnapshot();
        }
        if (!this.lock.tryLock()) {
            return this.lastOrEmpty();
        }
        try {
            final ResourceSnapshot snapshot = this.pass();
            this.latest = snapshot;
            return snapshot;
        } catch (RuntimeException _) {
            // a broken probe or a shutting down registry must not take the command down with it
            return this.lastOrEmpty();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Returns the unaccounted thread report, rescanning when it is older than the configured interval.
     *
     * @return an immutable list in thread id order, possibly empty
     */
    public List<ThreadSnapshot> unregisteredThreads() {
        if (this.closed) {
            return this.unregistered;
        }
        if (!this.enabled()) {
            return List.of();
        }
        if (this.lastScanNanos != Long.MIN_VALUE
            && this.clock.nanoTime() - this.lastScanNanos < this.config.unregisteredIntervalNanos()) {
            return this.unregistered;
        }
        return this.scanUnregistered();
    }

    /**
     * Forces an unaccounted thread scan.
     *
     * <p>This walks the thread groups once. It is a diagnostic, so it belongs to a command, to startup or to
     * a slow audit, never to a tick and never to the sampling pass.
     *
     * @return an immutable list in thread id order, possibly empty
     */
    public List<ThreadSnapshot> scanUnregistered() {
        if (this.closed) {
            return this.unregistered;
        }
        if (!this.enabled()) {
            return List.of();
        }
        if (!this.lock.tryLock()) {
            return this.unregistered;
        }
        try {
            final List<ThreadSnapshot> found = this.scan();
            this.unregistered = found;
            this.lastScanNanos = this.clock.nanoTime();
            return found;
        } catch (RuntimeException _) {
            return this.unregistered;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Stops sampling and keeps the last known values.
     *
     * <p>Idempotent. Called from the shutdown path before the pools are torn down: sampling in the window
     * between "the manager is stopping" and "everything is stopped" is what turns a slow read into a
     * watchdog kill. Reads after this point return the last snapshot that was actually taken, never a
     * zeroed one - the manager contract says a stopped manager keeps answering with the last known
     * values, and a fake zero is the one answer an incident responder cannot tell apart from health.
     */
    public void close() {
        this.closed = true;
        if (!this.lock.tryLock()) {
            return;
        }
        try {
            this.cpuBaseline = Map.of();
            this.lastSaturation.clear();
            this.lastBudget.clear();
            this.lastSampleNanos = Long.MIN_VALUE;
            this.lastScanNanos = Long.MIN_VALUE;
        } finally {
            this.lock.unlock();
        }
    }

    private ResourceSnapshot lastOrEmpty() {
        final ResourceSnapshot existing = this.latest;
        return existing == null ? this.structuralSnapshot() : existing;
    }

    /**
     * Builds a snapshot from registry bookkeeping only.
     *
     * <p>Used when collection is disabled and by a stopped sampler that never took a pass. It answers
     * what the registry knows without measuring anything: which pools are registered, how many threads
     * each accounts for and what states those threads are in. Queues, capacity, task counters and CPU
     * stay {@code -1}, saturation stays {@link SaturationReport#unknown(PoolId)}, the thread list is not
     * published, and the peak equals the current total because no history is kept while collection is
     * off. {@link DegradedReason#METRICS_UNAVAILABLE} at the top level is the explicit "metrics disabled"
     * status.
     *
     * @return a structural snapshot, never {@code null}
     */
    private ResourceSnapshot structuralSnapshot() {
        final List<ThreadSnapshot> registered = this.registry.snapshot();
        final Map<PoolId, List<ThreadSnapshot>> byPool = new HashMap<>();
        for (final ThreadSnapshot thread : registered) {
            byPool.computeIfAbsent(thread.pool(), id -> new ArrayList<>()).add(thread);
        }
        final List<PoolSnapshot> poolSnapshots = new ArrayList<>();
        int total = 0;
        int activeTotal = 0;
        int idleTotal = 0;
        for (final PoolSpec spec : this.pools.specs()) {
            final List<ThreadSnapshot> threads = byPool.remove(spec.id());
            final int alive = threads == null ? 0 : threads.size();
            final int active = threads == null ? 0 : countSnapshotStates(threads, false);
            final int idle = threads == null ? 0 : countSnapshotStates(threads, true);
            poolSnapshots.add(new PoolSnapshot(
                spec,
                alive,
                spec.minThreads(),
                active,
                idle,
                -1L,
                -1L,
                -1L,
                -1L,
                -1L,
                -1.0d,
                SaturationReport.unknown(spec.id()),
                this.controllerReasons(spec.id())
            ));
            total += alive;
            activeTotal += active;
            idleTotal += idle;
        }
        for (final List<ThreadSnapshot> leftovers : byPool.values()) {
            total += leftovers.size();
            activeTotal += countSnapshotStates(leftovers, false);
            idleTotal += countSnapshotStates(leftovers, true);
        }
        poolSnapshots.sort((left, right) -> left.id().compareTo(right.id()));
        return new ResourceSnapshot(
            this.clock.instant(),
            this.usableCores(),
            this.reservedCores,
            total,
            activeTotal,
            idleTotal,
            total,
            0.0d,
            0.0d,
            -1.0d,
            poolSnapshots,
            List.of(),
            Set.of(DegradedReason.METRICS_UNAVAILABLE),
            Optional.of(this.budget.budgetLedger()),
            this.shedding()
        );
    }

    private int usableCores() {
        return this.budget.usableCores();
    }

    /**
     * Number of threads whose CPU baseline is currently tracked.
     *
     * <p>Package private on purpose: only a test needs to see that the baseline map shrinks when threads
     * go away, and a public accessor for an internal counter would be noise.
     *
     * @return tracked thread count
     */
    int trackedCpuThreads() {
        return this.cpuBaseline.size();
    }

    /**
     * The sampling pass.
     *
     * @return a consistent snapshot of the current moment
     */
    private ResourceSnapshot pass() {
        final MetricsConfig policy = this.config;
        final int usable = this.usableCores();
        final int reserved = this.reservedCores;
        final long now = this.clock.nanoTime();
        final Instant capturedAt = this.clock.instant();
        final long elapsed = this.lastSampleNanos == Long.MIN_VALUE ? -1L : now - this.lastSampleNanos;
        final boolean windowed = elapsed > 0L;
        final double elapsedSeconds = windowed ? elapsed / 1_000_000_000.0d : 0.0d;
        final boolean cpuSupported = this.counters.cpuTimeSupported();

        final List<ThreadSnapshot> registered = this.registry.snapshot();
        final Map<PoolId, List<ThreadSnapshot>> registeredByPool = new HashMap<>();
        for (final ThreadSnapshot thread : registered) {
            registeredByPool.computeIfAbsent(thread.pool(), id -> new ArrayList<>()).add(thread);
        }
        final Map<PoolId, PoolProbe> probeByPool = this.probeMap();

        final List<PoolSnapshot> poolSnapshots = new ArrayList<>();
        final List<ThreadSnapshot> publishedThreads = new ArrayList<>();
        final Set<DegradedReason> degraded = new LinkedHashSet<>();
        final Map<Long, Long> nextBaseline = new HashMap<>(Math.max(16, registered.size() * 2));
        int total = 0;
        int activeTotal = 0;
        int idleTotal = 0;

        for (final PoolSpec spec : this.pools.specs()) {
            final PoolId id = spec.id();
            final @Nullable PoolProbe probe = probeByPool.get(id);
            final @Nullable PoolObservation observation = observe(probe);
            final List<ObservedThread> observed = new ArrayList<>();
            this.collect(observed, registeredByPool.remove(id), observation);
            // a probe that threw, or one that answered "I cannot classify these threads" without naming
            // any, is unknown - not a measured zero. a probe that can name its threads still falls back
            // to their states, which is the honest reading
            final boolean unreadable = probe != null
                && (observation == null
                    || (observation.activeThreads() < 0 && observation.idleThreads() < 0 && observed.isEmpty()));

            long cpuDelta = 0L;
            int cpuReadings = 0;
            for (final ObservedThread thread : observed) {
                if (thread.cpuNanos() < 0L) {
                    continue;
                }
                final Long previous = this.cpuBaseline.get(thread.id());
                if (previous != null && thread.cpuNanos() >= previous) {
                    cpuDelta += thread.cpuNanos() - previous;
                    cpuReadings++;
                }
                nextBaseline.put(thread.id(), thread.cpuNanos());
            }

            final int registeredAlive = observed.size();
            final int alive = observation == null
                ? registeredAlive
                : Math.max(registeredAlive, observation.aliveThreads());
            final int coreThreads = observation == null ? spec.minThreads() : Math.max(0, observation.coreThreads());
            final int derivedActive = countState(observed, false);
            final int derivedIdle = countState(observed, true);
            final int active = unreadable
                ? -1
                : observation != null && observation.activeThreads() >= 0
                    ? Math.min(observation.activeThreads(), alive)
                    : derivedActive;
            final int idle = unreadable
                ? -1
                : observation != null && observation.idleThreads() >= 0
                    ? Math.min(observation.idleThreads(), alive)
                    : derivedIdle;
            final long queued = observation == null ? -1L : observation.queuedTasks();
            final long capacity = observation == null ? -1L : observation.queueCapacity();
            final double cpuLoad = cpuLoad(cpuSupported, windowed, observed.isEmpty(), cpuReadings, cpuDelta,
                elapsed, usable);
            final SaturationReport saturation = saturation(
                id, alive, active, queued, capacity, this.leaseWaitNanos(id)
            );
            final List<DegradedReason> reasons = this.poolReasons(id, cpuSupported);
            // region data comes from the probe itself: the sampler has no region view of its own, and a
            // pool that does not tick regions simply has nothing here
            final Optional<RegionPoolMetrics> regionMetrics = observation == null
                ? Optional.empty()
                : observation.regionMetrics();

            final PoolSnapshot poolSnapshot = new PoolSnapshot(
                spec,
                alive,
                coreThreads,
                active,
                idle,
                queued,
                capacity,
                -1L,
                -1L,
                -1L,
                cpuLoad,
                saturation,
                reasons,
                regionMetrics
            );
            // degradation is a state, not a publication detail: it has to survive per-pool being off
            degraded.addAll(reasons);
            if (policy.perPool()) {
                poolSnapshots.add(poolSnapshot);
                this.emit(poolSnapshot);
                this.emitSaturation(saturation);
                this.emitBudget(spec);
            }
            if (policy.perThread()) {
                for (final ObservedThread thread : observed) {
                    publishedThreads.add(new ThreadSnapshot(
                        thread.id(),
                        thread.name(),
                        id,
                        thread.state(),
                        thread.daemon(),
                        thread.priority(),
                        thread.cpuNanos()
                    ));
                }
            }
            total += alive;
            activeTotal += active;
            idleTotal += idle;
        }

        for (final List<ThreadSnapshot> leftovers : registeredByPool.values()) {
            total += leftovers.size();
            for (final ThreadSnapshot thread : leftovers) {
                if (thread.active()) {
                    activeTotal++;
                } else if (thread.idle()) {
                    idleTotal++;
                }
            }
        }

        if (policy.perThread()) {
            // one moment, one list: with per thread publication on, the top level counts are exactly
            // the published threads. a probe that reports a count it cannot enumerate belongs to its
            // pool line only, so the header can never disagree with the list underneath it.
            total = publishedThreads.size();
            activeTotal = countSnapshotStates(publishedThreads, false);
            idleTotal = countSnapshotStates(publishedThreads, true);
        }

        this.cpuBaseline = nextBaseline;
        this.peakThreads = Math.max(this.peakThreads, total);
        this.updateRates(windowed, elapsedSeconds, now);
        publishedThreads.sort((left, right) -> Long.compare(left.threadId(), right.threadId()));
        poolSnapshots.sort((left, right) -> left.id().compareTo(right.id()));

        return new ResourceSnapshot(
            capturedAt,
            usable,
            reserved,
            total,
            activeTotal,
            idleTotal,
            this.peakThreads,
            this.creationRate,
            this.terminationRate,
            this.counters.processCpuLoad(),
            poolSnapshots,
            publishedThreads,
            degraded,
            Optional.of(this.budget.budgetLedger()),
            this.shedding()
        );
    }

    /**
     * Returns the reasons a pool carries after this pass.
     *
     * <p>Two sources on purpose: the measurement (CPU accounting unsupported) and the controller (budget
     * exhausted, a critical pool under pressure, capacity withheld by load shedding). Neither knows the
     * other's reason, and an operator needs both to tell "we cannot measure it" from "we decided not to
     * give it capacity".
     *
     * @param pool         pool id
     * @param cpuSupported whether the JVM exposes per thread CPU time
     * @return an immutable reason list, empty when everything is fine
     */
    private List<DegradedReason> poolReasons(final PoolId pool, final boolean cpuSupported) {
        final Set<DegradedReason> reasons = new LinkedHashSet<>(2);
        if (!cpuSupported) {
            reasons.add(DegradedReason.METRICS_UNAVAILABLE);
        }
        reasons.addAll(this.controllerReasons(pool));
        return List.copyOf(reasons);
    }

    /**
     * Asks the controller why it is not giving a pool its full capacity.
     *
     * <p>A broken controller is treated as "nothing to say" rather than failing the whole pass; the
     * numbers it owns are still guarded separately.
     *
     * @param pool pool id
     * @return the controller's reasons, empty when it has none or cannot answer
     */
    private List<DegradedReason> controllerReasons(final PoolId pool) {
        try {
            return this.budget.degradedReasons(pool);
        } catch (RuntimeException _) {
            return List.of();
        }
    }

    /**
     * Returns the controller's critical reserve and shedding state.
     *
     * <p>Read once per snapshot so the report and the snapshot describe the same moment. A controller
     * that cannot answer reports nothing held rather than failing the pass; that is the same trade the
     * ledger line makes.
     *
     * @return the report, never {@code null}
     */
    private SheddingReport shedding() {
        try {
            return this.budget.sheddingReport();
        } catch (RuntimeException _) {
            return SheddingReport.none();
        }
    }

    private void updateRates(final boolean windowed, final double elapsedSeconds, final long now) {
        final long started = this.counters.startedTotal();
        final long live = this.counters.liveThreads();
        final long finished = Math.max(0L, started - live);
        if (windowed && this.lastStarted >= 0L) {
            final long created = Math.max(0L, started - this.lastStarted);
            final long terminated = Math.max(0L, finished - this.lastFinished);
            this.creationRate = created / elapsedSeconds;
            this.terminationRate = terminated / elapsedSeconds;
        }
        this.lastStarted = started;
        this.lastFinished = finished;
        this.lastSampleNanos = now;
    }

    private Map<PoolId, PoolProbe> probeMap() {
        final Map<PoolId, PoolProbe> map = new HashMap<>();
        final List<PoolProbe> attached;
        try {
            attached = this.probes.probes();
        } catch (RuntimeException _) {
            return map;
        }
        for (final PoolProbe probe : attached) {
            if (probe != null) {
                map.put(probe.pool(), probe);
            }
        }
        return map;
    }

    private static @Nullable PoolObservation observe(final @Nullable PoolProbe probe) {
        if (probe == null) {
            return null;
        }
        try {
            return probe.observe();
        } catch (RuntimeException _) {
            return null;
        }
    }

    private void collect(
        final List<ObservedThread> target,
        final @Nullable List<ThreadSnapshot> fromRegistry,
        final @Nullable PoolObservation observation
    ) {
        if (observation != null && !observation.threads().isEmpty()) {
            for (final Thread thread : observation.threads()) {
                if (thread == null) {
                    continue;
                }
                target.add(new ObservedThread(
                    thread.threadId(),
                    nameOf(thread),
                    thread.getState(),
                    thread.isDaemon(),
                    thread.getPriority(),
                    this.counters.cpuTimeNanos(thread.threadId())
                ));
            }
            return;
        }
        if (fromRegistry == null) {
            return;
        }
        for (final ThreadSnapshot thread : fromRegistry) {
            target.add(new ObservedThread(
                thread.threadId(),
                thread.name(),
                thread.state(),
                thread.daemon(),
                thread.priority(),
                this.counters.cpuTimeNanos(thread.threadId())
            ));
        }
    }

    private static String nameOf(final Thread thread) {
        final String name = thread.getName();
        return name == null || name.isBlank() ? "thread-" + thread.threadId() : name;
    }

    private static int countState(final List<ObservedThread> threads, final boolean idle) {
        int count = 0;
        for (final ObservedThread thread : threads) {
            final boolean matches = idle
                ? thread.state() == Thread.State.WAITING || thread.state() == Thread.State.TIMED_WAITING
                : thread.state() == Thread.State.RUNNABLE;
            if (matches) {
                count++;
            }
        }
        return count;
    }

    private static int countSnapshotStates(final List<ThreadSnapshot> threads, final boolean idle) {
        int count = 0;
        for (final ThreadSnapshot thread : threads) {
            final boolean matches = idle
                ? thread.state() == Thread.State.WAITING || thread.state() == Thread.State.TIMED_WAITING
                : thread.state() == Thread.State.RUNNABLE;
            if (matches) {
                count++;
            }
        }
        return count;
    }

    private static double cpuLoad(
        final boolean supported,
        final boolean windowed,
        final boolean noThreads,
        final int readings,
        final long deltaNanos,
        final long elapsedNanos,
        final int usableCores
    ) {
        if (!supported || !windowed) {
            return -1.0d;
        }
        if (noThreads) {
            return 0.0d;
        }
        if (readings == 0) {
            // the window exists but no thread had a baseline yet: measuring, not measuring zero
            return -1.0d;
        }
        final double share = (double) deltaNanos / ((double) elapsedNanos * usableCores);
        return Math.min(1.0d, Math.max(0.0d, share));
    }

    private static SaturationReport saturation(
        final PoolId pool,
        final int alive,
        final int active,
        final long queued,
        final long capacity,
        final long longestWaitNanos
    ) {
        if (active < 0 && queued < 0) {
            return SaturationReport.unknown(pool);
        }
        final double utilisation = alive <= 0 || active < 0 ? 0.0d : clampShare((double) active / alive);
        final double pressure = queued <= 0L || capacity <= 0L ? 0.0d : clampShare((double) queued / capacity);
        final SaturationReport.Level level;
        if (alive <= 0 && queued <= 0L) {
            level = SaturationReport.Level.IDLE;
        } else if (utilisation >= SATURATED_UTILISATION || pressure >= SATURATED_PRESSURE) {
            level = SaturationReport.Level.SATURATED;
        } else if (utilisation >= BUSY_UTILISATION || pressure >= BUSY_PRESSURE) {
            level = SaturationReport.Level.BUSY;
        } else {
            level = SaturationReport.Level.NORMAL;
        }
        return new SaturationReport(
            pool,
            utilisation,
            pressure,
            Math.max(0L, queued),
            Math.max(0L, longestWaitNanos),
            level
        );
    }

    /**
     * Returns how long the oldest lease request has been waiting for a pool.
     *
     * <p>The probe cannot answer this - it knows queue depth, not queue age - so the controller is asked.
     * Zero is a real answer ("nothing is waiting"), not a placeholder; a broken controller is treated as
     * "nothing waits" rather than failing the whole pass.
     *
     * @param pool pool id
     * @return nanoseconds the oldest request has been parked, zero when nothing waits
     */
    private long leaseWaitNanos(final PoolId pool) {
        try {
            return this.budget.saturation(pool).longestWaitNanos();
        } catch (RuntimeException _) {
            return 0L;
        }
    }

    private static double clampShare(final double value) {
        return Math.min(1.0d, Math.max(0.0d, value));
    }

    private void emit(final PoolSnapshot pool) {
        try {
            this.sink.poolState(pool);
        } catch (RuntimeException _) {
            // a broken sink must never break a sampling pass
        }
    }

    private void emitSaturation(final SaturationReport report) {
        final SaturationReport.Level previous = this.lastSaturation.put(report.pool(), report.level());
        if (previous == report.level() && report.level() != SaturationReport.Level.SATURATED) {
            return;
        }
        try {
            this.sink.saturation(report);
        } catch (RuntimeException _) {
            // a broken sink must never break a sampling pass
        }
    }

    private void emitBudget(final PoolSpec spec) {
        final ResourceBudget previous = this.lastBudget.put(spec.id(), spec.budget());
        if (previous == null || previous.equals(spec.budget())) {
            return;
        }
        try {
            this.sink.budgetChanged(spec.id(), previous, spec.budget());
        } catch (RuntimeException _) {
            // a broken sink must never break a reload
        }
    }

    private List<ThreadSnapshot> scan() {
        final List<ThreadSnapshot> found = new ArrayList<>();
        // the walk and the allow list live in the registry's report class; the sampler only shapes
        // the same threads into its snapshot record
        for (final Thread thread : UnregisteredThreads.unaccounted(this.registry, OUTSIDE_ALLOW_LIST)) {
            found.add(new ThreadSnapshot(
                thread.threadId(),
                nameOf(thread),
                UNREGISTERED_POOL,
                thread.getState(),
                thread.isDaemon(),
                thread.getPriority(),
                this.counters.cpuTimeNanos(thread.threadId())
            ));
            try {
                this.sink.unregisteredThread(thread.threadId(), nameOf(thread));
            } catch (RuntimeException _) {
                // a diagnostic must never take the caller down
            }
        }
        found.sort((left, right) -> Long.compare(left.threadId(), right.threadId()));
        return List.copyOf(found);
    }

    /**
     * One thread as the pass saw it.
     *
     * @param id       JVM thread id
     * @param name     name at capture time
     * @param state    state at capture time
     * @param daemon   whether it is a daemon thread
     * @param priority Java priority
     * @param cpuNanos CPU time since start, or {@code -1} when unknown
     */
    private record ObservedThread(
        long id,
        String name,
        Thread.State state,
        boolean daemon,
        int priority,
        long cpuNanos
    ) {
    }
}

