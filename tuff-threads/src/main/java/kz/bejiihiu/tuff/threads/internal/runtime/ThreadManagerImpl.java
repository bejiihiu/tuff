package kz.bejiihiu.tuff.threads.internal.runtime;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.ExecutionPoolRegistry;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceMetrics;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.ThreadFactoryProvider;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.ThreadResourceController;
import kz.bejiihiu.tuff.threads.ThreadSnapshot;
import kz.bejiihiu.tuff.threads.UnifiedThreadManager;
import kz.bejiihiu.tuff.threads.internal.impl.controller.ResourceControllerImpl;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.ResourceMetricsImpl;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.ResourceSampler;

/**
 * Read only facade over the assembled components.
 *
 * <p>Everything it returns is the real component, not a copy: a command that asks for pools gets the
 * registry, a lease goes to the controller. The only thing this class adds is the manager's own view of
 * degradation and a snapshot that carries it.
 *
 * <p>Degradation has two layers on purpose. The metrics sampler reports what it can see (an unreadable
 * CPU account, a probe that gives up). The manager reports what only it knows: which pools are only
 * observed, which configured management this runtime cannot deliver and whether the budget ledger had
 * to give up on something. The snapshot is the union, so an operator reading one object sees both.
 *
 * <p>The state is not the same question as the reason list. Observe only is a reason, not a failure:
 * with the safe defaults every pool carries {@link DegradedReason#OBSERVE_ONLY}, the snapshot says so,
 * and the manager is still {@link State#RUNNING}. Only a reason that means "we promised more than we
 * can deliver" moves the state to {@link State#DEGRADED}.
 */
public final class ThreadManagerImpl implements UnifiedThreadManager {

    /**
     * Reasons that mean the manager cannot honour its own configuration.
     *
     * <p>Everything else is either a coverage statement ({@code OBSERVE_ONLY}, {@code FEATURE_DISABLED})
     * or a measurement gap ({@code METRICS_UNAVAILABLE}, {@code NUMA_UNAVAILABLE}); those belong in the
     * snapshot, not in the state.
     */
    private static final Set<DegradedReason> DEGRADING = Set.of(
        DegradedReason.BUDGET_EXHAUSTED,
        DegradedReason.TOPOLOGY_UNKNOWN,
        DegradedReason.SCHEDULER_LIMITATION,
        DegradedReason.THREAD_CREATION_FAILED,
        DegradedReason.POOL_UNRESPONSIVE,
        DegradedReason.CONFIG_CONFLICT
    );

    private final ThreadManagementSettings settings;
    private final ExecutionPoolRegistry pools;
    private final ResourceControllerImpl resources;
    private final ThreadFactoryProvider threadFactory;
    private final ResourceMetricsImpl metrics;
    private final ResourceSampler sampler;

    private volatile Phase phase = Phase.NEW;

    ThreadManagerImpl(
        final ThreadManagementSettings settings,
        final ExecutionPoolRegistry pools,
        final ResourceControllerImpl resources,
        final ThreadFactoryProvider threadFactory,
        final ResourceMetricsImpl metrics,
        final ResourceSampler sampler
    ) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.pools = Objects.requireNonNull(pools, "pools");
        this.resources = Objects.requireNonNull(resources, "resources");
        this.threadFactory = Objects.requireNonNull(threadFactory, "threadFactory");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.sampler = Objects.requireNonNull(sampler, "sampler");
    }

    @Override
    public ThreadManagementSettings settings() {
        return this.settings;
    }

    @Override
    public ExecutionPoolRegistry pools() {
        return this.pools;
    }

    @Override
    public ThreadResourceController resources() {
        return this.resources;
    }

    @Override
    public ThreadFactoryProvider threadFactory() {
        return this.threadFactory;
    }

    @Override
    public ResourceMetrics metrics() {
        return this.metrics;
    }

    @Override
    public State state() {
        return switch (this.phase) {
            case NEW -> State.NEW;
            case STARTED -> this.degrading() ? State.DEGRADED : State.RUNNING;
            case SHUTTING_DOWN -> State.SHUTTING_DOWN;
            case STOPPED -> State.STOPPED;
        };
    }

    /**
     * Takes a snapshot and folds the manager's own reasons into it.
     *
     * <p>Measured values are exactly what the metrics layer saw; only the reason lists grow. A pool that
     * is observed only says so in its own entry, and the union at the top carries the same fact for an
     * operator who reads the summary first.
     *
     * @return the snapshot, never {@code null}
     */
    @Override
    public ResourceSnapshot snapshot() {
        final ResourceSnapshot measured = this.metrics.snapshot();
        final Set<DegradedReason> managerReasons = this.managementReasons();
        final Set<DegradedReason> allReasons = new LinkedHashSet<>(measured.degradedReasons());
        allReasons.addAll(managerReasons);
        if (!this.isRunning()) {
            allReasons.add(DegradedReason.NOT_RUNNING);
        }
        final List<PoolSnapshot> poolSnapshots = new ArrayList<>(measured.pools().size());
        for (final PoolSnapshot pool : measured.pools()) {
            final Set<DegradedReason> reasons = new LinkedHashSet<>(pool.degradedReasons());
            reasons.addAll(this.specReasons(pool.spec()));
            poolSnapshots.add(withReasons(pool, reasons));
        }
        return new ResourceSnapshot(
            measured.capturedAt(),
            measured.usableCores(),
            measured.reservedCores(),
            measured.totalThreads(),
            measured.activeThreads(),
            measured.idleThreads(),
            measured.peakThreads(),
            measured.creationRatePerSecond(),
            measured.terminationRatePerSecond(),
            measured.processCpuLoad(),
            poolSnapshots,
            measured.threads(),
            allReasons
        );
    }

    @Override
    public Optional<PoolId> poolOf(final Thread thread) {
        return this.threadFactory.poolOf(thread);
    }

    /**
     * Returns every reason a pool or the manager reported.
     *
     * <p>This is the informational list, not a health check: {@link State#RUNNING} with a non empty set
     * is normal in the default observe only configuration. Use {@link #degraded()} for the health
     * question and the set for the "why is this pool not managed" question.
     *
     * @return an immutable set of reasons, empty only when nothing is registered and nothing was measured
     */
    @Override
    public Set<DegradedReason> degradedReasons() {
        final Set<DegradedReason> reasons = new LinkedHashSet<>(this.managementReasons());
        reasons.addAll(this.metrics.snapshot().degradedReasons());
        if (!this.isRunning()) {
            reasons.add(DegradedReason.NOT_RUNNING);
        }
        return Set.copyOf(reasons);
    }

    /**
     * Scans the JVM for threads that no pool accounts for.
     *
     * <p>This is the command path, not a metric: one pass walks the root thread group and allocates one
     * record per finding. It must never be called from a region tick thread, and it is intentionally not
     * part of the regular snapshot - the sampler stays away from thread enumeration for a reason.
     *
     * @return an immutable list in thread id order, possibly empty
     */
    public List<ThreadSnapshot> scanUnregistered() {
        return this.sampler.scanUnregistered();
    }

    /**
     * Returns the lifecycle phase the runtime drives.
     *
     * @return the phase, never {@code null}
     */
    Phase phase() {
        return this.phase;
    }

    /**
     * Marks the manager as accepting work.
     *
     * <p>Called by the runtime after the default pools are registered, never by a plugin.
     */
    void markStarted() {
        this.phase = Phase.STARTED;
    }

    /**
     * Marks the manager as no longer accepting new work.
     *
     * <p>Called at the beginning of shutdown, before anything is closed, so a concurrent factory call
     * sees a state that already says no.
     */
    void markShuttingDown() {
        this.phase = Phase.SHUTTING_DOWN;
    }

    /**
     * Marks the manager as stopped for good.
     */
    void markStopped() {
        this.phase = Phase.STOPPED;
    }

    /**
     * Checks whether any reason means the manager cannot deliver what it promised.
     *
     * @return {@code true} when a degrading reason is present
     */
    private boolean degrading() {
        for (final DegradedReason reason : this.managementReasons()) {
            if (DEGRADING.contains(reason)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Collects the reasons the manager itself can see.
     *
     * @return the reasons, empty before the first registration
     */
    private Set<DegradedReason> managementReasons() {
        final Set<DegradedReason> reasons = new LinkedHashSet<>();
        for (final PoolSpec spec : this.pools.specs()) {
            reasons.addAll(this.specReasons(spec));
        }
        if (!this.pools.ids().isEmpty() && this.resources.plan().degraded()) {
            reasons.add(DegradedReason.BUDGET_EXHAUSTED);
        }
        return reasons;
    }

    /**
     * Maps one pool to the reasons its spec and the configuration imply.
     *
     * @param spec registered pool
     * @return the reasons for that pool
     */
    private Set<DegradedReason> specReasons(final PoolSpec spec) {
        final Set<DegradedReason> reasons = new LinkedHashSet<>();
        if (spec.coverage() == CoverageLevel.L1) {
            reasons.add(DegradedReason.OBSERVE_ONLY);
        }
        final boolean configured = this.managesKind(spec, reasons);
        if (spec.managesCapacity() && !configured) {
            reasons.add(DegradedReason.CONFIG_CONFLICT);
        }
        return reasons;
    }

    /**
     * Adds the configuration driven reasons for a pool.
     *
     * @param spec    registered pool
     * @param reasons set to add to
     * @return {@code true} when the configuration asks this runtime to manage that kind
     */
    private boolean managesKind(final PoolSpec spec, final Set<DegradedReason> reasons) {
        return switch (spec.kind()) {
            case REGION_EXECUTION -> {
                final boolean managed = this.settings.regionExecution().managed();
                if (!managed) {
                    reasons.add(DegradedReason.FEATURE_DISABLED);
                } else if (this.settings.regionExecution().scheduler()
                    == ThreadManagementSettings.RegionScheduler.EDF) {
                    // EDF cannot be resized, so L2 is as deep as this integration can promise
                    reasons.add(DegradedReason.SCHEDULER_LIMITATION);
                }
                yield managed;
            }
            case COMPUTE, VIRTUAL -> {
                final boolean managed = this.settings.asyncCompute().managed();
                if (!managed) {
                    reasons.add(DegradedReason.FEATURE_DISABLED);
                } else if (!spec.coverage().managesThreads()) {
                    // the compute adapter owns the L4 spec and the budget; until it registers, the
                    // configuration asks for management this runtime cannot provide
                    reasons.add(DegradedReason.CONFIG_CONFLICT);
                }
                yield managed;
            }
            case CHUNK, IO -> {
                final boolean managed = this.settings.executionPools().chunkSystem().managed();
                if (!managed) {
                    reasons.add(DegradedReason.FEATURE_DISABLED);
                }
                yield managed;
            }
            case NETWORK -> {
                final boolean managed = this.settings.executionPools().network().managed();
                if (!managed) {
                    reasons.add(DegradedReason.FEATURE_DISABLED);
                }
                yield managed;
            }
            case SERVICE -> false;
        };
    }

    /**
     * Rebuilds a pool snapshot with an extended reason list.
     *
     * @param pool    measured pool snapshot
     * @param reasons reasons to publish
     * @return the copy
     */
    private static PoolSnapshot withReasons(final PoolSnapshot pool, final Set<DegradedReason> reasons) {
        return new PoolSnapshot(
            pool.spec(),
            pool.aliveThreads(),
            pool.coreThreads(),
            pool.activeThreads(),
            pool.idleThreads(),
            pool.queuedTasks(),
            pool.queueCapacity(),
            pool.submittedTasks(),
            pool.completedTasks(),
            pool.rejectedTasks(),
            pool.cpuLoad(),
            pool.saturation(),
            List.copyOf(reasons)
        );
    }

    /**
     * Lifecycle phase of the runtime behind this facade.
     */
    enum Phase {

        /**
         * Constructed, nothing registered.
         */
        NEW,

        /**
         * Accepting registration, factories and leases.
         */
        STARTED,

        /**
         * Shutdown in progress.
         */
        SHUTTING_DOWN,

        /**
         * Stopped for good.
         */
        STOPPED
    }
}
