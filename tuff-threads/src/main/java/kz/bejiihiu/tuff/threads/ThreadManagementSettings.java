package kz.bejiihiu.tuff.threads;

import java.util.Objects;

/**
 * Immutable configuration model of the {@code thread-management} section.
 *
 * <p>The record mirrors the YAML tree one to one, so a loader only has to translate values and call one
 * constructor. Every nested record validates itself; an instance of this type can be handed to the
 * manager without further checking. Sizing values use {@code -1} for "let the manager decide", and
 * thread counts are counts of threads, not cores: the controller resolves cores from the topology and
 * the reserve, then applies the counts.
 *
 * <p>Safe defaults are deliberately conservative and describe an <b>observe only</b> manager:
 * {@code enabled} is {@code true} so registration and metrics work, but every {@code managed} flag is
 * {@code false}, region execution keeps Folia's scheduler and the experimental Kairos path stays off.
 * Turning a flag on is a separate, reviewed integration step, not a side effect of upgrading.
 *
 * <p>The whole section is restart only, and the model does not pretend otherwise. The bootstrap parses
 * it once, the runtime registers its pool set and bakes its sampling policy in {@code start()}, and no
 * reconfigure call reaches a live manager: a changed key is listed by {@code /tuff reload} as "takes
 * effect after a restart", never applied on the fly. That includes the metrics switches and every
 * {@code managed} flag - they describe the manager that was built, so a running instance cannot
 * honour a change to them.
 *
 * @param enabled         whether the manager runs at all; a disabled manager registers nothing, creates
 *                        no thread and rejects every lease
 * @param resources       processor topology, reserve and global concurrency limits
 * @param lifecycle       whether new server threads must come from the provider and whether they are
 *                        monitored
 * @param regionExecution how Folia's region tick pool is treated
 * @param asyncCompute    how the compute engine's platform pool is treated
 * @param executionPools  per pool treatment for the specialised pools
 * @param metrics         which metrics are collected and published
 */
public record ThreadManagementSettings(
    boolean enabled,
    Resources resources,
    Lifecycle lifecycle,
    RegionExecution regionExecution,
    AsyncCompute asyncCompute,
    Pools executionPools,
    Metrics metrics
) {

    /**
     * Validates the configuration tree.
     *
     * @throws NullPointerException if a section is {@code null}
     */
    public ThreadManagementSettings {
        Objects.requireNonNull(resources, "resources");
        Objects.requireNonNull(lifecycle, "lifecycle");
        Objects.requireNonNull(regionExecution, "regionExecution");
        Objects.requireNonNull(asyncCompute, "asyncCompute");
        Objects.requireNonNull(executionPools, "executionPools");
        Objects.requireNonNull(metrics, "metrics");
    }

    /**
     * Returns the defaults that match a fresh {@code config/tuff.yml}.
     *
     * <p>The result is observe only: the manager is enabled, nothing is managed, region scheduling stays
     * with Folia and Kairos is off.
     *
     * @return the default settings
     */
    public static ThreadManagementSettings defaults() {
        return new ThreadManagementSettings(
            true,
            Resources.defaults(),
            Lifecycle.defaults(),
            RegionExecution.defaults(),
            AsyncCompute.defaults(),
            Pools.defaults(),
            Metrics.defaults()
        );
    }

    /**
     * Checks whether any pool is allowed to be managed, as opposed to merely observed.
     *
     * <p>This is the flag bootstrap and commands use to tell "UTMI runs in observe only mode" from "UTMI
     * owns capacity somewhere". It is derived, never stored.
     *
     * @return {@code true} if region execution, compute, network or chunk capacity is managed
     */
    public boolean anyManagementEnabled() {
        return this.regionExecution.managed()
            || this.asyncCompute.managed()
            || this.executionPools.anyManaged();
    }

    /**
     * Processor topology, reserve and global limits.
     *
     * @param cpuAware           whether sizing uses the platform's actual processor count and container
     *                           limits instead of a static table; production should keep this on
     * @param reserveCores       cores never handed to managed pools, kept for GC, JIT, the kernel and
     *                           unexpected work; zero is legal but risky on small machines
     * @param maxConcurrentTasks global cap on concurrently executing leased tasks, or {@code -1} for no
     *                           explicit cap; pools still apply their own limits on top
     */
    public record Resources(boolean cpuAware, int reserveCores, int maxConcurrentTasks) {

        /**
         * Validates the resource settings.
         *
         * @throws IllegalArgumentException if a count is outside its range
         */
        public Resources {
            if (reserveCores < 0) {
                throw new IllegalArgumentException("reserveCores must not be negative, got " + reserveCores);
            }
            if (maxConcurrentTasks != -1 && maxConcurrentTasks < 1) {
                throw new IllegalArgumentException(
                    "maxConcurrentTasks must be -1 or at least 1, got " + maxConcurrentTasks
                );
            }
        }

        /**
         * Returns the default resource settings.
         *
         * @return topology aware sizing, one reserved core and no global task cap
         */
        public static Resources defaults() {
            return new Resources(true, 1, -1);
        }
    }

    /**
     * Thread creation and monitoring policy.
     *
     * @param centralizedThreadFactory whether new server owned threads must be created through
     *                                 {@link ThreadFactoryProvider}; the policy is enforced by the
     *                                 static guard and code review, not by this flag alone
     * @param monitorThreads           whether the manager samples thread states, CPU time and
     *                                 creation and termination rates
     */
    public record Lifecycle(boolean centralizedThreadFactory, boolean monitorThreads) {

        /**
         * Returns the default lifecycle policy.
         *
         * @return centralised factory and monitoring enabled
         */
        public static Lifecycle defaults() {
            return new Lifecycle(true, true);
        }
    }

    /**
     * Treatment of Folia's region tick pool.
     *
     * <p>Region execution is the most constrained lane. Its threads carry region context, its scheduler
     * owns tick order and its EDF variant cannot be resized without a restart. The only supported
     * integrations are observation and, later, a factory plus registration; the tick model itself stays
     * Folia's.
     *
     * @param managed                 whether the region pool is registered at L2 instead of L1; Folia
     *                                still creates the tick threads itself, so this changes the claimed
     *                                coverage, not who builds the threads
     * @param preserveFoliaScheduling must stay {@code true}; Folia owns tick order, catch up and region
     *                                ownership, and disabling this is rejected rather than silently
     *                                ignored
     * @param scheduler               scheduler variant Folia runs; {@code EDF} is the default and
     *                                {@link RegionScheduler#KAIROS} is experimental
     * @param threads                 fixed tick thread count, or {@code -1} for Folia's automatic
     *                                formula; restart only while the scheduler is EDF
     * @param gridExponent            region grid exponent, between zero and thirty one; restart only
     * @param kairosUnstable          opt in to the experimental Kairos scheduler; required for
     *                                {@link RegionScheduler#KAIROS} and never enabled implicitly
     */
    public record RegionExecution(
        boolean managed,
        boolean preserveFoliaScheduling,
        RegionScheduler scheduler,
        int threads,
        int gridExponent,
        boolean kairosUnstable
    ) {

        /**
         * Validates the region execution settings.
         *
         * @throws NullPointerException     if the scheduler is {@code null}
         * @throws IllegalArgumentException if a count is out of range, Folia scheduling is disabled or
         *                                  Kairos is selected without the explicit opt in
         */
        public RegionExecution {
            Objects.requireNonNull(scheduler, "scheduler");
            if (threads != -1 && threads < 1) {
                throw new IllegalArgumentException("threads must be -1 or at least 1, got " + threads);
            }
            if (gridExponent < 0 || gridExponent > 31) {
                throw new IllegalArgumentException("gridExponent must be in [0, 31], got " + gridExponent);
            }
            if (!preserveFoliaScheduling) {
                throw new IllegalArgumentException(
                    "preserveFoliaScheduling must stay true: Folia owns tick order and region ownership"
                );
            }
            if (scheduler == RegionScheduler.KAIROS && !kairosUnstable) {
                throw new IllegalArgumentException(
                    "Kairos is experimental; set kairosUnstable to true to opt in explicitly"
                );
            }
        }

        /**
         * Returns the default region execution settings.
         *
         * @return observe only, Folia EDF, automatic thread count, grid exponent four, Kairos off
         */
        public static RegionExecution defaults() {
            return new RegionExecution(false, true, RegionScheduler.EDF, -1, 4, false);
        }
    }

    /**
     * Scheduler variant of the region tick pool.
     */
    public enum RegionScheduler {

        /**
         * Earliest deadline first, Folia's default. No live resize: a thread count change needs a
         * restart.
         */
        EDF,

        /**
         * Work stealing, Folia's alternative. Supports live resize, including per NUMA node allocation,
         * but changes the identity of threads when it happens.
         */
        WORK_STEALING,

        /**
         * Tuff's own deadline scheduler with an elastic fund. Experimental: it requires
         * {@code kairos-unstable: true} and must never become the default without its own A/B gate.
         */
        KAIROS
    }

    /**
     * Treatment of the compute engine's platform pool.
     *
     * <p>The compute engine already owns its queue, priorities and fairness. UTMI only decides how much
     * capacity it may take and when it has to yield. Virtual thread limits stay in the existing
     * {@code async-compute} section; this record does not duplicate them.
     *
     * @param managed    whether the compute pool is registered as managed capacity; the compute
     *                   adapter is what would deliver it, so without one the configuration is reported
     *                   as {@link DegradedReason#CONFIG_CONFLICT} instead of applied
     * @param maxThreads explicit ceiling, or {@code -1} to keep the engine's own configured maximum
     */
    public record AsyncCompute(boolean managed, int maxThreads) {

        /**
         * Validates the compute settings.
         *
         * @throws IllegalArgumentException if the ceiling is neither {@code -1} nor positive
         */
        public AsyncCompute {
            if (maxThreads != -1 && maxThreads < 1) {
                throw new IllegalArgumentException("maxThreads must be -1 or at least 1, got " + maxThreads);
            }
        }

        /**
         * Returns the default compute settings.
         *
         * @return not managed, engine maximum kept
         */
        public static AsyncCompute defaults() {
            return new AsyncCompute(false, -1);
        }
    }

    /**
     * Treatment of the specialised pools that are not region execution or compute.
     *
     * @param network     network event loop pools
     * @param chunkSystem chunk generation and chunk IO pools
     */
    public record Pools(PoolSettings network, PoolSettings chunkSystem) {

        /**
         * Validates the pool settings.
         *
         * @throws NullPointerException if a section is {@code null}
         */
        public Pools {
            Objects.requireNonNull(network, "network");
            Objects.requireNonNull(chunkSystem, "chunkSystem");
        }

        /**
         * Returns the default settings for the specialised pools.
         *
         * @return both pools observed but not managed
         */
        public static Pools defaults() {
            return new Pools(PoolSettings.defaults(), PoolSettings.defaults());
        }

        /**
         * Checks whether any specialised pool is managed.
         *
         * @return {@code true} if network or chunk system capacity is managed
         */
        public boolean anyManaged() {
            return this.network.managed() || this.chunkSystem.managed();
        }
    }

    /**
     * Treatment of one specialised pool.
     *
     * @param managed                whether the pool is registered at L2 instead of L1; the owner still
     *                               creates the threads, so this is a registration claim, not a merge
     * @param preserveSpecialization must stay {@code true}; these pools exist because their ordering,
     *                               affinity or lifecycle cannot be shared, and disabling this is
     *                               rejected rather than silently ignored
     */
    public record PoolSettings(boolean managed, boolean preserveSpecialization) {

        /**
         * Validates the pool settings.
         *
         * @throws IllegalArgumentException if specialisation is disabled
         */
        public PoolSettings {
            if (!preserveSpecialization) {
                throw new IllegalArgumentException(
                    "preserveSpecialization must stay true: these pools cannot be merged or reshaped"
                );
            }
        }

        /**
         * Returns the default settings of a specialised pool.
         *
         * @return observed, not managed, specialisation preserved
         */
        public static PoolSettings defaults() {
            return new PoolSettings(false, true);
        }
    }

    /**
     * Metrics collection switches.
     *
     * @param enabled   whether the manager measures pool and thread state at all; with collection off
     *                  the snapshot still lists the registered pools and the threads the registry
     *                  accounts for, but no probe, CPU, rate or per thread list is read, the top level
     *                  carries {@link DegradedReason#METRICS_UNAVAILABLE} and {@link ResourceMetrics#enabled()}
     *                  stays {@code false}
     * @param perPool   whether per pool counters and saturation are published
     * @param perThread whether per thread snapshots are published; disabling it removes the individual
     *                  thread list from snapshots but keeps pool totals
     */
    public record Metrics(boolean enabled, boolean perPool, boolean perThread) {

        /**
         * Returns the default metrics settings.
         *
         * @return collection, per pool and per thread metrics enabled
         */
        public static Metrics defaults() {
            return new Metrics(true, true, true);
        }
    }
}
