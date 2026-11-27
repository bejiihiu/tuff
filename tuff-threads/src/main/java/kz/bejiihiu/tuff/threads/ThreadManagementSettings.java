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
            || this.computeManaged()
            || this.executionPools.anyManaged();
    }

    /**
     * Checks whether the compute engine's capacity is managed by the thread layer.
     *
     * <p>Two keys describe one switch: {@code execution-pools.compute.managed} is the place a compute
     * pool belongs and {@code async-compute.managed} is the older spelling kept so existing files keep
     * working. Either turns it on, and reading both here is what keeps the answer single valued - a
     * caller never has to know which key the operator used.
     *
     * @return {@code true} when the compute adapter may take the engine's threads and budget
     */
    public boolean computeManaged() {
        return this.executionPools.compute().managed() || this.asyncCompute.managed();
    }

    /**
     * Returns the tick pressure policy of the compute pool.
     *
     * <p>The policy is nested in the compute section because it is a compute decision: the pool gives
     * cores back while regions lag and takes them back when the regions are calm. It is a convenience
     * accessor, not a second setting.
     *
     * @return the tick pressure policy, never {@code null}
     */
    public AsyncCompute.TickPressure tickPressure() {
        return this.asyncCompute.tickPressure();
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
     * @param loadShedding       whether a starving critical pool may temporarily reduce the capacity of
     *                           the non critical pools. Off by default: it is a policy that takes
     *                           background capacity away, so it is opt in. The step, the calm window and
     *                           the cooldown are fixed policy of the controller, documented in the
     *                           configuration page; only the switch is a key
     */
    public record Resources(boolean cpuAware, int reserveCores, int maxConcurrentTasks, boolean loadShedding) {

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
         * Creates resource settings without a load shedding policy.
         *
         * <p>The three argument shape predates the shedding switch; a caller that does not mention it gets
         * the safe default, which is off.
         *
         * @param cpuAware           whether sizing is topology aware
         * @param reserveCores       cores kept out of the managed budget
         * @param maxConcurrentTasks global cap on concurrent leased tasks, or {@code -1}
         */
        public Resources(final boolean cpuAware, final int reserveCores, final int maxConcurrentTasks) {
            this(cpuAware, reserveCores, maxConcurrentTasks, false);
        }

        /**
         * Returns the default resource settings.
         *
         * @return topology aware sizing, one reserved core, no global task cap, load shedding off
         */
        public static Resources defaults() {
            return new Resources(true, 1, -1, false);
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
     * @param kairos                  limits of the Kairos fund; only read while the scheduler is Kairos
     */
    public record RegionExecution(
        boolean managed,
        boolean preserveFoliaScheduling,
        RegionScheduler scheduler,
        int threads,
        int gridExponent,
        boolean kairosUnstable,
        Kairos kairos
    ) {

        /**
         * Validates the region execution settings.
         *
         * @throws NullPointerException     if the scheduler or the Kairos section is {@code null}
         * @throws IllegalArgumentException if a count is out of range, Folia scheduling is disabled or
         *                                  Kairos is selected without the explicit opt in
         */
        public RegionExecution {
            Objects.requireNonNull(scheduler, "scheduler");
            Objects.requireNonNull(kairos, "kairos");
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
         * Creates region settings without Kairos limits, keeping the fund fixed.
         *
         * <p>The six argument shape predates the elastic fund; a caller that does not mention the
         * limits gets the fixed prototype of the scheduler.
         *
         * @param managed                 whether the region pool is registered at L2 instead of L1
         * @param preserveFoliaScheduling must stay {@code true}
         * @param scheduler               scheduler variant Folia runs
         * @param threads                 fixed tick thread count, or {@code -1}
         * @param gridExponent            region grid exponent
         * @param kairosUnstable          opt in to Kairos
         */
        public RegionExecution(
            final boolean managed,
            final boolean preserveFoliaScheduling,
            final RegionScheduler scheduler,
            final int threads,
            final int gridExponent,
            final boolean kairosUnstable
        ) {
            this(managed, preserveFoliaScheduling, scheduler, threads, gridExponent, kairosUnstable, Kairos.defaults());
        }

        /**
         * Returns the default region execution settings.
         *
         * @return observe only, Folia EDF, automatic thread count, grid exponent four, Kairos off
         */
        public static RegionExecution defaults() {
            return new RegionExecution(false, true, RegionScheduler.EDF, -1, 4, false, Kairos.defaults());
        }

        /**
         * Limits of the Kairos fund.
         *
         * <p>The floor is the existing {@code region-execution.threads}; this section only describes
         * what happens above it. {@code elastic} is a separate switch on purpose: a server that selects
         * Kairos to test the port keeps the fixed fund of the first prototype, and only an operator who
         * asks for elasticity gets a supervisor thread.
         *
         * @param elastic              whether the fund may grow and shrink
         * @param maxThreads           ceiling of the fund, or {@code -1} for the core count rule
         * @param keepAliveSeconds     how long a runner must sit idle before it may be retired
         * @param spawnRatePerSecond   how many new tick threads the supervisor may start per second
         */
        public record Kairos(boolean elastic, int maxThreads, int keepAliveSeconds, int spawnRatePerSecond) {

            /**
             * Validates the Kairos limits.
             *
             * @throws IllegalArgumentException if a limit is out of range
             */
            public Kairos {
                if (maxThreads != -1 && maxThreads < 1) {
                    throw new IllegalArgumentException("maxThreads must be -1 or at least 1, got " + maxThreads);
                }
                if (keepAliveSeconds < 1) {
                    throw new IllegalArgumentException("keepAliveSeconds must be at least 1, got " + keepAliveSeconds);
                }
                if (spawnRatePerSecond < 1) {
                    throw new IllegalArgumentException(
                        "spawnRatePerSecond must be at least 1, got " + spawnRatePerSecond
                    );
                }
            }

            /**
             * Returns the shipped limits: fixed fund, ceiling from the core count, compute's keep alive.
             *
             * @return the default limits
             */
            public static Kairos defaults() {
                return new Kairos(false, -1, 30, 10);
            }
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
     * @param managed      whether the compute pool is registered as managed capacity; the compute
     *                     adapter is what delivers it, so without one the configuration is reported
     *                     as {@link DegradedReason#CONFIG_CONFLICT} instead of applied. The same switch
     *                     lives at {@code execution-pools.compute.managed}; either key turns it on
     * @param maxThreads   explicit ceiling, or {@code -1} to keep the engine's own configured maximum
     * @param tickPressure whether the pool yields cores while regions lag
     */
    public record AsyncCompute(boolean managed, int maxThreads, TickPressure tickPressure) {

        /**
         * Validates the compute settings.
         *
         * @throws NullPointerException     if {@code tickPressure} is {@code null}
         * @throws IllegalArgumentException if the ceiling is neither {@code -1} nor positive
         */
        public AsyncCompute {
            if (maxThreads != -1 && maxThreads < 1) {
                throw new IllegalArgumentException("maxThreads must be -1 or at least 1, got " + maxThreads);
            }
            Objects.requireNonNull(tickPressure, "tickPressure");
        }

        /**
         * Creates compute settings with the default tick pressure policy.
         *
         * @param managed    whether the pool is managed
         * @param maxThreads explicit ceiling, or {@code -1}
         */
        public AsyncCompute(final boolean managed, final int maxThreads) {
            this(managed, maxThreads, TickPressure.defaults());
        }

        /**
         * Returns the default compute settings.
         *
         * @return not managed, engine maximum kept, tick pressure off
         */
        public static AsyncCompute defaults() {
            return new AsyncCompute(false, -1, TickPressure.defaults());
        }

        /**
         * Policy of the compute pool under region pressure.
         *
         * <p>The signal is the region summary the thread layer already samples: the share of regions
         * below 18 TPS, the slowest region's TPS and the worst single tick. While regions lag the
         * compute budget is lowered by {@code reserve} cores, step by step; when the regions are calm
         * for {@code exitWindows} consecutive samples the reserve is released one step at a time.
         *
         * <p>The defaults are deliberately conservative and the feature is <b>off</b>. Every window is
         * the refresh interval of the thread sampler (one second), so the times below are approximate.
         *
         * @param enabled           whether the pool yields cores to lagging regions at all
         * @param enterLagFraction  share of regions that must be below {@code LAG_TPS} to enter
         *                          pressure, in {@code (0, 1]}; a single lagging region of four is
         *                          {@code 0.25}
         * @param enterMinTps       slowest region TPS below which pressure starts even when the lag
         *                          share is zero
         * @param exitMinTps        slowest region TPS at or above which a sample counts as calm; must
         *                          not be below {@code enterMinTps}, the gap is the hysteresis band
         * @param exitWindows       consecutive calm samples needed before the reserve is released
         * @param cooldownSeconds   minimum time between two reserve changes, so a sawtooth cannot move
         *                          the pool every second
         * @param maxReserveCores   most cores the pool may give up, before the owner's own floor
         * @param stepCores         cores moved per decision; one means a ramp, not a jump
         */
        public record TickPressure(
            boolean enabled,
            double enterLagFraction,
            double enterMinTps,
            double exitMinTps,
            int exitWindows,
            int cooldownSeconds,
            int maxReserveCores,
            int stepCores
        ) {

            /**
             * Validates the pressure policy.
             *
             * @throws IllegalArgumentException if a threshold is outside its range or the band is
             *                                  inverted
             */
            public TickPressure {
                if (!(enterLagFraction > 0.0d) || enterLagFraction > 1.0d) {
                    throw new IllegalArgumentException(
                        "enterLagFraction must be in (0, 1], got " + enterLagFraction
                    );
                }
                if (!(enterMinTps > 0.0d) || enterMinTps > 20.0d) {
                    throw new IllegalArgumentException("enterMinTps must be in (0, 20], got " + enterMinTps);
                }
                if (!(exitMinTps > 0.0d) || exitMinTps > 20.0d) {
                    throw new IllegalArgumentException("exitMinTps must be in (0, 20], got " + exitMinTps);
                }
                if (exitMinTps < enterMinTps) {
                    throw new IllegalArgumentException(
                        "exitMinTps must not be below enterMinTps: " + exitMinTps + " < " + enterMinTps
                    );
                }
                if (exitWindows < 1) {
                    throw new IllegalArgumentException("exitWindows must be at least 1, got " + exitWindows);
                }
                if (cooldownSeconds < 1) {
                    throw new IllegalArgumentException("cooldownSeconds must be at least 1, got " + cooldownSeconds);
                }
                if (maxReserveCores < 1) {
                    throw new IllegalArgumentException("maxReserveCores must be at least 1, got " + maxReserveCores);
                }
                if (stepCores < 1 || stepCores > maxReserveCores) {
                    throw new IllegalArgumentException(
                        "stepCores must be in [1, " + maxReserveCores + "], got " + stepCores
                    );
                }
            }

            /**
             * Returns the shipped policy.
             *
             * @return disabled, a quarter of the regions lagging or a region below 19 TPS starts it,
             *     three calm samples at 19.5 TPS release it, at most two cores move one at a time
             */
            public static TickPressure defaults() {
                return new TickPressure(false, 0.25d, 19.0d, 19.5d, 3, 5, 2, 1);
            }

            /**
             * Returns the minimum time between two reserve changes.
             *
             * @return the cooldown in nanoseconds
             */
            public long cooldownNanos() {
                return this.cooldownSeconds * 1_000_000_000L;
            }
        }
    }

    /**
     * Treatment of the execution pools that are not region execution.
     *
     * <p>Network, chunk system and compute each keep their own threads, queue and lifecycle; this is
     * where the configuration says how deeply the thread layer is allowed to see or influence them.
     *
     * @param network     network event loop pools
     * @param chunkSystem chunk generation and chunk IO pools
     * @param compute     platform workers and the virtual lane of the compute engine
     */
    public record Pools(PoolSettings network, PoolSettings chunkSystem, PoolSettings compute) {

        /**
         * Validates the pool settings.
         *
         * @throws NullPointerException if a section is {@code null}
         */
        public Pools {
            Objects.requireNonNull(network, "network");
            Objects.requireNonNull(chunkSystem, "chunkSystem");
            Objects.requireNonNull(compute, "compute");
        }

        /**
         * Creates the pool settings of a configuration that predates the compute section.
         *
         * @param network     network event loop pools
         * @param chunkSystem chunk generation and chunk IO pools
         * @throws NullPointerException if a section is {@code null}
         */
        public Pools(final PoolSettings network, final PoolSettings chunkSystem) {
            this(network, chunkSystem, PoolSettings.defaults());
        }

        /**
         * Returns the default settings for the specialised pools.
         *
         * @return every pool observed but not managed
         */
        public static Pools defaults() {
            return new Pools(PoolSettings.defaults(), PoolSettings.defaults(), PoolSettings.defaults());
        }

        /**
         * Checks whether any specialised pool is managed.
         *
         * @return {@code true} if network, chunk system or compute capacity is managed
         */
        public boolean anyManaged() {
            return this.network.managed() || this.chunkSystem.managed() || this.compute.managed();
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
