package kz.bejiihiu.tuff.compute;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable configuration model of the compute scheduler.
 *
 * <p>The record mirrors the {@code async-compute} section of {@code config/tuff.yml} one to one, so
 * the YAML loader only has to translate values and call one constructor. Every nested record owns
 * its own validation: an instance of this type can be handed to
 * {@link AsyncComputeScheduler} without further checking.
 *
 * <p>Sizing values use {@code -1} for "let the scheduler decide", which is resolved against the
 * processor budget when the scheduler starts (and again whenever the platform applies a new budget at
 * runtime). The budget is what is left after the server reserved its own region, chunk and IO
 * threads; it is not the raw core count.
 *
 * @param enabled              whether the scheduler runs at all; a disabled scheduler rejects every
 *                             submission and starts no worker threads
 * @param workers              worker pool sizing and lifecycle
 * @param queue                global queue sizing and overflow behaviour
 * @param scheduling           dispatch strategy and fairness options
 * @param consumers            default consumer limits and per consumer overrides
 * @param metrics              metrics and profiling options
 * @param adaptive             adaptive concurrency options
 * @param lanes                execution lane options (platform workers and virtual threads)
 * @param shutdownGracePeriod  how long a graceful shutdown waits for running tasks before it stops
 *                             claiming new work; queued tasks that did not start are cancelled
 */
public record ComputeSettings(
    boolean enabled,
    Workers workers,
    QueueSettings queue,
    Scheduling scheduling,
    Consumers consumers,
    Metrics metrics,
    Adaptive adaptive,
    Lanes lanes,
    Duration shutdownGracePeriod
) {

    /**
     * Validates the settings tree.
     *
     * @throws NullPointerException     if any section is {@code null}
     * @throws IllegalArgumentException if the grace period is negative
     */
    public ComputeSettings {
        Objects.requireNonNull(workers, "workers");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(scheduling, "scheduling");
        Objects.requireNonNull(consumers, "consumers");
        Objects.requireNonNull(metrics, "metrics");
        Objects.requireNonNull(adaptive, "adaptive");
        Objects.requireNonNull(lanes, "lanes");
        Objects.requireNonNull(shutdownGracePeriod, "shutdownGracePeriod");
        if (shutdownGracePeriod.isNegative()) {
            throw new IllegalArgumentException("shutdownGracePeriod must not be negative, got " + shutdownGracePeriod);
        }
    }

    /**
     * Creates settings with the default execution lanes.
     *
     * <p>Kept so code that predates the lane model keeps compiling: it is equivalent to the full
     * constructor with {@link Lanes#defaults()}.
     *
     * @param enabled             whether the scheduler runs at all
     * @param workers             worker pool sizing
     * @param queue               queue sizing
     * @param scheduling          dispatch strategy
     * @param consumers           consumer policies
     * @param metrics             metrics options
     * @param adaptive            adaptive concurrency options
     * @param shutdownGracePeriod shutdown grace period
     */
    public ComputeSettings(
        final boolean enabled,
        final Workers workers,
        final QueueSettings queue,
        final Scheduling scheduling,
        final Consumers consumers,
        final Metrics metrics,
        final Adaptive adaptive,
        final Duration shutdownGracePeriod
    ) {
        this(enabled, workers, queue, scheduling, consumers, metrics, adaptive, Lanes.defaults(), shutdownGracePeriod);
    }

    /**
     * Returns the defaults that match a fresh {@code config/tuff.yml}.
     *
     * @return the default settings
     */
    public static ComputeSettings defaults() {
        return new ComputeSettings(
            true,
            Workers.defaults(),
            QueueSettings.defaults(),
            Scheduling.defaults(),
            Consumers.defaults(),
            Metrics.defaults(),
            Adaptive.defaults(),
            Lanes.defaults(),
            Duration.ofSeconds(30)
        );
    }

    /**
     * Worker pool sizing.
     *
     * <p>{@code threads} is either a fixed worker count or {@code -1} for automatic sizing, in which
     * case {@code minThreads} is honoured as a floor and {@code maxThreads} as a ceiling. Workers
     * above the minimum are created lazily when the queue backs up and are retired after
     * {@code keepAlive} without work.
     *
     * @param threads           fixed worker count, or {@code -1} to size automatically
     * @param minThreads        minimum number of workers kept alive, at least {@code 1}
     * @param maxThreads        maximum number of workers, or {@code -1} for no explicit ceiling
     * @param keepAlive         how long an idle worker above {@code minThreads} is kept before it
     *                          exits, must be positive
     * @param threadNamePrefix  prefix for worker thread names, used verbatim in thread dumps
     */
    public record Workers(
        int threads,
        int minThreads,
        int maxThreads,
        Duration keepAlive,
        String threadNamePrefix
    ) {

        /**
         * Validates the pool sizing.
         *
         * @throws NullPointerException     if a non numeric value is {@code null}
         * @throws IllegalArgumentException if the counts are inconsistent or the keep alive is not
         *     positive
         */
        public Workers {
            Objects.requireNonNull(keepAlive, "keepAlive");
            Objects.requireNonNull(threadNamePrefix, "threadNamePrefix");
            if (threads != -1 && threads < 1) {
                throw new IllegalArgumentException("threads must be -1 or at least 1, got " + threads);
            }
            if (minThreads < 1) {
                throw new IllegalArgumentException("minThreads must be at least 1, got " + minThreads);
            }
            if (maxThreads != -1 && maxThreads < minThreads) {
                throw new IllegalArgumentException("maxThreads must be -1 or at least minThreads, got " + maxThreads);
            }
            if (threads > 0 && maxThreads != -1 && threads > maxThreads) {
                throw new IllegalArgumentException("threads must not exceed maxThreads, got " + threads + " > " + maxThreads);
            }
            if (keepAlive.isNegative() || keepAlive.isZero()) {
                throw new IllegalArgumentException("keepAlive must be positive, got " + keepAlive);
            }
            if (threadNamePrefix.isBlank()) {
                throw new IllegalArgumentException("threadNamePrefix must not be blank");
            }
        }

        /**
         * Returns the default pool sizing.
         *
         * @return automatic sizing with two minimum workers and a thirty second keep alive
         */
        public static Workers defaults() {
            return new Workers(-1, 2, -1, Duration.ofSeconds(30), "Tuff Compute Worker");
        }

        /**
         * Checks whether worker count is chosen automatically.
         *
         * @return {@code true} if {@link #threads()} is {@code -1}
         */
        public boolean automaticThreadCount() {
            return this.threads == -1;
        }

        /**
         * Resolves the initial worker count against the processor budget.
         *
         * <p>Automatic sizing keeps one processor as headroom, honours
         * {@link #minThreads()} as a floor and {@link #maxThreads()} as a ceiling. The argument is a
         * <b>budget</b>, not the raw core count: the platform has already subtracted its region tick
         * threads, chunk workers, IO threads and headroom, so the scheduler must not subtract them
         * again.
         *
         * @param availableProcessors processors available to the scheduler (the engine budget), at least {@code 1}
         * @return the initial number of workers, always between {@code 1} and the resolved maximum
         * @throws IllegalArgumentException if {@code availableProcessors} is not positive
         */
        public int resolveThreadCount(final int availableProcessors) {
            if (availableProcessors < 1) {
                throw new IllegalArgumentException("availableProcessors must be at least 1, got " + availableProcessors);
            }
            final int maximum = this.resolveMaxThreads(availableProcessors);
            final int target;
            if (this.threads > 0) {
                target = this.threads;
            } else {
                target = Math.max(this.minThreads, availableProcessors - 1);
            }
            return Math.max(1, Math.min(maximum, target));
        }

        /**
         * Resolves the maximum worker count against the processor budget.
         *
         * @param availableProcessors processors available to the scheduler (the engine budget), at least {@code 1}
         * @return the hard ceiling for the pool, never below {@link #minThreads()}
         * @throws IllegalArgumentException if {@code availableProcessors} is not positive
         */
        public int resolveMaxThreads(final int availableProcessors) {
            if (availableProcessors < 1) {
                throw new IllegalArgumentException("availableProcessors must be at least 1, got " + availableProcessors);
            }
            final int ceiling = this.maxThreads == -1 ? Math.max(this.minThreads, availableProcessors) : this.maxThreads;
            return Math.max(1, ceiling);
        }
    }

    /**
     * Global queue sizing.
     *
     * <p>The capacity is the hard bound on queued work across all consumers. It exists so a burst of
     * submissions can never grow heap without limit; when it is reached the scheduler refuses rather
     * than blocks the submitting thread.
     *
     * <p>The overflow policy is the default for consumers the scheduler creates on its own: the
     * {@link AsyncComputeScheduler#DEFAULT_CONSUMER_ID default consumer} and consumers registered on
     * the fly by {@link Consumers#allowDynamicRegistration() dynamic registration}. A policy supplied
     * explicitly through {@link AsyncComputeScheduler#registerConsumer(ComputeConsumer)} or through a
     * {@link Consumers#overrides() per consumer override} wins over this default.
     *
     * @param capacity       maximum number of queued tasks in the whole scheduler, at least {@code 1}
     * @param overflowPolicy default overflow behaviour for consumers the scheduler registers itself; DROP_OLDEST still only evicts the oldest queued task of the submitting consumer, so a consumer with nothing queued is refused exactly like with REJECT
     */
    public record QueueSettings(int capacity, OverflowPolicy overflowPolicy) {

        /**
         * Validates the queue sizing.
         *
         * @throws NullPointerException     if the overflow policy is {@code null}
         * @throws IllegalArgumentException if the capacity is not positive
         */
        public QueueSettings {
            Objects.requireNonNull(overflowPolicy, "overflowPolicy");
            if (capacity < 1) {
                throw new IllegalArgumentException("capacity must be at least 1, got " + capacity);
            }
        }

        /**
         * Returns the default queue sizing.
         *
         * @return capacity {@code 8192} with {@link OverflowPolicy#REJECT}
         */
        public static QueueSettings defaults() {
            return new QueueSettings(8192, OverflowPolicy.REJECT);
        }
    }

    /**
     * Dispatch strategy and fairness options.
     *
     * @param strategy       how the next task is chosen when several are runnable
     * @param workStealing   whether an idle worker may take a task that is currently owned by another
     *                       worker's local batch; only enable once benchmarks show it helps
     * @param priorityAging  whether waiting tasks gain effective priority over time so low priority
     *                       work cannot starve
     * @param agingInterval  how long a task waits before it gains one aging step, must be positive
     *                       when aging is enabled
     * @param maxAgingBoost  maximum number of levels a task may gain through aging, at least {@code 1}
     * @param idleWait       how long an idle worker waits for work before it parks or, above the
     *                       minimum pool size, retires; must be positive
     */
    public record Scheduling(
        Strategy strategy,
        boolean workStealing,
        boolean priorityAging,
        Duration agingInterval,
        int maxAgingBoost,
        Duration idleWait
    ) {

        /**
         * Validates the scheduling options.
         *
         * @throws NullPointerException     if a value is {@code null}
         * @throws IllegalArgumentException if a duration is not positive or the aging boost is below
         *     {@code 1}
         */
        public Scheduling {
            Objects.requireNonNull(strategy, "strategy");
            Objects.requireNonNull(agingInterval, "agingInterval");
            Objects.requireNonNull(idleWait, "idleWait");
            if (agingInterval.isNegative() || agingInterval.isZero()) {
                throw new IllegalArgumentException("agingInterval must be positive, got " + agingInterval);
            }
            if (maxAgingBoost < 1) {
                throw new IllegalArgumentException("maxAgingBoost must be at least 1, got " + maxAgingBoost);
            }
            if (idleWait.isNegative() || idleWait.isZero()) {
                throw new IllegalArgumentException("idleWait must be positive, got " + idleWait);
            }
        }

        /**
         * Returns the default scheduling options.
         *
         * @return weighted fair scheduling with aging enabled and work stealing off
         */
        public static Scheduling defaults() {
            return new Scheduling(Strategy.WEIGHTED_FAIR, false, true, Duration.ofSeconds(1), 3, Duration.ofMillis(50));
        }

        /**
         * Order in which runnable tasks are picked.
         */
        public enum Strategy {

            /**
             * Plain submission order, ignores priority and weight. Useful as a benchmark baseline.
             */
            FIFO,

            /**
             * Strictly highest priority first, aged by {@link Scheduling#priorityAging()}. Simple, but
             * a consumer that only submits low priority work can still wait for a long time.
             */
            PRIORITY,

            /**
             * Highest priority first, with dispatch turns distributed between consumers proportionally
             * to their {@link ConsumerPolicy#fairShareWeight()}. Prevents one busy subsystem from
             * monopolising the pool.
             */
            WEIGHTED_FAIR
        }
    }

    /**
     * Consumer defaults and per consumer overrides.
     *
     * <p>The default policy applies to every consumer registered without an explicit policy and to
     * consumers that are auto registered when dynamic registration is allowed. Overrides are keyed by
     * consumer id and replace the whole policy for that consumer.
     *
     * <p>Consumers created by the scheduler itself (the
     * {@link AsyncComputeScheduler#DEFAULT_CONSUMER_ID default consumer} and dynamic registrations
     * without an override) combine this default policy with
     * {@link QueueSettings#overflowPolicy()}: the queue overflow default wins unless the overrides map
     * or an explicit registration provides a policy for that id.
     *
     * @param defaultPolicy             policy for consumers that have no explicit one
     * @param overrides                 per consumer id policy overrides, may be empty
     * @param allowDynamicRegistration  whether submitting under an unknown consumer id registers it
     *                                  on the fly with {@code defaultPolicy} (its overflow policy
     *                                  comes from {@link QueueSettings#overflowPolicy()}); when
     *                                  {@code false} such a submission is rejected with
     *                                  {@link ComputeRejectedException.Reason#CONSUMER_UNKNOWN}
     */
    public record Consumers(
        ConsumerPolicy defaultPolicy,
        Map<String, ConsumerPolicy> overrides,
        boolean allowDynamicRegistration
    ) {

        /**
         * Validates and freezes the consumer configuration.
         *
         * @throws NullPointerException     if a value is {@code null} or an override key is {@code null}
         * @throws IllegalArgumentException if an override key is blank
         */
        public Consumers {
            Objects.requireNonNull(defaultPolicy, "defaultPolicy");
            Objects.requireNonNull(overrides, "overrides");
            overrides = Map.copyOf(overrides);
            for (final String consumerId : overrides.keySet()) {
                if (consumerId.isBlank()) {
                    throw new IllegalArgumentException("consumer override id must not be blank");
                }
            }
        }

        /**
         * Returns the default consumer configuration.
         *
         * @return default policy, no overrides, dynamic registration allowed
         */
        public static Consumers defaults() {
            return new Consumers(ConsumerPolicy.defaults(), Map.of(), true);
        }

        /**
         * Resolves the policy for a consumer id, falling back to {@link #defaultPolicy()}.
         *
         * @param consumerId consumer to look up
         * @return the override for that id, or the default policy
         */
        public ConsumerPolicy policyFor(final String consumerId) {
            return this.overrides.getOrDefault(consumerId, this.defaultPolicy);
        }
    }

    /**
     * Metrics and profiling options.
     *
     * @param enabled                whether counters and gauges are collected at all
     * @param jfrEnabled             whether JFR events are emitted for submissions, executions and
     *                               worker state changes
     * @param throughputWindow       rolling window used for throughput and averages, must be positive
     *                               when metrics are enabled
     * @param sampleCpuUtilization   whether the pool estimates CPU utilisation from busy worker time;
     *                               the estimate is coarse and never a system wide CPU reading
     * @param sampleInterval         how often gauges are sampled for JFR and periodic logging, must be
     *                               positive
     */
    public record Metrics(
        boolean enabled,
        boolean jfrEnabled,
        Duration throughputWindow,
        boolean sampleCpuUtilization,
        Duration sampleInterval
    ) {

        /**
         * Validates the metrics options.
         *
         * @throws NullPointerException     if a duration is {@code null}
         * @throws IllegalArgumentException if a duration is not positive
         */
        public Metrics {
            Objects.requireNonNull(throughputWindow, "throughputWindow");
            Objects.requireNonNull(sampleInterval, "sampleInterval");
            if (throughputWindow.isNegative() || throughputWindow.isZero()) {
                throw new IllegalArgumentException("throughputWindow must be positive, got " + throughputWindow);
            }
            if (sampleInterval.isNegative() || sampleInterval.isZero()) {
                throw new IllegalArgumentException("sampleInterval must be positive, got " + sampleInterval);
            }
        }

        /**
         * Returns the default metrics options.
         *
         * @return metrics on, JFR off, ten second window, CPU estimate on, one second sampling
         */
        public static Metrics defaults() {
            return new Metrics(true, false, Duration.ofSeconds(10), true, Duration.ofSeconds(1));
        }
    }

    /**
     * Adaptive concurrency options.
     *
     * <p>When enabled, the scheduler observes queue wait against execution time and moves the
     * concurrency limit between {@code minConcurrency} and {@code maxConcurrency}. The goal is to
     * stop claiming more work than the machine can actually chew; it is never allowed to disable the
     * pool completely.
     *
     * @param enabled            whether the adaptive controller may change the concurrency limit
     * @param minConcurrency     lowest limit the controller may pick, at least {@code 1}
     * @param maxConcurrency     highest limit, or {@code -1} for the current pool size
     * @param sampleWindow       window over which samples are aggregated before a decision is taken
     * @param minSamples         minimum number of finished tasks per window before the limit may move
     * @param increaseStep       additive increase applied when the pool is not saturated, in
     *                           {@code (0.0, 1.0]}
     * @param decreaseFactor     multiplicative decrease applied when latency degrades, in
     *                           {@code (0.0, 1.0)}
     * @param targetUtilization  estimated utilisation the controller aims for, in {@code (0.0, 1.0]}
     */
    public record Adaptive(
        boolean enabled,
        int minConcurrency,
        int maxConcurrency,
        Duration sampleWindow,
        int minSamples,
        double increaseStep,
        double decreaseFactor,
        double targetUtilization
    ) {

        /**
         * Validates the adaptive options.
         *
         * @throws NullPointerException     if a duration is {@code null}
         * @throws IllegalArgumentException if limits are inconsistent or a factor is out of range
         */
        public Adaptive {
            Objects.requireNonNull(sampleWindow, "sampleWindow");
            if (minConcurrency < 1) {
                throw new IllegalArgumentException("minConcurrency must be at least 1, got " + minConcurrency);
            }
            if (maxConcurrency != -1 && maxConcurrency < minConcurrency) {
                throw new IllegalArgumentException("maxConcurrency must be -1 or at least minConcurrency, got " + maxConcurrency);
            }
            if (sampleWindow.isNegative() || sampleWindow.isZero()) {
                throw new IllegalArgumentException("sampleWindow must be positive, got " + sampleWindow);
            }
            if (minSamples < 1) {
                throw new IllegalArgumentException("minSamples must be at least 1, got " + minSamples);
            }
            if (!Double.isFinite(increaseStep) || increaseStep <= 0.0d || increaseStep > 1.0d) {
                throw new IllegalArgumentException("increaseStep must be in (0.0, 1.0], got " + increaseStep);
            }
            if (!Double.isFinite(decreaseFactor) || decreaseFactor <= 0.0d || decreaseFactor >= 1.0d) {
                throw new IllegalArgumentException("decreaseFactor must be in (0.0, 1.0), got " + decreaseFactor);
            }
            if (!Double.isFinite(targetUtilization) || targetUtilization <= 0.0d || targetUtilization > 1.0d) {
                throw new IllegalArgumentException("targetUtilization must be in (0.0, 1.0], got " + targetUtilization);
            }
        }

        /**
         * Returns the default adaptive options.
         *
         * @return disabled, one to pool size, five second window, twenty samples, gentle AIMD steps
         */
        public static Adaptive defaults() {
            return new Adaptive(false, 1, -1, Duration.ofSeconds(5), 20, 0.1d, 0.5d, 0.9d);
        }
    }

    /**
     * Execution lane options.
     *
     * <p>A task runs on one of two lanes. The <b>platform lane</b> is the fixed pool of platform
     * workers described by {@link Workers}. The <b>virtual lane</b> runs work on
     * {@link Thread#ofVirtual() virtual threads} and has its own hard limit of concurrent tasks.
     *
     * <p>The lane a task uses is decided by its priority:
     * <ul>
     *     <li>{@link ComputePriority#LOW} always runs on the virtual lane, and never occupies a
     *     platform worker;</li>
     *     <li>{@link ComputePriority#NORMAL} runs on the platform lane and may spill to the virtual
     *     lane only while the pool is saturated and the spill thresholds are met;</li>
     *     <li>{@link ComputePriority#HIGH} and {@link ComputePriority#CRITICAL} never spill.</li>
     * </ul>
     *
     * <p>Virtual threads add no CPU parallelism. The lane exists for blocking or IO bound work and
     * as a deadlock breaker; it never changes the platform pool size, the processor budget or the
     * adaptive concurrency limit.
     *
     * @param virtual virtual lane options
     */
    public record Lanes(Virtual virtual) {

        /**
         * Validates the lane tree.
         *
         * @throws NullPointerException if {@code virtual} is {@code null}
         */
        public Lanes {
            Objects.requireNonNull(virtual, "virtual");
        }

        /**
         * Returns the default lanes: the virtual lane is enabled.
         *
         * @return the default lanes
         */
        public static Lanes defaults() {
            return new Lanes(Virtual.defaults());
        }

        /**
         * Returns lanes with the virtual lane turned off.
         *
         * <p>LOW tasks then fall back to the platform lane, which is the pre-lane behaviour.
         *
         * @return lanes without a virtual lane
         */
        public static Lanes withoutVirtual() {
            return new Lanes(Virtual.withoutVirtual());
        }

        /**
         * Virtual lane options.
         *
         * @param enabled         whether the virtual lane exists at all; when off, LOW work falls back
         *                        to the platform lane
         * @param lowPriority     whether {@link ComputePriority#LOW} tasks are routed to the virtual
         *                        lane; when off, LOW work falls back to the platform lane
         * @param normalSpill     whether saturated {@link ComputePriority#NORMAL} work may spill to the
         *                        virtual lane
         * @param maxConcurrent   hard limit of virtual tasks executing at the same time, at least
         *                        {@code 1}
         * @param spillWait       how long a NORMAL task must have waited before it becomes eligible to
         *                        spill, not negative
         * @param spillQueueDepth NORMAL queue depth at which spill becomes eligible regardless of the
         *                        wait, not negative; {@code 0} disables the depth trigger
         */
        public record Virtual(
            boolean enabled,
            boolean lowPriority,
            boolean normalSpill,
            int maxConcurrent,
            Duration spillWait,
            int spillQueueDepth
        ) {

            /**
             * Validates the virtual lane options.
             *
             * @throws NullPointerException     if {@code spillWait} is {@code null}
             * @throws IllegalArgumentException if the limit is not positive, a duration is negative or
             *     the queue depth is negative
             */
            public Virtual {
                Objects.requireNonNull(spillWait, "spillWait");
                if (maxConcurrent < 1) {
                    throw new IllegalArgumentException("maxConcurrent must be at least 1, got " + maxConcurrent);
                }
                if (spillWait.isNegative()) {
                    throw new IllegalArgumentException("spillWait must not be negative, got " + spillWait);
                }
                if (spillQueueDepth < 0) {
                    throw new IllegalArgumentException("spillQueueDepth must not be negative, got " + spillQueueDepth);
                }
            }

            /**
             * Returns the default virtual lane: enabled, LOW routed to it, NORMAL may spill, sixty
             * four concurrent tasks, fifty millisecond spill wait and a sixty four deep NORMAL queue
             * trigger.
             *
             * @return the default virtual lane
             */
            public static Virtual defaults() {
                return new Virtual(true, true, true, 64, Duration.ofMillis(50L), 64);
            }

            /**
             * Returns a virtual lane that is switched off.
             *
             * @return the lane options in their disabled shape
             */
            public static Virtual withoutVirtual() {
                return new Virtual(false, false, false, 1, Duration.ZERO, 0);
            }

            /**
             * Checks whether the lane is allowed to take {@link ComputePriority#LOW} work.
             *
             * @return {@code true} when the lane exists and routes LOW tasks
             */
            public boolean servesLow() {
                return this.enabled && this.lowPriority;
            }

            /**
             * Checks whether NORMAL work may spill onto the lane.
             *
             * @return {@code true} when the lane exists and spilling is enabled
             */
            public boolean allowsSpill() {
                return this.enabled && this.normalSpill;
            }
        }
    }
}
