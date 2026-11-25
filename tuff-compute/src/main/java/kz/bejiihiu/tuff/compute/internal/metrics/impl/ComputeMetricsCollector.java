package kz.bejiihiu.tuff.compute.internal.metrics.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ConsumerMetrics;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.internal.metrics.JfrRecorder;
import kz.bejiihiu.tuff.compute.internal.metrics.MetricsCollector;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.compute.internal.util.ThreadFactories;
import org.jspecify.annotations.Nullable;

/**
 * Default {@link MetricsCollector}: striped counters, two latency histograms and a periodic sampler.
 *
 * <p>Hot path rules implemented here:
 * <ul>
 *     <li>counters are {@link LongAdder}s, latency samples go into {@link LatencyHistogram} slots, so
 *     no hook allocates and none blocks;</li>
 *     <li>per consumer state is looked up once per event from a concurrent map;</li>
 *     <li>JFR is only touched after an {@link JfrRecorder#enabled()} style check, and only when the
 *     configuration asks for it.</li>
 * </ul>
 *
 * <p>Queue depth is reported two ways. The global depth is whatever the engine last reported through
 * {@link #onQueueState(int, int)}, because that number is authoritative. The per priority and per
 * consumer depths are derived from lifecycle deltas: a submission adds one, a start removes one, and a
 * cancellation removes one only if the task never started. Distinguishing those two cancellation cases
 * relies on the engine calling {@code onStarted} and the matching terminal event on the same thread,
 * which is what the worker loop does by design; a different reporting pattern would make the derived
 * per consumer depth drift, while the authoritative global depth stays correct.
 *
 * <p>Throughput is sampled, not counted on the hot path: the sampler reads the completion counters
 * once per interval and feeds {@link ThroughputMeter}. TPS is therefore accurate at the sample
 * granularity and cannot add work to a completing task.
 *
 * <p>The global queue depth and the worker gauges come from the engine's own samples
 * ({@link #onQueueState(int, int)} and {@link #onWorkerState(int, int, int, int)}), so a snapshot can
 * be at most one sample interval old for those values; everything derived from lifecycle events is
 * current. Create one collector per scheduler and {@link #close()} it when the scheduler terminates,
 * otherwise the sampler thread outlives the pool.
 */
public final class ComputeMetricsCollector implements MetricsCollector {

    private static final String SAMPLER_THREAD_PREFIX = "Tuff Compute Metrics";

    private final ComputeClock clock;
    private final JfrRecorder recorder;
    private final boolean metricsEnabled;
    private final boolean jfrEnabled;
    private final boolean cpuSampling;
    private final int configuredCapacity;
    private final Duration throughputWindow;
    private final Supplier<SchedulerMetrics.LaneMetrics> lanes;
    private final ConcurrentHashMap<String, ConsumerState> consumers = new ConcurrentHashMap<>();
    private final ThreadLocal<StartedMark> startedMark = ThreadLocal.withInitial(StartedMark::new);
    private final @Nullable ScheduledExecutorService sampler;

    private volatile @Nullable GlobalCounters counters;
    private volatile int queuedTasks;
    private volatile int queueCapacity;
    private volatile int totalWorkers;
    private volatile int activeWorkers;
    private volatile int idleWorkers;
    private volatile int concurrencyLimit;
    private volatile int peakWorkers;
    private volatile double cpuUtilization = Double.NaN;

    /**
     * Creates a collector.
     *
     * @param settings scheduler settings, the metrics section drives what is collected
     * @param clock    clock used for snapshot timestamps and throughput sampling
     * @param recorder flight recorder bridge, never {@code null}; it is only called when JFR is enabled
     * @throws NullPointerException if an argument is {@code null}
     */
    public ComputeMetricsCollector(final ComputeSettings settings, final ComputeClock clock, final JfrRecorder recorder) {
        this(settings, clock, recorder, SchedulerMetrics.LaneMetrics::disabled);
    }

    /**
     * Creates a collector that also reports the execution lane snapshot.
     *
     * @param settings scheduler settings, the metrics section drives what is collected
     * @param clock    clock used for snapshot timestamps and throughput sampling
     * @param recorder flight recorder bridge, never {@code null}
     * @param lanes    supplier of the lane snapshot, called once per snapshot, never {@code null}
     * @throws NullPointerException if an argument is {@code null}
     */
    public ComputeMetricsCollector(
        final ComputeSettings settings,
        final ComputeClock clock,
        final JfrRecorder recorder,
        final Supplier<SchedulerMetrics.LaneMetrics> lanes
    ) {
        Objects.requireNonNull(settings, "settings");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.recorder = Objects.requireNonNull(recorder, "recorder");
        this.lanes = Objects.requireNonNull(lanes, "lanes");
        final ComputeSettings.Metrics metrics = settings.metrics();
        this.metricsEnabled = metrics.enabled();
        this.jfrEnabled = metrics.jfrEnabled();
        this.cpuSampling = this.metricsEnabled && metrics.sampleCpuUtilization();
        this.configuredCapacity = settings.queue().capacity();
        this.queueCapacity = this.configuredCapacity;
        this.throughputWindow = metrics.throughputWindow();
        this.counters = this.metricsEnabled ? new GlobalCounters(clock, this.throughputWindow) : null;
        this.sampler = this.metricsEnabled ? this.startSampler(metrics.sampleInterval()) : null;
    }

    @Override
    public boolean enabled() {
        return this.metricsEnabled;
    }

    @Override
    public void onSubmitted(final QueueEntry entry) {
        final GlobalCounters current = this.counters;
        if (current != null) {
            current.submitted.increment();
            current.addQueued(entry.priority(), 1L);
            final ConsumerState state = this.consumers.get(entry.consumerId());
            if (state != null) {
                state.submitted.increment();
                state.queued.increment();
            }
        }
        if (!this.jfrEnabled) {
            return;
        }
        this.recorder.taskQueued(
            entry.taskId(),
            entry.consumerId(),
            entry.taskType(),
            entry.priority(),
            this.currentQueuedDepth(),
            labelOf(entry)
        );
    }

    @Override
    public void onRejected(final String consumerId, final ComputeRejectedException.Reason reason) {
        final GlobalCounters current = this.counters;
        if (current != null) {
            current.rejected.increment();
            current.rejectedByReason[reason.ordinal()].increment();
            final ConsumerState state = this.consumers.get(consumerId);
            if (state != null) {
                state.rejected.increment();
                state.rejectedByReason[reason.ordinal()].increment();
            }
        }
        if (this.jfrEnabled && this.recorder instanceof final FlightJfrRecorder flight) {
            flight.taskRejected(consumerId, reason);
        }
    }

    @Override
    public void onStarted(final QueueEntry entry, final long queueWaitNanos) {
        final GlobalCounters current = this.counters;
        int activeTasks = 0;
        if (current != null) {
            current.addQueued(entry.priority(), -1L);
            current.queueWait.record(queueWaitNanos);
            final ConsumerState state = this.consumers.get(entry.consumerId());
            if (state != null) {
                state.queued.decrement();
                state.active.increment();
                state.queueWait.record(queueWaitNanos);
                activeTasks = (int) state.active.sum();
            }
            this.startedMark.get().taskId = entry.taskId();
        }
        if (activeTasks == 0) {
            activeTasks = 1;
        }
        if (!this.jfrEnabled) {
            return;
        }
        this.recorder.taskStarted(
            entry.taskId(),
            entry.consumerId(),
            entry.taskType(),
            entry.priority(),
            queueWaitNanos,
            activeTasks,
            labelOf(entry)
        );
    }

    @Override
    public void onCompleted(final QueueEntry entry, final long executionNanos) {
        final GlobalCounters current = this.counters;
        if (current != null) {
            current.completed.increment();
            current.execution.record(executionNanos);
            final ConsumerState state = this.consumers.get(entry.consumerId());
            if (state != null) {
                state.completed.increment();
                state.active.decrement();
                state.execution.record(executionNanos);
            }
            this.clearStartedMark(entry.taskId());
        }
        if (this.jfrEnabled) {
            this.recorder.taskFinished(
                entry.taskId(),
                entry.consumerId(),
                entry.taskType(),
                entry.priority(),
                executionNanos,
                null,
                labelOf(entry)
            );
        }
    }

    @Override
    public void onFailed(final QueueEntry entry, final long executionNanos, final Throwable failure) {
        final GlobalCounters current = this.counters;
        if (current != null) {
            current.failed.increment();
            current.execution.record(executionNanos);
            final ConsumerState state = this.consumers.get(entry.consumerId());
            if (state != null) {
                state.failed.increment();
                state.active.decrement();
                state.execution.record(executionNanos);
            }
            this.clearStartedMark(entry.taskId());
        }
        if (this.jfrEnabled) {
            this.recorder.taskFinished(
                entry.taskId(),
                entry.consumerId(),
                entry.taskType(),
                entry.priority(),
                executionNanos,
                failure,
                labelOf(entry)
            );
        }
    }

    @Override
    public void onCancelled(final QueueEntry entry, final ComputeCancelledException.Reason reason) {
        final GlobalCounters current = this.counters;
        if (current != null) {
            current.cancelled.increment();
            current.cancelledByReason[reason.ordinal()].increment();
            if (reason == ComputeCancelledException.Reason.QUEUE_EXPIRED) {
                current.expired.increment();
            }
            if (reason == ComputeCancelledException.Reason.EVICTED) {
                current.evicted.increment();
            }
            final ConsumerState state = this.consumers.get(entry.consumerId());
            if (state != null) {
                state.cancelled.increment();
                state.cancelledByReason[reason.ordinal()].increment();
                if (reason == ComputeCancelledException.Reason.QUEUE_EXPIRED) {
                    state.expired.increment();
                }
                if (reason == ComputeCancelledException.Reason.EVICTED) {
                    state.evicted.increment();
                }
            }
            final StartedMark mark = this.startedMark.get();
            if (mark.taskId == entry.taskId()) {
                mark.taskId = -1L;
                if (state != null) {
                    state.active.decrement();
                }
            } else {
                current.addQueued(entry.priority(), -1L);
                if (state != null) {
                    state.queued.decrement();
                }
            }
        }
        if (this.jfrEnabled) {
            this.recorder.taskCancelled(entry.taskId(), entry.consumerId(), reason, labelOf(entry));
        }
    }

    @Override
    public void onQueueState(final int queuedTasks, final int capacity) {
        this.queuedTasks = Math.max(0, queuedTasks);
        this.queueCapacity = Math.max(0, capacity);
        if (this.jfrEnabled) {
            this.recorder.queueState(this.queuedTasks, this.queueCapacity);
        }
    }

    @Override
    public void onWorkerState(
        final int totalWorkers,
        final int activeWorkers,
        final int idleWorkers,
        final int concurrencyLimit
    ) {
        this.totalWorkers = Math.max(0, totalWorkers);
        this.activeWorkers = Math.max(0, activeWorkers);
        this.idleWorkers = Math.max(0, idleWorkers);
        this.concurrencyLimit = Math.max(0, concurrencyLimit);
        if (this.totalWorkers > this.peakWorkers) {
            this.peakWorkers = this.totalWorkers;
        }
        if (this.jfrEnabled) {
            this.recorder.workerState(this.totalWorkers, this.activeWorkers, this.idleWorkers, this.concurrencyLimit);
        }
    }

    @Override
    public void onConsumerRegistered(final ComputeConsumer consumer, final int concurrencyLimit) {
        if (!this.metricsEnabled) {
            return;
        }
        this.consumerState(consumer).applyPolicy(consumer, concurrencyLimit);
    }

    @Override
    public void onConsumerPolicyUpdated(final ComputeConsumer consumer, final int concurrencyLimit) {
        if (!this.metricsEnabled) {
            return;
        }
        this.consumerState(consumer).applyPolicy(consumer, concurrencyLimit);
    }

    @Override
    public void onConsumerUnregistered(final String consumerId) {
        if (!this.metricsEnabled) {
            return;
        }
        final ConsumerState state = this.consumers.get(consumerId);
        if (state != null) {
            state.registered = false;
        }
    }

    @Override
    public SchedulerMetrics schedulerMetrics() {
        final Instant capturedAt = this.clock.instant();
        final GlobalCounters current = this.counters;
        if (current == null) {
            return this.disabledSnapshot(capturedAt);
        }
        final Map<String, ConsumerMetrics> consumerSnapshots = new HashMap<>();
        for (final ConsumerState state : this.consumers.values()) {
            consumerSnapshots.put(state.consumerId, state.snapshot(capturedAt));
        }
        final LatencyStats queueWaitStats = current.queueWait.capture();
        final LatencyStats executionStats = current.execution.capture();
        return new SchedulerMetricsSnapshot(
            capturedAt,
            true,
            current.submitted.sum(),
            current.completed.sum(),
            current.failed.sum(),
            current.rejected.sum(),
            current.cancelled.sum(),
            current.expired.sum(),
            current.evicted.sum(),
            this.depthOrDefault(this.queuedTasks, current),
            this.queueCapacity > 0 ? this.queueCapacity : this.configuredCapacity,
            this.totalWorkers,
            this.activeWorkersOrDerived(consumerSnapshots),
            this.idleWorkers,
            this.peakWorkers,
            this.concurrencyLimit,
            current.throughput.perSecond(),
            current.throughput.lastSecondPerSecond(),
            this.cpuSampling ? this.cpuUtilization : Double.NaN,
            queueWaitStats.mean(),
            queueWaitStats.max(),
            executionStats.mean(),
            executionStats.max(),
            consumerSnapshots,
            this.reasonMap(current.rejectedByReason),
            this.cancelledReasonMap(current.cancelledByReason),
            this.priorityMap(current),
            queueWaitStats,
            executionStats,
            this.lanes.get()
        );
    }

    @Override
    public Map<String, ConsumerMetrics> consumerMetrics() {
        if (this.counters == null) {
            return Map.of();
        }
        final Instant capturedAt = this.clock.instant();
        final Map<String, ConsumerMetrics> snapshots = new HashMap<>();
        for (final ConsumerState state : this.consumers.values()) {
            snapshots.put(state.consumerId, state.snapshot(capturedAt));
        }
        return Map.copyOf(snapshots);
    }

    @Override
    public @Nullable ConsumerMetrics consumerMetrics(final String consumerId) {
        final ConsumerState state = this.consumers.get(consumerId);
        return state == null ? null : state.snapshot(this.clock.instant());
    }

    @Override
    public synchronized void reset() {
        this.counters = this.metricsEnabled ? new GlobalCounters(this.clock, this.throughputWindow) : null;
        this.consumers.clear();
        this.queuedTasks = 0;
        this.queueCapacity = this.configuredCapacity;
        this.totalWorkers = 0;
        this.activeWorkers = 0;
        this.idleWorkers = 0;
        this.concurrencyLimit = 0;
        this.peakWorkers = 0;
        this.cpuUtilization = Double.NaN;
    }

    @Override
    public void close() {
        final ScheduledExecutorService current = this.sampler;
        if (current != null) {
            current.shutdownNow();
        }
    }

    /**
     * Takes one throughput and CPU sample. Called by the sampler thread and callable directly, which
     * is what tests and a metrics command use.
     */
    public synchronized void sampleNow() {
        if (!this.metricsEnabled) {
            return;
        }
        final GlobalCounters current = this.counters;
        if (current == null) {
            return;
        }
        current.throughput.sample(current.completed.sum() + current.failed.sum());
        for (final ConsumerState state : this.consumers.values()) {
            state.throughput.sample(state.completed.sum() + state.failed.sum());
        }
        if (this.cpuSampling) {
            this.cpuUtilization = CpuUtilizationProbe.sample().orElse(Double.NaN);
        }
    }

    /**
     * Starts the periodic sampler.
     *
     * @param interval configured sample interval
     * @return the executor driving the sampler
     */
    private ScheduledExecutorService startSampler(final Duration interval) {
        final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(ThreadFactories.platform(SAMPLER_THREAD_PREFIX, true));
        final long millis = Math.max(1L, interval.toMillis());
        executor.scheduleAtFixedRate(this::sampleSafely, millis, millis, TimeUnit.MILLISECONDS);
        return executor;
    }

    /**
     * Samples without letting an unexpected failure kill the sampler thread.
     */
    private void sampleSafely() {
        try {
            this.sampleNow();
        } catch (Throwable _) {
            // metrics are diagnostic; a broken sample must not stop the series
        }
    }

    /**
     * Returns the state of a consumer, creating it on first sight.
     *
     * @param consumer the consumer
     * @return the existing or freshly created state
     */
    private ConsumerState consumerState(final ComputeConsumer consumer) {
        final ConsumerState existing = this.consumers.get(consumer.id());
        if (existing != null) {
            return existing;
        }
        final ConsumerState created = new ConsumerState(consumer.id(), this.clock, this.throughputWindow);
        final ConsumerState raced = this.consumers.putIfAbsent(consumer.id(), created);
        return raced == null ? created : raced;
    }

    /**
     * Clears the started marker of the calling thread when it belongs to the given task.
     *
     * @param taskId task that reached a terminal state
     */
    private void clearStartedMark(final long taskId) {
        final StartedMark mark = this.startedMark.get();
        if (mark.taskId == taskId) {
            mark.taskId = -1L;
        }
    }

    /**
     * Returns the diagnostic label of an entry, or the empty string when it has none.
     *
     * @param entry entry to inspect
     * @return the label, never {@code null}
     */
    private static String labelOf(final QueueEntry entry) {
        return entry.handle().label().orElse("");
    }

    /**
     * Returns the queue depth for the JFR submission event.
     *
     * @return derived depth when counters exist, otherwise the last sampled depth
     */
    private int currentQueuedDepth() {
        final GlobalCounters current = this.counters;
        return current == null ? this.queuedTasks : this.depthOrDefault(this.queuedTasks, current);
    }

    /**
     * Prefers the authoritative sampled depth and falls back to the derived one before the first
     * sample arrives.
     *
     * @param sampled the last sampled depth
     * @param current the counters to derive from
     * @return a non negative queue depth
     */
    private int depthOrDefault(final int sampled, final GlobalCounters current) {
        if (sampled > 0) {
            return sampled;
        }
        long derived = 0L;
        for (final LongAdder depth : current.queuedByPriority) {
            derived += depth.sum();
        }
        return (int) Math.max(0L, Math.min(Integer.MAX_VALUE, derived));
    }

    /**
     * Builds the per reason rejection map.
     *
     * @param adders counter per reason
     * @return an immutable map with every reason present
     */
    private Map<ComputeRejectedException.Reason, Long> reasonMap(final LongAdder[] adders) {
        final Map<ComputeRejectedException.Reason, Long> result = new EnumMap<>(ComputeRejectedException.Reason.class);
        for (final ComputeRejectedException.Reason reason : ComputeRejectedException.Reason.values()) {
            result.put(reason, adders[reason.ordinal()].sum());
        }
        return result;
    }

    /**
     * Builds the per reason cancellation map.
     *
     * @param adders counter per reason
     * @return an immutable map with every reason present
     */
    private Map<ComputeCancelledException.Reason, Long> cancelledReasonMap(final LongAdder[] adders) {
        final Map<ComputeCancelledException.Reason, Long> result = new EnumMap<>(ComputeCancelledException.Reason.class);
        for (final ComputeCancelledException.Reason reason : ComputeCancelledException.Reason.values()) {
            result.put(reason, adders[reason.ordinal()].sum());
        }
        return result;
    }

    /**
     * Builds the queue depth per priority map.
     *
     * @param current the counters
     * @return an immutable map with every priority present and non negative depths
     */
    private Map<ComputePriority, Integer> priorityMap(final GlobalCounters current) {
        final Map<ComputePriority, Integer> result = new EnumMap<>(ComputePriority.class);
        for (final ComputePriority priority : ComputePriority.values()) {
            final long depth = current.queuedByPriority[priority.level()].sum();
            result.put(priority, (int) Math.max(0L, Math.min(Integer.MAX_VALUE, depth)));
        }
        return result;
    }

    /**
     * Returns the number of busy workers, deriving it from the consumer states when the pool never
     * reported a sample.
     *
     * @param consumerSnapshots snapshots taken for this call
     * @return busy workers
     */
    private int activeWorkersOrDerived(final Map<String, ConsumerMetrics> consumerSnapshots) {
        if (this.totalWorkers > 0) {
            return this.activeWorkers;
        }
        int derived = 0;
        for (final ConsumerMetrics snapshot : consumerSnapshots.values()) {
            derived += snapshot.activeTasks();
        }
        return derived;
    }

    /**
     * Builds the snapshot of a disabled collector.
     *
     * @param capturedAt snapshot instant
     * @return an all zero snapshot
     */
    private SchedulerMetrics disabledSnapshot(final Instant capturedAt) {
        return new SchedulerMetricsSnapshot(
            capturedAt,
            false,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0,
            this.configuredCapacity,
            0,
            0,
            0,
            0,
            0,
            0.0d,
            0.0d,
            Double.NaN,
            Duration.ZERO,
            Duration.ZERO,
            Duration.ZERO,
            Duration.ZERO,
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            LatencyStats.empty(),
            LatencyStats.empty(),
            this.lanes.get()
        );
    }

    /**
     * Global counters and distributions, replaced as a whole on reset.
     */
    private static final class GlobalCounters {

        final LongAdder submitted = new LongAdder();
        final LongAdder completed = new LongAdder();
        final LongAdder failed = new LongAdder();
        final LongAdder cancelled = new LongAdder();
        final LongAdder expired = new LongAdder();
        final LongAdder evicted = new LongAdder();
        final LongAdder rejected = new LongAdder();
        final LongAdder[] rejectedByReason = new LongAdder[ComputeRejectedException.Reason.values().length];
        final LongAdder[] cancelledByReason = new LongAdder[ComputeCancelledException.Reason.values().length];
        final LongAdder[] queuedByPriority = new LongAdder[ComputePriority.values().length];
        final LatencyHistogram queueWait = new LatencyHistogram();
        final LatencyHistogram execution = new LatencyHistogram();
        final ThroughputMeter throughput;

        GlobalCounters(final ComputeClock clock, final Duration window) {
            for (int i = 0; i < this.rejectedByReason.length; i++) {
                this.rejectedByReason[i] = new LongAdder();
            }
            for (int i = 0; i < this.cancelledByReason.length; i++) {
                this.cancelledByReason[i] = new LongAdder();
            }
            for (int i = 0; i < this.queuedByPriority.length; i++) {
                this.queuedByPriority[i] = new LongAdder();
            }
            this.throughput = new ThroughputMeter(clock, window);
        }

        /**
         * Changes the derived queue depth of a priority.
         *
         * @param priority priority band
         * @param delta    one on submission, minus one when the entry leaves the queue
         */
        void addQueued(final ComputePriority priority, final long delta) {
            this.queuedByPriority[priority.level()].add(delta);
        }
    }

    /**
     * Mutable per consumer counters, limits and distributions.
     */
    private static final class ConsumerState {

        final String consumerId;
        final LongAdder submitted = new LongAdder();
        final LongAdder completed = new LongAdder();
        final LongAdder failed = new LongAdder();
        final LongAdder cancelled = new LongAdder();
        final LongAdder expired = new LongAdder();
        final LongAdder evicted = new LongAdder();
        final LongAdder rejected = new LongAdder();
        final LongAdder[] rejectedByReason = new LongAdder[ComputeRejectedException.Reason.values().length];
        final LongAdder[] cancelledByReason = new LongAdder[ComputeCancelledException.Reason.values().length];
        final LongAdder queued = new LongAdder();
        final LongAdder active = new LongAdder();
        final LatencyHistogram queueWait = new LatencyHistogram();
        final LatencyHistogram execution = new LatencyHistogram();
        final ThroughputMeter throughput;

        volatile boolean policyEnabled = true;
        volatile boolean registered = true;
        volatile int concurrencyLimit;
        volatile int maxQueuedTasks = -1;

        ConsumerState(final String consumerId, final ComputeClock clock, final Duration window) {
            this.consumerId = consumerId;
            for (int i = 0; i < this.rejectedByReason.length; i++) {
                this.rejectedByReason[i] = new LongAdder();
            }
            for (int i = 0; i < this.cancelledByReason.length; i++) {
                this.cancelledByReason[i] = new LongAdder();
            }
            this.throughput = new ThroughputMeter(clock, window);
        }

        /**
         * Applies the limits of a policy snapshot.
         *
         * @param consumer         the consumer
         * @param concurrencyLimit resolved concurrency limit
         */
        void applyPolicy(final ComputeConsumer consumer, final int concurrencyLimit) {
            this.policyEnabled = consumer.policy().enabled();
            this.concurrencyLimit = concurrencyLimit;
            this.maxQueuedTasks = consumer.policy().maxQueuedTasks();
            this.registered = true;
        }

        /**
         * Captures this consumer.
         *
         * @param capturedAt snapshot instant
         * @return the immutable snapshot
         */
        ConsumerMetrics snapshot(final Instant capturedAt) {
            final LatencyStats queueWaitStats = this.queueWait.capture();
            final LatencyStats executionStats = this.execution.capture();
            final Map<ComputeRejectedException.Reason, Long> rejectedReasons = new EnumMap<>(ComputeRejectedException.Reason.class);
            for (final ComputeRejectedException.Reason reason : ComputeRejectedException.Reason.values()) {
                rejectedReasons.put(reason, this.rejectedByReason[reason.ordinal()].sum());
            }
            final Map<ComputeCancelledException.Reason, Long> cancelledReasons = new EnumMap<>(ComputeCancelledException.Reason.class);
            for (final ComputeCancelledException.Reason reason : ComputeCancelledException.Reason.values()) {
                cancelledReasons.put(reason, this.cancelledByReason[reason.ordinal()].sum());
            }
            return new ConsumerMetricsSnapshot(
                capturedAt,
                this.consumerId,
                this.registered && this.policyEnabled,
                (int) Math.max(0L, Math.min(Integer.MAX_VALUE, this.active.sum())),
                (int) Math.max(0L, Math.min(Integer.MAX_VALUE, this.queued.sum())),
                this.concurrencyLimit,
                this.maxQueuedTasks,
                this.submitted.sum(),
                this.completed.sum(),
                this.failed.sum(),
                this.rejected.sum(),
                this.cancelled.sum(),
                this.expired.sum(),
                this.evicted.sum(),
                queueWaitStats.mean(),
                queueWaitStats.max(),
                executionStats.mean(),
                executionStats.max(),
                this.throughput.perSecond(),
                this.throughput.lastSecondPerSecond(),
                rejectedReasons,
                cancelledReasons,
                queueWaitStats,
                executionStats
            );
        }
    }

    /**
     * Per thread marker of the task the calling thread just started.
     */
    private static final class StartedMark {

        long taskId = -1L;
    }
}
