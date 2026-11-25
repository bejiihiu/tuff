package kz.bejiihiu.tuff.compute.internal.adaptive.impl;

import java.time.Duration;
import java.util.Arrays;
import java.util.OptionalDouble;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveConcurrencyController;
import kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveSnapshot;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.CpuUtilizationProbe;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.compute.internal.util.Preconditions;
import org.jspecify.annotations.Nullable;

/**
 * Adaptive concurrency controller driven by queueing signals and bounded AIMD steps.
 *
 * <p>The controller is deliberately small and honest about what it can measure. It never guesses a
 * "round trip time" the way a service side limiter does: the pool runs in one process, so the only
 * meaningful numbers are the sample stream the contract already provides.
 *
 * <h2>Signal</h2>
 *
 * <p>Inside one window the controller aggregates the last {@value #SAMPLE_CAPACITY} finished tasks
 * (failed tasks are dropped, their timing says nothing about capacity) and computes:
 * <ul>
 *     <li>{@code W} — average queue wait, {@code C} — average execution time, {@code W99} — the 99th
 *     percentile of the queue wait;</li>
 *     <li>{@code X} — completions per second over the window, {@code D = X * (W + C)} — demand in
 *     slots by Little's law ({@code L = lambda * W} in queueing notation);</li>
 *     <li>{@code rho = D / limit} — utilisation of the concurrency budget, clamped to
 *     {@code [0, 1]}. A value near {@code 1} means every slot the controller handed out was busy.
 *     </li>
 * </ul>
 *
 * <h2>Growth</h2>
 *
 * <p>The limit may grow only when every one of these holds, because each is a refuting condition on
 * its own:
 * <ul>
 *     <li>the dispatcher observed a non-empty queue in this window — waiting work that a new slot
 *     can actually take;</li>
 *     <li>{@code W <= C} and {@code W99 <= 2 * C} — the queue is short relative to service, so the
 *     pool is not already paying for a queueing backlog;</li>
 *     <li>{@code rho >= targetUtilization} — the existing slots are filled, not merely allocated;</li>
 *     <li>process CPU load, when the runtime reports it, is below {@value #CPU_CEILING} — a busy
 *     process has no headroom, whatever the pool thinks.</li>
 * </ul>
 *
 * <p>The step is additive and bounded:
 * {@code clamp(round(limit * increaseStep), 1, MAX_INCREASE_STEP)}. The lower bound keeps progress
 * possible for a pool of one; the upper bound stops a large configured step from jumping straight
 * into the saturation point. A window is not a decision: after any shrink the controller waits for
 * {@value #GROWTH_WINDOWS_AFTER_SHRINK} consecutive growth-favourable windows, which is the slow
 * start after an idle period.
 *
 * <h2>Shrink</h2>
 *
 * <p>Latency pressure ({@code W >= 2 * C} or {@code W99 >= 4 * C}) shrinks immediately by
 * {@code ceil(limit * (1 - decreaseFactor))}, at least one slot. Idle shrinking is much more
 * conservative: an empty queue and a utilisation below half the target must be observed for
 * {@value #IDLE_WINDOWS_BEFORE_SHRINK} consecutive windows. This is what kills the sawtooth: a load
 * that alternates faster than that never makes the controller chase it.
 *
 * <p>A window with no finished task is normally not enough to move anything, because the contract
 * requires {@code minSamples}. The one exception is a fully idle pool: when the dispatcher reported
 * an empty queue and the engine's busy worker count is exactly zero, that is a direct observation
 * rather than an estimate, so the idle shrink may proceed. Without that exception a quiet hour would
 * leave a pool of sixteen threads parked forever.
 *
 * <h2>Oscillation budget</h2>
 *
 * <ul>
 *     <li>one decision per {@code max(sampleWindow, 1 s)}, so the pool is not poked more than about
 *     once a second;</li>
 *     <li>a dead band between {@code W/C = 1} (do not grow) and {@code W/C = 2} (shrink), so noise
 *     around the boundary produces no movement;</li>
 *     <li>multiplicative decrease with an additive increase, the classic AIMD asymmetry: move away
 *     from saturation fast, probe towards it slowly.</li>
 * </ul>
 *
 * <p>Everything time related goes through {@link ComputeClock}, which makes the whole state machine
 * testable without a single sleep.
 *
 * <h2>Bounds and lifecycle</h2>
 *
 * <p>{@code [minLimit, maxLimit]} is the intersection of the adaptive section and the worker
 * configuration: the floor is {@code max(minConcurrency, workers.minThreads)}, and the ceiling is
 * {@code min(maxConcurrency, workers ceiling)} when a maximum is configured, or the worker ceiling
 * alone when it is {@code -1}. The worker ceiling (not the live worker count) is deliberate: using
 * the live count as an automatic maximum would make every shrink permanent, because the pool could
 * never be told to grow past the size it currently has. An explicit {@code workers.threads} wins over
 * adaptive sizing entirely and pins the limit.
 *
 * <p>The controller owns no thread. {@link #start()} only arms the evaluation window; the engine's
 * timer calls {@link #evaluate()} and pushes the returned number into the worker pool, the
 * concurrency gate and the quota controller. {@link #stop()} freezes the limit where it is.
 */
public final class AimdAdaptiveConcurrencyController implements AdaptiveConcurrencyController {

    /**
     * Upper bound on samples kept per window. More than this only refines the averages, and the p99
     * of a thousand samples is already stable enough for a decision taken once a second.
     */
    private static final int SAMPLE_CAPACITY = 1024;

    /**
     * Longest decision interval the controller accepts. A window shorter than this is raised to it,
     * so a configured one hundred millisecond window cannot resize the pool ten times a second.
     */
    private static final long MINIMUM_DECISION_INTERVAL = 1_000_000_000L;

    /**
     * Samples longer than this are clamped. A task that ran for an hour (stuck, cancelled late) must
     * not drag the average of a one second window into nonsense.
     */
    private static final long MAX_SAMPLE_LENGTH = Duration.ofHours(1).toNanos();

    /**
     * Largest additive growth step in workers. Keeps a big step from overshooting a small machine.
     */
    private static final int MAX_INCREASE_STEP = 4;

    /**
     * {@code W / C} at or below which the queue is considered short enough to grow.
     */
    private static final double GROWTH_WAIT_RATIO = 1.0d;

    /**
     * {@code W99 / C} at or below which the tail is considered healthy during growth.
     */
    private static final double GROWTH_TAIL_RATIO = 2.0d;

    /**
     * {@code W / C} at or above which the queue is considered to be hurting and the limit shrinks.
     */
    private static final double SHRINK_WAIT_RATIO = 2.0d;

    /**
     * {@code W99 / C} at or above which the tail alone justifies a shrink.
     */
    private static final double SHRINK_TAIL_RATIO = 4.0d;

    /**
     * Idle shrink needs utilisation below {@code targetUtilization * this}.
     */
    private static final double IDLE_UTILIZATION_FACTOR = 0.5d;

    /**
     * Consecutive idle windows required before the controller believes the pool is really idle.
     */
    private static final int IDLE_WINDOWS_BEFORE_SHRINK = 3;

    /**
     * Consecutive growth-favourable windows required after a shrink.
     */
    private static final int GROWTH_WINDOWS_AFTER_SHRINK = 2;

    /**
     * Process CPU load at or above which growth is refused. This is the whole-process figure from
     * {@link CpuUtilizationProbe}, so it also accounts for tick threads, GC and JIT; the pool's own
     * utilisation can be low while the machine is already full.
     */
    private static final double CPU_CEILING = 0.9d;

    private final ComputeClock clock;
    private final boolean adaptiveEnabled;
    private final boolean workerCountAutomatic;
    private final int minLimit;
    private final int maxLimit;
    private final long windowNanos;
    private final long decisionIntervalNanos;
    private final int minSamples;
    private final double increaseStep;
    private final double decreaseFactor;
    private final double targetUtilization;
    private final boolean jfrEnabled;
    private final @Nullable IntSupplier activeWorkersSupplier;
    private final Supplier<OptionalDouble> cpuLoadSupplier;

    private final AtomicLongArray queueWaitSamples = new AtomicLongArray(SAMPLE_CAPACITY);
    private final AtomicLongArray executionSamples = new AtomicLongArray(SAMPLE_CAPACITY);
    private final AtomicLong sampleSequence = new AtomicLong();
    private final AtomicLong failedSamples = new AtomicLong();
    private final AtomicInteger maxQueued = new AtomicInteger();
    private final AtomicInteger lastCapacity = new AtomicInteger();
    private final AtomicInteger queueSeen = new AtomicInteger();
    private final ReentrantLock decisionLock = new ReentrantLock();

    private volatile int current;
    private volatile int desired;
    private volatile boolean running;
    private volatile long windowStartNanos;
    private volatile long windowBaseSequence;
    private volatile int windowSampleCount;
    private volatile long windowAverageWait;
    private volatile long windowAverageExecution;
    private volatile long windowP99Wait;
    private volatile double windowUtilization;
    private volatile double windowThroughput;
    private volatile DecisionReason lastReason = DecisionReason.NOT_STARTED;
    private volatile int idleWindows;
    private volatile int growthWindows;
    private volatile boolean cautiousGrowth;

    /**
     * Creates a controller for the current JVM.
     *
     * <p>The process CPU load is sampled through {@link CpuUtilizationProbe}; busy worker counts are
     * not known to this constructor, so idle shrinking relies on observed samples only.
     *
     * @param settings     scheduler settings, adaptive and worker sections are used
     * @param clock        clock used for windows and cooldowns
     * @param initialLimit concurrency the pool starts with, at least {@code 1}
     */
    public AimdAdaptiveConcurrencyController(
        final ComputeSettings settings,
        final ComputeClock clock,
        final int initialLimit
    ) {
        this(settings, clock, initialLimit, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Creates a controller with an explicit processor count, for deterministic tests and runtimes
     * that override {@code ActiveProcessorCount}.
     *
     * @param settings            scheduler settings, adaptive and worker sections are used
     * @param clock               clock used for windows and cooldowns
     * @param initialLimit        concurrency the pool starts with, at least {@code 1}
     * @param availableProcessors processors available to the JVM, at least {@code 1}
     */
    public AimdAdaptiveConcurrencyController(
        final ComputeSettings settings,
        final ComputeClock clock,
        final int initialLimit,
        final int availableProcessors
    ) {
        this(settings, clock, initialLimit, availableProcessors, null, CpuUtilizationProbe::sample);
    }

    /**
     * Creates a controller with every external signal replaceable.
     *
     * <p>The two suppliers are the bridge to the worker pool, which the controller must not know:
     * {@code activeWorkers} returns the number of workers currently inside a task, and
     * {@code cpuLoad} returns the process CPU load estimate. Both may return "unknown" (a negative
     * count, an empty optional) and neither is allowed to break the decision, so exceptions are
     * treated as unknown rather than propagated.
     *
     * @param settings            scheduler settings, adaptive and worker sections are used
     * @param clock               clock used for windows and cooldowns
     * @param initialLimit        concurrency the pool starts with, at least {@code 1}
     * @param availableProcessors processors available to the JVM, at least {@code 1}
     * @param activeWorkers       supplier of the busy worker count, or {@code null} when unknown
     * @param cpuLoad             supplier of the process CPU load in {@code [0.0, 1.0]}
     */
    public AimdAdaptiveConcurrencyController(
        final ComputeSettings settings,
        final ComputeClock clock,
        final int initialLimit,
        final int availableProcessors,
        final @Nullable IntSupplier activeWorkers,
        final Supplier<OptionalDouble> cpuLoad
    ) {
        Preconditions.requireNonNull(settings, "settings");
        this.clock = Preconditions.requireNonNull(clock, "clock");
        Preconditions.checkAtLeast(initialLimit, 1, "initialLimit");
        Preconditions.checkAtLeast(availableProcessors, 1, "availableProcessors");
        this.activeWorkersSupplier = activeWorkers;
        this.cpuLoadSupplier = Preconditions.requireNonNull(cpuLoad, "cpuLoad");

        final ComputeSettings.Adaptive adaptive = settings.adaptive();
        this.adaptiveEnabled = adaptive.enabled();
        this.workerCountAutomatic = settings.workers().automaticThreadCount();
        this.windowNanos = adaptive.sampleWindow().toNanos();
        this.decisionIntervalNanos = Math.max(this.windowNanos, MINIMUM_DECISION_INTERVAL);
        this.minSamples = adaptive.minSamples();
        this.increaseStep = adaptive.increaseStep();
        this.decreaseFactor = adaptive.decreaseFactor();
        this.targetUtilization = adaptive.targetUtilization();
        this.jfrEnabled = settings.metrics().jfrEnabled();

        final int workerCeiling = this.workerCountAutomatic
            ? settings.workers().resolveMaxThreads(availableProcessors)
            : settings.workers().threads();
        final int configuredMax = adaptive.maxConcurrency();
        final int ceiling = configuredMax == -1 ? workerCeiling : Math.min(configuredMax, workerCeiling);
        this.maxLimit = Math.max(1, ceiling);
        this.minLimit = Math.clamp(
            Math.max(adaptive.minConcurrency(), settings.workers().minThreads()),
            1,
            this.maxLimit);

        final int start = Math.clamp(initialLimit, this.minLimit, this.maxLimit);
        this.current = start;
        this.desired = start;
        this.windowStartNanos = this.clock.nanoTime();
        this.windowBaseSequence = this.sampleSequence.get();
    }

    @Override
    public boolean enabled() {
        return this.adaptiveEnabled;
    }

    @Override
    public void start() {
        this.decisionLock.lock();
        try {
            if (this.running) {
                return;
            }
            this.running = true;
            this.windowStartNanos = this.clock.nanoTime();
            this.windowBaseSequence = this.sampleSequence.get();
            this.maxQueued.set(0);
            this.queueSeen.set(0);
            this.idleWindows = 0;
            this.growthWindows = 0;
            this.cautiousGrowth = false;
            this.lastReason = DecisionReason.NOT_STARTED;
        } finally {
            this.decisionLock.unlock();
        }
    }

    @Override
    public void stop() {
        this.decisionLock.lock();
        try {
            this.running = false;
            this.lastReason = DecisionReason.NOT_STARTED;
        } finally {
            this.decisionLock.unlock();
        }
    }

    @Override
    public int currentLimit() {
        return this.current;
    }

    @Override
    public int desiredLimit() {
        return this.desired;
    }

    @Override
    public int minLimit() {
        return this.minLimit;
    }

    @Override
    public int maxLimit() {
        return this.maxLimit;
    }

    @Override
    public void recordSample(final long queueWaitNanos, final long executionNanos, final boolean failed) {
        if (failed) {
            this.failedSamples.incrementAndGet();
            return;
        }
        final long wait = Math.clamp(queueWaitNanos, 0L, MAX_SAMPLE_LENGTH);
        final long execution = Math.clamp(executionNanos, 0L, MAX_SAMPLE_LENGTH);
        final long sequence = this.sampleSequence.getAndIncrement();
        final int slot = (int) (sequence % SAMPLE_CAPACITY);
        this.queueWaitSamples.set(slot, wait);
        this.executionSamples.set(slot, execution);
    }

    @Override
    public void onQueueState(final int queuedTasks, final int capacity) {
        final int queued = Math.max(0, queuedTasks);
        this.maxQueued.accumulateAndGet(queued, Math::max);
        this.lastCapacity.set(Math.max(0, capacity));
        this.queueSeen.set(1);
    }

    @Override
    public int evaluate() {
        this.decisionLock.lock();
        try {
            if (!this.adaptiveEnabled) {
                return this.hold(DecisionReason.DISABLED);
            }
            if (!this.running) {
                return this.hold(DecisionReason.NOT_STARTED);
            }
            final long now = this.clock.nanoTime();
            if (now - this.windowStartNanos < this.decisionIntervalNanos) {
                return this.hold(DecisionReason.COOLDOWN);
            }

            final Window window = this.collectWindow(now);
            try {
                return this.decide(window);
            } finally {
                this.emitDecision(this.lastReason);
            }
        } finally {
            this.decisionLock.unlock();
        }
    }

    private int decide(final Window window) {
        if (!this.workerCountAutomatic) {
            return this.hold(DecisionReason.EXPLICIT_WORKER_COUNT);
        }

        final int active = this.activeWorkers();
        final boolean idle = this.isIdleWindow(window, active);
        this.idleWindows = idle ? this.idleWindows + 1 : 0;

        if (window.totalSamples() < this.minSamples) {
            if (idle && this.idleWindows >= IDLE_WINDOWS_BEFORE_SHRINK) {
                if (!this.shrink(DecisionReason.SHRINK_IDLE)) {
                    this.hold(DecisionReason.AT_MINIMUM);
                }
            } else {
                this.hold(idle ? DecisionReason.HOLD_IDLE_PENDING : DecisionReason.INSUFFICIENT_SAMPLES);
            }
            return this.current;
        }

        final double waitRatio = ratio(window.averageWait(), window.averageExecution());
        final double tailRatio = ratio(window.p99Wait(), window.averageExecution());
        final boolean latencyPressure =
            waitRatio >= SHRINK_WAIT_RATIO || tailRatio >= SHRINK_TAIL_RATIO;

        if (latencyPressure) {
            if (!this.shrink(DecisionReason.SHRINK_WAIT)) {
                this.hold(DecisionReason.AT_MINIMUM);
            }
            return this.current;
        }

        if (idle) {
            if (this.idleWindows >= IDLE_WINDOWS_BEFORE_SHRINK) {
                if (!this.shrink(DecisionReason.SHRINK_IDLE)) {
                    this.hold(DecisionReason.AT_MINIMUM);
                }
            } else {
                this.hold(DecisionReason.HOLD_IDLE_PENDING);
            }
            return this.current;
        }

        this.evaluateGrowth(window);
        return this.current;
    }

    @Override
    public AdaptiveSnapshot snapshot() {
        return new AdaptiveSnapshot(
            this.clock.instant(),
            this.adaptiveEnabled,
            this.current,
            this.desired,
            this.minLimit,
            this.maxLimit,
            this.windowSampleCount,
            Duration.ofNanos(this.windowAverageWait),
            Duration.ofNanos(this.windowAverageExecution),
            this.windowUtilization);
    }

    @Override
    public Duration sampleWindow() {
        return Duration.ofNanos(this.windowNanos);
    }

    /**
     * Returns why the last evaluation did or did not move the limit.
     *
     * <p>The reason is also emitted in the {@code tuff.compute.AdaptiveDecision} JFR event when
     * metrics and the flight recorder are enabled. This accessor exists for logs, tests and the
     * engine's own diagnostics.
     *
     * @return the last decision reason, never {@code null}
     */
    public DecisionReason lastDecisionReason() {
        return this.lastReason;
    }

    /**
     * Returns the 99th percentile queue wait the last window saw.
     *
     * @return nanoseconds, zero before the first decision
     */
    public long lastP99QueueWaitNanos() {
        return this.windowP99Wait;
    }

    /**
     * Returns the completion rate the last window measured.
     *
     * @return completed tasks per second, zero before the first decision
     */
    public double lastThroughputPerSecond() {
        return this.windowThroughput;
    }

    /**
     * Returns the queue capacity of the last queue state observation.
     *
     * @return the capacity, zero when the dispatcher never reported one
     */
    public int lastQueueCapacity() {
        return this.lastCapacity.get();
    }

    /**
     * Returns the number of failed tasks the controller dropped since construction.
     *
     * @return failed samples, never negative
     */
    public long failedSampleCount() {
        return this.failedSamples.get();
    }

    private int hold(final DecisionReason reason) {
        this.lastReason = reason;
        return this.current;
    }

    private Window collectWindow(final long now) {
        final long sequenceEnd = this.sampleSequence.get();
        final long sequenceBase = this.windowBaseSequence;
        final long totalSamples = Math.max(0L, sequenceEnd - sequenceBase);
        final long first = Math.max(sequenceBase, sequenceEnd - SAMPLE_CAPACITY);
        final int counted = (int) Math.max(0L, sequenceEnd - first);

        final long[] waits = new long[counted];
        long waitSum = 0L;
        long executionSum = 0L;
        for (int i = 0; i < counted; i++) {
            final int slot = (int) ((first + i) % SAMPLE_CAPACITY);
            waits[i] = this.queueWaitSamples.get(slot);
            waitSum += waits[i];
            executionSum += this.executionSamples.get(slot);
        }
        Arrays.sort(waits);

        final long averageWait = counted == 0 ? 0L : waitSum / counted;
        final long averageExecution = counted == 0 ? 0L : executionSum / counted;
        final long p99Wait = counted == 0 ? 0L : waits[percentileIndex(counted)];

        final int observedQueued = this.maxQueued.getAndSet(0);
        final boolean queueWasSeen = this.queueSeen.getAndSet(0) != 0;
        final long elapsed = Math.max(1L, now - this.windowStartNanos);
        final double throughput = totalSamples * 1_000_000_000.0d / elapsed;
        final double demand = throughput * (averageWait + (double) averageExecution) / 1_000_000_000.0d;
        final double utilization = counted == 0 ? 0.0d : clamp01(demand / this.current);

        this.windowSampleCount = counted;
        this.windowAverageWait = averageWait;
        this.windowAverageExecution = averageExecution;
        this.windowP99Wait = p99Wait;
        this.windowThroughput = throughput;
        this.windowUtilization = utilization;
        this.windowBaseSequence = sequenceEnd;
        this.windowStartNanos = now;

        return new Window(totalSamples, averageWait, averageExecution, p99Wait, observedQueued, queueWasSeen, utilization);
    }

    private void evaluateGrowth(final Window window) {
        final boolean backlog = window.queueSeen() && window.maxQueued() > 0;
        final boolean waitHealthy = ratio(window.averageWait(), window.averageExecution()) <= GROWTH_WAIT_RATIO
            && ratio(window.p99Wait(), window.averageExecution()) <= GROWTH_TAIL_RATIO;
        final boolean loaded = window.utilization() >= this.targetUtilization;
        final boolean cpuExhausted = this.cpuBudgetExhausted();

        if (!backlog || !waitHealthy || !loaded) {
            this.growthWindows = 0;
            this.hold(DecisionReason.HOLD_STABLE);
            return;
        }
        if (cpuExhausted) {
            this.growthWindows = 0;
            this.hold(DecisionReason.HOLD_CPU_BUDGET);
            return;
        }
        if (this.current >= this.maxLimit) {
            this.growthWindows = 0;
            this.hold(DecisionReason.AT_MAXIMUM);
            return;
        }

        this.growthWindows++;
        final int required = this.cautiousGrowth ? GROWTH_WINDOWS_AFTER_SHRINK : 1;
        if (this.growthWindows < required) {
            this.hold(DecisionReason.HOLD_CAUTIOUS);
            return;
        }
        this.grow();
    }

    private void grow() {
        final int step = Math.clamp((int) Math.round(this.current * this.increaseStep), 1, MAX_INCREASE_STEP);
        final int next = Math.min(this.maxLimit, this.current + step);
        this.apply(next, DecisionReason.GROW_BACKLOG);
        this.growthWindows = 0;
        this.cautiousGrowth = false;
    }

    private boolean shrink(final DecisionReason reason) {
        if (this.current <= this.minLimit) {
            return false;
        }
        final int decrease = Math.max(1, (int) Math.ceil(this.current * (1.0d - this.decreaseFactor)));
        final int next = Math.max(this.minLimit, this.current - decrease);
        this.apply(next, reason);
        this.cautiousGrowth = true;
        this.growthWindows = 0;
        return true;
    }

    private void apply(final int limit, final DecisionReason reason) {
        this.current = limit;
        this.desired = limit;
        this.lastReason = reason;
        this.emitDecision(reason);
    }

    private boolean isIdleWindow(final Window window, final int active) {
        if (!window.queueSeen() || window.maxQueued() > 0) {
            return false;
        }
        if (window.utilization() >= this.targetUtilization * IDLE_UTILIZATION_FACTOR) {
            return false;
        }
        if (window.totalSamples() == 0) {
            // no tasks finished: only a direct "nothing is running" observation proves idleness
            return active == 0;
        }
        return active < 0 || active <= Math.max(1, this.current / 2);
    }

    private boolean cpuBudgetExhausted() {
        final OptionalDouble sample;
        try {
            sample = this.cpuLoadSupplier.get();
        } catch (RuntimeException _) {
            return false;
        }
        if (sample == null || sample.isEmpty()) {
            return false;
        }
        final double load = sample.getAsDouble();
        return Double.isFinite(load) && load >= CPU_CEILING;
    }

    /**
     * Returns the busy worker count, or {@code -1} when the engine did not supply a source.
     *
     * @return busy workers, negative when unknown
     */
    private int activeWorkers() {
        if (this.activeWorkersSupplier == null) {
            return -1;
        }
        try {
            return Math.max(-1, this.activeWorkersSupplier.getAsInt());
        } catch (RuntimeException _) {
            return -1;
        }
    }

    private void emitDecision(final DecisionReason reason) {
        if (!this.jfrEnabled) {
            return;
        }
        try {
            final AdaptiveDecisionEvent event = new AdaptiveDecisionEvent();
            event.currentLimit = this.current;
            event.desiredLimit = this.desired;
            event.minLimit = this.minLimit;
            event.maxLimit = this.maxLimit;
            event.samples = this.windowSampleCount;
            event.averageQueueWaitNanos = this.windowAverageWait;
            event.averageExecutionNanos = this.windowAverageExecution;
            event.p99QueueWaitNanos = this.windowP99Wait;
            event.utilization = this.windowUtilization;
            event.reason = reason.name();
            event.commit();
        } catch (RuntimeException | LinkageError _) {
            // a profiling hook must never break the decision loop
        }
    }

    private static double ratio(final long numerator, final long denominator) {
        return numerator / (double) Math.max(1L, denominator);
    }

    private static double clamp01(final double value) {
        if (!Double.isFinite(value)) {
            return 0.0d;
        }
        return Math.clamp(value, 0.0d, 1.0d);
    }

    private static int percentileIndex(final int count) {
        final int index = (int) Math.ceil(count * 0.99d) - 1;
        return Math.clamp(index, 0, count - 1);
    }

    /**
     * What the last evaluation did, and why.
     */
    public enum DecisionReason {

        /**
         * Adaptive concurrency is disabled in the configuration.
         */
        DISABLED,

        /**
         * The controller was never started, or was stopped.
         */
        NOT_STARTED,

        /**
         * The evaluation window has not elapsed yet.
         */
        COOLDOWN,

        /**
         * Fewer finished tasks than {@code minSamples} arrived in the window.
         */
        INSUFFICIENT_SAMPLES,

        /**
         * The pool has an explicit fixed worker count, so the controller does not move it.
         */
        EXPLICIT_WORKER_COUNT,

        /**
         * Growing because of a backlog with short waits, filled slots and CPU headroom.
         */
        GROW_BACKLOG,

        /**
         * Shrinking because the queue wait grew relative to service time.
         */
        SHRINK_WAIT,

        /**
         * Shrinking because the queue stayed empty and the pool stayed idle.
         */
        SHRINK_IDLE,

        /**
         * Holding: the window was idle but not for long enough yet.
         */
        HOLD_IDLE_PENDING,

        /**
         * Holding: growth conditions look right but the post-shrink slow start is not over.
         */
        HOLD_CAUTIOUS,

        /**
         * Holding: no backlog and no reason to move.
         */
        HOLD_STABLE,

        /**
         * Holding: a backlog exists but the process has no CPU headroom.
         */
        HOLD_CPU_BUDGET,

        /**
         * Holding at the configured maximum.
         */
        AT_MAXIMUM,

        /**
         * Holding at the configured minimum.
         */
        AT_MINIMUM
    }

    /**
     * Aggregated view of one evaluation window.
     *
     * @param totalSamples     finished tasks since the previous decision, before ring truncation
     * @param averageWait      average queue wait of the samples still in the ring
     * @param averageExecution average execution time of the samples still in the ring
     * @param p99Wait          99th percentile queue wait of the samples still in the ring
     * @param maxQueued        deepest queue the dispatcher reported in the window
     * @param queueSeen        whether the dispatcher reported a queue depth at all
     * @param utilization      Little's law utilisation of the current limit, in {@code [0, 1]}
     */
    private record Window(
        long totalSamples,
        long averageWait,
        long averageExecution,
        long p99Wait,
        int maxQueued,
        boolean queueSeen,
        double utilization
    ) {
    }
}
