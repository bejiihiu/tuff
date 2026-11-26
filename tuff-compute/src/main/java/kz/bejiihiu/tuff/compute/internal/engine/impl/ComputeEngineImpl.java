package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeRejectedException;
import kz.bejiihiu.tuff.compute.ComputeRequest;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeShutdownException;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.internal.adaptive.AdaptiveConcurrencyController;
import kz.bejiihiu.tuff.compute.internal.adaptive.impl.AimdAdaptiveConcurrencyController;
import kz.bejiihiu.tuff.compute.internal.engine.ComputeEngine;
import kz.bejiihiu.tuff.compute.internal.metrics.JfrRecorder;
import kz.bejiihiu.tuff.compute.internal.metrics.MetricsCollector;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.ComputeMetricsCollector;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.CpuUtilizationProbe;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.FlightJfrRecorder;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.LatencyStats;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.SchedulerMetricsSnapshot;
import kz.bejiihiu.tuff.compute.internal.queue.ComputeQueue;
import kz.bejiihiu.tuff.compute.internal.queue.LaneAwareQueue;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.queue.QuotaController;
import kz.bejiihiu.tuff.compute.internal.queue.impl.AtomicQuotaController;
import kz.bejiihiu.tuff.compute.internal.queue.impl.BoundedComputeQueue;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.compute.internal.util.Durations;
import kz.bejiihiu.tuff.compute.internal.util.Preconditions;
import kz.bejiihiu.tuff.compute.internal.util.ThreadFactories;
import kz.bejiihiu.tuff.compute.internal.worker.impl.DynamicWorkerPool;
import org.jspecify.annotations.Nullable;

/**
 * The engine: lifecycle, submission validation, handle ownership and the housekeeping wiring.
 *
 * <p>The class is the only place that knows every component. It creates the queue and the quota
 * controller eagerly (they are thread free and may be used by a {@code NEW} scheduler for consumer
 * registration), and it defers everything that owns a thread to {@link #start()}:
 *
 * <ul>
 *     <li>metrics collector (it starts its own sampler thread in its constructor),</li>
 *     <li>worker pool,</li>
 *     <li>queue janitor,</li>
 *     <li>adaptive evaluation timer.</li>
 * </ul>
 *
 * <p>That is what makes "a {@code NEW} engine has no threads" true even with metrics enabled, and
 * what makes "a {@code TERMINATED} engine has no threads" checkable by thread name.
 *
 * <p>Terminal ownership invariants:
 * <ul>
 *     <li>only {@link TaskHandle} transitions a task into a terminal state and completes its
 *     future;</li>
 *     <li>only the worker that received an entry from {@link DispatcherImpl#next(int)} releases that
 *     entry's consumer quota slot;</li>
 *     <li>a queued entry never held a slot, so cancellation, expiry, eviction, consumer removal and
 *     shutdown never release one;</li>
 *     <li>exactly one metrics terminal event per accepted task: the caller that won the handle's
 *     terminal compare and set emits it.</li>
 * </ul>
 */
final class ComputeEngineImpl implements ComputeEngine, TaskHandle.Owner {

    private static final System.Logger LOG = System.getLogger(ComputeEngineImpl.class.getName());

    /**
     * Janitor tick. Kept small so queue expiry is real even without adaptive concurrency; the gauge
     * sampling cadence is the configured metrics sample interval, not this value.
     */
    private static final long JANITOR_TICK_MILLIS = 100L;

    /**
     * Extra time granted on top of the configured grace period for housekeeping to stop. Bounded on
     * purpose: shutdown must reach {@code TERMINATED} even when a component is wedged.
     */
    private static final long SHUTDOWN_SLACK_NANOS = TimeUnit.SECONDS.toNanos(2L);

    /**
     * Upper bound for stopping the janitor thread.
     */
    private static final Duration JANITOR_STOP_TIMEOUT = Duration.ofSeconds(2L);

    private final ComputeSettings settings;
    private final ComputeClock clock;

    /**
     * Processors the engine may use. The platform budget (not the raw core count) is applied here;
     * {@link #applyProcessorBudget(int)} updates it at runtime.
     */
    private volatile int processorBudget;

    /**
     * Hard ceiling of the pool for the current processor budget. Not frozen at construction: an
     * automatically sized pool resolves its ceiling from the budget every time the budget changes
     * ({@code maxThreads == -1} means "as many as the budget allows"), while an explicit
     * {@code maxThreads} stays the ceiling and is only clamped by the budget.
     */
    private volatile int workerCeiling;

    /**
     * Serializes every writer of the pool size: submission driven growth, janitor autoscaling,
     * adaptive decisions and budget changes. The pool size, its concurrency gate and the quota
     * controller's total limit are moved together inside this lock, so a limit can never be left
     * behind on either side of a resize.
     */
    private final Object sizingLock = new Object();

    private final ComputeQueue queue;
    private final QuotaController quota;

    /**
     * Priority levels the platform pool may take. LOW is excluded while the virtual lane owns it, so
     * platform sizing and adaptive signals are never driven by work the pool cannot run.
     */
    private final int platformLevelMask;
    private final ConsumerRegistryImpl registry = new ConsumerRegistryImpl();
    private final Map<Long, TaskHandle<?>> handles = new ConcurrentHashMap<>();
    private final Set<String> unregisteredConsumers = ConcurrentHashMap.newKeySet();

    private final AtomicLong taskIds = new AtomicLong();
    private final AtomicLong sequences = new AtomicLong();
    private final AtomicBoolean growInFlight = new AtomicBoolean();
    private final AtomicInteger appliedLimit = new AtomicInteger();

    private final Object lifecycleLock = new Object();
    private final AtomicReference<AsyncComputeScheduler.SchedulerState> state =
        new AtomicReference<>(AsyncComputeScheduler.SchedulerState.NEW);
    private final CountDownLatch terminated = new CountDownLatch(1);

    private volatile @Nullable MetricsCollector metricsCollector;
    private volatile @Nullable DispatcherImpl dispatcher;
    private volatile @Nullable DynamicWorkerPool pool;
    private volatile @Nullable VirtualLane virtualLane;
    private volatile @Nullable QueueJanitor janitor;
    private volatile @Nullable AdaptiveConcurrencyController adaptive;
    private volatile @Nullable ScheduledExecutorService adaptiveTimer;
    private volatile @Nullable ExecutorService completionExecutor;
    private volatile @Nullable Thread shutdownThread;

    /**
     * Creates an engine. No thread is created by this constructor.
     *
     * @param settings scheduler configuration
     * @param clock    clock used by every component
     */
    ComputeEngineImpl(final ComputeSettings settings, final ComputeClock clock) {
        this(settings, clock, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Creates an engine with an explicit processor budget.
     *
     * <p>The value is a budget, not the raw core count: the platform already subtracted its region
     * tick threads, chunk workers, IO threads and headroom. Tests use this overload to make worker
     * sizing and adaptive bounds deterministic; the server passes the resolved budget (or the JVM
     * processor count before the real budget is known, then calls
     * {@link #applyProcessorBudget(int)}).
     *
     * @param settings        scheduler configuration
     * @param clock           clock used by every component
     * @param processorBudget processors available to the engine, at least {@code 1}
     */
    ComputeEngineImpl(final ComputeSettings settings, final ComputeClock clock, final int processorBudget) {
        this.settings = Preconditions.requireNonNull(settings, "settings");
        this.clock = Preconditions.requireNonNull(clock, "clock");
        Preconditions.checkAtLeast(processorBudget, 1, "processorBudget");
        this.processorBudget = processorBudget;
        this.workerCeiling = this.ceilingFor(processorBudget);
        final int initialConcurrency = Math.max(1, settings.workers().resolveThreadCount(processorBudget));
        this.platformLevelMask = settings.lanes().virtual().servesLow() ? 0b1110 : 0b1111;
        this.queue = new BoundedComputeQueue(settings, clock);
        this.quota = new AtomicQuotaController(initialConcurrency);
        this.queue.setEvictionListener(this::onEvicted);
    }

    @Override
    public void start() {
        synchronized (this.lifecycleLock) {
            final AsyncComputeScheduler.SchedulerState current = this.state.get();
            if (current == AsyncComputeScheduler.SchedulerState.TERMINATED) {
                throw new ComputeShutdownException("compute scheduler is terminated and cannot be restarted");
            }
            if (current != AsyncComputeScheduler.SchedulerState.NEW) {
                return;
            }
            try {
                this.registerConfiguredConsumers();
                if (!this.settings.enabled()) {
                    this.state.set(AsyncComputeScheduler.SchedulerState.RUNNING);
                    return;
                }
                this.wireRuntime();
                // publish RUNNING only after the whole runtime exists, so a concurrent submission
                // can never observe a running scheduler with a half built metrics collector
                this.state.set(AsyncComputeScheduler.SchedulerState.RUNNING);
                this.startRuntime();
            } catch (RuntimeException | Error failure) {
                this.abortStart();
                throw failure;
            }
        }
    }

    @Override
    public void shutdown(final Duration gracePeriod) {
        Preconditions.requireNonNull(gracePeriod, "gracePeriod");
        Preconditions.checkNonNegative(Durations.toNanosSaturated(gracePeriod), "gracePeriod");
        this.beginShutdown(false, gracePeriod);
    }

    @Override
    public void shutdownNow() {
        this.beginShutdown(true, Duration.ZERO);
    }

    @Override
    public boolean awaitTermination(final Duration timeout) throws InterruptedException {
        Preconditions.requireNonNull(timeout, "timeout");
        Preconditions.checkNonNegative(Durations.toNanosSaturated(timeout), "timeout");
        final long timeoutNanos = Durations.toNanosSaturated(timeout);
        final boolean inTime = this.terminated.await(timeoutNanos, TimeUnit.NANOSECONDS);
        if (inTime) {
            final Thread thread = this.shutdownThread;
            if (thread != null && thread != Thread.currentThread()) {
                thread.join();
            }
        }
        return this.state.get() == AsyncComputeScheduler.SchedulerState.TERMINATED;
    }

    @Override
    public AsyncComputeScheduler.SchedulerState state() {
        return this.state.get();
    }

    @Override
    public <T> ComputeTaskHandle<T> enqueue(final ComputeRequest<T> request) {
        Preconditions.requireNonNull(request, "request");
        final AsyncComputeScheduler.SchedulerState current = this.state.get();
        if (current != AsyncComputeScheduler.SchedulerState.RUNNING) {
            throw new ComputeShutdownException("compute scheduler is not running (state=" + current + ')');
        }
        if (!this.settings.enabled()) {
            throw this.reject(request.consumerId(), request.priority(), ComputeRejectedException.Reason.SCHEDULER_DISABLED,
                "compute scheduler is disabled by configuration");
        }
        final ComputeConsumer consumer = this.resolveConsumer(request.consumerId());
        if (!consumer.policy().enabled()) {
            throw this.reject(request.consumerId(), request.priority(), ComputeRejectedException.Reason.CONSUMER_DISABLED,
                "consumer " + consumer.id() + " is disabled");
        }
        if (!consumer.allowedTaskTypes().contains(request.taskType())) {
            throw this.reject(request.consumerId(), request.priority(), ComputeRejectedException.Reason.TASK_TYPE_NOT_ALLOWED,
                "consumer " + consumer.id() + " does not allow task type " + request.taskType());
        }
        if (request.priority().outranks(consumer.maxPriority())) {
            throw this.reject(request.consumerId(), request.priority(), ComputeRejectedException.Reason.PRIORITY_NOT_ALLOWED,
                "consumer " + consumer.id() + " may not submit priority " + request.priority());
        }
        return this.accept(consumer.id(), request);
    }

    @Override
    public boolean cancel(final long taskId) {
        final TaskHandle<?> handle = this.handles.get(taskId);
        return handle != null && handle.cancel();
    }

    @Override
    public ComputeConsumer registerConsumer(final ComputeConsumer consumer) {
        Preconditions.requireNonNull(consumer, "consumer");
        if (this.state.get() == AsyncComputeScheduler.SchedulerState.TERMINATED) {
            throw new ComputeShutdownException("compute scheduler is terminated");
        }
        this.unregisteredConsumers.remove(consumer.id());
        // register the queue stream and the quota before the registry becomes visible: a submission
        // that resolves the consumer from the registry must find a stream to offer into
        this.queue.registerConsumer(consumer.id(), consumer.policy());
        this.quota.updateConsumerPolicy(consumer.id(), consumer.policy());
        final ComputeConsumer stored = this.registry.register(consumer);
        final MetricsCollector collector = this.metricsCollector;
        if (collector != null) {
            collector.onConsumerRegistered(stored, this.quota.limit(stored.id()));
        }
        return stored;
    }

    @Override
    public boolean unregisterConsumer(final String consumerId) {
        Preconditions.requireNonBlank(consumerId, "consumerId");
        final Optional<ComputeConsumer> removed = this.registry.unregister(consumerId);
        if (removed.isEmpty()) {
            return false;
        }
        // queued work has not started, so it can still be cancelled; work that is already running is
        // deliberately left alone and completes with its result (workers are never interrupted)
        for (final QueueEntry entry : this.queue.unregisterConsumer(consumerId)) {
            this.cancelQueued(entry, ComputeCancelledException.Reason.CONSUMER_DISABLED);
        }
        final DispatcherImpl currentDispatcher = this.dispatcher;
        if (currentDispatcher != null) {
            currentDispatcher.cancelCarriedConsumer(consumerId, ComputeCancelledException.Reason.CONSUMER_DISABLED);
        }
        this.unregisteredConsumers.add(consumerId);
        this.removeQuotaIfIdle(consumerId);
        final MetricsCollector collector = this.metricsCollector;
        if (collector != null) {
            collector.onConsumerUnregistered(consumerId);
        }
        return true;
    }

    @Override
    public void updateConsumerPolicy(final String consumerId, final ConsumerPolicy policy) {
        Preconditions.requireNonBlank(consumerId, "consumerId");
        Preconditions.requireNonNull(policy, "policy");
        final ComputeConsumer updated = this.registry.updatePolicy(consumerId, policy);
        this.queue.updatePolicy(consumerId, policy);
        this.quota.updateConsumerPolicy(consumerId, policy);
        final MetricsCollector collector = this.metricsCollector;
        if (collector != null) {
            collector.onConsumerPolicyUpdated(updated, this.quota.limit(consumerId));
        }
    }

    @Override
    public Optional<ComputeConsumer> consumer(final String consumerId) {
        return this.registry.find(consumerId);
    }

    @Override
    public Set<String> consumerIds() {
        return this.registry.ids();
    }

    @Override
    public SchedulerMetrics metrics() {
        final MetricsCollector collector = this.metricsCollector;
        if (collector != null) {
            return collector.schedulerMetrics();
        }
        return this.emptyMetrics();
    }

    @Override
    public ComputeSettings settings() {
        return this.settings;
    }

    @Override
    public ComputeClock clock() {
        return this.clock;
    }

    @Override
    public boolean requestCancel(final TaskHandle<?> handle) {
        final QueueEntry entry = handle.entry();
        if (entry == null) {
            return false;
        }
        if (this.queue.remove(entry)) {
            this.cancelQueued(entry, handle.cancelReason());
            return true;
        }
        final DispatcherImpl currentDispatcher = this.dispatcher;
        if (currentDispatcher != null && currentDispatcher.removeCarried(entry)) {
            this.cancelQueued(entry, handle.cancelReason());
            return true;
        }
        final ComputeTaskHandle.State taskState = handle.state();
        // a claimed entry is owned by a worker; the flag makes it terminalize itself on that thread.
        // QUEUED here means the poll is in flight, which the same worker path covers.
        return taskState == ComputeTaskHandle.State.RUNNING || taskState == ComputeTaskHandle.State.QUEUED;
    }

    @Override
    public void onTerminal(final TaskHandle<?> handle) {
        this.handles.remove(handle.taskId());
        this.removeQuotaIfIdle(handle.consumerId());
    }

    @Override
    public String toString() {
        final DynamicWorkerPool currentPool = this.pool;
        final MetricsCollector collector = this.metricsCollector;
        final String workers = currentPool == null
            ? "none"
            : currentPool.workerCount() + "/" + currentPool.desiredWorkers() + " live/desired, limit "
                + currentPool.concurrencyLimit();
        final VirtualLane lane = this.virtualLane;
        final SchedulerMetrics.VirtualLane laneMetrics = lane == null ? null : lane.snapshot().virtual();
        final String virtual = laneMetrics == null
            ? "off"
            : laneMetrics.running() + "/" + laneMetrics.limit();
        return "AsyncComputeScheduler[state=" + this.state.get()
            + ", enabled=" + this.settings.enabled()
            + ", workers=" + workers
            + ", virtual=" + virtual
            + ", queued=" + this.queue.size() + '/' + this.queue.capacity()
            + ", consumers=" + this.registry.size()
            + ", metrics=" + (collector != null && collector.enabled()) + ']';
    }

    /**
     * Runs one janitor sweep immediately.
     *
     * <p>Diagnostics and deterministic tests use it to avoid waiting for the tick.
     */
    void sweepNow() {
        final QueueJanitor current = this.janitor;
        if (current != null) {
            current.sweepNow();
        }
    }

    /**
     * Runs one janitor tick and one gauge sample immediately.
     */
    void sampleNow() {
        final QueueJanitor current = this.janitor;
        if (current != null) {
            current.sampleNow();
        }
    }

    /**
     * Returns the adaptive controller, or {@code null} when adaptive concurrency is off.
     *
     * @return the controller of this engine
     */
    @Nullable AdaptiveConcurrencyController adaptiveController() {
        return this.adaptive;
    }

    /**
     * Returns the virtual lane, or {@code null} when it is disabled.
     *
     * <p>Diagnostics and leak tests use it to inspect the lane's own thread bookkeeping.
     *
     * @return the lane of this engine
     */
    @Nullable VirtualLane virtualLane() {
        return this.virtualLane;
    }

    /**
     * Evaluates the adaptive controller once and applies the result.
     *
     * <p>The timer calls this every decision window; tests call it directly with a manual clock.
     */
    void evaluateAdaptiveNow() {
        final AdaptiveConcurrencyController controller = this.adaptive;
        final DynamicWorkerPool currentPool = this.pool;
        if (controller == null || currentPool == null) {
            return;
        }
        final int requested = controller.evaluate();
        // the processor budget is a hard ceiling for the controller's decisions; the controller still
        // owns the short-term limit inside that ceiling
        final int budgetTarget = this.settings.workers().resolveThreadCount(this.processorBudget);
        final int limit = Math.min(requested, budgetTarget);
        if (limit == this.appliedLimit.get()) {
            return;
        }
        this.applyPoolSize(limit);
    }

    @Override
    public void applyProcessorBudget(final int processors) {
        Preconditions.checkPositive(processors, "processors");
        final DynamicWorkerPool currentPool = this.pool;
        if (currentPool == null
            || this.state.get() != AsyncComputeScheduler.SchedulerState.RUNNING
            || !this.settings.workers().automaticThreadCount()) {
            // fixed pool or not running: documented no-op, the budget cannot change anything
            return;
        }
        final int previousBudget = this.processorBudget;
        final int previousCeiling = this.workerCeiling;
        final int ceiling = this.ceilingFor(processors);
        final int target = this.settings.workers().resolveThreadCount(processors);
        this.processorBudget = processors;
        synchronized (this.sizingLock) {
            this.workerCeiling = ceiling;
            // the pool's hard ceiling moves first: a resize can only grow into indices the pool and
            // the dispatcher already made room for
            currentPool.setCeiling(ceiling);
        }
        final AdaptiveConcurrencyController controller = this.adaptive;
        if (controller != null && controller.enabled()) {
            // the controller keeps its short-term throttle, but it must not outlive the budget
            controller.updateCeiling(ceiling);
            this.applyPoolSize(Math.min(this.appliedLimit.get(), target));
        } else {
            this.applyPoolSize(target);
        }
        LOG.log(System.Logger.Level.INFO,
            "compute processor budget changed from " + previousBudget + " to " + processors
                + " (target workers " + target + ", ceiling " + previousCeiling + " -> " + ceiling + ')');
    }

    /**
     * Returns the current desired worker count.
     *
     * <p>Diagnostics and tests use it; it is the value the pool is currently aiming for, before its
     * own keep-alive retirement.
     *
     * @return desired workers, or {@code 0} when the pool does not exist
     */
    int desiredWorkers() {
        final DynamicWorkerPool currentPool = this.pool;
        return currentPool == null ? 0 : currentPool.desiredWorkers();
    }

    /**
     * Returns the current pool wide concurrency limit.
     *
     * @return the limit the quota controller enforces
     */
    int totalLimit() {
        return this.quota.totalLimit();
    }

    /**
     * Resolves the hard worker ceiling for a processor budget.
     *
     * <p>With {@code maxThreads == -1} the ceiling is the budget itself, so raising the budget later
     * can still grow the pool; an explicit {@code maxThreads} stays the ceiling and is only clamped
     * by the budget, never widened by it. {@code minThreads} is always the floor, because a pool that
     * cannot reach its configured minimum would not be sized the way the administrator asked.
     *
     * @param budget processors available to the engine, at least {@code 1}
     * @return the hard worker ceiling for that budget
     */
    private int ceilingFor(final int budget) {
        final ComputeSettings.Workers workers = this.settings.workers();
        final boolean automaticCeiling = workers.maxThreads() == -1;
        final int resolved = automaticCeiling
            ? workers.resolveMaxThreads(budget)
            : Math.min(workers.maxThreads(), budget);
        return Math.max(1, Math.max(workers.minThreads(), resolved));
    }

    private void wireRuntime() {
        final JfrRecorder recorder = new FlightJfrRecorder();
        final ComputeSettings.Lanes.Virtual laneConfig = this.settings.lanes().virtual();
        final boolean virtualOwnsLow = laneConfig.servesLow();
        final int virtualLowMask = virtualOwnsLow ? 1 : 0;
        final int virtualSlots = laneConfig.enabled() ? 1 : 0;
        final MetricsCollector collector = new ComputeMetricsCollector(
            this.settings,
            this.clock,
            recorder,
            this::laneSnapshot
        );
        this.metricsCollector = collector;
        final DispatcherImpl newDispatcher = new DispatcherImpl(
            this.settings,
            this.queue,
            this.quota,
            collector,
            this.registry,
            this.clock,
            this::isClaiming,
            this::recordAdaptiveSample,
            this.platformLevelMask,
            virtualLowMask,
            laneConfig.allowsSpill(),
            virtualSlots
        );
        this.dispatcher = newDispatcher;
        final EngineTaskRunner runner = new EngineTaskRunner(newDispatcher, collector, this.clock);
        final EngineWorkerLoop newLoop = new EngineWorkerLoop(newDispatcher, runner);
        final DynamicWorkerPool newPool = new DynamicWorkerPool(
            this.settings.workers(),
            this.settings.scheduling(),
            this.processorBudget,
            newLoop,
            this.clock,
            ThreadFactories.platform(this.settings.workers().threadNamePrefix(), true)
        );
        newLoop.bindPool(newPool);
        this.pool = newPool;
        // Future completions that would otherwise run on the janitor, the shutdown thread or a
        // submitting thread are dispatched here instead. One virtual thread per completion keeps a
        // blocking dependent stage from delaying any other completion or any scheduler thread.
        this.completionExecutor = Executors.newThreadPerTaskExecutor(
            ThreadFactories.virtual(this.settings.workers().threadNamePrefix() + "-completion")
        );
        if (laneConfig.enabled()) {
            if (!(this.queue instanceof LaneAwareQueue laneQueue)) {
                throw new IllegalStateException("virtual lane requires a lane aware queue, got " + this.queue.getClass());
            }
            this.virtualLane = new VirtualLane(
                laneConfig,
                laneQueue,
                newDispatcher,
                runner,
                newPool,
                this.clock,
                recorder,
                this.settings.metrics().jfrEnabled(),
                this::isClaiming,
                this.settings.workers().threadNamePrefix()
            );
        }
        this.quota.updateTotalLimit(newPool.concurrencyLimit());
        this.appliedLimit.set(newPool.concurrencyLimit());
        final AdaptiveConcurrencyController controller = new AimdAdaptiveConcurrencyController(
            this.settings,
            this.clock,
            newPool.concurrencyLimit(),
            this.processorBudget,
            newPool::activeWorkers,
            CpuUtilizationProbe::sample
        );
        this.adaptive = controller;
        this.replayConsumers();
    }

    private SchedulerMetrics.LaneMetrics laneSnapshot() {
        final VirtualLane lane = this.virtualLane;
        return lane == null ? SchedulerMetrics.LaneMetrics.disabled() : lane.snapshot();
    }

    private void startRuntime() {
        final DynamicWorkerPool newPool = this.pool;
        final DispatcherImpl newDispatcher = this.dispatcher;
        final MetricsCollector collector = this.metricsCollector;
        final AdaptiveConcurrencyController controller = this.adaptive;
        if (newPool == null || newDispatcher == null || collector == null || controller == null) {
            throw new IllegalStateException("compute runtime was not wired");
        }
        newPool.start();
        final VirtualLane lane = this.virtualLane;
        if (lane != null) {
            lane.start();
        }
        final QueueJanitor newJanitor = new QueueJanitor(
            this.queue,
            newDispatcher,
            collector,
            newPool,
            this.clock,
            controller.enabled() ? controller : null,
            controller.enabled() ? null : this::autoscale,
            this::platformBacklog,
            this.janitorTickMillis(),
            Durations.toNanosSaturated(this.settings.metrics().sampleInterval())
        );
        this.janitor = newJanitor;
        final Thread janitorThread = new Thread(newJanitor, this.settings.workers().threadNamePrefix() + "-janitor");
        janitorThread.setDaemon(true);
        janitorThread.setUncaughtExceptionHandler((thread, failure) ->
            LOG.log(System.Logger.Level.ERROR, "compute janitor died", failure));
        newJanitor.attach(janitorThread);
        janitorThread.start();
        newJanitor.sampleNow();
        this.startAdaptiveTimer(controller);
    }

    private void startAdaptiveTimer(final AdaptiveConcurrencyController controller) {
        if (!controller.enabled()) {
            return;
        }
        controller.start();
        final long periodMillis = Math.max(1_000L, Durations.toMillisSaturated(controller.sampleWindow()));
        final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
            ThreadFactories.platform(this.settings.workers().threadNamePrefix() + "-adaptive", true)
        );
        timer.scheduleWithFixedDelay(this::evaluateAdaptiveSafely, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
        this.adaptiveTimer = timer;
    }

    private void evaluateAdaptiveSafely() {
        try {
            this.evaluateAdaptiveNow();
        } catch (Throwable failure) {
            LOG.log(System.Logger.Level.WARNING, "adaptive evaluation failed", failure);
        }
    }

    private long janitorTickMillis() {
        final long configured = Durations.toMillisSaturated(this.settings.metrics().sampleInterval());
        return Math.clamp(configured, 10L, JANITOR_TICK_MILLIS);
    }

    private void registerConfiguredConsumers() {
        final ComputeSettings.Consumers consumers = this.settings.consumers();
        this.registerIfAbsent(
            AsyncComputeScheduler.DEFAULT_CONSUMER_ID,
            this.inheritedPolicy(AsyncComputeScheduler.DEFAULT_CONSUMER_ID)
        );
        for (final Map.Entry<String, ConsumerPolicy> override : consumers.overrides().entrySet()) {
            if (!AsyncComputeScheduler.DEFAULT_CONSUMER_ID.equals(override.getKey())) {
                this.registerIfAbsent(override.getKey(), this.inheritedPolicy(override.getKey()));
            }
        }
    }

    /**
     * Returns the policy the scheduler uses when it registers a consumer itself.
     *
     * <p>An explicit {@code consumers.<id>} override wins. Otherwise the default policy is used, with
     * its overflow behaviour replaced by {@link ComputeSettings.QueueSettings#overflowPolicy()}: that
     * key is the default for the built-in default consumer and for dynamic registrations, so a
     * globally configured REJECT/DROP_OLDEST actually has an effect.
     *
     * @param consumerId consumer that is about to be registered
     * @return the inherited policy
     */
    private ConsumerPolicy inheritedPolicy(final String consumerId) {
        final ConsumerPolicy explicit = this.settings.consumers().overrides().get(consumerId);
        if (explicit != null) {
            return explicit;
        }
        final ConsumerPolicy base = this.settings.consumers().defaultPolicy();
        final OverflowPolicy queueOverflow = this.settings.queue().overflowPolicy();
        if (base.overflowPolicy() == queueOverflow) {
            return base;
        }
        return new ConsumerPolicy(
            base.enabled(),
            base.concurrencyLimit(),
            base.maxQueuedTasks(),
            base.fairShareWeight(),
            queueOverflow,
            base.maxQueueWait()
        );
    }

    private void registerIfAbsent(final String consumerId, final ConsumerPolicy policy) {
        if (this.registry.find(consumerId).isPresent()) {
            return;
        }
        final ComputeConsumer consumer = ComputeConsumer.of(consumerId, policy);
        this.registry.register(consumer);
        this.queue.registerConsumer(consumerId, policy);
        this.quota.updateConsumerPolicy(consumerId, policy);
    }

    private void replayConsumers() {
        final MetricsCollector collector = this.metricsCollector;
        if (collector == null) {
            return;
        }
        for (final ComputeConsumer consumer : this.registry.all()) {
            collector.onConsumerRegistered(consumer, this.quota.limit(consumer.id()));
        }
    }

    private ComputeConsumer resolveConsumer(final String consumerId) {
        final Optional<ComputeConsumer> existing = this.registry.find(consumerId);
        if (existing.isPresent()) {
            return existing.get();
        }
        if (!this.settings.consumers().allowDynamicRegistration()) {
            throw this.reject(consumerId, ComputePriority.defaultPriority(),
                ComputeRejectedException.Reason.CONSUMER_UNKNOWN,
                "consumer " + consumerId + " is not registered and dynamic registration is disabled");
        }
        return this.registerConsumer(ComputeConsumer.of(consumerId, this.inheritedPolicy(consumerId)));
    }

    private <T> ComputeTaskHandle<T> accept(final String consumerId, final ComputeRequest<T> request) {
        final long taskId = this.taskIds.incrementAndGet();
        final long sequence = this.sequences.incrementAndGet();
        final long submittedAtNanos = this.clock.nanoTime();
        final ExecutorService completions = this.completionExecutor;
        final TaskHandle<T> handle = new TaskHandle<>(
            taskId,
            consumerId,
            request.taskType(),
            request.priority(),
            this.clock.instant(),
            submittedAtNanos,
            request.label(),
            this,
            completions == null ? Runnable::run : completions
        );
        final QueueEntry entry = new QueueEntry(
            taskId,
            consumerId,
            request.taskType(),
            request.priority(),
            sequence,
            submittedAtNanos,
            request.task(),
            handle
        );
        handle.bindEntry(entry);
        this.handles.put(taskId, handle);
        final MetricsCollector collector = this.metricsCollector;
        if (collector != null) {
            // publish the submission before the entry can be claimed, so a worker can never record a
            // completion before its submission and drive the derived depth negative
            collector.onSubmitted(entry);
        }
        final ComputeQueue.OfferResult result = this.queue.offerRegistered(entry);
        return switch (result) {
            case ACCEPTED -> {
                final VirtualLane lane = this.virtualLane;
                if (lane != null) {
                    lane.onSubmission(request.priority());
                }
                final DynamicWorkerPool currentPool = this.pool;
                if (currentPool != null) {
                    // an idle worker may be parked for idle-wait; a submission is the signal it is
                    // waiting for
                    currentPool.wakeUp();
                }
                this.maybeGrow();
                yield handle;
            }
            case CLOSED -> {
                this.withdraw(collector, entry);
                this.handles.remove(taskId);
                handle.reject();
                throw new ComputeShutdownException("compute scheduler stopped accepting work during submission");
            }
            case CONSUMER_UNKNOWN -> {
                this.withdraw(collector, entry);
                this.handles.remove(taskId);
                handle.reject();
                throw this.reject(consumerId, request.priority(), ComputeRejectedException.Reason.CONSUMER_UNKNOWN,
                    "consumer " + consumerId + " was unregistered while the task was being submitted");
            }
            case QUEUE_FULL -> {
                this.withdraw(collector, entry);
                this.handles.remove(taskId);
                handle.reject();
                throw this.reject(consumerId, request.priority(), ComputeRejectedException.Reason.QUEUE_FULL,
                    "global queue capacity " + this.queue.capacity() + " is exhausted");
            }
            case CONSUMER_QUEUE_FULL -> {
                this.withdraw(collector, entry);
                this.handles.remove(taskId);
                handle.reject();
                throw this.reject(consumerId, request.priority(), ComputeRejectedException.Reason.CONSUMER_QUEUE_FULL,
                    "consumer " + consumerId + " reached its queue bound");
            }
        };
    }

    /**
     * Takes back a submission counter that was published before a refused offer.
     *
     * @param collector collector to notify, or {@code null} when metrics are off
     * @param entry     entry that never reached the queue
     */
    private void withdraw(final @Nullable MetricsCollector collector, final QueueEntry entry) {
        if (collector != null) {
            collector.onSubmissionWithdrawn(entry);
        }
    }

    private ComputeRejectedException reject(
        final String consumerId,
        final ComputePriority priority,
        final ComputeRejectedException.Reason reason,
        final String message
    ) {
        final MetricsCollector collector = this.metricsCollector;
        if (collector != null) {
            collector.onRejected(consumerId, reason);
        }
        return new ComputeRejectedException(reason, consumerId, priority, message);
    }

    private void maybeGrow() {
        if (!this.settings.workers().automaticThreadCount()) {
            return;
        }
        final AdaptiveConcurrencyController controller = this.adaptive;
        if (controller != null && controller.enabled()) {
            return;
        }
        final DynamicWorkerPool currentPool = this.pool;
        if (currentPool == null || !this.isClaiming()) {
            return;
        }
        final int ceiling = this.workerCeiling;
        final int desired = currentPool.desiredWorkers();
        if (desired >= ceiling || currentPool.idleWorkers() > 0 || this.platformBacklog() <= 0) {
            return;
        }
        if (!this.growInFlight.compareAndSet(false, true)) {
            return;
        }
        try {
            if (currentPool.desiredWorkers() < ceiling
                && currentPool.idleWorkers() == 0
                && this.platformBacklog() > 0) {
                this.applyPoolSize(currentPool.desiredWorkers() + 1);
            }
        } finally {
            this.growInFlight.set(false);
        }
    }

    private void autoscale() {
        if (!this.settings.workers().automaticThreadCount() || !this.isClaiming()) {
            return;
        }
        final AdaptiveConcurrencyController controller = this.adaptive;
        if (controller != null && controller.enabled()) {
            // the adaptive timer owns pool size in that mode; two writers would fight
            return;
        }
        final DynamicWorkerPool currentPool = this.pool;
        final DispatcherImpl currentDispatcher = this.dispatcher;
        if (currentPool == null) {
            return;
        }
        final int ceiling = this.workerCeiling;
        final int minimum = Math.min(this.settings.workers().minThreads(), ceiling);
        final int backlog = this.platformBacklog()
            + (currentDispatcher == null ? 0 : currentDispatcher.platformCarriedEntries());
        final int target;
        if (backlog <= 0) {
            target = minimum;
        } else {
            final long wanted = (long) currentPool.activeWorkers() + backlog;
            target = (int) Math.min(ceiling, Math.max(minimum, wanted));
        }
        if (target != currentPool.desiredWorkers()) {
            this.applyPoolSize(target);
        }
    }

    /**
     * Moves the pool, its concurrency gate and the quota limit together.
     *
     * <p>Every writer of the pool size goes through here, which is what keeps the three numbers from
     * drifting apart: a pool that grew without its gate and quota would run fewer bodies than it has
     * workers (the bug autoscaling used to have), and a gate above the worker count would admit work
     * no worker can pick up, so the queue would grow for no reason.
     *
     * @param target requested worker count; the pool clamps it into its own min/max window
     */
    private void applyPoolSize(final int target) {
        final DynamicWorkerPool currentPool = this.pool;
        if (currentPool == null) {
            return;
        }
        synchronized (this.sizingLock) {
            currentPool.resize(target);
            // the pool owns the min/max clamp, so the limit follows the applied size, not the request
            final int applied = currentPool.desiredWorkers();
            currentPool.setConcurrencyLimit(applied);
            this.quota.updateTotalLimit(applied);
            this.appliedLimit.set(applied);
        }
    }

    private void recordAdaptiveSample(final long queueWaitNanos, final long executionNanos, final boolean failed) {
        final AdaptiveConcurrencyController controller = this.adaptive;
        if (controller != null) {
            controller.recordSample(queueWaitNanos, executionNanos, failed);
        }
    }

    /**
     * Returns the queue depth the platform pool can actually act on.
     *
     * <p>LOW work belongs to the virtual lane when that lane owns it, so it must not make the
     * platform pool grow or count as backlog in the adaptive signal.
     *
     * @return queued entries eligible for the platform lane
     */
    int platformBacklog() {
        if (this.queue instanceof LaneAwareQueue aware) {
            return aware.queuedInLane(LaneAwareQueue.Lane.PLATFORM, this.platformLevelMask);
        }
        return this.queue.size();
    }

    private boolean isClaiming() {
        return this.state.get() == AsyncComputeScheduler.SchedulerState.RUNNING;
    }

    private void beginShutdown(final boolean immediate, final Duration gracePeriod) {
        synchronized (this.lifecycleLock) {
            final AsyncComputeScheduler.SchedulerState current = this.state.get();
            if (current == AsyncComputeScheduler.SchedulerState.NEW) {
                this.state.set(AsyncComputeScheduler.SchedulerState.TERMINATED);
                this.terminated.countDown();
                return;
            }
            if (current != AsyncComputeScheduler.SchedulerState.RUNNING) {
                return;
            }
            this.state.set(AsyncComputeScheduler.SchedulerState.SHUTTING_DOWN);
            this.queue.close();
            this.cancelAllQueued(ComputeCancelledException.Reason.SHUTDOWN, true);
            final DispatcherImpl pendingDispatcher = this.dispatcher;
            if (pendingDispatcher != null) {
                // carried entries have not started either; make sure they are cancelled even if their
                // worker never asks for work again
                pendingDispatcher.cancelAllCarried(ComputeCancelledException.Reason.SHUTDOWN, true);
            }
            final VirtualLane lane = this.virtualLane;
            if (lane != null) {
                if (immediate) {
                    lane.shutdownNow();
                } else {
                    lane.shutdown();
                }
            }
            final DynamicWorkerPool currentPool = this.pool;
            if (currentPool == null) {
                this.state.set(AsyncComputeScheduler.SchedulerState.TERMINATED);
                this.terminated.countDown();
                return;
            }
            if (immediate) {
                currentPool.shutdownNow();
            } else {
                currentPool.shutdown();
            }
            final long graceNanos = Durations.toNanosSaturated(gracePeriod);
            final Thread thread = new Thread(
                () -> this.awaitAndTerminate(graceNanos),
                this.settings.workers().threadNamePrefix() + "-shutdown"
            );
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((failed, failure) ->
                LOG.log(System.Logger.Level.ERROR, "compute shutdown failed", failure));
            this.shutdownThread = thread;
            thread.start();
        }
    }

    /**
     * Waits for the runtime to drain, bounded by the grace period plus a fixed safety margin.
     *
     * <p>Every step is bounded. A component that refuses to stop is logged and the engine still
     * moves to {@code TERMINATED}: an operator must never end up with a scheduler that cannot be
     * shut down because one thread is wedged.
     *
     * @param graceNanos grace period in nanoseconds, already saturated
     */
    private void awaitAndTerminate(final long graceNanos) {
        try {
            final long deadline = Durations.deadline(
                Durations.deadline(System.nanoTime(), graceNanos),
                SHUTDOWN_SLACK_NANOS
            );
            this.stopJanitor();
            this.stopAdaptiveTimer();
            boolean abandoned = false;
            final DynamicWorkerPool currentPool = this.pool;
            if (currentPool != null) {
                final long graceLeft = Math.max(0L, deadline - System.nanoTime());
                abandoned |= !currentPool.awaitTermination(Duration.ofNanos(graceLeft));
            }
            final VirtualLane lane = this.virtualLane;
            if (lane != null) {
                final long graceLeft = Math.max(0L, deadline - System.nanoTime());
                abandoned |= !lane.awaitTermination(Duration.ofNanos(graceLeft));
            }
            if (abandoned) {
                // the grace period is over: abandon the running bodies instead of blocking shutdown
                // forever; they are never interrupted and complete after termination
                this.logAbandonedTasks();
            }
            // safety net: nothing may remain outside the queue after the workers stopped. completion
            // is deferred so a blocked dependent stage cannot wedge this thread either
            this.cancelAllQueued(ComputeCancelledException.Reason.SHUTDOWN, true);
            final DispatcherImpl currentDispatcher = this.dispatcher;
            if (currentDispatcher != null) {
                currentDispatcher.cancelAllCarried(ComputeCancelledException.Reason.SHUTDOWN, true);
            }
            final MetricsCollector collector = this.metricsCollector;
            if (collector != null) {
                // stops the sampler; hooks stay valid so an abandoned task can still finish its
                // counters when its body returns
                collector.close();
            }
            this.quota.reset();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            LOG.log(System.Logger.Level.WARNING, "compute shutdown was interrupted while waiting for the grace period");
        } finally {
            final ExecutorService completions = this.completionExecutor;
            if (completions != null) {
                // let already scheduled completions finish, but never wait for user code: the
                // workers are daemon virtual threads and a blocked one cannot keep the JVM alive
                completions.shutdown();
            }
            this.state.set(AsyncComputeScheduler.SchedulerState.TERMINATED);
            this.terminated.countDown();
        }
    }

    /**
     * Logs the tasks that are still running when the grace period expires.
     *
     * <p>They are not cancelled: the worker keeps executing the body and the handle is completed
     * normally when it returns, even though the scheduler is already terminated.
     */
    private void logAbandonedTasks() {
        final Map<String, Integer> perConsumer = new HashMap<>();
        int total = 0;
        for (final TaskHandle<?> handle : this.handles.values()) {
            if (handle.state() == ComputeTaskHandle.State.RUNNING) {
                perConsumer.merge(handle.consumerId(), 1, Integer::sum);
                total++;
            }
        }
        if (total == 0) {
            return;
        }
        LOG.log(System.Logger.Level.WARNING,
            "compute scheduler terminated with " + total + " task(s) still running after the grace period; "
                + "they are not interrupted and their futures complete after termination: " + perConsumer);
    }

    private void stopJanitor() {
        final QueueJanitor current = this.janitor;
        if (current != null && !current.stop(JANITOR_STOP_TIMEOUT)) {
            LOG.log(System.Logger.Level.WARNING,
                "compute janitor was still running after " + JANITOR_STOP_TIMEOUT + "; terminating anyway");
        }
    }

    private void stopAdaptiveTimer() {
        final AdaptiveConcurrencyController controller = this.adaptive;
        if (controller != null) {
            controller.stop();
        }
        final ScheduledExecutorService timer = this.adaptiveTimer;
        if (timer == null) {
            return;
        }
        timer.shutdownNow();
        try {
            if (!timer.awaitTermination(1L, TimeUnit.SECONDS)) {
                LOG.log(System.Logger.Level.WARNING,
                    "compute adaptive timer did not stop within one second; terminating anyway");
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    private void abortStart() {
        this.stopAdaptiveTimer();
        this.stopJanitor();
        final VirtualLane lane = this.virtualLane;
        if (lane != null) {
            lane.shutdownNow();
        }
        final DynamicWorkerPool currentPool = this.pool;
        if (currentPool != null) {
            currentPool.shutdownNow();
        }
        final MetricsCollector collector = this.metricsCollector;
        if (collector != null) {
            collector.close();
        }
        final ExecutorService completions = this.completionExecutor;
        if (completions != null) {
            completions.shutdown();
        }
        this.state.set(AsyncComputeScheduler.SchedulerState.TERMINATED);
        this.terminated.countDown();
    }

    private void cancelAllQueued(final ComputeCancelledException.Reason reason, final boolean deferred) {
        final Collection<QueueEntry> drained = this.queue.clear();
        for (final QueueEntry entry : drained) {
            this.cancelQueued(entry, reason, deferred);
        }
    }

    private void cancelQueued(final QueueEntry entry, final ComputeCancelledException.Reason reason) {
        this.cancelQueued(entry, reason, false);
    }

    private void cancelQueued(
        final QueueEntry entry,
        final ComputeCancelledException.Reason reason,
        final boolean deferred
    ) {
        final DispatcherImpl currentDispatcher = this.dispatcher;
        if (currentDispatcher != null) {
            currentDispatcher.cancelQueued(entry, reason, deferred);
            return;
        }
        final TaskHandle<?> handle = (TaskHandle<?>) entry.handle();
        if (handle.completeCancelled(reason, deferred)) {
            final MetricsCollector collector = this.metricsCollector;
            if (collector != null) {
                collector.onCancelled(entry, reason);
            }
        }
    }

    private void removeQuotaIfIdle(final String consumerId) {
        if (!this.unregisteredConsumers.contains(consumerId)
            || this.registry.find(consumerId).isPresent()
            || this.quota.activeTasks(consumerId) != 0) {
            return;
        }
        this.unregisteredConsumers.remove(consumerId);
        try {
            this.quota.removeConsumer(consumerId);
        } catch (IllegalStateException _) {
            // a task slipped in between the active check and the removal; keep it for the next
            // terminal event, the accounting stays correct because the quota entry is still there.
            this.unregisteredConsumers.add(consumerId);
        }
    }

    private void onEvicted(final QueueEntry entry) {
        final DispatcherImpl currentDispatcher = this.dispatcher;
        if (currentDispatcher != null) {
            // this runs on the submitting thread inside offer; a dependent stage must not run there
            currentDispatcher.cancelQueued(entry, ComputeCancelledException.Reason.EVICTED, true);
            return;
        }
        final TaskHandle<?> handle = (TaskHandle<?>) entry.handle();
        if (handle.completeCancelled(ComputeCancelledException.Reason.EVICTED, true)) {
            final MetricsCollector collector = this.metricsCollector;
            if (collector != null) {
                collector.onCancelled(entry, ComputeCancelledException.Reason.EVICTED);
            }
        }
    }

    private SchedulerMetrics emptyMetrics() {
        return new SchedulerMetricsSnapshot(
            this.clock.instant(),
            this.settings.metrics().enabled(),
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0,
            this.queue.capacity(),
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
            this.laneSnapshot()
        );
    }
}
