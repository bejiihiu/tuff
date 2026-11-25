package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.queue.QuotaController;
import kz.bejiihiu.tuff.compute.internal.queue.impl.AtomicQuotaController;
import kz.bejiihiu.tuff.compute.internal.queue.impl.BoundedComputeQueue;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
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
 * <p>Terminal ownership invariants (the short version, see the report in the module notes):
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
     * Poll slice used while waiting for running tasks during shutdown. Running tasks are never
     * interrupted, so a task that never returns can delay termination; waiting in slices keeps the
     * shutdown thread responsive to spurious interrupts.
     */
    private static final Duration SHUTDOWN_POLL = Duration.ofMillis(100);

    /**
     * Janitor tick. Kept small so queue expiry is real even without adaptive concurrency; the gauge
     * sampling cadence is the configured metrics sample interval, not this value.
     */
    private static final long JANITOR_TICK_MILLIS = 100L;

    private final ComputeSettings settings;
    private final ComputeClock clock;
    private final int availableProcessors;
    private final int workerCeiling;

    private final ComputeQueue queue;
    private final QuotaController quota;
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
    private volatile @Nullable QueueJanitor janitor;
    private volatile @Nullable AdaptiveConcurrencyController adaptive;
    private volatile @Nullable ScheduledExecutorService adaptiveTimer;
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
     * Creates an engine with an explicit processor count.
     *
     * <p>Tests use this overload to make worker sizing and adaptive bounds deterministic.
     *
     * @param settings            scheduler configuration
     * @param clock               clock used by every component
     * @param availableProcessors processors available to the JVM, at least {@code 1}
     */
    ComputeEngineImpl(final ComputeSettings settings, final ComputeClock clock, final int availableProcessors) {
        this.settings = Preconditions.requireNonNull(settings, "settings");
        this.clock = Preconditions.requireNonNull(clock, "clock");
        Preconditions.checkAtLeast(availableProcessors, 1, "availableProcessors");
        this.availableProcessors = availableProcessors;
        this.workerCeiling = Math.max(
            1,
            Math.max(settings.workers().minThreads(), settings.workers().resolveMaxThreads(availableProcessors))
        );
        final int initialConcurrency = Math.max(1, settings.workers().resolveThreadCount(availableProcessors));
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
        Preconditions.checkNonNegative(gracePeriod.toNanos(), "gracePeriod");
        this.beginShutdown(false, gracePeriod);
    }

    @Override
    public void shutdownNow() {
        this.beginShutdown(true, Duration.ZERO);
    }

    @Override
    public boolean awaitTermination(final Duration timeout) throws InterruptedException {
        Preconditions.requireNonNull(timeout, "timeout");
        Preconditions.checkNonNegative(timeout.toNanos(), "timeout");
        final boolean inTime = this.terminated.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
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
        final ComputeConsumer stored = this.registry.register(consumer);
        this.queue.registerConsumer(stored.id(), stored.policy());
        this.quota.updateConsumerPolicy(stored.id(), stored.policy());
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
        for (final TaskHandle<?> handle : this.handles.values()) {
            if (handle.consumerId().equals(consumerId)) {
                handle.requestCooperativeCancel(ComputeCancelledException.Reason.CONSUMER_DISABLED);
            }
        }
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
        return "AsyncComputeScheduler[state=" + this.state.get()
            + ", enabled=" + this.settings.enabled()
            + ", workers=" + workers
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
        final int limit = controller.evaluate();
        if (limit == this.appliedLimit.get()) {
            return;
        }
        this.appliedLimit.set(limit);
        currentPool.resize(limit);
        currentPool.setConcurrencyLimit(limit);
        this.quota.updateTotalLimit(limit);
    }

    /**
     * Returns the current pool wide concurrency limit.
     *
     * @return the limit the quota controller enforces
     */
    int totalLimit() {
        return this.quota.totalLimit();
    }

    private void wireRuntime() {
        final JfrRecorder recorder = new FlightJfrRecorder();
        final MetricsCollector collector = new ComputeMetricsCollector(this.settings, this.clock, recorder);
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
            this.workerCeiling
        );
        this.dispatcher = newDispatcher;
        final EngineWorkerLoop newLoop = new EngineWorkerLoop(newDispatcher, collector, this.clock);
        final DynamicWorkerPool newPool = new DynamicWorkerPool(
            this.settings.workers(),
            this.settings.scheduling(),
            this.availableProcessors,
            newLoop,
            this.clock,
            ThreadFactories.platform(this.settings.workers().threadNamePrefix(), true)
        );
        this.pool = newPool;
        this.quota.updateTotalLimit(newPool.concurrencyLimit());
        this.appliedLimit.set(newPool.concurrencyLimit());
        final AdaptiveConcurrencyController controller = new AimdAdaptiveConcurrencyController(
            this.settings,
            this.clock,
            newPool.concurrencyLimit(),
            this.availableProcessors,
            newPool::activeWorkers,
            CpuUtilizationProbe::sample
        );
        this.adaptive = controller;
        this.replayConsumers();
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
        final QueueJanitor newJanitor = new QueueJanitor(
            this.queue,
            newDispatcher,
            collector,
            newPool,
            this.clock,
            controller.enabled() ? controller : null,
            controller.enabled() ? null : this::autoscale,
            this.janitorTickMillis(),
            this.settings.metrics().sampleInterval().toNanos()
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
        final long periodMillis = Math.max(1_000L, controller.sampleWindow().toMillis());
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
        final long configured = this.settings.metrics().sampleInterval().toMillis();
        return Math.clamp(configured, 10L, JANITOR_TICK_MILLIS);
    }

    private void registerConfiguredConsumers() {
        final ComputeSettings.Consumers consumers = this.settings.consumers();
        this.registerIfAbsent(AsyncComputeScheduler.DEFAULT_CONSUMER_ID, consumers.policyFor(AsyncComputeScheduler.DEFAULT_CONSUMER_ID));
        for (final Map.Entry<String, ConsumerPolicy> override : consumers.overrides().entrySet()) {
            if (!AsyncComputeScheduler.DEFAULT_CONSUMER_ID.equals(override.getKey())) {
                this.registerIfAbsent(override.getKey(), override.getValue());
            }
        }
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
        return this.registerConsumer(ComputeConsumer.of(consumerId, this.settings.consumers().policyFor(consumerId)));
    }

    private <T> ComputeTaskHandle<T> accept(final String consumerId, final ComputeRequest<T> request) {
        final long taskId = this.taskIds.incrementAndGet();
        final long sequence = this.sequences.incrementAndGet();
        final long submittedAtNanos = this.clock.nanoTime();
        final TaskHandle<T> handle = new TaskHandle<>(
            taskId,
            consumerId,
            request.taskType(),
            request.priority(),
            this.clock.instant(),
            submittedAtNanos,
            this
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
        final ComputeQueue.OfferResult result = this.queue.offer(entry);
        return switch (result) {
            case ACCEPTED -> {
                final MetricsCollector collector = this.metricsCollector;
                if (collector != null) {
                    collector.onSubmitted(entry);
                }
                this.maybeGrow();
                yield handle;
            }
            case CLOSED -> {
                this.handles.remove(taskId);
                handle.reject();
                throw new ComputeShutdownException("compute scheduler stopped accepting work during submission");
            }
            case QUEUE_FULL -> {
                this.handles.remove(taskId);
                handle.reject();
                throw this.reject(consumerId, request.priority(), ComputeRejectedException.Reason.QUEUE_FULL,
                    "global queue capacity " + this.queue.capacity() + " is exhausted");
            }
            case CONSUMER_QUEUE_FULL -> {
                this.handles.remove(taskId);
                handle.reject();
                throw this.reject(consumerId, request.priority(), ComputeRejectedException.Reason.CONSUMER_QUEUE_FULL,
                    "consumer " + consumerId + " reached its queue bound");
            }
        };
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
        final int desired = currentPool.desiredWorkers();
        if (desired >= this.workerCeiling || currentPool.idleWorkers() > 0 || this.queue.size() <= 0) {
            return;
        }
        if (!this.growInFlight.compareAndSet(false, true)) {
            return;
        }
        try {
            if (currentPool.desiredWorkers() < this.workerCeiling
                && currentPool.idleWorkers() == 0
                && this.queue.size() > 0) {
                currentPool.resize(currentPool.desiredWorkers() + 1);
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
        final int minimum = Math.min(this.settings.workers().minThreads(), this.workerCeiling);
        final int backlog = this.queue.size() + (currentDispatcher == null ? 0 : currentDispatcher.carriedEntries());
        final int target;
        if (backlog <= 0) {
            target = minimum;
        } else {
            final long wanted = (long) currentPool.activeWorkers() + backlog;
            target = (int) Math.min(this.workerCeiling, Math.max(minimum, wanted));
        }
        if (target != currentPool.desiredWorkers()) {
            currentPool.resize(target);
        }
    }

    private void recordAdaptiveSample(final long queueWaitNanos, final long executionNanos, final boolean failed) {
        final AdaptiveConcurrencyController controller = this.adaptive;
        if (controller != null) {
            controller.recordSample(queueWaitNanos, executionNanos, failed);
        }
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
            this.cancelAllQueued(ComputeCancelledException.Reason.SHUTDOWN);
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
            final long graceNanos = gracePeriod.toNanos();
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

    private void awaitAndTerminate(final long graceNanos) {
        try {
            this.stopJanitor();
            this.stopAdaptiveTimer();
            final DynamicWorkerPool currentPool = this.pool;
            if (currentPool != null) {
                final long deadline = System.nanoTime() + graceNanos;
                final long graceLeft = Math.max(0L, deadline - System.nanoTime());
                if (!currentPool.awaitTermination(Duration.ofNanos(graceLeft))) {
                    // running tasks are never interrupted, so the grace period is an upper bound on
                    // waiting for them, not a way to kill them; keep waiting in slices.
                    boolean stopped = currentPool.awaitTermination(SHUTDOWN_POLL);
                    while (!stopped) {
                        stopped = currentPool.awaitTermination(SHUTDOWN_POLL);
                    }
                }
            }
            // safety net: nothing may remain outside the queue after the workers stopped
            this.cancelAllQueued(ComputeCancelledException.Reason.SHUTDOWN);
            final DispatcherImpl currentDispatcher = this.dispatcher;
            if (currentDispatcher != null) {
                currentDispatcher.cancelAllCarried(ComputeCancelledException.Reason.SHUTDOWN);
            }
            final MetricsCollector collector = this.metricsCollector;
            if (collector != null) {
                collector.close();
            }
            this.quota.reset();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            LOG.log(System.Logger.Level.WARNING, "compute shutdown was interrupted");
        } finally {
            this.state.set(AsyncComputeScheduler.SchedulerState.TERMINATED);
            this.terminated.countDown();
        }
    }

    private void stopJanitor() {
        final QueueJanitor current = this.janitor;
        if (current != null) {
            current.stop();
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
            timer.awaitTermination(1L, TimeUnit.SECONDS);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    private void abortStart() {
        this.stopAdaptiveTimer();
        this.stopJanitor();
        final DynamicWorkerPool currentPool = this.pool;
        if (currentPool != null) {
            currentPool.shutdownNow();
        }
        final MetricsCollector collector = this.metricsCollector;
        if (collector != null) {
            collector.close();
        }
        this.state.set(AsyncComputeScheduler.SchedulerState.TERMINATED);
        this.terminated.countDown();
    }

    private void cancelAllQueued(final ComputeCancelledException.Reason reason) {
        final Collection<QueueEntry> drained = this.queue.clear();
        for (final QueueEntry entry : drained) {
            this.cancelQueued(entry, reason);
        }
    }

    private void cancelQueued(final QueueEntry entry, final ComputeCancelledException.Reason reason) {
        final DispatcherImpl currentDispatcher = this.dispatcher;
        if (currentDispatcher != null) {
            currentDispatcher.cancelQueued(entry, reason);
            return;
        }
        final TaskHandle<?> handle = (TaskHandle<?>) entry.handle();
        if (handle.completeCancelled(reason)) {
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
            currentDispatcher.cancelQueued(entry, ComputeCancelledException.Reason.EVICTED);
            return;
        }
        final TaskHandle<?> handle = (TaskHandle<?>) entry.handle();
        if (handle.completeCancelled(ComputeCancelledException.Reason.EVICTED)) {
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
            LatencyStats.empty()
        );
    }
}
