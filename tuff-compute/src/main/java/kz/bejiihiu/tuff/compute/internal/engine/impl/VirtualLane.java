package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.SchedulerMetrics;
import kz.bejiihiu.tuff.compute.internal.metrics.JfrRecorder;
import kz.bejiihiu.tuff.compute.internal.queue.LaneAwareQueue;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.compute.internal.util.Durations;
import kz.bejiihiu.tuff.compute.internal.util.Preconditions;
import kz.bejiihiu.tuff.compute.internal.util.ThreadFactories;
import kz.bejiihiu.tuff.compute.internal.worker.impl.DynamicWorkerPool;
import org.jspecify.annotations.Nullable;

/**
 * The virtual-thread execution lane.
 *
 * <p>One daemon pump thread owns the decisions, and every claimed task runs on its own virtual
 * thread. The pump parks on a condition and wakes on a LOW submission, on a task completion, on
 * shutdown or after a short re-evaluation tick (NORMAL spill depends on the platform pool getting
 * saturated after the task was queued, which no submission signal covers). It never spins.
 *
 * <p>The hard limit is a {@link Semaphore} with {@code maxConcurrent} permits: the pump must hold a
 * permit before it may claim an entry, and the permit is released when the task's virtual thread
 * ends. A task therefore cannot start unless it fits, and there is no thread per submission outside
 * the bound.
 *
 * <p>Lane ownership invariants:
 * <ul>
 *     <li>only the pump claims from the queue, and it claims with the lane mask computed from
 *     {@link ComputeSettings.Lanes.Virtual#servesLow()} plus the live spill decision;</li>
 *     <li>the dispatcher reserves the consumer quota slot during the claim and releases it on the
 *     terminal path, exactly like a platform worker, so a VT task holds one quota slot;</li>
 *     <li>cancellation is cooperative: this class never interrupts a task; a running task is only
 *     flagged by the engine and its result is discarded by the shared runner;</li>
 *     <li>virtual threads are daemon by definition, and this class tracks every thread it starts so
 *     a terminated lane can be checked for leaks.</li>
 *     <li>the published snapshot derives {@code running} from {@code started - terminal}, so
 *     {@code submitted == completed + failed + cancelled + running} holds for every snapshot, even
 *     one taken while a task is finishing.</li>
 * </ul>
 */
final class VirtualLane {

    private static final System.Logger LOG = System.getLogger(VirtualLane.class.getName());

    /**
     * How long the pump parks when it has nothing to do before it re-evaluates the spill decision.
     * The wait is a park, not a spin; it exists because pool saturation can change without a queue
     * submission.
     */
    private static final long REEVALUATE_MILLIS = 25L;

    private final ComputeSettings.Lanes.Virtual config;
    private final LaneAwareQueue queue;
    private final DispatcherImpl dispatcher;
    private final EngineTaskRunner runner;
    private final DynamicWorkerPool pool;
    private final ComputeClock clock;
    private final JfrRecorder recorder;
    private final boolean jfrEnabled;
    private final BooleanSupplier accepting;
    private final String threadPrefix;

    private final Semaphore permits;
    private final ThreadFactory virtualFactory;
    private final Object workLock = new Object();
    private final ReentrantLock terminationLock = new ReentrantLock();
    private final Condition terminated = this.terminationLock.newCondition();
    private final Set<Thread> liveThreads = ConcurrentHashMap.newKeySet();

    /**
     * Tasks whose body has not returned yet. This is the execution and termination gate; the public
     * snapshot does not report it directly, see {@link #snapshot()}.
     */
    private final AtomicInteger runningTasks = new AtomicInteger();
    private final LongAdder started = new LongAdder();
    private final LongAdder completed = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder cancelled = new LongAdder();
    private final LongAdder spilled = new LongAdder();

    private final long spillWaitNanos;
    private final int normalLevel;

    private volatile boolean running;
    private volatile boolean pumpStopped = true;
    private volatile @Nullable Thread pump;

    /**
     * Creates the lane. No thread is created until {@link #start()}.
     *
     * @param config       virtual lane configuration
     * @param queue        lane-aware queue the lane claims from
     * @param dispatcher   dispatcher that owns quotas and lane masks
     * @param runner       shared task runner
     * @param pool         platform pool, used only to detect saturation
     * @param clock        scheduler clock
     * @param recorder     JFR bridge
     * @param jfrEnabled   whether JFR events are configured
     * @param accepting    predicate answering whether the engine still claims work
     * @param threadPrefix worker thread name prefix
     */
    VirtualLane(
        final ComputeSettings.Lanes.Virtual config,
        final LaneAwareQueue queue,
        final DispatcherImpl dispatcher,
        final EngineTaskRunner runner,
        final DynamicWorkerPool pool,
        final ComputeClock clock,
        final JfrRecorder recorder,
        final boolean jfrEnabled,
        final BooleanSupplier accepting,
        final String threadPrefix
    ) {
        this.config = Preconditions.requireNonNull(config, "config");
        this.queue = Preconditions.requireNonNull(queue, "queue");
        this.dispatcher = Preconditions.requireNonNull(dispatcher, "dispatcher");
        this.runner = Preconditions.requireNonNull(runner, "runner");
        this.pool = Preconditions.requireNonNull(pool, "pool");
        this.clock = Preconditions.requireNonNull(clock, "clock");
        this.recorder = Preconditions.requireNonNull(recorder, "recorder");
        this.jfrEnabled = jfrEnabled;
        this.accepting = Preconditions.requireNonNull(accepting, "accepting");
        this.threadPrefix = Preconditions.requireNonBlank(threadPrefix, "threadPrefix");
        this.permits = new Semaphore(config.maxConcurrent());
        this.virtualFactory = ThreadFactories.virtual(this.threadPrefix + "-virtual");
        this.spillWaitNanos = Durations.toNanosSaturated(config.spillWait());
        this.normalLevel = ComputePriority.NORMAL.level();
    }

    /**
     * Starts the pump thread.
     *
     * <p>Idempotent: starting a started lane does nothing.
     */
    void start() {
        synchronized (this.workLock) {
            if (this.running) {
                return;
            }
            this.running = true;
            this.pumpStopped = false;
        }
        final Thread pumpThread = new Thread(this::pumpLoop, this.threadPrefix + "-virtual-pump");
        pumpThread.setDaemon(true);
        pumpThread.setUncaughtExceptionHandler((thread, failure) ->
            LOG.log(System.Logger.Level.ERROR, "virtual lane pump died", failure));
        this.pump = pumpThread;
        pumpThread.start();
    }

    /**
     * Asks the lane to stop claiming work.
     *
     * <p>Running tasks are never interrupted; they finish and release their permit on their own.
     */
    void shutdown() {
        this.running = false;
        this.signalPump();
    }

    /**
     * Immediate stop of the same shape: the pump stops claiming, running tasks finish cooperatively.
     */
    void shutdownNow() {
        this.shutdown();
    }

    /**
     * Waits until the pump stopped and no virtual task is still running.
     *
     * @param timeout maximum time to wait, must not be negative
     * @return {@code true} if the lane is fully idle within the timeout
     * @throws InterruptedException if the caller was interrupted
     */
    boolean awaitTermination(final Duration timeout) throws InterruptedException {
        Preconditions.checkNonNegative(Durations.toNanosSaturated(timeout), "timeout");
        final long deadline = Durations.deadline(System.nanoTime(), Durations.toNanosSaturated(timeout));
        this.terminationLock.lockInterruptibly();
        try {
            while (!this.isTerminated()) {
                final long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    break;
                }
                this.terminated.awaitNanos(remaining);
            }
            return this.isTerminated();
        } finally {
            this.terminationLock.unlock();
        }
    }

    /**
     * Checks whether the pump stopped and every virtual task finished.
     *
     * @return {@code true} when the lane holds no thread and no running task
     */
    boolean isTerminated() {
        return this.pumpStopped && this.runningTasks.get() == 0;
    }

    /**
     * Tells the lane that work was submitted, so it can wake up without waiting for the tick.
     *
     * <p>Called on the submitting thread after the entry was accepted. It only signals for work the
     * lane actually owns or may own: LOW always, NORMAL only when spilling is enabled and the pool is
     * already saturated.
     *
     * @param priority priority of the accepted submission
     */
    void onSubmission(final ComputePriority priority) {
        if (!this.running) {
            return;
        }
        if (priority == ComputePriority.LOW && this.config.servesLow()) {
            this.signalPump();
            return;
        }
        if (priority == ComputePriority.NORMAL && this.config.allowsSpill() && this.platformSaturated()) {
            this.signalPump();
        }
    }

    /**
     * Returns the lane snapshot.
     *
     * <p>The terminal counters are read before {@code started}. A task is always started before it
     * can finish, so a later {@code started} reading can never be smaller than an earlier terminal
     * count and the derived running value cannot go negative. Deriving it instead of reporting the
     * separate execution counter is what makes the documented invariant hold for every snapshot:
     * two independent atomics cannot be read atomically, so a snapshot taken while a task finishes
     * would otherwise see it both completed and running.
     *
     * @return the immutable lane metrics
     */
    SchedulerMetrics.LaneMetrics snapshot() {
        final long completedTasks = this.completed.sum();
        final long failedTasks = this.failed.sum();
        final long cancelledTasks = this.cancelled.sum();
        final long spilledTasks = this.spilled.sum();
        final long submittedTasks = this.started.sum();
        final long running = Math.max(0L, submittedTasks - completedTasks - failedTasks - cancelledTasks);
        final SchedulerMetrics.VirtualLaneMetrics virtual = new SchedulerMetrics.VirtualLaneMetrics(
            true,
            (int) Math.min(Integer.MAX_VALUE, running),
            this.config.maxConcurrent(),
            this.queuedForLane(),
            submittedTasks,
            completedTasks,
            failedTasks,
            cancelledTasks,
            spilledTasks
        );
        return new SchedulerMetrics.LaneMetricsSnapshot(virtual);
    }

    /**
     * Returns how many virtual threads this lane currently has alive.
     *
     * <p>Diagnostics and leak tests use it; virtual threads do not show up in a normal thread dump.
     *
     * @return live virtual threads owned by this lane
     */
    int liveVirtualThreads() {
        return this.liveThreads.size();
    }

    /**
     * Checks whether every live lane thread is virtual.
     *
     * @return {@code true} when the lane only owns virtual threads
     */
    boolean onlyVirtualThreads() {
        for (final Thread thread : this.liveThreads) {
            if (!thread.isVirtual()) {
                return false;
            }
        }
        return true;
    }

    private void pumpLoop() {
        try {
            while (this.running && this.accepting.getAsBoolean()) {
                if (!this.permits.tryAcquire()) {
                    // every slot is busy; a finishing task signals and releases
                    this.awaitWork();
                    continue;
                }
                final QueueEntry entry;
                try {
                    entry = this.dispatcher.nextVirtual(this.spillOpen());
                } catch (Throwable failure) {
                    this.permits.release();
                    LOG.log(System.Logger.Level.ERROR, "virtual lane could not claim a task", failure);
                    break;
                }
                if (entry == null) {
                    // no eligible work, or a carried entry lost the quota race. either way a finishing
                    // task signals, and the bounded wait re-evaluates the spill decision; never spin.
                    this.permits.release();
                    this.awaitWork();
                    continue;
                }
                this.launch(entry);
            }
        } finally {
            this.pumpStopped = true;
            this.dispatcher.workerStoppedVirtual();
            this.signalTermination();
        }
    }

    private void launch(final QueueEntry entry) {
        final boolean spill = entry.priority() == ComputePriority.NORMAL;
        final long queueWaitNanos = Math.max(0L, this.clock.nanoTime() - entry.submittedAtNanos());
        this.started.increment();
        if (spill) {
            this.spilled.increment();
        }
        this.runningTasks.incrementAndGet();
        if (this.jfrEnabled) {
            this.emitStart(entry, queueWaitNanos, spill);
        }
        final Runnable body = () -> {
            try {
                this.runner.run(entry, LaneAwareQueue.Lane.VIRTUAL);
            } finally {
                this.recordOutcome(entry);
                this.runningTasks.decrementAndGet();
                this.permits.release();
                this.liveThreads.remove(Thread.currentThread());
                this.signalTermination();
                this.signalPump();
            }
        };
        final Thread thread;
        try {
            thread = this.virtualFactory.newThread(body);
        } catch (Throwable failure) {
            // no thread object at all: keep the claimed entry's invariant and run it on the pump
            LOG.log(System.Logger.Level.ERROR, "could not create a virtual thread, running inline", failure);
            body.run();
            return;
        }
        this.liveThreads.add(thread);
        try {
            thread.start();
        } catch (Throwable failure) {
            // the JVM could not start a virtual thread (extremely rare); falling back to running the
            // already claimed task on the pump keeps the invariant that a claimed entry is always
            // terminalized, instead of stranding its future and quota slot.
            this.liveThreads.remove(thread);
            LOG.log(System.Logger.Level.ERROR, "could not start a virtual thread, running inline", failure);
            body.run();
        }
    }

    private void recordOutcome(final QueueEntry entry) {
        final ComputeTaskHandle.State state = entry.handle().state();
        switch (state) {
            case COMPLETED -> this.completed.increment();
            case FAILED -> this.failed.increment();
            case CANCELLED -> this.cancelled.increment();
            default -> {
                // still queued or running cannot happen after the runner returned; nothing to count
            }
        }
    }

    private void emitStart(final QueueEntry entry, final long queueWaitNanos, final boolean spill) {
        try {
            this.recorder.virtualTask(
                entry.taskId(),
                entry.consumerId(),
                entry.priority(),
                queueWaitNanos,
                spill,
                this.runningTasks.get(),
                this.config.maxConcurrent(),
                entry.handle().label().orElse("")
            );
        } catch (Throwable failure) {
            LOG.log(System.Logger.Level.WARNING, "JFR rejected a virtual lane event", failure);
        }
    }

    boolean spillOpen() {
        if (!this.config.allowsSpill() || !this.platformSaturated()) {
            return false;
        }
        final int depth = this.queue.queuedAtLevel(this.normalLevel);
        if (depth <= 0) {
            return false;
        }
        if (this.config.spillQueueDepth() > 0 && depth >= this.config.spillQueueDepth()) {
            return true;
        }
        return this.queue.oldestWaitNanos(this.normalLevel, this.clock.nanoTime()) >= this.spillWaitNanos;
    }

    private boolean platformSaturated() {
        final int limit = this.pool.concurrencyLimit();
        return limit > 0 && this.pool.activeWorkers() >= limit;
    }

    private int queuedForLane() {
        int mask = 0;
        if (this.config.servesLow()) {
            mask |= 1 << ComputePriority.LOW.level();
        }
        if (this.spillOpen()) {
            mask |= 1 << this.normalLevel;
        }
        return this.queue.queuedInLane(LaneAwareQueue.Lane.VIRTUAL, mask);
    }

    private void awaitWork() {
        synchronized (this.workLock) {
            if (!this.running) {
                return;
            }
            try {
                this.workLock.wait(REEVALUATE_MILLIS);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                this.running = false;
            }
        }
    }

    private void signalPump() {
        synchronized (this.workLock) {
            this.workLock.notifyAll();
        }
    }

    private void signalTermination() {
        this.terminationLock.lock();
        try {
            if (this.pumpStopped) {
                this.terminated.signalAll();
            }
        } finally {
            this.terminationLock.unlock();
        }
    }
}
