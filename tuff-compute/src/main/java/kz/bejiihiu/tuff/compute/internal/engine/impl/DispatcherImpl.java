package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.internal.engine.ConsumerRegistry;
import kz.bejiihiu.tuff.compute.internal.engine.Dispatcher;
import kz.bejiihiu.tuff.compute.internal.metrics.MetricsCollector;
import kz.bejiihiu.tuff.compute.internal.queue.ComputeQueue;
import kz.bejiihiu.tuff.compute.internal.queue.LaneAwareQueue;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.queue.QuotaController;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.compute.internal.util.Durations;
import kz.bejiihiu.tuff.compute.internal.util.Preconditions;
import org.jspecify.annotations.Nullable;

/**
 * Binds the queue, the consumer quotas, the metrics collector and the adaptive feed together.
 *
 * <p>The interesting part is the race between {@link ComputeQueue#poll(ComputeQueue.ConsumerAdmission)}
 * and {@link QuotaController#tryAcquire(String)}. The queue checks admission as a read-only
 * predicate for every consumer head before it picks the best one, so acquiring inside the admission
 * object would grab several slots per pass with no release hook. The dispatcher therefore polls with
 * the quota as a pure predicate and then tries to acquire the slot for the chosen entry. When that
 * acquire loses the race against another worker, the entry has already left the queue and must not be
 * dropped, re-offered (a re-offer can evict an innocent entry under drop-oldest and can resurrect a
 * retired consumer) or spun on.
 *
 * <p>Instead every worker owns exactly one carry slot: the polled-but-unreserved entry stays with
 * that worker and is retried at the start of its next {@code next()} call. That bounds the number of
 * out-of-queue entries by the worker count, never blocks other workers from polling, and keeps the
 * entry's sequence and submission timestamp so aging and FIFO survive. If a worker dies while
 * carrying, the entry moves to a tiny orphan queue that any worker or the janitor can consume.
 */
final class DispatcherImpl implements Dispatcher {

    private static final System.Logger LOG = System.getLogger(DispatcherImpl.class.getName());

    /**
     * How many cancelled entries one {@code next()} call may skip before returning to the pool. The
     * worker loop is hot, so it should not loop forever on a queue full of already cancelled work.
     */
    private static final int MAX_SELECTION_ATTEMPTS = 8;

    /**
     * Carry slot of the virtual pump. Platform workers use their own worker index as the slot, so
     * the map key space is {@code -1..workerCeiling-1}.
     */
    private static final int VIRTUAL_SLOT = -1;

    /**
     * Sink for finished-task samples, implemented by the engine so the dispatcher does not have to
     * know whether adaptive concurrency is configured.
     */
    @FunctionalInterface
    interface AdaptiveSampleSink {

        /**
         * Records one finished task sample.
         *
         * @param queueWaitNanos how long the task waited
         * @param executionNanos how long the task body ran
         * @param failed         whether the task failed
         */
        void onSample(long queueWaitNanos, long executionNanos, boolean failed);
    }

    private final ComputeQueue queue;
    private final QuotaController quota;
    private final MetricsCollector metrics;
    private final ConsumerRegistry registry;
    private final ComputeClock clock;
    private final BooleanSupplier accepting;
    private final AdaptiveSampleSink adaptiveSink;
    private final boolean workStealing;

    /**
     * Lane-aware view of the queue, or {@code null} for a plain {@link ComputeQueue}. The engine
     * always installs the lane-aware implementation; the fallback keeps the dispatcher usable with a
     * queue that only implements the base contract.
     */
    private final @Nullable LaneAwareQueue laneQueue;

    /**
     * Priority levels a platform worker may take.
     */
    private final int platformMask;

    /**
     * LOW bit when the virtual lane owns LOW work, otherwise {@code 0}.
     */
    private final int virtualLowMask;

    /**
     * Whether NORMAL work may spill onto the virtual lane.
     */
    private final boolean spillEnabled;

    /**
     * Whether the dispatcher reserves a carry slot for the virtual pump.
     */
    private final boolean virtualSlot;

    /**
     * Per slot carry cells: platform worker index or {@link #VIRTUAL_SLOT} as the key.
     *
     * <p>A plain array would have to be sized by the worker ceiling as it was known at
     * construction, and that ceiling can grow when the processor budget does. A map grows with the
     * pool without ever losing an in flight carry, which an array swap could: a worker that captured
     * the old array reference would leave its entry invisible to every later scan.
     */
    private final ConcurrentHashMap<Integer, AtomicReference<QueueEntry>> carried = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<QueueEntry> orphans = new ConcurrentLinkedQueue<>();

    /**
     * Creates a dispatcher.
     *
     * @param settings        scheduler settings, used for the work stealing flag
     * @param queue           the scheduling queue
     * @param quota           consumer quota controller
     * @param metrics         metrics collector
     * @param registry        consumer registry
     * @param clock           clock used for expiry checks on carried entries
     * @param accepting       predicate that answers whether the engine still claims new work
     * @param adaptiveSink    sink fed with every finished task
     * @param platformMask    priority levels a platform worker may take
     * @param virtualLowMask  LOW bit when the virtual lane owns LOW work, otherwise {@code 0}
     * @param spillEnabled    whether NORMAL work may spill onto the virtual lane
     * @param virtualSlots    carry slots reserved for the virtual pump, {@code 0} without a lane
     */
    DispatcherImpl(
        final ComputeSettings settings,
        final ComputeQueue queue,
        final QuotaController quota,
        final MetricsCollector metrics,
        final ConsumerRegistry registry,
        final ComputeClock clock,
        final BooleanSupplier accepting,
        final AdaptiveSampleSink adaptiveSink,
        final int platformMask,
        final int virtualLowMask,
        final boolean spillEnabled,
        final int virtualSlots
    ) {
        Preconditions.requireNonNull(settings, "settings");
        this.queue = Preconditions.requireNonNull(queue, "queue");
        this.quota = Preconditions.requireNonNull(quota, "quota");
        this.metrics = Preconditions.requireNonNull(metrics, "metrics");
        this.registry = Preconditions.requireNonNull(registry, "registry");
        this.clock = Preconditions.requireNonNull(clock, "clock");
        this.accepting = Preconditions.requireNonNull(accepting, "accepting");
        this.adaptiveSink = Preconditions.requireNonNull(adaptiveSink, "adaptiveSink");
        this.workStealing = settings.scheduling().workStealing();
        this.laneQueue = queue instanceof LaneAwareQueue aware ? aware : null;
        this.platformMask = platformMask;
        this.virtualLowMask = virtualLowMask;
        this.spillEnabled = spillEnabled;
        Preconditions.checkAtLeast(virtualSlots, 0, "virtualSlots");
        this.virtualSlot = virtualSlots > 0;
    }

    @Override
    public @Nullable QueueEntry next(final int workerIndex) {
        Preconditions.checkAtLeast(workerIndex, 0, "workerIndex");
        return this.claim(workerIndex, LaneAwareQueue.Lane.PLATFORM, this.platformMask);
    }

    /**
     * Claims the next entry the virtual lane may run.
     *
     * <p>Uses the same carry slot, admission and quota dance as {@link #next(int)}, so a virtual
     * task holds a consumer quota slot exactly like a platform worker. The lane mask is LOW when the
     * lane owns LOW work, plus NORMAL when {@code spillOpen} is true.
     *
     * @param spillOpen whether NORMAL work is currently allowed to spill onto the virtual lane
     * @return the claimed entry with its consumer slot already reserved, or {@code null}
     */
    @Nullable QueueEntry nextVirtual(final boolean spillOpen) {
        if (!this.virtualSlot) {
            return null;
        }
        int mask = this.virtualLowMask;
        if (spillOpen && this.spillEnabled) {
            mask |= 1 << ComputePriority.NORMAL.level();
        }
        if (mask == 0) {
            return null;
        }
        return this.claim(VIRTUAL_SLOT, LaneAwareQueue.Lane.VIRTUAL, mask);
    }

    /**
     * Releases the virtual carry slot when the pump goes away.
     *
     * <p>A carried entry has already left the queue, so it moves to the orphan pool where the janitor
     * and the next lane pump can terminalize or run it.
     */
    void workerStoppedVirtual() {
        if (this.virtualSlot) {
            this.workerStopped(VIRTUAL_SLOT);
        }
    }

    private @Nullable QueueEntry claim(final int index, final LaneAwareQueue.Lane lane, final int levelMask) {
        if (!this.accepting.getAsBoolean()) {
            return null;
        }
        final QueueEntry carriedEntry = this.takeCarried(index);
        if (carriedEntry != null) {
            return this.tryStart(carriedEntry, index, levelMask);
        }
        final QueueEntry orphan = this.drainOrphan(index, levelMask);
        if (orphan != null) {
            return orphan;
        }
        for (int attempt = 0; attempt < MAX_SELECTION_ATTEMPTS; attempt++) {
            if (!this.accepting.getAsBoolean()) {
                return null;
            }
            QueueEntry entry = this.poll(lane, levelMask);
            if (entry == null && this.workStealing) {
                entry = this.steal(index, lane, levelMask);
            }
            if (entry == null) {
                return null;
            }
            if (!this.accepting.getAsBoolean()) {
                // the engine started shutting down between the poll and now: the entry is out of the
                // queue, so this thread owns it and must terminalize it.
                this.cancelQueued(entry, ComputeCancelledException.Reason.SHUTDOWN);
                return null;
            }
            final TaskHandle<?> handle = handleOf(entry);
            if (handle.isCancellationRequested()) {
                this.cancelQueued(entry, handle.cancelReason());
                continue;
            }
            if (this.registry.find(entry.consumerId()).isEmpty()) {
                // the consumer was unregistered while the entry was being polled
                this.cancelQueued(entry, ComputeCancelledException.Reason.CONSUMER_DISABLED);
                continue;
            }
            if (this.quota.tryAcquire(entry.consumerId())) {
                return entry;
            }
            // lost the quota race: keep ownership in this worker's carry slot and retry next time
            this.carry(index, entry);
            return null;
        }
        return null;
    }

    private @Nullable QueueEntry poll(final LaneAwareQueue.Lane lane, final int levelMask) {
        final LaneAwareQueue aware = this.laneQueue;
        return aware == null ? this.queue.poll(this.quota) : aware.poll(lane, levelMask, this.quota);
    }

    private @Nullable QueueEntry steal(final int index, final LaneAwareQueue.Lane lane, final int levelMask) {
        final LaneAwareQueue aware = this.laneQueue;
        return aware == null ? this.queue.steal(index, this.quota) : aware.steal(index, lane, levelMask, this.quota);
    }

    @Override
    public void taskFinished(
        final QueueEntry entry,
        final long queueWaitNanos,
        final long executionNanos,
        final @Nullable Throwable failure
    ) {
        this.taskFinished(entry, queueWaitNanos, executionNanos, failure, true);
    }

    /**
     * Completes a task that ran to an end and optionally feeds the adaptive controller.
     *
     * <p>Virtual lane tasks call this with {@code feedAdaptive = false}: their execution time is
     * dominated by blocking and would distort the platform pool's capacity model.
     *
     * @param entry          the finished entry
     * @param queueWaitNanos how long it waited before starting
     * @param executionNanos how long the task body ran
     * @param failure        throwable thrown by the task, or {@code null} on success
     * @param feedAdaptive   whether the sample may influence the adaptive controller
     */
    void taskFinished(
        final QueueEntry entry,
        final long queueWaitNanos,
        final long executionNanos,
        final @Nullable Throwable failure,
        final boolean feedAdaptive
    ) {
        final TaskHandle<?> handle = handleOf(entry);
        handle.recordExecution(executionNanos);
        this.quota.release(entry.consumerId());
        if (failure == null) {
            this.accountSuccess(entry, handle, queueWaitNanos, executionNanos, feedAdaptive);
            return;
        }
        final TaskHandle.Completion outcome = handle.finish(null, failure);
        if (outcome == null) {
            if (handle.completeCancelled(handle.cancelReason())) {
                // the task was cancelled while it was failing; the cancellation wins, counted once
                this.emitCancelled(entry, handle.cancelReason());
            }
            return;
        }
        this.accountOutcome(entry, handle, queueWaitNanos, executionNanos, outcome, feedAdaptive);
    }

    /**
     * Accounts a task whose handle was already completed by the worker loop.
     *
     * <p>Split from {@link #taskFinished(QueueEntry, long, long, Throwable)} because the dispatcher
     * contract does not carry the produced value: the worker completes the future with it and then
     * asks the dispatcher for the exactly-once metrics accounting.
     *
     * @param entry          finished entry
     * @param queueWaitNanos how long it waited
     * @param executionNanos how long the body ran
     * @param outcome        how the handle ended
     */
    void taskCompleted(
        final QueueEntry entry,
        final long queueWaitNanos,
        final long executionNanos,
        final TaskHandle.Completion outcome
    ) {
        this.taskCompleted(entry, queueWaitNanos, executionNanos, outcome, true);
    }

    /**
     * Accounts a task whose handle was already completed and optionally feeds the adaptive controller.
     *
     * @param entry          finished entry
     * @param queueWaitNanos how long it waited
     * @param executionNanos how long the body ran
     * @param outcome        how the handle ended
     * @param feedAdaptive   whether the sample may influence the adaptive controller
     */
    void taskCompleted(
        final QueueEntry entry,
        final long queueWaitNanos,
        final long executionNanos,
        final TaskHandle.Completion outcome,
        final boolean feedAdaptive
    ) {
        final TaskHandle<?> handle = handleOf(entry);
        handle.recordExecution(executionNanos);
        this.quota.release(entry.consumerId());
        this.accountOutcome(entry, handle, queueWaitNanos, executionNanos, outcome, feedAdaptive);
    }

    @Override
    public void taskCancelled(final QueueEntry entry, final ComputeCancelledException.Reason reason) {
        this.quota.release(entry.consumerId());
        this.cancelQueued(entry, reason);
    }

    @Override
    public int queuedTasks() {
        return this.queue.size();
    }

    /**
     * Terminalizes one entry that never held a quota slot.
     *
     * <p>Used for queue cancellation, expiry, eviction, consumer removal and shutdown. The handle's
     * terminal compare and set is what guarantees exactly one metrics event: a second caller that
     * lost the race does nothing.
     *
     * @param entry  entry to cancel
     * @param reason why it is cancelled
     */
    void cancelQueued(final QueueEntry entry, final ComputeCancelledException.Reason reason) {
        this.cancelQueued(entry, reason, false);
    }

    /**
     * Terminalizes one entry and optionally moves the future completion off the calling thread.
     *
     * <p>Service threads (janitor, shutdown) and the eviction path must pass {@code true}: dependent
     * stages run on the completing thread and user code may block there.
     *
     * @param entry    entry to cancel
     * @param reason   why it is cancelled
     * @param deferred whether the future completion may be deferred to the completion path
     */
    void cancelQueued(
        final QueueEntry entry,
        final ComputeCancelledException.Reason reason,
        final boolean deferred
    ) {
        final TaskHandle<?> handle = handleOf(entry);
        if (handle.completeCancelled(reason, deferred)) {
            this.emitCancelled(entry, reason);
        }
    }

    /**
     * Removes a specific entry from the carry slots or the orphan queue.
     *
     * <p>Used by cancellation: the caller completes the entry itself when this returns {@code true}.
     *
     * @param entry entry to remove
     * @return {@code true} if this call took ownership of the entry
     */
    boolean removeCarried(final QueueEntry entry) {
        for (final AtomicReference<QueueEntry> cell : this.carried.values()) {
            if (cell.compareAndSet(entry, null)) {
                return true;
            }
        }
        return this.orphans.remove(entry);
    }

    /**
     * Cancels every entry the dispatcher still owns outside the queue.
     *
     * @param reason why the entries are cancelled
     */
    void cancelAllCarried(final ComputeCancelledException.Reason reason) {
        this.cancelAllCarried(reason, false);
    }

    /**
     * Cancels every carried or orphaned entry, optionally deferring the future completion.
     *
     * @param reason   why the entries are cancelled
     * @param deferred whether the future completion may leave the calling thread
     */
    void cancelAllCarried(final ComputeCancelledException.Reason reason, final boolean deferred) {
        for (final AtomicReference<QueueEntry> cell : this.carried.values()) {
            final QueueEntry entry = cell.getAndSet(null);
            if (entry != null) {
                this.cancelQueued(entry, reason, deferred);
            }
        }
        QueueEntry orphan = this.orphans.poll();
        while (orphan != null) {
            this.cancelQueued(orphan, reason, deferred);
            orphan = this.orphans.poll();
        }
    }

    /**
     * Cancels and flags every carried entry of one consumer.
     *
     * <p>Entries not owned by the dispatcher right now (a worker is between the acquire and the
     * start) are only flagged; the worker loop notices the flag and terminalizes them.
     *
     * @param consumerId consumer that was unregistered
     * @param reason     cancellation reason
     */
    void cancelCarriedConsumer(final String consumerId, final ComputeCancelledException.Reason reason) {
        for (final AtomicReference<QueueEntry> cell : this.carried.values()) {
            final QueueEntry entry = cell.get();
            if (entry == null || !entry.consumerId().equals(consumerId)) {
                continue;
            }
            handleOf(entry).requestCooperativeCancel(reason);
            if (cell.compareAndSet(entry, null)) {
                this.cancelQueued(entry, reason);
            }
        }
        for (final QueueEntry orphan : new ArrayList<>(this.orphans)) {
            if (!orphan.consumerId().equals(consumerId)) {
                continue;
            }
            handleOf(orphan).requestCooperativeCancel(reason);
            if (this.orphans.remove(orphan)) {
                this.cancelQueued(orphan, reason);
            }
        }
    }

    /**
     * Expires carried and orphaned entries whose queue wait limit passed.
     *
     * <p>Called by the janitor. The queue itself sweeps the queued entries; entries that left the
     * queue but never started are the dispatcher's responsibility.
     *
     * @param nowNanos current clock reading
     */
    void sweepCarried(final long nowNanos) {
        this.sweepCarried(nowNanos, false);
    }

    /**
     * Expires carried and orphaned entries, optionally deferring the future completion.
     *
     * @param nowNanos current clock reading
     * @param deferred whether the future completion may leave the calling thread
     */
    void sweepCarried(final long nowNanos, final boolean deferred) {
        for (final AtomicReference<QueueEntry> cell : this.carried.values()) {
            final QueueEntry entry = cell.get();
            if (entry != null && this.isExpired(entry, nowNanos) && cell.compareAndSet(entry, null)) {
                this.cancelQueued(entry, ComputeCancelledException.Reason.QUEUE_EXPIRED, deferred);
            }
        }
        for (final QueueEntry orphan : new ArrayList<>(this.orphans)) {
            if (this.isExpired(orphan, nowNanos) && this.orphans.remove(orphan)) {
                this.cancelQueued(orphan, ComputeCancelledException.Reason.QUEUE_EXPIRED, deferred);
            }
        }
    }

    /**
     * Returns an entry owned by a worker that is going away to the orphan pool.
     *
     * @param workerIndex index of the exiting worker
     */
    void workerStopped(final int workerIndex) {
        final QueueEntry entry = this.takeCarried(workerIndex);
        if (entry != null) {
            this.orphans.add(entry);
        }
    }

    /**
     * Returns how many entries currently sit outside the queue in carry slots or the orphan pool.
     *
     * @return dispatcher-owned entries that have not started
     */
    int carriedEntries() {
        int total = this.orphans.size();
        for (final AtomicReference<QueueEntry> cell : this.carried.values()) {
            if (cell.get() != null) {
                total++;
            }
        }
        return total;
    }

    /**
     * Returns how many entries platform workers currently carry.
     *
     * <p>The virtual carry slot is excluded, so platform autoscaling never reacts to work the pool
     * cannot run.
     *
     * @return platform-owned entries that have not started
     */
    int platformCarriedEntries() {
        int total = 0;
        for (final Map.Entry<Integer, AtomicReference<QueueEntry>> slot : this.carried.entrySet()) {
            if (slot.getKey() != VIRTUAL_SLOT && slot.getValue().get() != null) {
                total++;
            }
        }
        return total;
    }

    private void accountSuccess(
        final QueueEntry entry,
        final TaskHandle<?> handle,
        final long queueWaitNanos,
        final long executionNanos,
        final boolean feedAdaptive
    ) {
        final ComputeTaskHandle.State state = handle.state();
        if (state == ComputeTaskHandle.State.COMPLETED) {
            this.emitCompleted(entry, executionNanos);
            this.emitSample(queueWaitNanos, executionNanos, false, feedAdaptive);
            return;
        }
        if (state == ComputeTaskHandle.State.CANCELLED) {
            this.emitCancelled(entry, handle.cancelReason());
            return;
        }
        final IllegalStateException bug = new IllegalStateException(
            "task " + entry.taskId() + " of consumer " + entry.consumerId() + " finished without a result"
        );
        final TaskHandle.Completion outcome = handle.finish(null, bug);
        if (outcome == null) {
            if (handle.completeCancelled(handle.cancelReason())) {
                this.emitCancelled(entry, handle.cancelReason());
            }
            return;
        }
        this.accountOutcome(entry, handle, queueWaitNanos, executionNanos, outcome, feedAdaptive);
    }

    private void accountOutcome(
        final QueueEntry entry,
        final TaskHandle<?> handle,
        final long queueWaitNanos,
        final long executionNanos,
        final TaskHandle.Completion outcome,
        final boolean feedAdaptive
    ) {
        switch (outcome) {
            case COMPLETED -> {
                this.emitCompleted(entry, executionNanos);
                this.emitSample(queueWaitNanos, executionNanos, false, feedAdaptive);
            }
            case FAILED -> {
                final Throwable failure = handle.failure();
                this.emitFailed(entry, executionNanos,
                    failure == null ? new IllegalStateException("task failed without a cause") : failure);
                this.emitSample(queueWaitNanos, executionNanos, true, feedAdaptive);
            }
            case CANCELLED -> this.emitCancelled(entry, handle.cancelReason());
        }
    }

    private void emitCompleted(final QueueEntry entry, final long executionNanos) {
        try {
            this.metrics.onCompleted(entry, executionNanos);
        } catch (Throwable failure) {
            LOG.log(System.Logger.Level.WARNING, "metrics rejected a completion for task " + entry.taskId(), failure);
        }
    }

    private void emitFailed(final QueueEntry entry, final long executionNanos, final Throwable cause) {
        try {
            this.metrics.onFailed(entry, executionNanos, cause);
        } catch (Throwable failure) {
            LOG.log(System.Logger.Level.WARNING, "metrics rejected a failure for task " + entry.taskId(), failure);
        }
    }

    private void emitCancelled(final QueueEntry entry, final ComputeCancelledException.Reason reason) {
        try {
            this.metrics.onCancelled(entry, reason);
        } catch (Throwable failure) {
            LOG.log(System.Logger.Level.WARNING, "metrics rejected a cancellation for task " + entry.taskId(), failure);
        }
    }

    private void emitSample(
        final long queueWaitNanos,
        final long executionNanos,
        final boolean failed,
        final boolean feedAdaptive
    ) {
        if (!feedAdaptive) {
            return;
        }
        try {
            this.adaptiveSink.onSample(queueWaitNanos, executionNanos, failed);
        } catch (Throwable failure) {
            LOG.log(System.Logger.Level.WARNING, "adaptive feed rejected a sample", failure);
        }
    }

    private @Nullable QueueEntry tryStart(final QueueEntry entry, final int workerIndex, final int levelMask) {
        if (!this.accepting.getAsBoolean()) {
            this.cancelQueued(entry, ComputeCancelledException.Reason.SHUTDOWN);
            return null;
        }
        if (!laneAllows(entry, levelMask)) {
            // a carried entry never changes priority, so this can only happen after a topology change;
            // hand it to the lane that owns it instead of running it on the wrong one
            this.orphans.add(entry);
            return null;
        }
        final TaskHandle<?> handle = handleOf(entry);
        if (handle.isCancellationRequested()) {
            this.cancelQueued(entry, handle.cancelReason());
            return null;
        }
        if (this.registry.find(entry.consumerId()).isEmpty()) {
            this.cancelQueued(entry, ComputeCancelledException.Reason.CONSUMER_DISABLED);
            return null;
        }
        if (this.quota.tryAcquire(entry.consumerId())) {
            return entry;
        }
        this.carry(workerIndex, entry);
        return null;
    }

    private @Nullable QueueEntry drainOrphan(final int workerIndex, final int levelMask) {
        for (int remaining = this.orphans.size(); remaining > 0; remaining--) {
            final QueueEntry entry = this.orphans.poll();
            if (entry == null) {
                return null;
            }
            if (!this.accepting.getAsBoolean()) {
                this.cancelQueued(entry, ComputeCancelledException.Reason.SHUTDOWN);
                continue;
            }
            if (!laneAllows(entry, levelMask)) {
                // leave it for the lane that owns its priority; the bounded loop prevents a spin
                this.orphans.add(entry);
                continue;
            }
            final TaskHandle<?> handle = handleOf(entry);
            if (handle.isCancellationRequested()) {
                this.cancelQueued(entry, handle.cancelReason());
                continue;
            }
            if (this.registry.find(entry.consumerId()).isEmpty()) {
                this.cancelQueued(entry, ComputeCancelledException.Reason.CONSUMER_DISABLED);
                continue;
            }
            if (this.quota.tryAcquire(entry.consumerId())) {
                return entry;
            }
            this.orphans.add(entry);
        }
        return null;
    }

    private static boolean laneAllows(final QueueEntry entry, final int levelMask) {
        return (levelMask & (1 << entry.priority().level())) != 0;
    }

    private @Nullable QueueEntry takeCarried(final int slot) {
        final AtomicReference<QueueEntry> cell = this.carried.get(slot);
        return cell == null ? null : cell.getAndSet(null);
    }

    private void carry(final int workerIndex, final QueueEntry entry) {
        if (!this.cell(workerIndex).compareAndSet(null, entry)) {
            // another thread already left an entry there (only possible after an index reuse), so the
            // orphan pool is the safe owner.
            this.orphans.add(entry);
        }
    }

    private AtomicReference<QueueEntry> cell(final int slot) {
        return this.carried.computeIfAbsent(slot, _ -> new AtomicReference<>());
    }

    private boolean isExpired(final QueueEntry entry, final long nowNanos) {
        final ConsumerPolicy policy = this.registry.find(entry.consumerId())
            .map(ComputeConsumer::policy)
            .orElse(null);
        if (policy == null || !policy.expiresQueuedTasks()) {
            return false;
        }
        return nowNanos - entry.submittedAtNanos() >= Durations.toNanosSaturated(policy.maxQueueWait());
    }

    private static TaskHandle<?> handleOf(final QueueEntry entry) {
        // the engine is the only producer of QueueEntry and it always binds a TaskHandle
        return (TaskHandle<?>) entry.handle();
    }
}
