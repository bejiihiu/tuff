package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BooleanSupplier;
import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputeConsumer;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.internal.engine.ConsumerRegistry;
import kz.bejiihiu.tuff.compute.internal.engine.Dispatcher;
import kz.bejiihiu.tuff.compute.internal.metrics.MetricsCollector;
import kz.bejiihiu.tuff.compute.internal.queue.ComputeQueue;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.queue.QuotaController;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
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

    private final AtomicReferenceArray<QueueEntry> carried;
    private final ConcurrentLinkedQueue<QueueEntry> orphans = new ConcurrentLinkedQueue<>();

    /**
     * Creates a dispatcher.
     *
     * @param settings      scheduler settings, used for the work stealing flag
     * @param queue         the scheduling queue
     * @param quota         consumer quota controller
     * @param metrics       metrics collector
     * @param registry      consumer registry
     * @param clock         clock used for expiry checks on carried entries
     * @param accepting     predicate that answers whether the engine still claims new work
     * @param adaptiveSink  sink fed with every finished task
     * @param workerCeiling maximum worker index plus one, used to size the carry slots
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
        final int workerCeiling
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
        this.carried = new AtomicReferenceArray<>(Preconditions.checkPositive(workerCeiling, "workerCeiling"));
    }

    @Override
    public @Nullable QueueEntry next(final int workerIndex) {
        Preconditions.checkAtLeast(workerIndex, 0, "workerIndex");
        if (!this.accepting.getAsBoolean()) {
            return null;
        }
        final QueueEntry carriedEntry = this.carried.getAndSet(workerIndex, null);
        if (carriedEntry != null) {
            return this.tryStart(carriedEntry, workerIndex);
        }
        final QueueEntry orphan = this.drainOrphan();
        if (orphan != null) {
            return orphan;
        }
        for (int attempt = 0; attempt < MAX_SELECTION_ATTEMPTS; attempt++) {
            if (!this.accepting.getAsBoolean()) {
                return null;
            }
            QueueEntry entry = this.queue.poll(this.quota);
            if (entry == null && this.workStealing) {
                entry = this.queue.steal(workerIndex, this.quota);
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
            this.carry(workerIndex, entry);
            return null;
        }
        return null;
    }

    @Override
    public void taskFinished(
        final QueueEntry entry,
        final long queueWaitNanos,
        final long executionNanos,
        final @Nullable Throwable failure
    ) {
        final TaskHandle<?> handle = handleOf(entry);
        handle.recordExecution(executionNanos);
        this.quota.release(entry.consumerId());
        if (failure == null) {
            this.accountSuccess(entry, handle, queueWaitNanos, executionNanos);
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
        this.accountOutcome(entry, handle, queueWaitNanos, executionNanos, outcome);
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
        final TaskHandle<?> handle = handleOf(entry);
        handle.recordExecution(executionNanos);
        this.quota.release(entry.consumerId());
        this.accountOutcome(entry, handle, queueWaitNanos, executionNanos, outcome);
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
        final TaskHandle<?> handle = handleOf(entry);
        if (handle.completeCancelled(reason)) {
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
        for (int index = 0; index < this.carried.length(); index++) {
            if (this.carried.compareAndSet(index, entry, null)) {
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
        for (int index = 0; index < this.carried.length(); index++) {
            final QueueEntry entry = this.carried.getAndSet(index, null);
            if (entry != null) {
                this.cancelQueued(entry, reason);
            }
        }
        QueueEntry orphan = this.orphans.poll();
        while (orphan != null) {
            this.cancelQueued(orphan, reason);
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
        for (int index = 0; index < this.carried.length(); index++) {
            final QueueEntry entry = this.carried.get(index);
            if (entry == null || !entry.consumerId().equals(consumerId)) {
                continue;
            }
            handleOf(entry).requestCooperativeCancel(reason);
            if (this.carried.compareAndSet(index, entry, null)) {
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
        for (int index = 0; index < this.carried.length(); index++) {
            final QueueEntry entry = this.carried.get(index);
            if (entry != null && this.isExpired(entry, nowNanos) && this.carried.compareAndSet(index, entry, null)) {
                this.cancelQueued(entry, ComputeCancelledException.Reason.QUEUE_EXPIRED);
            }
        }
        for (final QueueEntry orphan : new ArrayList<>(this.orphans)) {
            if (this.isExpired(orphan, nowNanos) && this.orphans.remove(orphan)) {
                this.cancelQueued(orphan, ComputeCancelledException.Reason.QUEUE_EXPIRED);
            }
        }
    }

    /**
     * Returns an entry owned by a worker that is going away to the orphan pool.
     *
     * @param workerIndex index of the exiting worker
     */
    void workerStopped(final int workerIndex) {
        final QueueEntry entry = this.carried.getAndSet(workerIndex, null);
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
        for (int index = 0; index < this.carried.length(); index++) {
            if (this.carried.get(index) != null) {
                total++;
            }
        }
        return total;
    }

    private void accountSuccess(
        final QueueEntry entry,
        final TaskHandle<?> handle,
        final long queueWaitNanos,
        final long executionNanos
    ) {
        final ComputeTaskHandle.State state = handle.state();
        if (state == ComputeTaskHandle.State.COMPLETED) {
            this.emitCompleted(entry, executionNanos);
            this.emitSample(queueWaitNanos, executionNanos, false);
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
        this.accountOutcome(entry, handle, queueWaitNanos, executionNanos, outcome);
    }

    private void accountOutcome(
        final QueueEntry entry,
        final TaskHandle<?> handle,
        final long queueWaitNanos,
        final long executionNanos,
        final TaskHandle.Completion outcome
    ) {
        switch (outcome) {
            case COMPLETED -> {
                this.emitCompleted(entry, executionNanos);
                this.emitSample(queueWaitNanos, executionNanos, false);
            }
            case FAILED -> {
                final Throwable failure = handle.failure();
                this.emitFailed(entry, executionNanos,
                    failure == null ? new IllegalStateException("task failed without a cause") : failure);
                this.emitSample(queueWaitNanos, executionNanos, true);
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

    private void emitSample(final long queueWaitNanos, final long executionNanos, final boolean failed) {
        try {
            this.adaptiveSink.onSample(queueWaitNanos, executionNanos, failed);
        } catch (Throwable failure) {
            LOG.log(System.Logger.Level.WARNING, "adaptive feed rejected a sample", failure);
        }
    }

    private @Nullable QueueEntry tryStart(final QueueEntry entry, final int workerIndex) {
        if (!this.accepting.getAsBoolean()) {
            this.cancelQueued(entry, ComputeCancelledException.Reason.SHUTDOWN);
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

    private @Nullable QueueEntry drainOrphan() {
        for (int remaining = this.orphans.size(); remaining > 0; remaining--) {
            final QueueEntry entry = this.orphans.poll();
            if (entry == null) {
                return null;
            }
            if (!this.accepting.getAsBoolean()) {
                this.cancelQueued(entry, ComputeCancelledException.Reason.SHUTDOWN);
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

    private void carry(final int workerIndex, final QueueEntry entry) {
        if (!this.carried.compareAndSet(workerIndex, null, entry)) {
            // another thread already left an entry there (only possible after an index reuse), so the
            // orphan pool is the safe owner.
            this.orphans.add(entry);
        }
    }

    private boolean isExpired(final QueueEntry entry, final long nowNanos) {
        final ConsumerPolicy policy = this.registry.find(entry.consumerId())
            .map(ComputeConsumer::policy)
            .orElse(null);
        if (policy == null || !policy.expiresQueuedTasks()) {
            return false;
        }
        return nowNanos - entry.submittedAtNanos() >= policy.maxQueueWait().toNanos();
    }

    private static TaskHandle<?> handleOf(final QueueEntry entry) {
        // the engine is the only producer of QueueEntry and it always binds a TaskHandle
        return (TaskHandle<?>) entry.handle();
    }
}
