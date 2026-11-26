package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.util.Objects;
import kz.bejiihiu.tuff.compute.internal.metrics.MetricsCollector;
import kz.bejiihiu.tuff.compute.internal.queue.LaneAwareQueue;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;

/**
 * Runs one already claimed entry and terminalizes it exactly once.
 *
 * <p>Both execution lanes share this code: a platform worker calls it after
 * {@link DispatcherImpl#next(int)}, the virtual lane calls it on a virtual thread after
 * {@link DispatcherImpl#nextVirtual(boolean)}. Keeping the claim bookkeeping, the null result rule,
 * the cooperative cancellation handshake and the terminal accounting in one place is what lets the
 * two lanes differ in <em>where</em> a task runs without drifting in <em>how</em> it ends.
 *
 * <p>The caller owns the entry and its consumer quota slot before calling {@link #run(QueueEntry)}.
 * When this method returns the slot has been released and the handle has reached a terminal state,
 * including when the task body throws or when cancellation won before the body started.
 *
 * <p>Terminal ordering is fixed at the ends: the terminal metrics event is always emitted last, after
 * the handle published its result (or cancellation) and after the dispatcher released the consumer
 * quota slot. The middle two are ordered differently on the success and failure paths, which is why
 * neither may be used as a "metrics are current now" signal. A completed future therefore does not
 * imply that {@code metrics()} already moved, so anything that reads counters right after
 * {@code join()} has to wait for them. The order exists because a slot must never be free while the
 * handle still looks pending, and because the metrics accounting must stay on the thread that saw
 * {@code onStarted} (the collector's derived queue depth depends on it).
 */
final class EngineTaskRunner {

    private static final System.Logger LOG = System.getLogger(EngineTaskRunner.class.getName());

    private static final String NULL_RESULT_MESSAGE =
        "compute task returned null; a task must produce a value (use Optional when absence is meaningful)";

    private final DispatcherImpl dispatcher;
    private final MetricsCollector metrics;
    private final ComputeClock clock;

    /**
     * Creates a runner.
     *
     * @param dispatcher dispatcher that owns the queue and the quotas
     * @param metrics    metrics collector
     * @param clock      clock used for queue wait and execution measurement
     */
    EngineTaskRunner(final DispatcherImpl dispatcher, final MetricsCollector metrics, final ComputeClock clock) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Executes a claimed entry.
     *
     * @param entry entry whose consumer quota slot is already reserved
     * @param lane  execution lane the caller runs the entry on
     */
    void run(final QueueEntry entry, final LaneAwareQueue.Lane lane) {
        final TaskHandle<?> handle = (TaskHandle<?>) entry.handle();
        final boolean platform = lane == LaneAwareQueue.Lane.PLATFORM;
        final String laneName = platform ? "platform" : "virtual";
        final long startNanos = this.clock.nanoTime();
        final long queueWaitNanos = Math.max(0L, startNanos - entry.submittedAtNanos());
        if (!handle.markRunning(laneName, startNanos, this.clock.instant())) {
            // cancellation (caller, unregister or shutdown) won before the body started; the slot is
            // held by this runner, so taskCancelled releases it and counts the task exactly once.
            this.dispatcher.taskCancelled(entry, handle.cancelReason());
            return;
        }
        handle.recordStart(queueWaitNanos);
        boolean terminalized = false;
        try {
            this.recordStarted(entry, queueWaitNanos);
            Object value = null;
            Throwable failure = null;
            try {
                value = entry.execute();
                if (value == null) {
                    failure = new NullPointerException(NULL_RESULT_MESSAGE);
                }
            } catch (Throwable thrown) {
                // a task must never kill the worker, not even with an Error
                failure = thrown;
            }
            final long executionNanos = Math.max(0L, this.clock.nanoTime() - startNanos);
            handle.recordExecution(executionNanos);
            if (failure != null) {
                this.dispatcher.taskFinished(entry, queueWaitNanos, executionNanos, failure, platform);
                terminalized = true;
            } else {
                final TaskHandle.Completion outcome = handle.finish(value, null);
                if (outcome == null) {
                    // the cancellation flag won before the completion; complete it as cancelled on
                    // this thread so the metrics bookkeeping stays on one thread
                    this.dispatcher.taskCancelled(entry, handle.cancelReason());
                } else {
                    this.dispatcher.taskCompleted(entry, queueWaitNanos, executionNanos, outcome, platform);
                }
                terminalized = true;
            }
        } catch (Throwable escaped) {
            LOG.log(System.Logger.Level.ERROR, "compute runner failed while terminalizing task " + entry.taskId(), escaped);
        } finally {
            if (!terminalized) {
                // last resort: the entry must not stay pending with a reserved slot
                try {
                    this.dispatcher.taskCancelled(entry, handle.cancelReason());
                } catch (Throwable escaped) {
                    LOG.log(System.Logger.Level.ERROR, "could not terminalize task " + entry.taskId(), escaped);
                }
            }
        }
    }

    private void recordStarted(final QueueEntry entry, final long queueWaitNanos) {
        try {
            this.metrics.onStarted(entry, queueWaitNanos);
        } catch (Throwable failure) {
            // metrics are diagnostic; a broken metric must not fail the task
            LOG.log(System.Logger.Level.WARNING, "metrics rejected a start for task " + entry.taskId(), failure);
        }
    }
}
