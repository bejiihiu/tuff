package kz.bejiihiu.tuff.compute.internal.engine.impl;

import java.util.Objects;
import kz.bejiihiu.tuff.compute.internal.metrics.MetricsCollector;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.compute.internal.worker.WorkerLoop;
import org.jspecify.annotations.Nullable;

/**
 * One worker iteration: claim, start, execute, finish.
 *
 * <p>The loop performs exactly one task attempt per call and returns {@code false} when the
 * dispatcher has nothing runnable, which is the signal the pool uses to apply keep-alive. Every
 * claimed entry is terminalized in this method, including when the body throws or when the worker is
 * cancelled before the body starts, because the pool never inspects entries and never cancels them.
 */
final class EngineWorkerLoop implements WorkerLoop {

    private static final System.Logger LOG = System.getLogger(EngineWorkerLoop.class.getName());

    private static final String NULL_RESULT_MESSAGE =
        "compute task returned null; a task must produce a value (use Optional when absence is meaningful)";

    private final DispatcherImpl dispatcher;
    private final MetricsCollector metrics;
    private final ComputeClock clock;

    /**
     * Creates the loop.
     *
     * @param dispatcher dispatcher that owns the queue and the quotas
     * @param metrics    metrics collector
     * @param clock      clock used for queue wait and execution measurement
     */
    EngineWorkerLoop(final DispatcherImpl dispatcher, final MetricsCollector metrics, final ComputeClock clock) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public boolean executeNext(final int workerIndex) {
        final QueueEntry entry = this.dispatcher.next(workerIndex);
        if (entry == null) {
            return false;
        }
        final TaskHandle<?> handle = (TaskHandle<?>) entry.handle();
        if (!handle.markRunning()) {
            // cancellation (caller, unregister or shutdown) won before the body started; the slot is
            // held by this worker, so taskCancelled releases it and counts the task exactly once.
            this.dispatcher.taskCancelled(entry, handle.cancelReason());
            return true;
        }
        final long startNanos = this.clock.nanoTime();
        final long queueWaitNanos = Math.max(0L, startNanos - entry.submittedAtNanos());
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
                this.dispatcher.taskFinished(entry, queueWaitNanos, executionNanos, failure);
                terminalized = true;
            } else {
                final TaskHandle.Completion outcome = handle.finish(value, null);
                if (outcome == null) {
                    // the cancellation flag won before the completion; complete it as cancelled on
                    // this thread so the metrics bookkeeping stays on one thread
                    this.dispatcher.taskCancelled(entry, handle.cancelReason());
                } else {
                    this.dispatcher.taskCompleted(entry, queueWaitNanos, executionNanos, outcome);
                }
                terminalized = true;
            }
        } catch (Throwable escaped) {
            LOG.log(System.Logger.Level.ERROR, "compute worker failed while terminalizing task " + entry.taskId(), escaped);
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
        return true;
    }

    private void recordStarted(final QueueEntry entry, final long queueWaitNanos) {
        try {
            this.metrics.onStarted(entry, queueWaitNanos);
        } catch (Throwable failure) {
            // metrics are diagnostic; a broken metric must not fail the task
            LOG.log(System.Logger.Level.WARNING, "metrics rejected a start for task " + entry.taskId(), failure);
        }
    }

    @Override
    public void workerStarted(final int workerIndex) {
        // nothing per worker yet; the hook stays here so the pool contract is implemented locally
    }

    @Override
    public void workerStopped(final int workerIndex, final @Nullable Throwable cause) {
        this.dispatcher.workerStopped(workerIndex);
    }
}
