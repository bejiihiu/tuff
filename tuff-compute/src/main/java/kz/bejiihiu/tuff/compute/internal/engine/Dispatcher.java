package kz.bejiihiu.tuff.compute.internal.engine;

import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.internal.queue.QueueEntry;
import org.jspecify.annotations.Nullable;

/**
 * Picks the next task and does the bookkeeping around its execution.
 *
 * <p>The dispatcher is where queue order, consumer quotas and the concurrency limit meet. It is the
 * only component that decides "this entry may run now", and it is responsible for making sure that
 * every started task ends up completed exactly once.
 *
 * <p>Contract details the worker loop relies on:
 * <ul>
 *     <li>{@link #next(int)} never returns an entry whose consumer quota it did not reserve; a
 *     returned entry always has a slot held for its consumer;</li>
 *     <li>every returned entry is followed by exactly one call to {@link #taskFinished} or
 *     {@link #taskCancelled}, which releases the slot;</li>
 *     <li>both methods complete the task's handle, so the caller's future never stays pending;</li>
 *     <li>neither method throws for task failures, they are normal outcomes.</li>
 * </ul>
 */
public interface Dispatcher {

    /**
     * Claims the next task that may run on the calling worker.
     *
     * <p>Consults the queue with the quota controller as admission check, preferring the calling
     * worker's own batch and stealing from others only when work stealing is enabled. Returns
     * {@code null} when nothing is runnable right now, which is a normal situation and not an error.
     *
     * @param workerIndex zero based index of the calling worker
     * @return the claimed entry with its consumer slot already reserved, or {@code null}
     */
    @Nullable QueueEntry next(int workerIndex);

    /**
     * Completes a task that ran to an end, successfully or not.
     *
     * <p>Releases the consumer slot, completes the handle with the result or the failure, records
     * metrics, feeds the adaptive controller and emits JFR events.
     *
     * @param entry          the finished entry
     * @param queueWaitNanos how long it waited before starting
     * @param executionNanos how long the task body ran
     * @param failure        throwable thrown by the task, or {@code null} on success
     */
    void taskFinished(QueueEntry entry, long queueWaitNanos, long executionNanos, @Nullable Throwable failure);

    /**
     * Completes a task that was cancelled, queued or running.
     *
     * <p>Releases the consumer slot if one was held, completes the handle with a
     * {@link ComputeCancelledException} carrying the reason, and records metrics.
     *
     * @param entry  the cancelled entry
     * @param reason why it was cancelled
     */
    void taskCancelled(QueueEntry entry, ComputeCancelledException.Reason reason);

    /**
     * Returns the current queue depth seen by the dispatcher.
     *
     * @return queued tasks across all consumers
     */
    int queuedTasks();
}
