package kz.bejiihiu.tuff.compute.internal.queue;

import java.time.Duration;
import java.util.Objects;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeTask;
import kz.bejiihiu.tuff.compute.ComputeTaskHandle;
import kz.bejiihiu.tuff.compute.ComputeTaskType;

/**
 * One queued task, as seen by the queue.
 *
 * <p>An entry is a value object: the engine creates it once per accepted submission and never
 * changes it. Everything mutable about a submission (state, cancellation, result) lives behind
 * {@link #handle()}, which is owned by the engine.
 *
 * @param taskId           unique id assigned by the engine
 * @param consumerId       consumer the task belongs to
 * @param taskType         declared category of the task
 * @param priority         submitted priority
 * @param sequence         monotonically increasing submission sequence, keeps FIFO order between equal priorities
 * @param submittedAtNanos monotonic timestamp of acceptance, taken from the scheduler clock
 * @param task             the work to execute
 * @param handle           handle of the task, used for cancellation checks
 */
public record QueueEntry(
    long taskId,
    String consumerId,
    ComputeTaskType taskType,
    ComputePriority priority,
    long sequence,
    long submittedAtNanos,
    ComputeTask<?> task,
    ComputeTaskHandle<?> handle
) {

    /**
     * Validates the entry.
     *
     * @throws NullPointerException if a reference is {@code null}
     */
    public QueueEntry {
        Objects.requireNonNull(consumerId, "consumerId");
        Objects.requireNonNull(taskType, "taskType");
        Objects.requireNonNull(priority, "priority");
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(handle, "handle");
    }

    /**
     * Executes the wrapped task, if the queue is ever asked to do that.
     *
     * <p>Kept on the entry so the raw {@code ComputeTask<?>} never has to be cast at the call site:
     * a wildcard call yields {@code Object}, which is exactly what the handle expects.
     *
     * @return the value produced by the task
     * @throws Exception whatever the task throws, unchanged
     */
    public Object execute() throws Exception {
        return this.task.execute();
    }

    /**
     * Returns how long this entry has been waiting.
     *
     * @param nowNanos current reading of the scheduler clock
     * @return nanoseconds between acceptance and {@code nowNanos}
     */
    public long waitNanos(final long nowNanos) {
        return nowNanos - this.submittedAtNanos;
    }

    /**
     * Returns how long this entry has been waiting as a duration.
     *
     * @param nowNanos current reading of the scheduler clock
     * @return the wait as a duration, never negative
     */
    public Duration waitDuration(final long nowNanos) {
        return Duration.ofNanos(Math.max(0L, this.waitNanos(nowNanos)));
    }
}
