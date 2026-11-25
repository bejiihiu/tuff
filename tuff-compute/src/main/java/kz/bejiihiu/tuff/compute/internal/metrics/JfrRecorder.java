package kz.bejiihiu.tuff.compute.internal.metrics;

import kz.bejiihiu.tuff.compute.ComputeCancelledException;
import kz.bejiihiu.tuff.compute.ComputePriority;
import kz.bejiihiu.tuff.compute.ComputeTaskType;
import org.jspecify.annotations.Nullable;

/**
 * Emits JFR events for the compute scheduler.
 *
 * <p>The recorder maps scheduler lifecycle points to flight recorder events so a recording made
 * during a lag spike can answer "which consumer occupied the pool and what did it wait for". Event
 * names and fields are part of the recording schema:
 * <ul>
 *     <li>{@code tuff.compute.Task} — one event per task, with queue wait, execution time and
 *     failure, duration measured on the event itself;</li>
 *     <li>{@code tuff.compute.QueueState} — periodic gauge of queue depth;</li>
 *     <li>{@code tuff.compute.WorkerState} — periodic gauge of worker counts and concurrency
 *     limit.</li>
 * </ul>
 *
 * <p>Implementations must be cheap when disabled: {@link #enabled()} is checked by callers before
 * they build arguments, and a disabled recorder silently ignores everything.
 *
 * <p>Must not throw. A recorder failure must never break a task, so implementations catch and drop
 * their own errors.
 */
public interface JfrRecorder {

    /**
     * Checks whether events are actually recorded.
     *
     * @return {@code true} if the flight recorder is available and recording is enabled
     */
    boolean enabled();

    /**
     * Records that a task was accepted into the queue.
     *
     * @param taskId     id of the task
     * @param consumerId consumer of the task
     * @param taskType   declared category
     * @param priority   submitted priority
     * @param queueDepth queue depth right after the entry was accepted
     */
    void taskQueued(long taskId, String consumerId, ComputeTaskType taskType, ComputePriority priority, int queueDepth);

    /**
     * Records that a task started executing.
     *
     * @param taskId         id of the task
     * @param consumerId     consumer of the task
     * @param taskType       declared category
     * @param priority       effective priority at dispatch time, the submitted one plus aging
     * @param queueWaitNanos how long the task waited in the queue
     * @param activeTasks    tasks of that consumer running right after this one started
     */
    void taskStarted(
        long taskId,
        String consumerId,
        ComputeTaskType taskType,
        ComputePriority priority,
        long queueWaitNanos,
        int activeTasks
    );

    /**
     * Records that a task finished, successfully or not.
     *
     * @param taskId         id of the task
     * @param consumerId     consumer of the task
     * @param taskType       declared category
     * @param priority       effective priority at dispatch time
     * @param executionNanos how long the task body ran
     * @param failure        throwable thrown by the task, or {@code null} on success
     */
    void taskFinished(
        long taskId,
        String consumerId,
        ComputeTaskType taskType,
        ComputePriority priority,
        long executionNanos,
        @Nullable Throwable failure
    );

    /**
     * Records that a task was cancelled before it produced a result.
     *
     * @param taskId     id of the task
     * @param consumerId consumer of the task
     * @param reason     why the task was cancelled
     */
    void taskCancelled(long taskId, String consumerId, ComputeCancelledException.Reason reason);

    /**
     * Records a queue depth sample.
     *
     * @param queuedTasks current queue depth
     * @param capacity    configured capacity
     */
    void queueState(int queuedTasks, int capacity);

    /**
     * Records an accepted submission with its diagnostic label.
     *
     * <p>Default implementations delegate to the unlabelled method so recorders written before labels
     * keep working. Recorders that store the label override it.
     *
     * @param taskId     id of the task
     * @param consumerId consumer of the task
     * @param taskType   declared category
     * @param priority   submitted priority
     * @param queueDepth queue depth right after the entry was accepted
     * @param label      normalized task label, may be empty
     */
    default void taskQueued(
        final long taskId,
        final String consumerId,
        final ComputeTaskType taskType,
        final ComputePriority priority,
        final int queueDepth,
        final String label
    ) {
        this.taskQueued(taskId, consumerId, taskType, priority, queueDepth);
    }

    /**
     * Records that a task started executing, with its diagnostic label.
     *
     * @param taskId         id of the task
     * @param consumerId     consumer of the task
     * @param taskType       declared category
     * @param priority       effective priority at dispatch time
     * @param queueWaitNanos how long the task waited
     * @param activeTasks    tasks of that consumer running right after this one started
     * @param label          normalized task label, may be empty
     */
    default void taskStarted(
        final long taskId,
        final String consumerId,
        final ComputeTaskType taskType,
        final ComputePriority priority,
        final long queueWaitNanos,
        final int activeTasks,
        final String label
    ) {
        this.taskStarted(taskId, consumerId, taskType, priority, queueWaitNanos, activeTasks);
    }

    /**
     * Records that a task finished, with its diagnostic label.
     *
     * @param taskId         id of the task
     * @param consumerId     consumer of the task
     * @param taskType       declared category
     * @param priority       effective priority at dispatch time
     * @param executionNanos how long the task body ran
     * @param failure        throwable thrown by the task, or {@code null} on success
     * @param label          normalized task label, may be empty
     */
    default void taskFinished(
        final long taskId,
        final String consumerId,
        final ComputeTaskType taskType,
        final ComputePriority priority,
        final long executionNanos,
        final @Nullable Throwable failure,
        final String label
    ) {
        this.taskFinished(taskId, consumerId, taskType, priority, executionNanos, failure);
    }

    /**
     * Records that a task was cancelled before it produced a result, with its diagnostic label.
     *
     * @param taskId     id of the task
     * @param consumerId consumer of the task
     * @param reason     why the task was cancelled
     * @param label      normalized task label, may be empty
     */
    default void taskCancelled(
        final long taskId,
        final String consumerId,
        final ComputeCancelledException.Reason reason,
        final String label
    ) {
        this.taskCancelled(taskId, consumerId, reason);
    }

    /**
     * Records a worker pool sample.
     *
     * @param totalWorkers     live worker threads
     * @param activeWorkers    workers executing a task
     * @param idleWorkers      workers waiting for a task
     * @param concurrencyLimit current pool wide concurrency limit
     */
    void workerState(int totalWorkers, int activeWorkers, int idleWorkers, int concurrencyLimit);

    /**
     * Records that a task started on the virtual lane.
     *
     * <p>Kept as a default no-op so a recorder written before the lane existed keeps compiling; a
     * recorder that cares about the lane overrides it.
     *
     * @param taskId         id of the task
     * @param consumerId     consumer of the task
     * @param priority       submitted priority
     * @param queueWaitNanos how long it waited before the lane claimed it
     * @param spilled        whether the task was spilled from the platform lane
     * @param running        virtual tasks running right after this one started
     * @param limit          hard virtual lane limit
     * @param label          normalized task label, may be empty
     */
    default void virtualTask(
        final long taskId,
        final String consumerId,
        final ComputePriority priority,
        final long queueWaitNanos,
        final boolean spilled,
        final int running,
        final int limit,
        final String label
    ) {
        // no-op for recorders that predate the lane
    }
}
