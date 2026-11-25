package kz.bejiihiu.tuff.compute;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.jspecify.annotations.Nullable;

/**
 * Handle of a submitted compute task.
 *
 * <p>A handle is the only way to observe and cancel a task after submission. It is created for
 * every accepted submission, including the ones submitted through
 * {@link AsyncComputeScheduler#submit(ComputeTask)} which only hand back the future.
 *
 * <p>All accessors are safe to call at any time from any thread and stay usable after the task
 * finished: a handle never changes identity and its terminal state never changes again.
 *
 * @param <T> type of the produced value
 */
public interface ComputeTaskHandle<T> {

    /**
     * Returns the identifier assigned by the scheduler.
     *
     * <p>Identifiers are unique for the lifetime of one scheduler instance, they are not reused and
     * are only meaningful for diagnostics and metrics.
     *
     * @return the task id, always positive
     */
    long taskId();

    /**
     * Returns the id of the consumer this task was submitted for.
     *
     * @return the consumer id, never blank
     */
    String consumerId();

    /**
     * Returns the declared category of this task.
     *
     * @return the task type, never {@code null}
     */
    ComputeTaskType taskType();

    /**
     * Returns the priority this task was submitted with.
     *
     * @return the submitted priority, never {@code null}
     */
    ComputePriority priority();

    /**
     * Returns the wall clock instant at which the submission was accepted.
     *
     * @return the submission instant
     */
    Instant submittedAt();

    /**
     * Returns the future that completes with the result of this task.
     *
     * <p>The future completes normally with the value returned by {@link ComputeTask#execute()}, or
     * exceptionally with the throwable the task threw. Cancellation completes it with
     * {@link ComputeCancelledException}, which also makes {@link #isCancelled()} return
     * {@code true}. Cancelling this future directly is equivalent to calling {@link #cancel()}:
     * queued work is removed from the queue, running work is asked to stop cooperatively and the
     * result is discarded.
     *
     * @return the result future, the same instance on every call
     */
    CompletableFuture<T> future();

    /**
     * Returns the current lifecycle state of the task.
     *
     * @return the current state, never {@code null}
     */
    State state();

    /**
     * Requests cancellation of this task.
     *
     * <p>Cancellation is cooperative and never interrupts a worker thread. A queued task is removed
     * from the queue immediately and its future completes with {@link ComputeCancelledException}.
     * A running task is only flagged: the computation keeps running until it returns or checks
     * {@link #isCancellationRequested()}, and its result is discarded when it does.
     *
     * <p>This method is idempotent. It returns {@code false} when the task already finished
     * (successfully or not), when it was already cancelled, or when it was rejected at submission
     * time.
     *
     * @return {@code true} if this call moved the task to the cancelled state
     */
    boolean cancel();

    /**
     * Checks whether cancellation was requested for this task.
     *
     * <p>This is the flag a cooperative task body polls while it computes. It becomes {@code true}
     * for queued and running tasks as soon as cancellation is requested, and stays {@code true}
     * afterwards.
     *
     * @return {@code true} if cancellation was requested
     */
    boolean isCancellationRequested();

    /**
     * Checks whether this task ended in the cancelled state.
     *
     * @return {@code true} if the task was cancelled and will produce no result
     */
    boolean isCancelled();

    /**
     * Checks whether this task reached a terminal state, whatever it was.
     *
     * @return {@code true} if the state is terminal
     * @see State#isTerminal()
     */
    boolean isDone();

    /**
     * Returns the time this task spent waiting in the queue.
     *
     * @return nanoseconds between acceptance and the start of execution, or {@code -1} while the
     *     task has not started yet
     */
    long queueWaitNanos();

    /**
     * Returns the time this task spent executing.
     *
     * @return nanoseconds between the start of execution and its end, or {@code -1} while the task
     *     has not finished yet
     */
    long executionNanos();

    /**
     * Returns the throwable that failed this task.
     *
     * @return the failure of a {@link State#FAILED} task, {@code null} in every other state
     */
    @Nullable Throwable failure();

    /**
     * Lifecycle state of a submitted task.
     */
    enum State {

        /**
         * Accepted and waiting in the queue.
         */
        QUEUED,

        /**
         * Claimed by a worker and currently executing.
         */
        RUNNING,

        /**
         * Finished successfully, the future holds the result.
         */
        COMPLETED,

        /**
         * Finished with a throwable, the future completed exceptionally.
         */
        FAILED,

        /**
         * Cancelled before producing a result.
         */
        CANCELLED,

        /**
         * Refused at submission time, no task was ever queued. A handle in this state is only
         * produced by the internal submission path; {@link AsyncComputeScheduler} throws instead.
         */
        REJECTED;

        /**
         * Checks whether this state is terminal.
         *
         * @return {@code true} for every state except {@link #QUEUED} and {@link #RUNNING}
         */
        public boolean isTerminal() {
            return this != QUEUED && this != RUNNING;
        }
    }
}
