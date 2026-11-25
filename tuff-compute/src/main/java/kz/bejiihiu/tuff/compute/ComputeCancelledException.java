package kz.bejiihiu.tuff.compute;

import java.util.Objects;
import java.util.concurrent.CancellationException;

/**
 * Marks a compute task that ended without producing a result.
 *
 * <p>It extends {@link CancellationException} so that standard future handling, for example
 * {@link java.util.concurrent.CompletableFuture#isCancelled()}, works without special casing. The
 * reason tells apart a caller initiated cancellation from scheduler initiated ones.
 */
public final class ComputeCancelledException extends CancellationException {

    private final Reason reason;
    private final long taskId;

    /**
     * Creates the exception.
     *
     * @param taskId  id of the cancelled task
     * @param reason  why the task was cancelled
     * @param message detail message for logs
     */
    public ComputeCancelledException(final long taskId, final Reason reason, final String message) {
        super(message);
        this.reason = Objects.requireNonNull(reason, "reason");
        this.taskId = taskId;
    }

    /**
     * Returns the id of the cancelled task.
     *
     * @return the task id
     */
    public long taskId() {
        return this.taskId;
    }

    /**
     * Returns why the task was cancelled.
     *
     * @return the cancellation reason
     */
    public Reason reason() {
        return this.reason;
    }

    /**
     * Why a task was cancelled.
     */
    public enum Reason {

        /**
         * The caller asked for it through {@link ComputeTaskHandle#cancel()} or by cancelling the
         * result future.
         */
        CALLER,

        /**
         * The task waited in the queue longer than {@link ConsumerPolicy#maxQueueWait()}.
         */
        QUEUE_EXPIRED,

        /**
         * The task was evicted to make room for newer work of the same consumer, see
         * {@link OverflowPolicy#DROP_OLDEST}.
         */
        EVICTED,

        /**
         * The scheduler shut down before the task could start.
         */
        SHUTDOWN,

        /**
         * The consumer was disabled while the task was still queued.
         */
        CONSUMER_DISABLED
    }
}
