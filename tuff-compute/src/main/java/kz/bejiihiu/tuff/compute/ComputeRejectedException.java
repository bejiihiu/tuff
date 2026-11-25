package kz.bejiihiu.tuff.compute;

import java.util.Objects;

/**
 * Thrown when a submission is refused instead of being queued.
 *
 * <p>A rejection is a normal, recoverable outcome: the caller keeps ownership of the task and
 * decides whether to retry, drop it or escalate. The scheduler never silently discards a rejected
 * task, and a rejected submission does not occupy a queue slot.
 */
public final class ComputeRejectedException extends ComputeException {

    private final Reason reason;
    private final String consumerId;
    private final ComputePriority priority;

    /**
     * Creates a rejection.
     *
     * @param reason     why the submission was refused
     * @param consumerId consumer the submission was made for
     * @param priority   priority the submission was made with
     * @param message    detail message for logs
     */
    public ComputeRejectedException(
        final Reason reason,
        final String consumerId,
        final ComputePriority priority,
        final String message
    ) {
        super(message);
        this.reason = Objects.requireNonNull(reason, "reason");
        this.consumerId = Objects.requireNonNull(consumerId, "consumerId");
        this.priority = Objects.requireNonNull(priority, "priority");
    }

    /**
     * Returns why the submission was refused.
     *
     * @return the rejection reason
     */
    public Reason reason() {
        return this.reason;
    }

    /**
     * Returns the consumer the refused submission was made for.
     *
     * @return the consumer id
     */
    public String consumerId() {
        return this.consumerId;
    }

    /**
     * Returns the priority the refused submission was made with.
     *
     * @return the priority
     */
    public ComputePriority priority() {
        return this.priority;
    }

    /**
     * Why a submission was refused.
     */
    public enum Reason {

        /**
         * The global queue is full and its {@link OverflowPolicy} does not allow making room.
         */
        QUEUE_FULL,

        /**
         * The consumer reached {@link ConsumerPolicy#maxQueuedTasks()}.
         */
        CONSUMER_QUEUE_FULL,

        /**
         * The task type is not in {@link ComputeConsumer#allowedTaskTypes()}.
         */
        TASK_TYPE_NOT_ALLOWED,

        /**
         * The priority outranks {@link ComputeConsumer#maxPriority()}.
         */
        PRIORITY_NOT_ALLOWED,

        /**
         * The consumer policy is disabled.
         */
        CONSUMER_DISABLED,

        /**
         * The consumer id is unknown and dynamic registration is turned off.
         */
        CONSUMER_UNKNOWN,

        /**
         * The scheduler itself is disabled by configuration.
         */
        SCHEDULER_DISABLED
    }
}
