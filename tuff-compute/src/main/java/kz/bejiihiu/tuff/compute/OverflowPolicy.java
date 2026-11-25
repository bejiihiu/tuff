package kz.bejiihiu.tuff.compute;

/**
 * How a full queue behaves when a submission does not fit.
 *
 * <p>The policy is evaluated twice: against the global queue capacity and against the per consumer
 * queue bound from {@link ConsumerPolicy#maxQueuedTasks()}. It never blocks the submitting thread,
 * because submissions usually come from region threads that must not park.
 */
public enum OverflowPolicy {

    /**
     * Refuse the new submission with {@link ComputeRejectedException} and leave the queue as it is.
     * This is the default and the safest choice: the caller keeps ownership of its task and decides
     * whether to retry, drop or escalate.
     */
    REJECT,

    /**
     * Evict the oldest queued task of the same consumer to make room for the new one. The evicted
     * task is cancelled with {@link ComputeCancelledException.Reason#EVICTED} and never runs.
     *
     * <p>Only the oldest task of the submitting consumer is considered: the scheduler never throws
     * away work of another subsystem. If the consumer has no queued task to evict, the submission is
     * rejected as with {@link #REJECT}.
     */
    DROP_OLDEST
}
