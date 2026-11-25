package kz.bejiihiu.tuff.compute.internal.queue;

import kz.bejiihiu.tuff.compute.ConsumerPolicy;

/**
 * Tracks how many tasks of each consumer are executing right now.
 *
 * <p>This is the second half of consumer isolation: the queue decides what is eligible by priority
 * and fairness, the quota controller decides whether the consumer may actually run one more task.
 * The two are combined by passing this controller as a
 * {@link ComputeQueue.ConsumerAdmission}.
 *
 * <p>A slot is acquired exactly once per started task and released exactly once when the task
 * finishes, fails or is cancelled. Implementations must guarantee that a release without a matching
 * acquire is impossible from the caller's point of view: the engine only releases for tasks it
 * started.
 *
 * <p>Lowering a limit never interrupts a running task. It only makes
 * {@link #admits(String)} return {@code false} until the consumer is below its limit again.
 */
public interface QuotaController extends ComputeQueue.ConsumerAdmission {

    /**
     * Reserves one execution slot for a consumer.
     *
     * <p>Normally the worker first asks {@link #admits(String)} and then calls this method before the
     * task actually starts. Implementations must make the pair atomic enough that the limit is never
     * exceeded: an optimistic admission followed by a failed acquire must be handled by discarding
     * the candidate, not by running it anyway.
     *
     * @param consumerId consumer to reserve for
     * @return {@code true} if a slot was reserved, {@code false} if the consumer is already at its
     *     limit
     */
    boolean tryAcquire(String consumerId);

    /**
     * Releases one execution slot of a consumer.
     *
     * <p>Called exactly once per acquired slot, including for tasks that failed or were cancelled
     * while running. Releasing an unknown consumer is a bug in the caller and may be logged, but must
     * not throw.
     *
     * @param consumerId consumer to release
     */
    void release(String consumerId);

    /**
     * Returns how many tasks of a consumer are executing.
     *
     * @param consumerId consumer to inspect
     * @return active tasks of that consumer
     */
    int activeTasks(String consumerId);

    /**
     * Returns how many tasks are executing across all consumers.
     *
     * @return active tasks in total
     */
    int activeTasks();

    /**
     * Returns the effective concurrency limit of a consumer.
     *
     * @param consumerId consumer to inspect
     * @return the resolved limit, or {@code 0} for an unknown consumer
     */
    int limit(String consumerId);

    /**
     * Returns the current pool wide limit.
     *
     * @return the maximum number of concurrently executing tasks across all consumers
     */
    int totalLimit();

    /**
     * Applies a new policy to a registered consumer.
     *
     * <p>The relative limits are resolved lazily against {@link #totalLimit()}, so a policy update
     * does not need to know the pool size.
     *
     * @param consumerId consumer to update
     * @param policy     replacement policy
     */
    void updateConsumerPolicy(String consumerId, ConsumerPolicy policy);

    /**
     * Forgets a consumer.
     *
     * <p>Only valid when the consumer has no active tasks left, which the engine guarantees by
     * unregistering after the queue was drained and running tasks were cancelled cooperatively.
     *
     * @param consumerId consumer to forget
     */
    void removeConsumer(String consumerId);

    /**
     * Updates the pool wide limit.
     *
     * <p>Called when the worker pool grows, shrinks or when adaptive concurrency moves the limit.
     * Per consumer relative limits are re-resolved against the new value.
     *
     * @param totalLimit new maximum number of concurrently executing tasks, at least {@code 1}
     */
    void updateTotalLimit(int totalLimit);

    /**
     * Clears all counters and reservations.
     *
     * <p>Only used when the scheduler stops. Callers must ensure no task is running anymore.
     */
    void reset();
}
