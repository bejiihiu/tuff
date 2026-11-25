package kz.bejiihiu.tuff.compute.internal.queue;

import java.util.Collection;
import kz.bejiihiu.tuff.compute.ConsumerPolicy;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import org.jspecify.annotations.Nullable;

/**
 * The bounded, multi consumer scheduling queue.
 *
 * <p>The queue holds every task that was accepted but not yet claimed by a worker. It is the only
 * place where ordering is decided, and it implements the three ordering inputs configured through
 * {@link kz.bejiihiu.tuff.compute.ComputeSettings.Scheduling}:
 * <ul>
 *     <li><b>priority</b> — the highest effective priority wins;</li>
 *     <li><b>aging</b> — a task that waits longer than the configured interval gains effective
 *     priority levels, up to the configured boost, so a flood of high priority work cannot starve
 *     low priority work forever;</li>
 *     <li><b>fairness</b> — under weighted fair scheduling the dispatch turns are shared between
 *     consumers proportionally to {@link ConsumerPolicy#fairShareWeight()}, measured in virtual
 *     service time so a consumer that just ran is served later than one that did not.</li>
 * </ul>
 *
 * <p>Between two tasks with the same effective priority and the same consumer, the lower
 * {@link QueueEntry#sequence()} is returned first. That is the only FIFO promise the queue makes;
 * concurrent workers obviously finish tasks in whatever order the CPU gives them.
 *
 * <p>Capacity is enforced in two places, exactly as the policies describe: the global
 * {@link #capacity()} and the per consumer {@link ConsumerPolicy#maxQueuedTasks()}. When a bound is
 * hit, the submitting consumer's {@link OverflowPolicy} decides between refusal and evicting its own
 * oldest queued task. The queue never blocks and never throws on overflow; it reports the outcome in
 * {@link OfferResult}.
 *
 * <p>Threading: implementations must be safe for concurrent {@code offer}, {@code poll},
 * {@code remove} and reads from many threads. {@code poll} is allowed to be lock free or lock based,
 * but it must not block waiting for producers and must return {@code null} quickly when nothing is
 * runnable.
 */
public interface ComputeQueue {

    /**
     * Offers a task to the queue.
     *
     * <p>The entry is accepted when both the global and the per consumer bounds have room. When they
     * do not, the consumer's {@link OverflowPolicy} is applied: {@link OverflowPolicy#REJECT} leaves
     * the queue untouched and reports which bound was hit, {@link OverflowPolicy#DROP_OLDEST} evicts
     * the oldest queued entry of the same consumer and accepts the new one.
     *
     * <p>Offering to a closed queue never accepts anything and reports {@link OfferResult#CLOSED}.
     *
     * @param entry entry to enqueue
     * @return how the offer was handled
     */
    OfferResult offer(QueueEntry entry);

    /**
     * Removes and returns the next task that a worker may run.
     *
     * <p>The best entry is chosen by the configured strategy, skipping entries whose consumer is
     * rejected by {@code admission}. Skipped entries stay queued, they are not dropped, so a worker
     * never has to push work back.
     *
     * @param admission admission check, typically the {@link QuotaController}
     * @return the next runnable entry, or {@code null} if the queue is empty or no entry is currently
     *     admitted
     */
    @Nullable QueueEntry poll(ConsumerAdmission admission);

    /**
     * Steals a task from the batch owned by another worker.
     *
     * <p>Only called when work stealing is enabled. The thief index is used to keep the stealing
     * order stable, so workers do not all steal from the same victim at the same time. The returned
     * entry is removed from the queue exactly like with {@link #poll(ConsumerAdmission)}, and the
     * same admission rules apply.
     *
     * @param thiefIndex zero based index of the stealing worker
     * @param admission  admission check, typically the {@link QuotaController}
     * @return a stolen entry, or {@code null} if stealing is disabled or nothing is available
     */
    @Nullable QueueEntry steal(int thiefIndex, ConsumerAdmission admission);

    /**
     * Returns the next runnable entry without removing it.
     *
     * <p>For inspection and tests only; the hot path uses {@link #poll(ConsumerAdmission)}.
     *
     * @return the next entry, or {@code null} if the queue is empty
     */
    @Nullable QueueEntry peek();

    /**
     * Removes a specific entry from the queue.
     *
     * <p>Used for cancellation and for consumer removal. Returns {@code false} when the entry is not
     * queued anymore, which happens when it was already claimed by a worker or evicted; the caller
     * then has to fall back to cooperative cancellation.
     *
     * @param entry entry to remove
     * @return {@code true} if the entry was still queued and is now removed
     */
    boolean remove(QueueEntry entry);

    /**
     * Returns the number of queued tasks across all consumers.
     *
     * @return the queue depth
     */
    int size();

    /**
     * Returns the number of queued tasks of one consumer.
     *
     * @param consumerId consumer to count
     * @return the depth of that consumer, {@code 0} if it has nothing queued
     */
    int size(String consumerId);

    /**
     * Returns the configured global capacity.
     *
     * @return the maximum queue depth
     */
    int capacity();

    /**
     * Checks whether the queue holds no tasks.
     *
     * @return {@code true} if {@link #size()} is zero
     */
    boolean isEmpty();

    /**
     * Moves up to {@code maxEntries} tasks out of the queue, oldest and highest priority first.
     *
     * <p>Used on shutdown to cancel or hand over whatever is left. Moved entries are no longer in the
     * queue and will never be returned by {@link #poll(ConsumerAdmission)}.
     *
     * @param target     collection receiving the entries
     * @param maxEntries maximum number to move, must be positive
     * @return the number of entries actually moved
     */
    int drainTo(Collection<? super QueueEntry> target, int maxEntries);

    /**
     * Removes every queued entry without returning them.
     *
     * <p>Only used when the scheduler is destroyed; entries cleared this way are not cancelled by the
     * queue, the caller is responsible for completing their futures.
     */
    void clear();

    /**
     * Returns a point in time snapshot of the queue.
     *
     * @return the snapshot
     */
    QueueSnapshot snapshot();

    /**
     * Registers a consumer so the queue can apply its policy.
     *
     * <p>Registering an unknown id twice replaces the stored policy. The queue only needs
     * {@link ConsumerPolicy#maxQueuedTasks()}, {@link ConsumerPolicy#fairShareWeight()} and
     * {@link ConsumerPolicy#overflowPolicy()}; the rest is the engine's business.
     *
     * @param consumerId consumer to register
     * @param policy     policy of that consumer
     */
    void registerConsumer(String consumerId, ConsumerPolicy policy);

    /**
     * Replaces the policy of a registered consumer.
     *
     * <p>Queued entries stay queued even if the new policy would reject them; the new limits are
     * applied to future offers. Withdrawing a fair share weight does not retroactively change the
     * virtual service time already accrued.
     *
     * @param consumerId consumer to update
     * @param policy     replacement policy
     */
    void updatePolicy(String consumerId, ConsumerPolicy policy);

    /**
     * Forgets a consumer and everything it has queued.
     *
     * <p>The queue drops the consumer's policy and unlinks its queued entries without returning
     * them; the caller is responsible for cancelling those entries so their futures complete.
     *
     * @param consumerId consumer to remove
     * @return the entries that were queued for that consumer, in dispatch order
     */
    Collection<QueueEntry> unregisterConsumer(String consumerId);

    /**
     * Closes the queue to new offers.
     *
     * <p>Already queued entries can still be polled and drained, but {@link #offer(QueueEntry)}
     * returns {@link OfferResult#CLOSED} from now on. Idempotent.
     */
    void close();

    /**
     * Checks whether the queue is closed to new offers.
     *
     * @return {@code true} after {@link #close()}
     */
    boolean isClosed();

    /**
     * Outcome of an {@link #offer(QueueEntry)} call.
     */
    enum OfferResult {

        /**
         * The entry is queued.
         */
        ACCEPTED,

        /**
         * The global capacity is exhausted and the consumer's policy rejects overflow.
         */
        QUEUE_FULL,

        /**
         * The per consumer bound is exhausted and the consumer's policy rejects overflow.
         */
        CONSUMER_QUEUE_FULL,

        /**
         * The queue is closed, nothing is accepted anymore.
         */
        CLOSED
    }

    /**
     * Admission check applied while polling.
     *
     * <p>Implementations must be cheap and must not throw; they are called for candidates that are
     * already at the head of the queue.
     */
    interface ConsumerAdmission {

        /**
         * Checks whether a consumer may start one more task right now.
         *
         * @param consumerId consumer of the candidate entry
         * @return {@code true} if the entry may be handed to a worker
         */
        boolean admits(String consumerId);
    }
}
