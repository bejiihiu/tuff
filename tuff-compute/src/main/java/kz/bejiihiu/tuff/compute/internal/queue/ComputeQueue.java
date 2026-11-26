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
 * oldest queued task. The queue never blocks; refusal is reported in {@link OfferResult}.
 *
 * <p><b>Ownership invariant.</b> An accepted entry leaves the queue through exactly one path, and
 * that path always gives the entry back to the scheduler: {@link #poll(ConsumerAdmission)} and
 * {@link #steal(int, ConsumerAdmission)} return it, {@link #remove(QueueEntry)} reports removal to
 * the caller, {@link #drainTo(Collection, int)}, {@link #drainExpired(long, Collection)},
 * {@link #clear()} and {@link #unregisterConsumer(String)} hand it back as a collection, and
 * {@link #offer(QueueEntry)} eviction reports it through the
 * {@link #setEvictionListener(EvictionListener) eviction listener}. The queue never destroys an
 * entry behind the scheduler's back, because only the scheduler can complete the entry's
 * {@link kz.bejiihiu.tuff.compute.ComputeTaskHandle#future()}.
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
     * the oldest queued entry of the same consumer, reports it to the
     * {@link #setEvictionListener(EvictionListener) eviction listener} and accepts the new one.
     *
     * <p>Eviction never silently discards work: the entry is unlinked, the listener is called on the
     * offering thread before this method returns, and only then the result is reported. The evicted
     * entry is never returned by this method, so the listener is the only way the caller learns
     * about it.
     *
     * <p>Offering to a closed queue never accepts anything and reports {@link OfferResult#CLOSED}.
     *
     * @param entry entry to enqueue
     * @return how the offer was handled
     * @throws IllegalStateException if eviction is required by
     *     {@link OverflowPolicy#DROP_OLDEST} but no eviction listener was installed, because
     *     dropping the entry would strand its future forever
     */
    OfferResult offer(QueueEntry entry);

    /**
     * Offers a task that is only accepted while its consumer is still registered.
     *
     * <p>The engine resolves a consumer before it builds an entry, so by the time the entry reaches
     * the queue that consumer may have been unregistered concurrently. A plain {@link #offer} would
     * silently recreate a stream for the unknown id, which leaves the entry with no quota, no
     * registry entry and no dispatch path: a future that never completes. This variant answers
     * {@link OfferResult#CONSUMER_UNKNOWN} instead, and never creates consumer state.
     *
     * <p>The check is racy by nature, but it is safe in both directions: an entry accepted just
     * before the consumer was unregistered is drained and cancelled by
     * {@link #unregisterConsumer(String)}, and an entry that loses the race is refused. A refused
     * offer transfer no work, and a re-registration never resurrects the refused entry.
     *
     * @param entry entry to enqueue
     * @return how the offer was handled
     * @throws IllegalStateException if eviction is required by
     *     {@link OverflowPolicy#DROP_OLDEST} but no eviction listener was installed
     */
    OfferResult offerRegistered(QueueEntry entry);

    /**
     * Installs the sink that receives entries evicted by {@link #offer(QueueEntry)}.
     *
     * <p>The scheduler owns task handles and metrics, so the queue cannot cancel an evicted task
     * itself; it hands the entry over instead. The engine installs the listener once, before the
     * scheduler starts; a later call replaces it, which is only useful for tests and for a scheduler
     * restart.
     *
     * <p>A listener is mandatory as soon as {@link OverflowPolicy#DROP_OLDEST} is reachable for any
     * consumer. Implementations must refuse such an offer with {@link IllegalStateException} when no
     * listener is installed rather than drop the entry: a silently dropped entry is a future that
     * never completes.
     *
     * @param listener sink for evicted entries, must not be {@code null}
     */
    void setEvictionListener(EvictionListener listener);

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
     * Takes a task outside the normal dispatch order, from another consumer's stream.
     *
     * <p>Only called when work stealing is enabled and {@link #poll(ConsumerAdmission)} found nothing
     * runnable. There are no per worker batches: every worker draws from the same queue, so this
     * method walks the registered consumers starting at {@code thiefIndex} modulo the consumer
     * count and returns at most one entry, in that consumer's own dispatch order, skipping any
     * consumer whose admission check fails. The rotation only spreads idle thieves over different
     * consumers; it does not change the order inside a consumer.
     *
     * <p>The returned entry is claimed and unlinked exactly like one returned by
     * {@link #poll(ConsumerAdmission)}, and its consumer is charged the same dispatch turn.
     *
     * @param thiefIndex zero based index of the stealing worker, used only to rotate the starting
     *                   consumer
     * @param admission  admission check, typically the {@link QuotaController}
     * @return a stolen entry, or {@code null} if stealing is disabled or nothing is both eligible and
     *     admitted
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
     * queue and will never be returned by {@link #poll(ConsumerAdmission)}. Ownership transfers to
     * the caller, which must complete every returned handle itself, normally with
     * {@link kz.bejiihiu.tuff.compute.ComputeCancelledException.Reason#SHUTDOWN}.
     *
     * @param target     collection receiving the entries
     * @param maxEntries maximum number to move, must be positive
     * @return the number of entries actually moved
     */
    int drainTo(Collection<? super QueueEntry> target, int maxEntries);

    /**
     * Checks whether any queued entry is already past its consumer's
     * {@link ConsumerPolicy#maxQueueWait()}.
     *
     * <p>This is the cheap probe the engine's expiry janitor runs before draining, because a sweep
     * with nothing to sweep is pure overhead. Implementations must answer in {@code O(consumers)} or
     * better and must never walk the queued entries: the per consumer heads are already the oldest
     * entries of their consumer in dispatch order, so the answer can be derived from them. Returning
     * {@code true} conservatively is allowed; returning {@code false} while
     * {@link #drainExpired(long, Collection)} would move something is not.
     *
     * <p>An entry is expired when its consumer's {@link ConsumerPolicy#maxQueueWait()} is not
     * {@link java.time.Duration#ZERO zero} and {@code nowNanos - entry.submittedAtNanos()} is greater
     * than or equal to that limit. The policy in force at the moment of the call is used, so shortening
     * a limit makes already queued work expire on the next sweep.
     *
     * @param nowNanos current reading of the scheduler clock, the same clock that stamped the entries
     * @return {@code true} if {@link #drainExpired(long, Collection)} called with the same
     *     {@code nowNanos} would move at least one entry
     */
    boolean hasExpired(long nowNanos);

    /**
     * Removes every queued entry that outlived its consumer's {@link ConsumerPolicy#maxQueueWait()}
     * and hands them back.
     *
     * <p>Called by the engine's janitor, normally once per 100 ms, never from a worker and never from
     * inside a callback. This sweep is what makes the queue wait limit real: a task that is only
     * checked when a worker polls would stay in a bounded queue for as long as higher priority work
     * keeps winning, holding capacity and lying to backpressure.
     *
     * <p>Rules, identical to {@link #drainTo(Collection, int)}:
     * <ul>
     *     <li>moved entries are unlinked and are never returned by {@link #poll(ConsumerAdmission)} or
     *     {@link #steal(int, ConsumerAdmission)} afterwards;</li>
     *     <li>ownership transfers to the caller, entries are appended to {@code target} in dispatch
     *     order;</li>
     *     <li>safe against concurrent {@code offer}, {@code poll}, {@code steal}, {@code remove} and
     *     {@code unregisterConsumer}: an entry claimed by one of them is not returned here, an entry
     *     is never returned twice, and nothing is lost;</li>
     *     <li>the queue completes nothing itself and never calls the
     *     {@link #setEvictionListener(EvictionListener) eviction listener} for expired entries.</li>
     * </ul>
     *
     * <p>What the engine does with every returned entry: complete its handle with
     * {@link kz.bejiihiu.tuff.compute.ComputeCancelledException} carrying
     * {@link kz.bejiihiu.tuff.compute.ComputeCancelledException.Reason#QUEUE_EXPIRED}, then record the
     * cancellation in metrics. No consumer quota slot has to be released, because a queued entry never
     * held one.
     *
     * <p>The call never waits for producers or workers. Its cost is proportional to the number of
     * consumers plus the number of entries it actually removes, never to the queue depth.
     *
     * @param nowNanos current reading of the scheduler clock, the same clock that stamped the entries
     * @param target   collection receiving the expired entries
     * @return the number of entries moved, {@code 0} when nothing expired
     */
    int drainExpired(long nowNanos, Collection<? super QueueEntry> target);

    /**
     * Removes every queued entry and hands them back.
     *
     * <p>This is the bulk form of {@link #drainTo(Collection, int)} and the only supported way to
     * empty the queue on teardown: ownership of the returned entries transfers to the caller, which
     * must complete every handle. Entries are returned in dispatch order.
     *
     * <p>The queue itself never completes or cancels a task, so a variant that dropped entries
     * silently would strand their futures; that is why this method returns them.
     *
     * @return the entries that were queued, in dispatch order, empty if nothing was queued
     */
    Collection<QueueEntry> clear();

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
     * <p>The queue drops the consumer's policy and unlinks its queued entries. Ownership of the
     * returned entries transfers to the caller, which must complete every handle, normally with
     * {@link kz.bejiihiu.tuff.compute.ComputeCancelledException.Reason#CONSUMER_DISABLED}.
     *
     * @param consumerId consumer to remove
     * @return the entries that were queued for that consumer, in dispatch order
     */
    Collection<QueueEntry> unregisterConsumer(String consumerId);

    /**
     * Closes the queue to new offers.
     *
     * <p>Already queued entries stay queued and can still be polled and drained; {@link #close()}
     * never removes work. {@link #offer(QueueEntry)} returns {@link OfferResult#CLOSED} from now on.
     * Idempotent. The caller is expected to drain the leftovers afterwards and complete their
     * handles.
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
        CLOSED,

        /**
         * The consumer is not registered (any more), so the entry has nowhere to run.
         */
        CONSUMER_UNKNOWN
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

    /**
     * Sink for entries evicted by {@link OverflowPolicy#DROP_OLDEST}.
     *
     * <p>Invocation guarantees the queue must uphold:
     * <ul>
     *     <li>exactly one callback per evicted entry, made by the {@code offer} call that evicted
     *     it;</li>
     *     <li>called on the offering thread, before that {@code offer} returns, so an accepted
     *     submission cannot be observed before its eviction was reported;</li>
     *     <li>the entry is already unlinked and both depth counters are updated, so the listener sees
     *     a consistent queue;</li>
     *     <li>called outside the queue's internal locks, so reading queue state is allowed;</li>
     *     <li>claiming an entry for eviction is atomic, so two concurrent offers cannot both evict
     *     the same entry and no entry is ever reported twice.</li>
     * </ul>
     *
     * <p>What the listener must do: complete the evicted entry's handle with
     * {@link kz.bejiihiu.tuff.compute.ComputeCancelledException} carrying
     * {@link kz.bejiihiu.tuff.compute.ComputeCancelledException.Reason#EVICTED}, and record the
     * cancellation in metrics. It must not touch consumer quotas, because a queued entry never held a
     * slot. Complete the handle before doing anything else, so a later metrics failure cannot leave a
     * future pending.
     *
     * <p>The listener must not call back into the queue: {@code offer}, {@code poll}, {@code steal},
     * {@code remove}, {@code drainTo}, {@code clear}, {@code close}, {@code registerConsumer},
     * {@code updatePolicy}, {@code unregisterConsumer} and {@code setEvictionListener} are all
     * forbidden from inside a callback. Reading methods such as {@code size} and {@code snapshot} are
     * safe but unnecessary. The callback must be fast, non blocking and must not throw.
     */
    @FunctionalInterface
    interface EvictionListener {

        /**
         * Handles one entry that was just evicted.
         *
         * @param evicted entry that was removed from the queue and will never run
         */
        void onEvicted(QueueEntry evicted);
    }
}
