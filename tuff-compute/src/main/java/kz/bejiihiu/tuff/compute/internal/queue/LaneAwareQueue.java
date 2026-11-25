package kz.bejiihiu.tuff.compute.internal.queue;

import org.jspecify.annotations.Nullable;

/**
 * Optional extension of {@link ComputeQueue} that can hand work to one of two execution lanes.
 *
 * <p>The base queue contract answers "what is the best runnable entry" without knowing where it will
 * run. The engine has two lanes: the fixed pool of platform workers and the virtual-thread lane. A
 * lane-aware queue keeps that decision cheap by selecting per consumer bucket heads under a level
 * mask, so filtering is {@code O(consumers)} and never walks the queued entries.
 *
 * <p>A mask is a bit set of {@link kz.bejiihiu.tuff.compute.ComputePriority#level()} values:
 * bit {@code 0} is LOW, bit {@code 1} NORMAL, bit {@code 2} HIGH and bit {@code 3} CRITICAL. The
 * caller decides the mask; the queue only skips excluded buckets, keeps the configured strategy,
 * aging and fairness inside the mask, and never re-offers an entry.
 *
 * <p>This interface is internal plumbing between the engine and the queue implementation. It must
 * not be exposed through {@link kz.bejiihiu.tuff.compute.AsyncComputeScheduler}.
 */
public interface LaneAwareQueue extends ComputeQueue {

    /**
     * Execution lane a dispatch call is made for.
     */
    enum Lane {

        /**
         * The fixed pool of platform workers.
         */
        PLATFORM,

        /**
         * The bounded virtual-thread lane.
         */
        VIRTUAL
    }

    /**
     * Removes and returns the best entry eligible for one lane.
     *
     * <p>Identical to {@link #poll(ConsumerAdmission)} except that entries whose priority level is
     * not in {@code levelMask} are ignored. Excluded entries stay queued and keep their sequence and
     * submission timestamp, so aging and FIFO are unaffected when a lane takes them later.
     *
     * @param lane      lane the caller is dispatching for, used for diagnostics
     * @param levelMask bit set of priority levels the lane may take
     * @param admission admission check, typically the quota controller
     * @return the next entry, or {@code null} if no eligible entry is admitted
     */
    @Nullable QueueEntry poll(Lane lane, int levelMask, ConsumerAdmission admission);

    /**
     * Steals an eligible entry of another batch for one lane.
     *
     * @param thiefIndex zero based index of the stealing worker
     * @param lane       lane the caller is dispatching for
     * @param levelMask  bit set of priority levels the lane may take
     * @param admission  admission check, typically the quota controller
     * @return a stolen entry, or {@code null} if stealing is disabled or nothing is eligible
     */
    @Nullable QueueEntry steal(int thiefIndex, Lane lane, int levelMask, ConsumerAdmission admission);

    /**
     * Returns the number of queued entries of one priority level.
     *
     * <p>Derived from a maintained counter, so it is {@code O(1)} and safe to read on the pump path.
     *
     * @param level priority level, {@code 0} to {@code 3}
     * @return queued entries of that level
     */
    int queuedAtLevel(int level);

    /**
     * Returns the number of queued entries a lane may take under a mask.
     *
     * @param lane      lane the caller is dispatching for
     * @param levelMask bit set of priority levels the lane may take
     * @return queued entries covered by the mask
     */
    int queuedInLane(Lane lane, int levelMask);

    /**
     * Returns how long the oldest queued entry of one priority level has waited.
     *
     * <p>The answer comes from the per consumer heads, so it is {@code O(consumers)} and never walks
     * the backlog. It is the input of the NORMAL spill wait trigger.
     *
     * @param level    priority level, {@code 0} to {@code 3}
     * @param nowNanos current reading of the scheduler clock
     * @return the oldest wait in nanoseconds, or {@code 0} when that level is empty
     */
    long oldestWaitNanos(int level, long nowNanos);
}
