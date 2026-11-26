package kz.bejiihiu.tuff.compute;

import java.time.Duration;
import java.util.Objects;

/**
 * Limits and scheduling hints that apply to one consumer.
 *
 * <p>A policy is immutable. Changing limits at runtime goes through
 * {@link AsyncComputeScheduler#updateConsumerPolicy(String, ConsumerPolicy)}, which replaces the
 * stored policy atomically; already queued tasks are kept and are re-evaluated against the new
 * policy from then on.
 *
 * <p>The relative concurrency limit is a share of the total execution capacity (the platform worker
 * pool plus the virtual lane permits when the lane is enabled), not a CPU reservation, so one
 * consumer can use both lanes at the same time. An absolute limit is not a share and is never capped
 * by that capacity. The same limit is enforced across both lanes, so a virtual task holds a slot
 * exactly like a worker task. It only caps how many tasks of one consumer may run at the same time,
 * it never guarantees that those slots are actually available.
 *
 * <p>The queue wait limit is enforced by the queue itself with a periodic sweep, not only when a
 * worker picks a task up, so expired work never stays in the bounded queue just because higher
 * priority work keeps winning.
 *
 * @param enabled            whether the consumer may submit at all; a disabled consumer rejects every
 *                           submission with {@link ComputeRejectedException.Reason#CONSUMER_DISABLED}
 *                           but keeps its metrics
 * @param concurrencyLimit   how many tasks of this consumer may execute concurrently, across both
 *                           execution lanes
 * @param maxQueuedTasks     upper bound on queued (not running) tasks of this consumer, or {@code -1}
 *                           for no per consumer bound, in which case only the global queue capacity
 *                           applies
 * @param fairShareWeight    relative weight used by weighted fair scheduling; every consumer gets a
 *                           share proportional to its weight while queues are contended, so a weight
 *                           of {@code 2.0} means twice as many dispatch turns as {@code 1.0}
 * @param overflowPolicy     what happens when this consumer cannot queue more work
 * @param maxQueueWait       maximum time a task of this consumer may wait in the queue before it is
 *                           cancelled with {@link ComputeCancelledException.Reason#QUEUE_EXPIRED};
 *                           {@link Duration#ZERO} disables expiry
 */
public record ConsumerPolicy(
    boolean enabled,
    ConcurrencyLimit concurrencyLimit,
    int maxQueuedTasks,
    double fairShareWeight,
    OverflowPolicy overflowPolicy,
    Duration maxQueueWait
) {

    /**
     * Validates the policy.
     *
     * @throws IllegalArgumentException if a limit is negative, a weight is not a positive finite
     *     number, the queue wait is negative or the overflow policy is {@code null}
     */
    public ConsumerPolicy {
        Objects.requireNonNull(concurrencyLimit, "concurrencyLimit");
        Objects.requireNonNull(overflowPolicy, "overflowPolicy");
        Objects.requireNonNull(maxQueueWait, "maxQueueWait");
        if (maxQueuedTasks < -1) {
            throw new IllegalArgumentException("maxQueuedTasks must be -1 or non negative, got " + maxQueuedTasks);
        }
        if (!Double.isFinite(fairShareWeight) || fairShareWeight <= 0.0d) {
            throw new IllegalArgumentException("fairShareWeight must be a positive finite number, got " + fairShareWeight);
        }
        if (maxQueueWait.isNegative()) {
            throw new IllegalArgumentException("maxQueueWait must not be negative, got " + maxQueueWait);
        }
    }

    /**
     * Returns the policy used for consumers that are registered without an explicit one.
     *
     * <p>It allows the whole execution capacity, does not bound the queue on its own, uses a fair
     * share weight of {@code 1.0}, rejects overflow and never expires queued work.
     *
     * @return the default policy
     */
    public static ConsumerPolicy defaults() {
        return new ConsumerPolicy(true, ConcurrencyLimit.automatic(), -1, 1.0d, OverflowPolicy.REJECT, Duration.ZERO);
    }

    /**
     * Creates a policy with an absolute number of concurrent tasks.
     *
     * <p>The number is shared by both lanes and is not a share of anything, so it is not capped by
     * the current execution capacity.
     *
     * @param permits absolute concurrency limit, at least {@code 1}
     * @return a policy with an absolute concurrency limit and the remaining defaults
     * @throws IllegalArgumentException if {@code permits} is not positive
     */
    public static ConsumerPolicy absolute(final int permits) {
        return new ConsumerPolicy(true, ConcurrencyLimit.absolute(permits), -1, 1.0d, OverflowPolicy.REJECT, Duration.ZERO);
    }

    /**
     * Creates a policy that may use a fraction of the execution capacity concurrently.
     *
     * @param fraction share of the execution capacity, greater than {@code 0.0} and at most {@code 1.0}
     * @return a policy with a relative concurrency limit and the remaining defaults
     * @throws IllegalArgumentException if the fraction is outside {@code (0.0, 1.0]}
     */
    public static ConsumerPolicy relative(final double fraction) {
        return new ConsumerPolicy(true, ConcurrencyLimit.relative(fraction), -1, 1.0d, OverflowPolicy.REJECT, Duration.ZERO);
    }

    /**
     * Resolves the configured limit against the current execution capacity.
     *
     * <p>A relative limit is rounded to the nearest whole slot of the capacity and never exceeds it.
     * An absolute limit is not a share, so it is returned as configured even when the capacity is
     * smaller; the lanes themselves still bound how many of those slots can actually run. The result
     * is always at least {@code 1}, so an enabled consumer is never silently starved by rounding.
     *
     * @param executionCapacity platform workers plus virtual lane permits, at least {@code 1}
     * @return number of tasks of this consumer that may run concurrently across both lanes
     * @throws IllegalArgumentException if {@code executionCapacity} is not positive
     */
    public int resolveConcurrencyLimit(final int executionCapacity) {
        return this.concurrencyLimit.resolve(executionCapacity);
    }

    /**
     * Checks whether this policy bounds the consumer queue.
     *
     * @return {@code true} if {@link #maxQueuedTasks()} is not {@code -1}
     */
    public boolean boundsQueue() {
        return this.maxQueuedTasks >= 0;
    }

    /**
     * Checks whether this policy expires queued tasks.
     *
     * @return {@code true} if {@link #maxQueueWait()} is not {@link Duration#ZERO}
     */
    public boolean expiresQueuedTasks() {
        return !this.maxQueueWait.isZero();
    }

    /**
     * Concurrency limit of a consumer.
     *
     * <p>Two flavours exist because a fixed number of slots is easy to reason about, while a share of
     * the execution capacity follows automatic worker sizing and the virtual lane without editing
     * every consumer.
     */
    public sealed interface ConcurrencyLimit permits AbsoluteLimit, RelativeLimit {

        /**
         * Resolves this limit against the current execution capacity.
         *
         * @param executionCapacity platform workers plus virtual lane permits, at least {@code 1}
         * @return number of tasks that may run concurrently, always at least {@code 1}; a relative
         *     limit never exceeds the capacity, an absolute limit is returned as configured
         */
        int resolve(int executionCapacity);

        /**
         * Limits the consumer to the whole execution capacity, which is the most permissive form.
         *
         * @return a relative limit of {@code 1.0}
         */
        static ConcurrencyLimit automatic() {
            return new RelativeLimit(1.0d);
        }

        /**
         * Limits the consumer to a fixed number of concurrent tasks.
         *
         * @param permits absolute limit, at least {@code 1}
         * @return an absolute limit
         * @throws IllegalArgumentException if {@code permits} is not positive
         */
        static ConcurrencyLimit absolute(final int permits) {
            return new AbsoluteLimit(permits);
        }

        /**
         * Limits the consumer to a fraction of the execution capacity.
         *
         * @param fraction share of the capacity, greater than {@code 0.0} and at most {@code 1.0}
         * @return a relative limit
         * @throws IllegalArgumentException if the fraction is outside {@code (0.0, 1.0]}
         */
        static ConcurrencyLimit relative(final double fraction) {
            return new RelativeLimit(fraction);
        }
    }

    /**
     * Fixed number of concurrent tasks, shared by both lanes.
     *
     * @param permits absolute limit, at least {@code 1}
     */
    public record AbsoluteLimit(int permits) implements ConcurrencyLimit {

        /**
         * Validates the limit.
         *
         * @throws IllegalArgumentException if {@code permits} is not positive
         */
        public AbsoluteLimit {
            if (permits < 1) {
                throw new IllegalArgumentException("permits must be at least 1, got " + permits);
            }
        }

        /**
         * Returns the configured number of slots.
         *
         * <p>An absolute limit is not a share of anything, so it is deliberately not capped by the
         * capacity. When the capacity is smaller, the lanes simply cannot fill every slot; a larger
         * capacity does not widen it either.
         *
         * @param executionCapacity platform workers plus virtual lane permits, at least {@code 1}
         * @return the configured permits
         * @throws IllegalArgumentException if {@code executionCapacity} is not positive
         */
        @Override
        public int resolve(final int executionCapacity) {
            if (executionCapacity < 1) {
                throw new IllegalArgumentException("executionCapacity must be at least 1, got " + executionCapacity);
            }
            return this.permits;
        }
    }

    /**
     * Share of the execution capacity.
     *
     * @param fraction share of the capacity, greater than {@code 0.0} and at most {@code 1.0}
     */
    public record RelativeLimit(double fraction) implements ConcurrencyLimit {

        /**
         * Validates the share.
         *
         * @throws IllegalArgumentException if the fraction is outside {@code (0.0, 1.0]}
         */
        public RelativeLimit {
            if (!Double.isFinite(fraction) || fraction <= 0.0d || fraction > 1.0d) {
                throw new IllegalArgumentException("fraction must be in (0.0, 1.0], got " + fraction);
            }
        }

        @Override
        public int resolve(final int executionCapacity) {
            if (executionCapacity < 1) {
                throw new IllegalArgumentException("executionCapacity must be at least 1, got " + executionCapacity);
            }
            final int resolved = (int) Math.round(this.fraction * executionCapacity);
            return Math.min(executionCapacity, Math.max(1, resolved));
        }
    }
}
