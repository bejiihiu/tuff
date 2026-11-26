package kz.bejiihiu.tuff.threads;

import java.time.Duration;
import java.util.Objects;

/**
 * Request for one unit of execution capacity.
 *
 * <p>The deadline is an absolute point on the same monotonic timeline as
 * {@link System#nanoTime()}: a tick passes its next scheduled start, a background job passes the
 * moment its result stops being useful. The controller uses it to order waiters and to decide whether
 * a request is still worth granting; it never kills a lease that is already granted, because the work
 * behind it may already be touching state.
 *
 * <p>{@link #maxWait()} is the caller's patience, not the deadline. Zero means "try once and tell me
 * now"; a positive duration means the caller is willing to block up to that long or until the deadline,
 * whichever comes first. A zero wait request never blocks, which is what a tick thread or a network
 * event loop must use. A positive wait is only legal outside the region execution pool: a tick thread
 * waiting for a lease would be waiting for itself.
 *
 * @param pool          pool the capacity is requested from
 * @param deadlineNanos absolute deadline on the {@link System#nanoTime()} timeline
 * @param maxWait       how long the caller may block, zero for a non blocking attempt
 */
public record LeaseRequest(PoolId pool, long deadlineNanos, Duration maxWait) {

    /**
     * Validates a lease request.
     *
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if {@code maxWait} is negative
     */
    public LeaseRequest {
        Objects.requireNonNull(pool, "pool");
        Objects.requireNonNull(maxWait, "maxWait");
        if (maxWait.isNegative()) {
            throw new IllegalArgumentException("maxWait must not be negative, got " + maxWait);
        }
    }

    /**
     * Creates a non blocking request.
     *
     * @param pool          pool the capacity is requested from
     * @param deadlineNanos absolute deadline on the {@link System#nanoTime()} timeline
     * @return the request with a zero wait
     * @throws NullPointerException if {@code pool} is {@code null}
     */
    public static LeaseRequest immediate(final PoolId pool, final long deadlineNanos) {
        return new LeaseRequest(pool, deadlineNanos, Duration.ZERO);
    }

    /**
     * Checks whether the caller refuses to block.
     *
     * @return {@code true} if {@link #maxWait()} is zero
     */
    public boolean immediate() {
        return this.maxWait.isZero();
    }
}
