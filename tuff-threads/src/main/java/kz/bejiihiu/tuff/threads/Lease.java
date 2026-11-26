package kz.bejiihiu.tuff.threads;

/**
 * Granted right to use one slot or core of a pool.
 *
 * <p>A lease is the only mutable handle the public API exposes, and only in the sense that releasing it
 * changes its state. Everything else about it is fixed at grant time. The holder must release it when
 * the unit of work ends, before {@link #deadlineNanos()} whenever possible, and exactly once in the
 * happy path; {@link #release()} is idempotent, so a {@code finally} block plus an explicit release is
 * safe.
 *
 * <p>Releasing late is not an error the controller can fix: the work already ran. It is reported as
 * degraded saturation and lease statistics, because a pool that systematically overruns its deadline
 * is undersized and the operator needs to see that instead of a silent overcommit.
 *
 * <p>Threading: all methods are thread safe. A lease may be released from a different thread than the
 * one that acquired it, which happens when work is handed off; {@link #isReleased()} is a hint for
 * diagnostics, not a synchronisation primitive.
 */
public interface Lease extends AutoCloseable {

    /**
     * Returns the unique id of this lease.
     *
     * <p>Ids are monotonic per manager lifetime and never reused, which makes them usable as keys in
     * logs, JFR events and diagnostics.
     *
     * @return the lease id
     */
    long uid();

    /**
     * Returns the pool the capacity was taken from.
     *
     * @return the pool id
     */
    PoolId pool();

    /**
     * Returns the moment the lease was granted.
     *
     * @return a {@link System#nanoTime()} value
     */
    long grantedNanos();

    /**
     * Returns the deadline the holder promised to finish by.
     *
     * @return a {@link System#nanoTime()} value, copied from the request
     */
    long deadlineNanos();

    /**
     * Checks whether the lease has already been released.
     *
     * @return {@code true} after the first successful {@link #release()}
     */
    boolean isReleased();

    /**
     * Releases the lease.
     *
     * <p>Idempotent and non throwing: releasing twice, releasing after shutdown and releasing from
     * another thread are all legal and have no effect beyond the first call. The controller may hand the
     * capacity to the next waiter before this method returns, so the caller must not use the lease or
     * the work slot after releasing it.
     */
    void release();

    /**
     * Releases the lease when used in a try with resources block.
     *
     * <p>Equivalent to {@link #release()}.
     */
    @Override
    default void close() {
        this.release();
    }
}
