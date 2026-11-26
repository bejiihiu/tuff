package kz.bejiihiu.tuff.threads;

import java.time.Duration;
import java.util.Objects;

/**
 * Thrown when a lease could not be granted in time.
 *
 * <p>This is a normal outcome under pressure, not a crash: the caller still owns the work, has not
 * started it and decides whether to defer, drop or run it elsewhere. The exception carries the pool and
 * the wait it actually performed so the caller can report a useful message instead of "timeout".
 */
public final class LeaseTimeoutException extends ThreadManagementException {

    /**
     * Pool the lease was requested from.
     */
    private final PoolId pool;

    /**
     * How long the request waited.
     */
    private final Duration waited;

    /**
     * Creates a timeout failure.
     *
     * @param pool   pool the lease was requested from
     * @param waited how long the request waited
     * @throws NullPointerException if a reference is {@code null}
     */
    public LeaseTimeoutException(final PoolId pool, final Duration waited) {
        super(
            "no lease from pool '" + Objects.requireNonNull(pool, "pool").value()
                + "' within " + Objects.requireNonNull(waited, "waited")
        );
        this.pool = pool;
        this.waited = waited;
    }

    /**
     * Returns the pool the lease was requested from.
     *
     * @return the pool id
     */
    public PoolId pool() {
        return this.pool;
    }

    /**
     * Returns how long the request waited.
     *
     * @return the wait duration, zero for a non blocking attempt
     */
    public Duration waited() {
        return this.waited;
    }
}
