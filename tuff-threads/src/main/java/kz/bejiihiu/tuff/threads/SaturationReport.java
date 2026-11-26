package kz.bejiihiu.tuff.threads;

import java.util.Objects;

/**
 * How saturated a pool is at one point in time.
 *
 * <p>Two independent pressures are reported separately because they mean different things:
 * {@link #utilisation()} is how much of the pool's own capacity is busy, while
 * {@link #queuePressure()} is how much work is waiting relative to what the pool can absorb. A pool
 * can be fully busy with an empty queue (healthy saturation), or half idle with a full queue (a
 * dispatch bug). {@link Level} is the summary the controller and commands use; the thresholds that map
 * numbers to a level are a policy of the metrics implementation, not part of this contract.
 *
 * @param pool             pool the report belongs to
 * @param utilisation      share of the pool's capacity in use, from zero to one
 * @param queuePressure    share of the pool's queue capacity in use, from zero to one
 * @param queuedTasks      tasks waiting in the pool queue at capture time
 * @param longestWaitNanos age of the oldest queued task at capture time, zero when nothing waits
 * @param level            summary level
 */
public record SaturationReport(
    PoolId pool,
    double utilisation,
    double queuePressure,
    long queuedTasks,
    long longestWaitNanos,
    Level level
) {

    /**
     * Validates a saturation report.
     *
     * @throws NullPointerException     if a reference is {@code null}
     * @throws IllegalArgumentException if a share is outside {@code [0, 1]} or a count is negative
     */
    public SaturationReport {
        Objects.requireNonNull(pool, "pool");
        Objects.requireNonNull(level, "level");
        if (!(utilisation >= 0.0d) || utilisation > 1.0d) {
            throw new IllegalArgumentException("utilisation must be in [0, 1], got " + utilisation);
        }
        if (!(queuePressure >= 0.0d) || queuePressure > 1.0d) {
            throw new IllegalArgumentException("queuePressure must be in [0, 1], got " + queuePressure);
        }
        if (queuedTasks < 0L) {
            throw new IllegalArgumentException("queuedTasks must not be negative, got " + queuedTasks);
        }
        if (longestWaitNanos < 0L) {
            throw new IllegalArgumentException("longestWaitNanos must not be negative, got " + longestWaitNanos);
        }
    }

    /**
     * Creates a report for a pool whose queue cannot be observed.
     *
     * <p>Everything is zero and the level is {@link Level#UNKNOWN}; this is what a pool gets when it is
     * registered but has no probe attached yet, so commands can print "not measured" instead of a fake
     * healthy zero.
     *
     * @param pool pool the report belongs to
     * @return the placeholder report
     * @throws NullPointerException if {@code pool} is {@code null}
     */
    public static SaturationReport unknown(final PoolId pool) {
        return new SaturationReport(pool, 0.0d, 0.0d, 0L, 0L, Level.UNKNOWN);
    }

    /**
     * Checks whether the pool is at its summary saturation level.
     *
     * @return {@code true} if {@link #level()} is {@link Level#SATURATED}
     */
    public boolean saturated() {
        return this.level == Level.SATURATED;
    }

    /**
     * Summary saturation level of a pool.
     */
    public enum Level {

        /**
         * The pool is not measured or has no work at all.
         */
        UNKNOWN,

        /**
         * Capacity is mostly free and nothing waits.
         */
        IDLE,

        /**
         * Work arrives at roughly the rate the pool drains it.
         */
        NORMAL,

        /**
         * The pool is busy and work starts to wait, but it still keeps up.
         */
        BUSY,

        /**
         * Work waits longer than the pool can absorb; the owner should shed, defer or ask for more
         * capacity.
         */
        SATURATED
    }
}
