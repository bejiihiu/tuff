package kz.bejiihiu.tuff.compute.internal.util;

import java.time.Duration;

/**
 * Duration helpers that never throw on absurd configuration values.
 *
 * <p>Every duration in the scheduler ends up as a nanosecond count. {@link Duration#toNanos()} and
 * {@link Duration#toMillis()} throw {@link ArithmeticException} once the value does not fit, which
 * used to turn a huge {@code max-queue-wait} or {@code keep-alive} into a broken expiry path or a
 * failed start. Configuration is user input, so the scheduler saturates instead of dying: a value
 * that cannot be represented is treated as "effectively forever".
 *
 * <p>Saturation is documented in {@link kz.bejiihiu.tuff.compute.ComputeSettings}: a duration above
 * about 292 years in nanoseconds is clamped to {@link Long#MAX_VALUE} nanoseconds.
 */
public final class Durations {

    private Durations() {
    }

    /**
     * Converts a duration to nanoseconds, saturating instead of throwing.
     *
     * @param duration duration to convert, must not be {@code null}
     * @return the duration in nanoseconds; {@link Long#MAX_VALUE} for a positive overflow and
     *     {@link Long#MIN_VALUE} for a negative one
     */
    public static long toNanosSaturated(final Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException _) {
            return duration.isNegative() ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
    }

    /**
     * Converts a duration to milliseconds, saturating instead of throwing.
     *
     * @param duration duration to convert, must not be {@code null}
     * @return the duration in milliseconds; {@link Long#MAX_VALUE} for a positive overflow and
     *     {@link Long#MIN_VALUE} for a negative one
     */
    public static long toMillisSaturated(final Duration duration) {
        try {
            return duration.toMillis();
        } catch (ArithmeticException _) {
            return duration.isNegative() ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
    }

    /**
     * Adds a timeout to a monotonic reading without wrapping around.
     *
     * @param nowNanos     current monotonic reading
     * @param timeoutNanos timeout to add, negative values are treated as {@code 0}
     * @return the deadline; {@link Long#MAX_VALUE} when the sum would overflow
     */
    public static long deadline(final long nowNanos, final long timeoutNanos) {
        if (timeoutNanos <= 0L) {
            return nowNanos;
        }
        if (nowNanos > Long.MAX_VALUE - timeoutNanos) {
            return Long.MAX_VALUE;
        }
        return nowNanos + timeoutNanos;
    }
}
