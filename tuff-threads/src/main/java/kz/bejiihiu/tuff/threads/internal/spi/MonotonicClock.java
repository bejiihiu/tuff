package kz.bejiihiu.tuff.threads.internal.spi;

import java.time.Instant;

/**
 * Time source of the manager.
 *
 * <p>Deadlines, lease age and sampling windows must all use the same monotonic timeline, otherwise a
 * wall clock adjustment can make a lease look like it was granted in the future. Instant is only for
 * human facing timestamps in snapshots and events; arithmetic always uses
 * {@link #nanoTime()}.
 *
 * <p>Injecting the clock is what makes lease and saturation tests deterministic: a test clock advances
 * time without sleeping.
 */
public interface MonotonicClock {

    /**
     * Returns the current value of the monotonic clock.
     *
     * <p>The origin is arbitrary and may be negative. Only differences are meaningful and only against
     * values read from this same clock.
     *
     * @return nanoseconds on this clock's timeline
     */
    long nanoTime();

    /**
     * Returns the wall clock instant used for display.
     *
     * <p>Never used for deadlines or durations; it exists so snapshots and JFR events carry a timestamp
     * an operator can read.
     *
     * @return the current instant
     */
    Instant instant();
}
