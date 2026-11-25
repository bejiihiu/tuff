package kz.bejiihiu.tuff.compute.internal.metrics.impl;

import java.time.Duration;
import java.util.Objects;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;

/**
 * Throughput meter driven by periodic samples of a monotonically growing completion counter.
 *
 * <p>The meter keeps a short ring of {@code (timestamp, cumulative count)} samples, so it can report
 * three different numbers without a background thread of its own:
 * <ul>
 *     <li>{@link #perSecond()} — completed tasks per second across the configured window, which is the
 *     value the public snapshot exposes as throughput;</li>
 *     <li>{@link #ewmaPerSecond()} — an exponentially weighted moving average with a time constant
 *     equal to the window, responsive to load changes;</li>
 *     <li>{@link #lastSecondPerSecond()} — the rate of the most recent sample interval, one second
 *     with the default configuration.</li>
 * </ul>
 *
 * <p>{@link #sample(long)} is meant to be called by one sampler thread, but it is synchronized so
 * tests and a forced snapshot cannot corrupt the ring. The clock is injected, which makes every
 * number here deterministic in tests.
 */
public final class ThroughputMeter {

    private static final int RING_SIZE = 64;

    private final ComputeClock clock;
    private final long windowNanos;
    private final long[] totals = new long[RING_SIZE];
    private final long[] times = new long[RING_SIZE];

    private int writeIndex;
    private int size;
    private boolean primed;
    private long lastTotal;
    private long lastSampleNanos;
    private volatile double windowedPerSecond;
    private volatile double ewmaPerSecond;
    private volatile double lastSecondPerSecond;

    /**
     * Creates a meter.
     *
     * @param clock  time source, must not be {@code null}
     * @param window rolling window used by {@link #perSecond()} and by the EWMA time constant, must be
     *               positive
     * @throws NullPointerException     if the clock is {@code null}
     * @throws IllegalArgumentException if the window is not positive
     */
    public ThroughputMeter(final ComputeClock clock, final Duration window) {
        this.clock = Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(window, "window");
        if (window.isNegative() || window.isZero()) {
            throw new IllegalArgumentException("window must be positive, got " + window);
        }
        this.windowNanos = window.toNanos();
    }

    /**
     * Records one throughput sample.
     *
     * <p>The first call only establishes a baseline, because a rate needs two points in time.
     *
     * @param totalCompleted cumulative number of completed tasks, must not decrease between calls
     */
    public synchronized void sample(final long totalCompleted) {
        final long now = this.clock.nanoTime();
        this.totals[this.writeIndex] = totalCompleted;
        this.times[this.writeIndex] = now;
        this.writeIndex = (this.writeIndex + 1) % RING_SIZE;
        if (this.size < RING_SIZE) {
            this.size++;
        }
        if (!this.primed) {
            this.primed = true;
            this.lastTotal = totalCompleted;
            this.lastSampleNanos = now;
            return;
        }
        final long elapsed = now - this.lastSampleNanos;
        if (elapsed > 0L) {
            this.lastSecondPerSecond = (double) (totalCompleted - this.lastTotal) * 1_000_000_000.0d / (double) elapsed;
            final double alpha = Math.min(1.0d, (double) elapsed / (double) this.windowNanos);
            this.ewmaPerSecond = this.ewmaPerSecond + alpha * (this.lastSecondPerSecond - this.ewmaPerSecond);
        }
        this.lastTotal = totalCompleted;
        this.lastSampleNanos = now;
        this.windowedPerSecond = this.windowedRate(now, totalCompleted);
    }

    /**
     * Returns completed tasks per second across the configured window.
     *
     * @return the windowed rate, {@code 0.0} before the second sample
     */
    public double perSecond() {
        return this.windowedPerSecond;
    }

    /**
     * Returns the exponentially weighted rate.
     *
     * @return the smoothed rate, {@code 0.0} before the second sample
     */
    public double ewmaPerSecond() {
        return this.ewmaPerSecond;
    }

    /**
     * Returns the rate of the most recent sample interval.
     *
     * @return the latest rate, {@code 0.0} before the second sample
     */
    public double lastSecondPerSecond() {
        return this.lastSecondPerSecond;
    }

    /**
     * Drops every sample and rate, as if the meter was just created.
     */
    public synchronized void reset() {
        this.writeIndex = 0;
        this.size = 0;
        this.primed = false;
        this.lastTotal = 0L;
        this.lastSampleNanos = 0L;
        this.windowedPerSecond = 0.0d;
        this.ewmaPerSecond = 0.0d;
        this.lastSecondPerSecond = 0.0d;
    }

    /**
     * Computes completed tasks per second over the ring entries that are still inside the window.
     *
     * <p>The baseline is the newest sample at or before {@code now - window}; when the meter is
     * younger than the window the first sample is used instead, so the rate is a lifetime average
     * until enough history exists.
     *
     * @param now            current monotonic time
     * @param totalCompleted current cumulative count
     * @return the windowed rate
     */
    private double windowedRate(final long now, final long totalCompleted) {
        final long oldestAllowed = now - this.windowNanos;
        final int oldestIndex = (this.writeIndex - this.size + RING_SIZE) % RING_SIZE;
        long baselineTotal = Long.MIN_VALUE;
        long baselineTime = Long.MIN_VALUE;
        for (int i = 0; i < this.size; i++) {
            final int index = (this.writeIndex - 1 - i + RING_SIZE) % RING_SIZE;
            final long sampleTime = this.times[index];
            final long sampleTotal = this.totals[index];
            if (sampleTime <= oldestAllowed) {
                baselineTime = sampleTime;
                baselineTotal = sampleTotal;
                break;
            }
        }
        if (baselineTime == Long.MIN_VALUE) {
            baselineTime = this.times[oldestIndex];
            baselineTotal = this.totals[oldestIndex];
        }
        final long elapsed = now - baselineTime;
        if (elapsed <= 0L) {
            return 0.0d;
        }
        return (double) (totalCompleted - baselineTotal) * 1_000_000_000.0d / (double) elapsed;
    }
}
