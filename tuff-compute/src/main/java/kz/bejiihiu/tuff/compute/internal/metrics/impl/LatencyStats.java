package kz.bejiihiu.tuff.compute.internal.metrics.impl;

import java.time.Duration;

/**
 * Immutable view of a {@link LatencyHistogram}.
 *
 * <p>Means and maxima are exact; percentiles are bucket midpoints and carry the relative error
 * documented on {@link LatencyHistogram}.
 *
 * @param count     number of samples in the histogram
 * @param meanNanos arithmetic mean in nanoseconds
 * @param maxNanos  largest sample in nanoseconds
 * @param p50Nanos  50th percentile in nanoseconds
 * @param p90Nanos  90th percentile in nanoseconds
 * @param p95Nanos  95th percentile in nanoseconds
 * @param p99Nanos  99th percentile in nanoseconds
 * @param p999Nanos 99.9th percentile in nanoseconds
 */
public record LatencyStats(
    long count,
    double meanNanos,
    long maxNanos,
    long p50Nanos,
    long p90Nanos,
    long p95Nanos,
    long p99Nanos,
    long p999Nanos
) {

    private static final LatencyStats EMPTY = new LatencyStats(0L, 0.0d, 0L, 0L, 0L, 0L, 0L, 0L);

    /**
     * Returns the stats of an empty histogram.
     *
     * @return all zero stats
     */
    public static LatencyStats empty() {
        return EMPTY;
    }

    /**
     * Returns the arithmetic mean as a duration.
     *
     * @return the mean, truncated to whole nanoseconds
     */
    public Duration mean() {
        return Duration.ofNanos((long) this.meanNanos);
    }

    /**
     * Returns the largest sample as a duration.
     *
     * @return the maximum
     */
    public Duration max() {
        return Duration.ofNanos(this.maxNanos);
    }

    /**
     * Returns the 50th percentile as a duration.
     *
     * @return the median
     */
    public Duration p50() {
        return Duration.ofNanos(this.p50Nanos);
    }

    /**
     * Returns the 90th percentile as a duration.
     *
     * @return the p90
     */
    public Duration p90() {
        return Duration.ofNanos(this.p90Nanos);
    }

    /**
     * Returns the 95th percentile as a duration.
     *
     * @return the p95
     */
    public Duration p95() {
        return Duration.ofNanos(this.p95Nanos);
    }

    /**
     * Returns the 99th percentile as a duration.
     *
     * @return the p99
     */
    public Duration p99() {
        return Duration.ofNanos(this.p99Nanos);
    }

    /**
     * Returns the 99.9th percentile as a duration.
     *
     * @return the p999
     */
    public Duration p999() {
        return Duration.ofNanos(this.p999Nanos);
    }
}
