package kz.bejiihiu.tuff.compute.internal.metrics.impl;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * Compact log-linear histogram for non negative latency samples, measured in nanoseconds.
 *
 * <p>The layout mirrors the idea behind HdrHistogram, without the dependency: values below
 * {@value #SUB_BUCKET_COUNT} are stored exactly, and every power of two range above that is split
 * into {@value #SUB_BUCKETS_PER_OCTAVE} equal buckets. A sample is written with a single atomic
 * increment of a preallocated slot, so recording allocates nothing and takes no lock.
 *
 * <p>Accuracy: the returned percentile is the midpoint of its bucket, which bounds the relative
 * error by {@link #RELATIVE_ERROR_BOUND} (one part in 256, about 0.39%) for values above
 * {@value #SUB_BUCKET_COUNT}. The maximum is tracked exactly from the raw samples, and samples above
 * the configured highest trackable value are saturated at that value instead of wrapping around.
 *
 * <p>The histogram is safe for concurrent recording and reading. Percentiles are computed from a
 * live view: a sample recorded while {@link #capture()} runs may or may not be included, which is
 * acceptable for diagnostics but means two consecutive captures can overlap rather than partition
 * the samples.
 */
public final class LatencyHistogram {

    /**
     * Values below this bound are stored with one nanosecond resolution.
     */
    public static final int SUB_BUCKET_COUNT = 256;

    /**
     * Number of buckets each power of two range above {@link #SUB_BUCKET_COUNT} is split into.
     */
    public static final int SUB_BUCKETS_PER_OCTAVE = 128;

    /**
     * Upper bound of the relative error of a reported percentile, one part in 256.
     */
    public static final double RELATIVE_ERROR_BOUND = 1.0d / 256.0d;

    /**
     * Default highest trackable value: ten seconds in nanoseconds.
     */
    public static final long DEFAULT_HIGHEST_TRACKABLE_NANOS = 10_000_000_000L;

    private static final int SUB_BUCKET_MAGNITUDE = 8;

    private static final double[] PERCENTILES = {0.50d, 0.90d, 0.95d, 0.99d, 0.999d};

    private final AtomicLongArray counts;
    private final LongAdder total = new LongAdder();
    private final DoubleAdder sumNanos = new DoubleAdder();
    private final LongAccumulator maxNanos = new LongAccumulator(Math::max, 0L);
    private final long highestTrackableValue;
    private final int octaves;

    /**
     * Creates a histogram with the default highest trackable value.
     */
    public LatencyHistogram() {
        this(DEFAULT_HIGHEST_TRACKABLE_NANOS);
    }

    /**
     * Creates a histogram.
     *
     * @param highestTrackableValue largest sample that is represented; later samples are saturated at
     *                              this value, must be at least {@value #SUB_BUCKET_COUNT}
     * @throws IllegalArgumentException if the value is too small
     */
    public LatencyHistogram(final long highestTrackableValue) {
        if (highestTrackableValue < SUB_BUCKET_COUNT) {
            throw new IllegalArgumentException(
                "highestTrackableValue must be at least " + SUB_BUCKET_COUNT + ", got " + highestTrackableValue
            );
        }
        this.highestTrackableValue = highestTrackableValue;
        final int highestBit = 63 - Long.numberOfLeadingZeros(highestTrackableValue);
        this.octaves = highestBit - (SUB_BUCKET_MAGNITUDE - 1);
        this.counts = new AtomicLongArray(SUB_BUCKET_COUNT + this.octaves * SUB_BUCKETS_PER_OCTAVE);
    }

    /**
     * Records one sample.
     *
     * <p>Negative values, which can only come from a broken clock reading, are recorded as zero.
     * The call never blocks and never allocates.
     *
     * @param nanos sample to record, in nanoseconds
     */
    public void record(final long nanos) {
        final long value = Math.min(Math.max(nanos, 0L), this.highestTrackableValue);
        this.total.add(1L);
        this.sumNanos.add((double) value);
        this.maxNanos.accumulate(value);
        this.counts.getAndIncrement(this.indexFor(value));
    }

    /**
     * Returns how many samples were recorded.
     *
     * @return the sample count
     */
    public long count() {
        return this.total.sum();
    }

    /**
     * Returns the arithmetic mean of the samples.
     *
     * @return mean in nanoseconds, {@code 0.0} when nothing was recorded
     */
    public double meanNanos() {
        final long sampleCount = this.total.sum();
        return sampleCount == 0L ? 0.0d : this.sumNanos.sum() / (double) sampleCount;
    }

    /**
     * Returns the largest sample that was recorded.
     *
     * @return maximum in nanoseconds, {@code 0} when nothing was recorded
     */
    public long maxNanos() {
        return this.maxNanos.get();
    }

    /**
     * Returns the largest sample that can be represented without saturation.
     *
     * @return the highest trackable value
     */
    public long highestTrackableValue() {
        return this.highestTrackableValue;
    }

    /**
     * Captures count, mean, maximum and the common percentiles in one pass over the buckets.
     *
     * @return an immutable view of the histogram
     */
    public LatencyStats capture() {
        final long sampleCount = this.total.sum();
        if (sampleCount <= 0L) {
            return LatencyStats.empty();
        }
        final long[] targets = new long[PERCENTILES.length];
        for (int i = 0; i < PERCENTILES.length; i++) {
            targets[i] = Math.max(1L, Math.min(sampleCount, (long) Math.ceil(sampleCount * PERCENTILES[i])));
        }
        final long[] values = new long[PERCENTILES.length];
        final int length = this.counts.length();
        long cumulative = 0L;
        int next = 0;
        for (int index = 0; index < length && next < targets.length; index++) {
            final long slot = this.counts.get(index);
            if (slot == 0L) {
                continue;
            }
            cumulative += slot;
            final long value = this.valueAt(index);
            while (next < targets.length && cumulative >= targets[next]) {
                values[next] = value;
                next++;
            }
        }
        final long top = this.valueAt(length - 1);
        while (next < values.length) {
            values[next] = top;
            next++;
        }
        return new LatencyStats(
            sampleCount,
            this.sumNanos.sum() / (double) sampleCount,
            this.maxNanos.get(),
            values[0],
            values[1],
            values[2],
            values[3],
            values[4]
        );
    }

    /**
     * Maps a sample to its bucket index.
     *
     * @param value the sample, already clamped to the trackable range
     * @return the bucket index
     */
    private int indexFor(final long value) {
        if (value < SUB_BUCKET_COUNT) {
            return (int) value;
        }
        final int bucket = 63 - Long.numberOfLeadingZeros(value) - (SUB_BUCKET_MAGNITUDE - 1);
        final int offset = (int) (value >>> bucket) - (SUB_BUCKET_COUNT >>> 1);
        return SUB_BUCKET_COUNT + (bucket - 1) * SUB_BUCKETS_PER_OCTAVE + offset;
    }

    /**
     * Returns the representative value of a bucket: the sample itself below the exact range and the
     * midpoint above it.
     *
     * @param index the bucket index
     * @return the representative value in nanoseconds
     */
    private long valueAt(final int index) {
        if (index < SUB_BUCKET_COUNT) {
            return index;
        }
        final int relative = index - SUB_BUCKET_COUNT;
        final int bucket = relative / SUB_BUCKETS_PER_OCTAVE + 1;
        final int offset = relative % SUB_BUCKETS_PER_OCTAVE;
        final long start = (long) SUB_BUCKET_COUNT << (bucket - 1);
        final long width = 1L << bucket;
        return Math.min(this.highestTrackableValue, start + offset * width + (width >>> 1));
    }
}
