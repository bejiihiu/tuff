package kz.bejiihiu.tuff.compute.internal.metrics;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.LatencyHistogram;
import kz.bejiihiu.tuff.compute.internal.metrics.impl.LatencyStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LatencyHistogramTest {

    @Test
    void emptyHistogramReportsZeros() {
        final LatencyHistogram histogram = new LatencyHistogram();

        final LatencyStats stats = histogram.capture();

        assertEquals(0L, histogram.count());
        assertEquals(0L, stats.count());
        assertEquals(0.0d, stats.meanNanos());
        assertEquals(0L, stats.maxNanos());
        assertEquals(0L, stats.p50Nanos());
        assertEquals(0L, stats.p999Nanos());
    }

    @Test
    void valuesBelowExactRangeAreRecordedExactly() {
        final LatencyHistogram histogram = new LatencyHistogram();
        for (int value = 0; value < LatencyHistogram.SUB_BUCKET_COUNT; value++) {
            histogram.record(value);
        }

        final LatencyStats stats = histogram.capture();

        assertEquals(LatencyHistogram.SUB_BUCKET_COUNT, stats.count());
        assertEquals(127L, stats.p50Nanos());
        assertEquals(230L, stats.p90Nanos());
        assertEquals(243L, stats.p95Nanos());
        assertEquals(253L, stats.p99Nanos());
        assertEquals(255L, stats.maxNanos());
    }

    @Test
    void singleSampleIsRepresentedWithinTheDeclaredError() {
        final LatencyHistogram histogram = new LatencyHistogram();
        final long value = 5_000_000L;
        histogram.record(value);

        final LatencyStats stats = histogram.capture();

        assertEquals(1L, stats.count());
        assertEquals((double) value, stats.meanNanos());
        assertEquals(value, stats.maxNanos());
        assertWithinRelativeError(value, stats.p50Nanos());
        assertWithinRelativeError(value, stats.p99Nanos());
        assertWithinRelativeError(value, stats.p999Nanos());
    }

    @Test
    void percentilesStayInsideTheDeclaredErrorOnALargeKnownSet() {
        final Random random = new Random(0x5EEDL);
        final long[] samples = new long[200_000];
        final LatencyHistogram histogram = new LatencyHistogram();
        for (int i = 0; i < samples.length; i++) {
            samples[i] = 1L + random.nextInt(50_000_000);
            histogram.record(samples[i]);
        }
        Arrays.sort(samples);

        final LatencyStats stats = histogram.capture();

        assertPercentileWithinError(samples, 0.50d, stats.p50Nanos());
        assertPercentileWithinError(samples, 0.90d, stats.p90Nanos());
        assertPercentileWithinError(samples, 0.95d, stats.p95Nanos());
        assertPercentileWithinError(samples, 0.99d, stats.p99Nanos());
        assertPercentileWithinError(samples, 0.999d, stats.p999Nanos());
    }

    @Test
    void percentilesAreNonDecreasingAcrossBucketBoundaries() {
        final LatencyHistogram histogram = new LatencyHistogram();
        for (long value = 1L; value <= 2_000_000L; value += 7L) {
            histogram.record(value);
        }

        final LatencyStats stats = histogram.capture();

        assertTrue(stats.p50Nanos() <= stats.p90Nanos());
        assertTrue(stats.p90Nanos() <= stats.p95Nanos());
        assertTrue(stats.p95Nanos() <= stats.p99Nanos());
        assertTrue(stats.p99Nanos() <= stats.p999Nanos());
        assertTrue(stats.p999Nanos() <= stats.maxNanos());
        assertWithinRelativeError(2_000_000L, stats.maxNanos());
    }

    @Test
    void negativeSamplesAreRecordedAsZero() {
        final LatencyHistogram histogram = new LatencyHistogram();

        histogram.record(-123L);

        assertEquals(1L, histogram.count());
        assertEquals(0L, histogram.maxNanos());
        assertEquals(0L, histogram.capture().p99Nanos());
    }

    @Test
    void maximumLongSaturatesInsteadOfOverflowing() {
        final LatencyHistogram histogram = new LatencyHistogram();

        histogram.record(Long.MAX_VALUE);

        final LatencyStats stats = histogram.capture();
        assertEquals(1L, stats.count());
        assertEquals(LatencyHistogram.DEFAULT_HIGHEST_TRACKABLE_NANOS, stats.maxNanos());
        assertTrue(stats.p999Nanos() <= LatencyHistogram.DEFAULT_HIGHEST_TRACKABLE_NANOS);
        assertTrue(stats.p999Nanos() > 0L);
    }

    @Test
    void meanAndMaximumAreExact() {
        final LatencyHistogram histogram = new LatencyHistogram();
        histogram.record(100L);
        histogram.record(200L);
        histogram.record(900L);

        final LatencyStats stats = histogram.capture();

        assertEquals(400.0d, stats.meanNanos());
        assertEquals(900L, stats.maxNanos());
    }

    @Test
    @Timeout(30)
    void concurrentRecordingKeepsTheCountExact() throws InterruptedException {
        final int threads = 8;
        final int perThread = 25_000;
        final LatencyHistogram histogram = new LatencyHistogram();
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            final int worker = i;
            final Thread thread = new Thread(() -> {
                try {
                    start.await();
                    for (int sample = 0; sample < perThread; sample++) {
                        histogram.record(1_000L + worker * 1_000L + sample);
                    }
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }, "histogram-test-" + i);
            thread.start();
        }
        start.countDown();
        assertTrue(done.await(20, TimeUnit.SECONDS));

        assertEquals((long) threads * perThread, histogram.count());
        assertEquals((long) threads * perThread, histogram.capture().count());
    }

    @Test
    void rejectsAHighestValueBelowTheExactRange() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new LatencyHistogram(LatencyHistogram.SUB_BUCKET_COUNT - 1L)
        );
    }

    @Test
    void bucketBoundariesStayInsideTheDeclaredError() {
        final long[] boundaries = {
            256L, 511L, 512L, 1_024L, 2_048L, 4_096L, 65_536L, 1_048_576L, 16_777_216L, 536_870_912L
        };
        final LatencyHistogram histogram = new LatencyHistogram();
        for (final long boundary : boundaries) {
            histogram.record(boundary);
        }

        final LatencyStats stats = histogram.capture();

        assertEquals(boundaries.length, stats.count());
        assertEquals(536_870_912L, stats.maxNanos());
        assertWithinRelativeError(2_048L, stats.p50Nanos());
        assertWithinRelativeError(16_777_216L, stats.p90Nanos());
        assertWithinRelativeError(536_870_912L, stats.p999Nanos());
    }

    private static void assertPercentileWithinError(final long[] sorted, final double percentile, final long reported) {
        final long exact = exactPercentile(sorted, percentile);
        final double allowed = Math.max(1.0d, exact * LatencyHistogram.RELATIVE_ERROR_BOUND);
        assertTrue(
            Math.abs((double) reported - (double) exact) <= allowed,
            "percentile " + percentile + ": reported " + reported + ", exact " + exact + ", allowed " + allowed
        );
    }

    private static void assertWithinRelativeError(final long exact, final long reported) {
        final double allowed = Math.max(1.0d, exact * LatencyHistogram.RELATIVE_ERROR_BOUND);
        assertTrue(
            Math.abs((double) reported - (double) exact) <= allowed,
            "reported " + reported + ", exact " + exact + ", allowed " + allowed
        );
    }

    private static long exactPercentile(final long[] sorted, final double percentile) {
        final int rank = Math.max(1, Math.min(sorted.length, (int) Math.ceil(sorted.length * percentile)));
        return sorted[rank - 1];
    }
}
