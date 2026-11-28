package kz.bejiihiu.tuff.threads.region;

import java.util.List;
import kz.bejiihiu.tuff.threads.RegionPoolMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Locks the arithmetic of the region summary and, just as important, what it says about a region it could
 * not measure.
 *
 * <p>No server is involved: a reading is a value object, so every case - no regions, one region, many,
 * a lagging region, a region that has not ticked yet - is a list literal.
 */
@Timeout(120)
class RegionPoolMetricsAggregatorTest {

    @Test
    void anEmptyPoolHasNoNumbersToReport() {
        final RegionPoolMetrics metrics = RegionPoolMetricsAggregator.aggregate(List.of());

        assertEquals(0, metrics.regions());
        assertEquals(-1.0d, metrics.averageMspt5s(), 0.0d);
        assertEquals(-1.0d, metrics.maximumMspt5s(), 0.0d);
        assertEquals(-1.0d, metrics.averageMspt15s(), 0.0d);
        assertEquals(-1.0d, metrics.maximumMspt15s(), 0.0d);
        assertEquals(-1.0d, metrics.minimumTps(), 0.0d);
        assertEquals(0, metrics.laggingRegions());
        assertEquals(0.0d, metrics.lagFraction(), 0.0d);
        assertEquals(0, metrics.entities());
    }

    @Test
    void oneRegionIsItsOwnAverageAndMaximum() {
        final RegionPoolMetrics metrics = RegionPoolMetricsAggregator.aggregate(List.of(
            reading(19.8d, 19.9d, 2.5d, 7.0d, 2.7d, 9.5d, 10, 1, 30)
        ));

        assertEquals(1, metrics.regions());
        assertEquals(2.5d, metrics.averageMspt5s(), 0.0d);
        assertEquals(7.0d, metrics.maximumMspt5s(), 0.0d);
        assertEquals(2.7d, metrics.averageMspt15s(), 0.0d);
        assertEquals(9.5d, metrics.maximumMspt15s(), 0.0d);
        assertEquals(19.8d, metrics.minimumTps(), 0.0d);
        assertEquals(0, metrics.laggingRegions());
        assertEquals(10, metrics.entities());
        assertEquals(1, metrics.players());
        assertEquals(30, metrics.chunks());
    }

    @Test
    void manyRegionsAverageMeansAndKeepTheWorstTickAndSlowestRegion() {
        final RegionPoolMetrics metrics = RegionPoolMetricsAggregator.aggregate(List.of(
            reading(20.0d, 20.0d, 2.0d, 4.0d, 2.2d, 5.0d, 100, 2, 400),
            reading(19.0d, 19.5d, 4.0d, 11.0d, 4.2d, 14.0d, 200, 3, 500),
            reading(15.0d, 16.0d, 6.0d, 30.0d, 6.5d, 35.0d, 300, 4, 600)
        ));

        assertEquals(3, metrics.regions());
        assertEquals(4.0d, metrics.averageMspt5s(), 1.0e-9d, "2 + 4 + 6 over three regions");
        assertEquals(30.0d, metrics.maximumMspt5s(), 0.0d, "the maximum is one region's worst tick");
        assertEquals(4.3d, metrics.averageMspt15s(), 1.0e-9d);
        assertEquals(35.0d, metrics.maximumMspt15s(), 0.0d);
        assertEquals(15.0d, metrics.minimumTps(), 0.0d);
        assertEquals(1, metrics.laggingRegions());
        assertEquals(1.0d / 3.0d, metrics.lagFraction(), 1.0e-9d);
        assertEquals(600, metrics.entities());
        assertEquals(9, metrics.players());
        assertEquals(1500, metrics.chunks());
    }

    @Test
    void aRegionAtTheThresholdIsNotLagging() {
        final RegionPoolMetrics metrics = RegionPoolMetricsAggregator.aggregate(List.of(
            reading(RegionPoolMetrics.LAG_TPS, 18.0d, 5.0d, 5.0d, 5.0d, 5.0d, 1, 0, 1),
            reading(17.99d, 17.5d, 6.0d, 6.0d, 6.0d, 6.0d, 1, 0, 1)
        ));

        assertEquals(1, metrics.laggingRegions(), "only the region below the threshold counts");
        assertEquals(0.5d, metrics.lagFraction(), 0.0d);
    }

    @Test
    void aRegionWithoutATickReportCountsAsARegionButNotInTheNumbers() {
        final RegionPoolMetrics metrics = RegionPoolMetricsAggregator.aggregate(List.of(
            reading(20.0d, 20.0d, 2.0d, 4.0d, 2.0d, 4.0d, 10, 1, 20),
            unmeasured(5, 0, 6)
        ));

        assertEquals(2, metrics.regions(), "the region exists, it just has nothing to average yet");
        assertEquals(2.0d, metrics.averageMspt5s(), 0.0d, "the empty window must not drag the mean to zero");
        assertEquals(4.0d, metrics.maximumMspt5s(), 0.0d);
        assertEquals(20.0d, metrics.minimumTps(), 0.0d);
        assertEquals(0, metrics.laggingRegions(), "a region with no report is not a lagging region");
        assertEquals(15, metrics.entities());
        assertEquals(1, metrics.players());
        assertEquals(26, metrics.chunks());
    }

    @Test
    void everyRegionUnmeasuredStillCountsAndReportsNoNumbers() {
        final RegionPoolMetrics metrics = RegionPoolMetricsAggregator.aggregate(List.of(
            unmeasured(0, 0, 0),
            unmeasured(0, 0, 0)
        ));

        assertEquals(2, metrics.regions());
        assertEquals(-1.0d, metrics.averageMspt5s(), 0.0d);
        assertEquals(-1.0d, metrics.maximumMspt5s(), 0.0d);
        assertEquals(-1.0d, metrics.minimumTps(), 0.0d);
        assertEquals(0, metrics.laggingRegions());
        assertEquals(0.0d, metrics.lagFraction(), 0.0d);
    }

    @Test
    void negativeMeasurementsBelowTheSentinelAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> reading(-2.0d, 20.0d, 1.0d, 1.0d, 1.0d, 1.0d, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> reading(20.0d, 20.0d, -2.0d, 1.0d, 1.0d, 1.0d, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> reading(20.0d, 20.0d, 1.0d, 1.0d, 1.0d, 1.0d, -1, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new RegionPoolMetrics(
            1, 1.0d, 1.0d, 1.0d, 1.0d, 20.0d, 2, 0, 0, 0
        ));
    }

    private static RegionReading reading(
        final double tps5s,
        final double tps15s,
        final double averageMspt5s,
        final double maximumMspt5s,
        final double averageMspt15s,
        final double maximumMspt15s,
        final int entities,
        final int players,
        final int chunks
    ) {
        return new RegionReading(
            tps5s, tps15s, averageMspt5s, maximumMspt5s, averageMspt15s, maximumMspt15s, entities, players, chunks
        );
    }

    private static RegionReading unmeasured(final int entities, final int players, final int chunks) {
        return reading(-1.0d, -1.0d, -1.0d, -1.0d, -1.0d, -1.0d, entities, players, chunks);
    }
}
