package kz.bejiihiu.tuff.threads.probe;

import java.util.List;
import kz.bejiihiu.tuff.threads.RegionPoolMetrics;
import kz.bejiihiu.tuff.threads.internal.spi.PoolObservation;
import kz.bejiihiu.tuff.threads.observe.TuffErrorLogCapture;
import kz.bejiihiu.tuff.threads.region.RegionPoolMetricsAggregator;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The probe must survive a server that has no regions yet, or no region scheduler at all.
 *
 * <p>Observe-only starts before Folia's regions exist, and it also runs against a process without a
 * region scheduler. Both cases have to read as "nothing measured": zero threads, {@code -1} for the
 * queue depth and capacity, and no region summary at all - an empty summary line would look like a
 * measured pool of zero regions. Never an exception, never an error line, never an invented zero.
 *
 * <p>The aggregates themselves are checked separately: folding zero readings has to answer zero
 * regions and {@code -1} for every timing and TPS value, which is the "unknown" the report prints.
 */
@Timeout(120)
class RegionPoolProbeEmptyDataTest {

    @Test
    void anEmptyGroupAndAZeroCountReadAsEmptyAndUnknown() {
        final RegionThreadScan scan = new RegionThreadScan(() -> List.of(), () -> 0);

        final PoolObservation observation = new RegionPoolProbe(scan).observe();

        assertEquals(0, observation.aliveThreads());
        assertEquals(0, observation.coreThreads(), "observe-only claims no floor");
        assertEquals(0, observation.activeThreads());
        assertEquals(0, observation.idleThreads());
        assertEquals(-1L, observation.queuedTasks(), "queue depth is unmeasured, not zero");
        assertEquals(-1L, observation.queueCapacity());
        assertTrue(observation.threads().isEmpty());
        assertTrue(observation.regionMetrics().isEmpty(), "no regions is nothing to say, not a zero reading");
    }

    @Test
    void foldingNoRegionsAnswersZeroAndUnknown() {
        final RegionPoolMetrics metrics = RegionPoolMetricsAggregator.aggregate(List.of());

        assertEquals(0, metrics.regions());
        assertEquals(-1.0d, metrics.averageMspt5s());
        assertEquals(-1.0d, metrics.maximumMspt5s());
        assertEquals(-1.0d, metrics.averageMspt15s());
        assertEquals(-1.0d, metrics.maximumMspt15s());
        assertEquals(-1.0d, metrics.minimumTps(), "no region reported, so no minimum TPS exists");
        assertEquals(0, metrics.laggingRegions());
        assertEquals(0.0d, metrics.lagFraction(), "an empty pool has nothing late");
        assertEquals(0, metrics.entities() + metrics.players() + metrics.chunks());
    }

    @Test
    void aCountWithoutAGroupStillReportsTheSchedulerNumber() {
        // the scheduler exists and reports its threads, but the group scan finds none - a shut down
        // scheduler or a renamed group. the count is authoritative, the list is best effort
        final RegionThreadScan scan = new RegionThreadScan(() -> List.of(), () -> 4);

        final PoolObservation observation = new RegionPoolProbe(scan).observe();

        assertEquals(4, observation.aliveThreads(), "the scheduler count is authoritative");
        assertEquals(0, observation.activeThreads());
        assertEquals(0, observation.idleThreads());
        assertTrue(observation.threads().isEmpty());
    }

    @Test
    void theLiveProbeInAProcessWithoutRegionsDoesNotThrowOrLog() {
        final @Nullable TuffErrorLogCapture capture = TuffErrorLogCapture.attach();
        Assumptions.assumeTrue(capture != null, "no log4j2 core on the test classpath");
        try (capture) {
            final PoolObservation observation = new RegionPoolProbe().observe();

            assertEquals(0, observation.aliveThreads(), "no scheduler means no live region threads");
            assertEquals(0, observation.coreThreads());
            assertEquals(-1L, observation.queuedTasks());
            assertEquals(-1L, observation.queueCapacity());
            assertTrue(observation.threads().isEmpty());
            assertTrue(observation.regionMetrics().isEmpty());
            assertTrue(capture.errors().isEmpty(), () -> "an empty region pool logged an error: " + capture.errors());
        }
    }
}
