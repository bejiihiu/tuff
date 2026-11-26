package kz.bejiihiu.tuff.threads.bootstrap;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.RegionPoolMetrics;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadsReportTest {

    @Test
    void aNotInitialisedLayerSaysSo() {
        assertContains(ThreadsReport.overview(ThreadsStatus.notInitialised()), "has not been initialized");
    }

    @Test
    void aDisabledLayerExplainsHowToEnableIt() {
        final List<String> lines = ThreadsReport.overview(
            ThreadsStatus.disabled(ThreadManagementSettings.defaults())
        );

        assertContains(lines, "disabled in config/tuff.yml");
        assertContains(lines, "thread-management.enabled: false");
        assertContains(lines, "restart the server");
    }

    @Test
    void aFailedStartPointsAtTheLogger() {
        assertContains(ThreadsReport.overview(ThreadsStatus.failed()), "\"Tuff/Threads\" logger");
    }

    @Test
    void aRunningLayerWithoutASnapshotSaysToRetry() {
        final ThreadsStatus status = ThreadsStatus.running(ThreadManagementSettings.defaults());

        assertContains(ThreadsReport.overview(status), "no snapshot has been taken yet");
    }

    @Test
    void aRunningLayerListsEveryPoolWithCoverageAndCounts() {
        final String text = String.join("\n", ThreadsReport.overview(runningStatus(12)));

        assertTrue(text.contains("running, all pools observed only"), text);
        assertTrue(text.contains("8 usable after the reserve, 1 reserved"), text);
        assertTrue(text.contains("region-execution"), text);
        assertTrue(text.contains("L1"), text);
        assertTrue(text.contains("alive 8"), text);
        assertTrue(text.contains("active 3"), text);
        assertTrue(text.contains("idle 5"), text);
        assertTrue(text.contains("OBSERVE_ONLY"), text);
        assertTrue(text.contains("unknown threads: 12"), text);
        assertTrue(text.contains("after a restart"), text);
    }

    @Test
    void unknownCountsArePrintedAsUnknownNotZero() {
        final PoolSpec spec = PoolSpec.observeOnly(
            PoolId.REGION_EXECUTION, PoolKind.REGION_EXECUTION, "folia", "region tick execution"
        );
        final PoolSnapshot pool = new PoolSnapshot(
            spec, 4, 0, -1, -1, -1L, -1L, -1L, -1L, -1L, -1.0d,
            SaturationReport.unknown(PoolId.REGION_EXECUTION), List.of()
        );
        final ResourceSnapshot snapshot = new ResourceSnapshot(
            Instant.EPOCH, 8, 1, 4, 0, 0, 4, 0.0d, 0.0d, -1.0d, List.of(pool), List.of(), Set.of(), Optional.empty()
        );
        final ThreadsStatus status = new ThreadsStatus(
            ThreadsStatus.State.RUNNING, ThreadManagementSettings.defaults(), snapshot, 0, true
        );

        final String text = String.join("\n", ThreadsReport.overview(status));

        assertTrue(text.contains("active unknown"), text);
        assertTrue(text.contains("idle unknown"), text);
        assertFalse(text.contains("active -1"), "an unknown count must never be printed as a negative number");
    }

    @Test
    void disabledMetricsReportTheUnknownThreadCountAsUnavailable() {
        final String text = String.join("\n", ThreadsReport.overview(runningStatus(-1, false)));

        assertTrue(text.contains("unknown threads: n/a (not measured"), text);
        assertTrue(text.contains("metrics: disabled"), text);
        assertTrue(text.contains("degraded: unknown (metrics are disabled)"), text);
    }

    @Test
    void aRegionPoolPrintsItsTickHealth() {
        final PoolSpec spec = PoolSpec.observeOnly(
            PoolId.REGION_EXECUTION, PoolKind.REGION_EXECUTION, "folia", "region tick execution"
        );
        final PoolSnapshot pool = new PoolSnapshot(
            spec, 4, 0, 0, 4, -1L, -1L, -1L, -1L, -1L, -1.0d,
            SaturationReport.unknown(PoolId.REGION_EXECUTION),
            List.of(DegradedReason.OBSERVE_ONLY),
            Optional.of(new RegionPoolMetrics(12, 4.24d, 9.5d, 4.4d, 12.0d, 19.4d, 1, 8123, 4, 3951))
        );
        final ResourceSnapshot snapshot = new ResourceSnapshot(
            Instant.EPOCH, 8, 1, 4, 0, 4, 4, 0.0d, 0.0d, -1.0d, List.of(pool), List.of(),
            Set.of(DegradedReason.OBSERVE_ONLY)
        );
        final ThreadsStatus status = new ThreadsStatus(
            ThreadsStatus.State.RUNNING, ThreadManagementSettings.defaults(), snapshot, 0, true
        );

        final String text = String.join("\n", ThreadsReport.overview(status));

        assertTrue(text.contains("regions 12 / avg MSPT 4.2 ms / min TPS 19.4 / lag 8% (1 of 12)"), text);
        assertTrue(text.contains("ticks: 5s max 9.5 ms, 15s avg 4.4 ms max 12.0 ms"), text);
        assertTrue(text.contains("entities 8123 / players 4 / chunks 3951"), text);
    }

    @Test
    void aPoolWithoutRegionMetricsPrintsNoRegionLine() {
        final String text = String.join("\n", ThreadsReport.overview(runningStatus(0)));

        assertFalse(text.contains("regions "), "an observe-only pool with no source must not claim zero regions");
        assertFalse(text.contains("avg MSPT"));
    }

    private static ThreadsStatus runningStatus(final int unregisteredThreads) {
        return runningStatus(unregisteredThreads, true);
    }

    private static ThreadsStatus runningStatus(final int unregisteredThreads, final boolean metricsEnabled) {
        final PoolSpec spec = PoolSpec.observeOnly(
            PoolId.REGION_EXECUTION, PoolKind.REGION_EXECUTION, "folia", "region tick execution"
        );
        final PoolSnapshot pool = new PoolSnapshot(
            spec,
            8,
            0,
            3,
            5,
            -1L,
            -1L,
            -1L,
            -1L,
            -1L,
            -1.0d,
            SaturationReport.unknown(PoolId.REGION_EXECUTION),
            List.of(DegradedReason.OBSERVE_ONLY)
        );
        final ResourceSnapshot snapshot = new ResourceSnapshot(
            Instant.EPOCH,
            8,
            1,
            8,
            3,
            5,
            8,
            0.0d,
            0.0d,
            -1.0d,
            List.of(pool),
            List.of(),
            Set.of(DegradedReason.OBSERVE_ONLY),
            Optional.empty()
        );
        return new ThreadsStatus(
            ThreadsStatus.State.RUNNING,
            ThreadManagementSettings.defaults(),
            snapshot,
            unregisteredThreads,
            metricsEnabled
        );
    }

    private static void assertContains(final List<String> lines, final String needle) {
        final String text = String.join("\n", lines);
        assertTrue(text.contains(needle), () -> "expected '" + needle + "' in:\n" + text);
    }
}
