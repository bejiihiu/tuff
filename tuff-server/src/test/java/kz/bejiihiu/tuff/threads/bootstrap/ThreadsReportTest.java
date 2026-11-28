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
import kz.bejiihiu.tuff.threads.SheddingReport;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.pressure.TickPressureController;
import kz.bejiihiu.tuff.threads.pressure.TickPressureSnapshot;
import kz.bejiihiu.tuff.threads.pressure.TickSignal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
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
    void aRunningLayerWithoutASampleSaysItIsWarmingUp() {
        final ThreadsStatus status = ThreadsStatus.running(ThreadManagementSettings.defaults());

        assertContains(ThreadsReport.overview(status), "warming up (no sample yet)");
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
        assertTrue(text.contains("sample: taken 412 ms ago"), text);
        assertTrue(text.contains("after a restart"), text);
        assertFalse(text.contains("["), "reasons are summarised in degraded:, not per pool: " + text);
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
            Instant.EPOCH, 8, 1, 4, 0, 0, 4, 0.0d, 0.0d, -1.0d, List.of(pool), List.of(), Set.of()
        );
        final ThreadsStatus status = new ThreadsStatus(
            ThreadsStatus.State.RUNNING, ThreadManagementSettings.defaults(), snapshot, 0, 10L, true, null
        );

        final String text = String.join("\n", ThreadsReport.overview(status));

        assertTrue(text.contains("active unknown"), text);
        assertTrue(text.contains("idle unknown"), text);
        assertFalse(text.contains("active -1"), "an unknown count must never be printed as a negative number");
    }

    @Test
    void disabledMetricsReportTheUnknownThreadCountAsUnavailable() {
        final String text = String.join("\n", ThreadsReport.overview(runningStatus(-1, false)));

        assertTrue(text.contains("unknown threads: n/a (metrics collection is disabled)"), text);
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
            ThreadsStatus.State.RUNNING, ThreadManagementSettings.defaults(), snapshot, 0, 10L, true, null
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

    @Test
    void tickPressureIsDisabledInTheShippedConfiguration() {
        final String text = String.join("\n", ThreadsReport.overview(runningStatus(0)));

        assertTrue(text.contains("pressure: disabled"), text);
        assertTrue(text.contains("async-compute.tick-pressure.enabled: false"), text);
    }

    @Test
    void tickPressurePrintsTheReserveTheSignalAndTheReason() {
        final TickPressureSnapshot pressure = new TickPressureSnapshot(
            true,
            TickPressureController.State.PRESSURE,
            2,
            2,
            6,
            8,
            6,
            8,
            TickPressureController.Reason.LAG_STEP_UP,
            new TickSignal(true, 4, 0.5d, 17.2d, 41.0d)
        );
        final ThreadsStatus status = new ThreadsStatus(
            ThreadsStatus.State.RUNNING,
            settingsWithPressure(),
            runningStatus(0).snapshot(),
            0,
            412L,
            true,
            pressure
        );

        final String text = String.join("\n", ThreadsReport.overview(status));

        assertTrue(text.contains(
            "pressure: yielding, reserve 2 of 2 core(s), budget 6 applied of 8 pool core(s), 8 usable"), text);
        assertTrue(text.contains("lag 50%, min TPS 17.2, max MSPT 41.0 ms"), text);
        assertTrue(text.contains("(LAG_STEP_UP)"), text);
    }

    @Test
    void enabledTickPressureWithoutASignalSaysItIsWaiting() {
        final ThreadsStatus status = new ThreadsStatus(
            ThreadsStatus.State.RUNNING,
            settingsWithPressure(),
            runningStatus(0).snapshot(),
            0,
            412L,
            true,
            null
        );

        assertTrue(
            String.join("\n", ThreadsReport.overview(status)).contains("pressure: enabled, waiting for a region reading")
        );
    }

    @Test
    void theReserveLineExplainsTheCriticalReserveAndTheShed() {
        final SheddingReport report = new SheddingReport(
            2, true, SheddingReport.State.SHEDDING, 1, List.of(PoolId.COMPUTE),
            "load shedding withholding 1 core(s) from compute while a critical pool waits; "
                + "2 core(s) held for the critical lanes"
        );
        final ResourceSnapshot snapshot = new ResourceSnapshot(
            Instant.EPOCH, 8, 1, 8, 3, 5, 8, 0.0d, 0.0d, -1.0d, List.of(), List.of(), Set.of(), report
        );
        final ThreadManagementSettings defaults = ThreadManagementSettings.defaults();
        final ThreadManagementSettings settings = new ThreadManagementSettings(
            defaults.enabled(),
            new ThreadManagementSettings.Resources(true, 1, -1, true),
            defaults.lifecycle(),
            defaults.regionExecution(),
            defaults.asyncCompute(),
            defaults.executionPools(),
            defaults.metrics()
        );
        final ThreadsStatus status = new ThreadsStatus(
            ThreadsStatus.State.RUNNING, settings, snapshot, 0, 412L, true, null
        );

        final String text = String.join("\n", ThreadsReport.overview(status));

        assertTrue(text.contains("reserve: load shedding withholding 1 core(s) from compute"), text);
        assertTrue(text.contains("2 core(s) held for the critical lanes"), text);
    }

    @Test
    void theReserveLineSaysWhenSheddingIsOff() {
        final String text = String.join("\n", ThreadsReport.overview(runningStatus(0)));

        assertTrue(
            text.contains("reserve: 0 core(s) held for the critical lanes; load shedding off "
                + "(thread-management.resources.load-shedding: false)"),
            text
        );
    }

    @Test
    void aDormantSheddingPolicySaysWhyItCannotFire() {
        final SheddingReport dormant = new SheddingReport(
            1, true, SheddingReport.State.DORMANT, 0, List.of(),
            "1 core(s) held for the critical lanes; load shedding on but dormant: no critical pool can "
                + "lease capacity (region and network are at most L2), so the trigger cannot fire"
        );
        final ResourceSnapshot snapshot = new ResourceSnapshot(
            Instant.EPOCH, 8, 1, 4, 0, 4, 4, 0.0d, 0.0d, -1.0d, List.of(), List.of(), Set.of(), dormant
        );
        final ThreadsStatus status = new ThreadsStatus(
            ThreadsStatus.State.RUNNING, settingsWithShedding(), snapshot, 0, 412L, true, null
        );

        final String text = String.join("\n", ThreadsReport.overview(status));

        assertTrue(text.contains("reserve: 1 core(s) held for the critical lanes; load shedding on but dormant"), text);
        assertTrue(text.contains("the trigger cannot fire"), text);
    }

    @Test
    void anEmptyPoolListWithPerPoolOffIsNotReportedAsNoPools() {
        final ThreadManagementSettings defaults = ThreadManagementSettings.defaults();
        final ThreadManagementSettings settings = new ThreadManagementSettings(
            defaults.enabled(),
            defaults.resources(),
            defaults.lifecycle(),
            defaults.regionExecution(),
            defaults.asyncCompute(),
            defaults.executionPools(),
            new ThreadManagementSettings.Metrics(true, false, true)
        );
        final ResourceSnapshot snapshot = new ResourceSnapshot(
            Instant.EPOCH, 8, 1, 2, 1, 1, 2, 0.0d, 0.0d, -1.0d, List.of(), List.of(), Set.of()
        );
        final ThreadsStatus status = new ThreadsStatus(
            ThreadsStatus.State.RUNNING, settings, snapshot, 0, 412L, true, null
        );

        final String text = String.join("\n", ThreadsReport.overview(status));

        assertFalse(text.contains("no pool is registered"), text);
        assertTrue(text.contains("thread-management.metrics.per-pool: false"), text);
    }

    private static ThreadManagementSettings settingsWithShedding() {
        final ThreadManagementSettings defaults = ThreadManagementSettings.defaults();
        return new ThreadManagementSettings(
            defaults.enabled(),
            new ThreadManagementSettings.Resources(true, 1, -1, true),
            defaults.lifecycle(),
            defaults.regionExecution(),
            defaults.asyncCompute(),
            defaults.executionPools(),
            defaults.metrics()
        );
    }

    private static ThreadManagementSettings settingsWithPressure() {
        final ThreadManagementSettings defaults = ThreadManagementSettings.defaults();
        final ThreadManagementSettings.AsyncCompute.TickPressure policy =
            new ThreadManagementSettings.AsyncCompute.TickPressure(true, 0.25d, 19.0d, 19.5d, 3, 5, 2, 1);
        return new ThreadManagementSettings(
            defaults.enabled(),
            defaults.resources(),
            defaults.lifecycle(),
            defaults.regionExecution(),
            new ThreadManagementSettings.AsyncCompute(false, -1, policy),
            defaults.executionPools(),
            defaults.metrics()
        );
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
            Instant.EPOCH, 8, 1, 8, 3, 5, 8, 0.0d, 0.0d, -1.0d, List.of(pool), List.of(),
            Set.of(DegradedReason.OBSERVE_ONLY)
        );
        return new ThreadsStatus(
            ThreadsStatus.State.RUNNING,
            ThreadManagementSettings.defaults(),
            snapshot,
            unregisteredThreads,
            412L,
            metricsEnabled,
            null
        );
    }

    private static void assertContains(final List<String> lines, final String needle) {
        final String text = String.join("\n", lines);
        assertTrue(text.contains(needle), () -> "expected '" + needle + "' in:\n" + text);
    }
}
