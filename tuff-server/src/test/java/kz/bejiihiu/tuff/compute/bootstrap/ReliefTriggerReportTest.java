package kz.bejiihiu.tuff.compute.bootstrap;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.OverflowPolicy;
import kz.bejiihiu.tuff.compute.internal.engine.impl.ComputeSchedulers;
import kz.bejiihiu.tuff.compute.internal.util.ComputeClock;
import kz.bejiihiu.tuff.threads.relief.ReliefTriggerSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The trigger block of {@code /tuff compute consumers}: what it says when the trigger is off, when no
 * unit is registered and when regions are behind.
 */
@Timeout(60)
class ReliefTriggerReportTest {

    @Test
    void aDisabledTriggerIsStillPrinted() {
        final List<String> lines = ComputeReport.describeTrigger(ReliefTriggerSnapshot.empty());

        assertTrue(String.join("\n", lines).contains("region relief trigger: off"), lines.toString());
    }

    @Test
    void anArmedTriggerWithNoUnitsSaysSoAndKeepsCounting() {
        final ReliefTriggerSnapshot snapshot = new ReliefTriggerSnapshot(
            true, 0, List.of(), 2, 1, 3L, 2L, 0L, 41L, 900L, 1L, 0L, 0L, 2_000_000_000L, List.of()
        );

        final String report = String.join("\n", ComputeReport.describeTrigger(snapshot));

        assertTrue(report.contains("region relief trigger: on, no work units registered"), report);
        assertTrue(report.contains("2 region(s), 1 window(s) open"), report);
        assertTrue(report.contains("3 windows entered, 2 exited, 0 abandoned"), report);
        assertTrue(report.contains("41 overrun / 900 calm bodies"), report);
        assertTrue(report.contains("last window 2.00 s ago"), report);
        assertTrue(report.contains("suppressed 1 by cooldown, 0 by rate"), report);
    }

    @Test
    void unitsAndOffendersAreListed() {
        final ReliefTriggerSnapshot snapshot = new ReliefTriggerSnapshot(
            true,
            2,
            List.of("paths", "spawns"),
            3,
            1,
            1L,
            0L,
            0L,
            7L,
            4L,
            0L,
            0L,
            0L,
            -1L,
            List.of(
                new ReliefTriggerSnapshot.RegionOffence(5L, 7L, 1L, true),
                new ReliefTriggerSnapshot.RegionOffence(6L, 2L, 0L, false)
            )
        );

        final String report = String.join("\n", ComputeReport.describeTrigger(snapshot));

        assertTrue(report.contains("2 work units (paths, spawns)"), report);
        assertTrue(report.contains("no window yet"), report);
        assertTrue(report.contains("top offenders: region 5 (7 overrun, 1 window, open), region 6 (2 overrun, 0 windows)"), report);
    }

    @Test
    void theConsumerBreakdownAppendsTheTriggerBlock() throws Exception {
        final AsyncComputeScheduler scheduler = startScheduler();
        try {
            final ReliefTriggerSnapshot snapshot = new ReliefTriggerSnapshot(
                true, 0, List.of(), 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, -1L, List.of()
            );

            final String report = String.join("\n", ComputeReport.consumers(
                ComputeStatus.running(scheduler),
                Optional.of(snapshot)
            ));

            assertTrue(report.contains("region relief trigger: on"), report);
        } finally {
            stopScheduler(scheduler);
        }
    }

    @Test
    void noSnapshotMeansNoTriggerLine() throws Exception {
        final AsyncComputeScheduler scheduler = startScheduler();
        try {
            final String report = String.join("\n", ComputeReport.consumers(
                ComputeStatus.running(scheduler),
                Optional.empty()
            ));

            assertFalse(report.contains("region relief trigger"), report);
        } finally {
            stopScheduler(scheduler);
        }
    }

    private static AsyncComputeScheduler startScheduler() {
        final ComputeSettings defaults = ComputeSettings.defaults();
        final ComputeSettings settings = new ComputeSettings(
            true,
            new ComputeSettings.Workers(2, 1, 2, Duration.ofSeconds(5L), "Tuff Compute Report Test"),
            new ComputeSettings.QueueSettings(64, OverflowPolicy.REJECT),
            defaults.scheduling(),
            defaults.consumers(),
            defaults.metrics(),
            defaults.adaptive(),
            Duration.ofSeconds(1L)
        );
        final AsyncComputeScheduler scheduler = ComputeSchedulers.create(settings, ComputeClock.system(), 2);
        scheduler.start();
        return scheduler;
    }

    private static void stopScheduler(final AsyncComputeScheduler scheduler) throws InterruptedException {
        scheduler.shutdown(Duration.ZERO);
        assertTrue(scheduler.awaitTermination(Duration.ofSeconds(10L)));
    }
}
