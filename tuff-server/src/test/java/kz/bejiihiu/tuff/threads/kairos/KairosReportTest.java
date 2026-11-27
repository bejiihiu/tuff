package kz.bejiihiu.tuff.threads.kairos;

import java.util.List;
import java.util.concurrent.TimeUnit;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.bootstrap.ThreadsReport;
import kz.bejiihiu.tuff.threads.bootstrap.ThreadsStatus;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The shape of the two console surfaces: {@code /tuff scheduler} and the scheduler line of
 * {@code /tuff threads}.
 *
 * <p>The report is a pure function, so the EDF, work stealing, unknown and not-started branches are
 * tested without a scheduler. The Kairos branch uses a real fixed fund, because then the fund, queue and
 * supervisor lines are the ones an operator will actually see.
 */
@Timeout(120)
class KairosReportTest {

    private RecordingThreadFactory threads;
    private @Nullable KairosScheduler scheduler;

    @BeforeEach
    void newFactory() {
        this.threads = new RecordingThreadFactory();
    }

    @AfterEach
    void stopEverything() {
        if (this.scheduler != null) {
            this.scheduler.halt();
            assertTrue(this.scheduler.join(10_000L));
        }
        KairosGate.disarm();
    }

    @Test
    void edfWorkStealingAndUnknownSayThereIsNoFundTelemetry() {
        final List<String> edf = KairosReport.overview("EDF", 2, null);
        assertEquals("Tuff scheduler: EDF", edf.get(0));
        assertTrue(edf.stream().anyMatch(line -> line.contains("no fund telemetry")));
        assertTrue(edf.stream().anyMatch(line -> line.contains("configured region threads: 2")));

        final List<String> workStealing = KairosReport.overview("WORK_STEALING", -1, null);
        assertEquals("Tuff scheduler: WORK_STEALING", workStealing.get(0));
        assertTrue(workStealing.stream().anyMatch(line -> line.contains("no fund telemetry")));

        final List<String> unknown = KairosReport.overview(null, -1, null);
        assertEquals("Tuff scheduler: unknown", unknown.get(0));

        final List<String> notStarted = KairosReport.overview("KAIROS", 2, null);
        assertTrue(notStarted.get(0).contains("not started"), notStarted.get(0));
    }

    @Test
    void theKairosReportCarriesTheFundQueueAndSupervisor() {
        KairosGate.arm(true, new KairosTuning(true, -1, 5_000, 10, 20L),
            new KairosServices(floor -> 4, KairosSupervisorSource.plain()));
        this.scheduler = new KairosScheduler(1, this.threads, tick -> ((FakeTick) tick).deadline());
        this.scheduler.start();
        this.awaitLive(1);
        final KairosSnapshot snapshot = this.scheduler.snapshot();

        final List<String> lines = KairosReport.overview("KAIROS", 1, snapshot);
        assertEquals("Tuff scheduler: Kairos (experimental) - status: ok", lines.get(0));
        assertTrue(lines.stream().anyMatch(line -> line.contains("fund: min 1, live 1, target 1, max 4")));
        assertTrue(lines.stream().anyMatch(line -> line.contains("elasticity on")));
        assertTrue(lines.stream().anyMatch(line -> line.contains("0 queued, 0 awaiting, 0 executing")));
        assertTrue(lines.stream().anyMatch(line -> line.contains("threshold 100 ms")));
        assertTrue(lines.stream().anyMatch(line -> line.contains("supervisor: running")));
        assertTrue(lines.stream().anyMatch(line -> line.contains("degraded: none")));
    }

    @Test
    void aBudgetWithoutHeadroomShowsUpAsDegraded() {
        KairosGate.arm(true, new KairosTuning(true, -1, 5_000, 10, 20L),
            new KairosServices(floor -> 1, KairosSupervisorSource.plain()));
        this.scheduler = new KairosScheduler(2, this.threads, tick -> ((FakeTick) tick).deadline());
        this.scheduler.start();
        this.awaitLive(2);

        final List<String> lines = KairosReport.overview("KAIROS", 2, this.scheduler.snapshot());
        assertTrue(lines.get(0).contains("status: degraded"), lines.get(0));
        assertTrue(lines.stream().anyMatch(line -> line.contains("degraded: NO_HEADROOM")));
        assertTrue(lines.stream().anyMatch(line -> line.contains("supervisor: off (no headroom above the floor)")));
    }

    @Test
    void aFixedFundReportsNoElasticityAndNoSupervisor() throws InterruptedException {
        KairosGate.arm(true, new KairosTuning(false, 2, 5_000, 10, 20L));
        this.scheduler = new KairosScheduler(2, this.threads, tick -> ((FakeTick) tick).deadline());
        this.scheduler.start();
        this.awaitLive(2);

        final List<String> lines = KairosReport.overview("KAIROS", 2, this.scheduler.snapshot());
        assertTrue(lines.stream().anyMatch(line -> line.contains("elasticity off")), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains("supervisor: off (fixed fund")), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains("max 2")), lines.toString());
    }

    @Test
    void theThreadsReportGetsOneSchedulerLine() {
        final List<String> edf = ThreadsReport.overview(
            ThreadsStatus.running(ThreadManagementSettings.defaults())
        );
        assertTrue(edf.stream().anyMatch(line -> line.contains("scheduler: edf - no fund telemetry")), edf.toString());

        KairosGate.arm(true, new KairosTuning(true, -1, 5_000, 10, 20L),
            new KairosServices(floor -> 3, KairosSupervisorSource.plain()));
        this.scheduler = new KairosScheduler(1, this.threads, tick -> ((FakeTick) tick).deadline());
        this.scheduler.start();
        this.awaitLive(1);

        final ThreadsStatus withKairos = ThreadsStatus.running(ThreadManagementSettings.defaults())
            .withKairos(this.scheduler.snapshot());
        final List<String> kairos = ThreadsReport.overview(withKairos);
        assertTrue(
            kairos.stream().anyMatch(line -> line.contains("scheduler: kairos - elastic fund 1/3 live/max")),
            kairos.toString()
        );
    }

    private void awaitLive(final int expected) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (this.scheduler.liveThreads() != expected) {
            if (System.nanoTime() - deadline >= 0L) {
                throw new AssertionError("expected " + expected + " live threads, got "
                    + this.scheduler.liveThreads());
            }
            Thread.onSpinWait();
        }
    }
}
