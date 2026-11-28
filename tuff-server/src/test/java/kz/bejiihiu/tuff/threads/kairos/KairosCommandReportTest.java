package kz.bejiihiu.tuff.threads.kairos;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.SaturationReport;
import kz.bejiihiu.tuff.threads.SheddingReport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The three scheduler details and the usage line, built from hand-made snapshots.
 *
 * <p>No fund is started here: the report is a pure function, and a fake snapshot is the only way to
 * assert the exact ranking and wording without depending on how fast a test machine schedules a
 * thread. The behaviour behind the numbers is covered by the scheduler tests.
 */
@Timeout(30)
class KairosCommandReportTest {

    @Test
    void leasesRankByTheWorstWaitAndRespectTheLimit() {
        final KairosSnapshot snapshot = KairosSnapshotBuilder.aSnapshot()
            .leaseWait(new KairosSnapshot.LeaseWait(
                812L, 240L, 41L, 3L, 100L,
                List.of(
                    new KairosSnapshot.LeaseWait.Sample(3L, 4811L, 240L, 41L, 1_200L),
                    new KairosSnapshot.LeaseWait.Sample(-1L, 77L, 10L, 400L, 500L),
                    new KairosSnapshot.LeaseWait.Sample(5L, 900L, 90L, 90L, 0L)
                )
            ))
            .build();

        final List<String> lines = KairosReport.leases("KAIROS", snapshot, 2);

        assertEquals("Tuff scheduler: leases (recent permit waits)", lines.get(0));
        assertTrue(lines.stream().anyMatch(line -> line.contains("812 measured")), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains("3 over the 100 ms timeout")), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains("worst queued 240 ms")), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains("top 2 of 3 recent sample(s)")), lines.toString());
        assertEquals(
            "    #1 region unknown, tick 77: queued 10 ms, parked 400 ms, age 500 ms ago",
            lines.get(3)
        );
        assertEquals(
            "    #2 region 3, tick 4811: queued 240 ms, parked 41 ms, age 1.2 s ago",
            lines.get(4)
        );
    }

    @Test
    void theOverviewCarriesTheExecutedTickAndWarningCounters() {
        final KairosSnapshot snapshot = KairosSnapshotBuilder.aSnapshot()
            .counters(9_120L, 214L, 3L, 3L)
            .warnings(2L, 1L)
            .build();

        final List<String> lines = KairosReport.overview("KAIROS", 2, snapshot);

        assertTrue(lines.stream().anyMatch(
            line -> line.contains("ticks: executed 9120, affinity hits 214, misses 3, steals 3")
        ), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains("warnings 2 (1 suppressed)")), lines.toString());
    }

    @Test
    void aLimitBelowOneStillPrintsOneSample() {
        final KairosSnapshot snapshot = KairosSnapshotBuilder.aSnapshot()
            .leaseWait(new KairosSnapshot.LeaseWait(
                2L, 30L, 1L, 0L, 0L,
                List.of(new KairosSnapshot.LeaseWait.Sample(1L, 5L, 30L, 1L, 10L))
            ))
            .build();

        final List<String> lines = KairosReport.leases(null, snapshot, 0);

        assertTrue(lines.stream().anyMatch(line -> line.contains("top 1 of 1 recent sample(s)")), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains("timeout off (lease.timeout-seconds: 0)")));
    }

    @Test
    void leasesSayWhenNothingWasMeasuredYet() {
        final List<String> lines = KairosReport.leases("KAIROS", KairosSnapshotBuilder.aSnapshot().build(), 5);

        assertTrue(lines.stream().anyMatch(line -> line.contains("grants: none measured yet")), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains("recent waits: none yet")), lines.toString());
    }

    @Test
    void leasesAndTheOtherDetailsSayThereIsNoFundWithoutKairos() {
        final List<String> leases = KairosReport.leases("EDF", null, 5);
        assertTrue(leases.stream().anyMatch(line -> line.contains("no permit waits")), leases.toString());
        assertTrue(leases.stream().anyMatch(line -> line.contains("the active scheduler is EDF")), leases.toString());
    }

    @Test
    void budgetPrintsTheLedgerDeductionsComputeFloorAndKairosCeiling() {
        final ResourceSnapshot threads = threads(
            "cores 16: ceiling 12 = region 2 + chunk 4 + io 1 + reserve 1 + usable 4",
            4,
            1,
            Set.of(),
            computePool()
        );

        final List<String> lines = KairosReport.budget(new KairosReport.BudgetInput(
            "KAIROS", threads, 412L, null, KairosSnapshotBuilder.aSnapshot().build(), 1, -1
        ));

        assertEquals("Tuff scheduler: budget", lines.get(0));
        assertTrue(lines.stream().anyMatch(line -> line.contains("sample taken 412 ms ago")), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains(
            "ledger: cores 16: ceiling 12 = region 2 + chunk 4 + io 1 + reserve 1 + usable 4"
        )), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains(
            "deductions: region 2, chunk 4, io 1, reserve 1 core(s)"
        )), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains(
            "usable: 4 core(s) after every deduction; 1 core(s) reserved by configuration, 0 held for the critical lanes"
        )), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains(
            "compute: floor 1 worker(s), max auto; effective budget 4 core(s)"
        )), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains(
            "kairos: min 2, max 4 tick thread(s) (the resolved ceiling); budget source: utmi budget"
        )), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains("degraded: none")), lines.toString());
    }

    @Test
    void budgetSaysNAWithAReasonWhenTheThreadLayerHasNoSample() {
        final List<String> lines = KairosReport.budget(new KairosReport.BudgetInput(
            "EDF",
            null,
            -1L,
            "disabled (thread-management.enabled: false); the ledger, the deductions and usable cores are n/a",
            null,
            1,
            -1
        ));

        assertTrue(lines.stream().anyMatch(line -> line.contains("thread layer: disabled")), lines.toString());
        assertFalse(lines.stream().anyMatch(line -> line.contains("ledger:")), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains(
            "effective budget n/a (no thread layer ledger to resolve it against)"
        )), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains("kairos: not running")), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains(
            "degraded: unknown (no thread layer snapshot and no Kairos fund to read)"
        )), lines.toString());
    }

    @Test
    void budgetExplainsEveryDegradedReason() {
        final ResourceSnapshot threads = threads(
            "cores 16: ceiling 12 = region 2 + chunk 4 + io 1 + reserve 1 + usable 4",
            4,
            1,
            Set.of(DegradedReason.BUDGET_EXHAUSTED, DegradedReason.SCHEDULER_LIMITATION),
            null
        );
        final KairosSnapshot kairos = KairosSnapshotBuilder.aSnapshot()
            .degraded(KairosSnapshot.Degraded.NO_HEADROOM)
            .build();

        final List<String> lines = KairosReport.budget(new KairosReport.BudgetInput(
            "KAIROS", threads, 10L, null, kairos, 0, -1
        ));

        final String degraded = lines.stream()
            .filter(line -> line.contains("degraded:"))
            .findFirst()
            .orElseThrow();
        assertTrue(degraded.contains("BUDGET_EXHAUSTED (the reserves and minimums do not fit the usable cores)"),
            degraded);
        assertTrue(degraded.contains("SCHEDULER_LIMITATION (the pool owner cannot expose the control this layer needs)"),
            degraded);
        assertTrue(degraded.contains("kairos NO_HEADROOM (the resolved ceiling is not above the floor)"), degraded);
        assertTrue(lines.stream().anyMatch(line -> line.contains(
            "compute: not running (async-compute.enabled is false or the engine failed)"
        )), lines.toString());
    }

    @Test
    void aComputePoolThatIsNotRegisteredPrintsTheEffectiveBudgetAsNA() {
        final ResourceSnapshot threads = threads(
            "cores 8: ceiling 6 = region 2 + chunk 1 + io 0 + reserve 1 + usable 2",
            2,
            1,
            Set.of(),
            null
        );

        final List<String> lines = KairosReport.budget(new KairosReport.BudgetInput(
            "EDF", threads, 30L, null, null, 2, 4
        ));

        assertTrue(lines.stream().anyMatch(line -> line.contains("effective budget n/a (the compute pool is not "
            + "registered with the thread layer")), lines.toString());
    }

    @Test
    void resetIsOneLineAndSaysWhatItZeroed() {
        final List<String> lines = KairosReport.reset(true, "KAIROS");

        assertEquals(1, lines.size());
        assertTrue(lines.get(0).contains("counters reset"), lines.get(0));
        assertTrue(lines.get(0).contains("affinity hits and misses"), lines.get(0));
        assertTrue(lines.get(0).contains("the fund, the regions and the permits are untouched"), lines.get(0));
    }

    @Test
    void aRefusedResetSaysWhy() {
        final List<String> lines = KairosReport.reset(false, "EDF");

        assertEquals(1, lines.size());
        assertTrue(lines.get(0).contains("counters not reset"), lines.get(0));
        assertTrue(lines.get(0).contains("the active scheduler is EDF"), lines.get(0));
    }

    @Test
    void helpListsTheWholeSchedulerGrammar() {
        final List<String> lines = KairosReport.help();

        assertEquals("Tuff scheduler commands:", lines.get(0));
        final String text = String.join("\n", lines);
        assertTrue(text.contains("/tuff scheduler leases [n]"), text);
        assertTrue(text.contains("/tuff scheduler budget"), text);
        assertTrue(text.contains("/tuff scheduler reset"), text);
        assertTrue(text.contains("/tuff scheduler help"), text);
    }

    private static PoolSnapshot computePool() {
        return new PoolSnapshot(
            new PoolSpec(
                PoolId.COMPUTE,
                PoolKind.COMPUTE,
                "compute",
                "platform workers of the async compute engine",
                1,
                -1,
                CoverageLevel.L4,
                ResourceBudget.elastic(1.0d, 0)
            ),
            3,
            1,
            0,
            3,
            -1L,
            -1L,
            -1L,
            -1L,
            -1L,
            -1.0d,
            SaturationReport.unknown(PoolId.COMPUTE),
            List.of()
        );
    }

    private static ResourceSnapshot threads(
        final String ledger,
        final int usableCores,
        final int reservedCores,
        final Set<DegradedReason> reasons,
        final PoolSnapshot pool
    ) {
        return new ResourceSnapshot(
            Instant.EPOCH,
            usableCores,
            reservedCores,
            3,
            1,
            2,
            3,
            0.0d,
            0.0d,
            0.0d,
            pool == null ? List.of() : List.of(pool),
            List.of(),
            reasons,
            Optional.of(ledger),
            SheddingReport.none()
        );
    }
}
