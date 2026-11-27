package kz.bejiihiu.tuff.threads.internal.impl.budget;

import java.util.List;
import kz.bejiihiu.tuff.threads.CoverageLevel;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.PoolKind;
import kz.bejiihiu.tuff.threads.PoolSpec;
import kz.bejiihiu.tuff.threads.ResourceBudget;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks the budget arithmetic and the degradation policy.
 */
class BudgetPlannerTest {

    @ParameterizedTest
    @CsvSource({
        "4, 3, -1, 1",
        "8, 6, 1, 1",
        "16, 12, 4, 4",
        "32, 25, 11, 11"
    })
    void ledgerFollowsTheEightyPercentRule(
        final int cores,
        final int ceiling,
        final int rawUsable,
        final int usable
    ) {
        final BudgetPlan plan = plan(cores, foliaDefaults(cores), List.of(compute(2)));

        assertEquals(cores, plan.availableProcessors());
        assertEquals(ceiling, plan.ceilingCores());
        assertEquals(rawUsable, plan.rawUsableCores());
        assertEquals(usable, plan.usableCores());
    }

    @Test
    void noHeadroomIsReportedInsteadOfInvented() {
        final BudgetPlan plan = plan(4, foliaDefaults(4), List.of(compute(2)));

        assertTrue(plan.degraded());
        assertTrue(
            plan.degradedReasons().stream().anyMatch(reason -> reason.contains("no headroom")),
            () -> "expected a no headroom reason, got " + plan.degradedReasons()
        );
    }

    @Test
    void computeBelowItsMinimumIsNotExpanded() {
        final BudgetPlan plan = plan(8, foliaDefaults(8), List.of(compute(2)));
        final PoolBudget compute = plan.pool(PoolId.COMPUTE).orElseThrow();

        assertEquals(2, compute.minCores());
        assertEquals(1, compute.maxCores());
        assertFalse(compute.satisfied());
        assertFalse(compute.critical());
        assertEquals(1, compute.capacity(), "a non critical pool never grows past its budget");
        assertTrue(compute.degradedReason().contains("not expanding"));
        assertTrue(plan.degraded());
    }

    @Test
    void criticalPoolKeepsItsMinimumAndReportsTheOvercommit() {
        final PoolSpec network = new PoolSpec(
            PoolId.NETWORK,
            PoolKind.NETWORK,
            "netty",
            "event loops",
            4,
            4,
            CoverageLevel.L3,
            ResourceBudget.fixed(1, 0)
        );

        final PoolBudget budget = plan(8, foliaDefaults(8), List.of(network)).pool(PoolId.NETWORK).orElseThrow();

        assertEquals(1, budget.maxCores());
        assertEquals(4, budget.minCores());
        assertFalse(budget.satisfied());
        assertTrue(budget.critical());
        assertEquals(4, budget.capacity());
        assertEquals(3, budget.overcommit());
        assertTrue(budget.degradedReason().contains("critical minimum"));
    }

    @Test
    void fixedRegionBudgetIsClampedToTheRegionThreads() {
        final PoolSpec region = new PoolSpec(
            PoolId.REGION_EXECUTION,
            PoolKind.REGION_EXECUTION,
            "folia",
            "ticks",
            1,
            -1,
            CoverageLevel.L3,
            ResourceBudget.fixed(64, 1)
        );

        final PoolBudget budget = plan(4, new BudgetDeductions(3, 0, 0), List.of(region))
            .pool(PoolId.REGION_EXECUTION)
            .orElseThrow();

        assertEquals(3, budget.maxCores());
        assertEquals(3, budget.capacity());
        assertTrue(budget.satisfied());
    }

    @Test
    void regionPermitsComeFromTheRegionThreadCountNotTheCeiling() {
        final PoolSpec region = new PoolSpec(
            PoolId.REGION_EXECUTION,
            PoolKind.REGION_EXECUTION,
            "folia",
            "ticks",
            1,
            -1,
            CoverageLevel.L3,
            ResourceBudget.defaults()
        );

        final BudgetPlan plan = plan(16, new BudgetDeductions(2, 4, 1), List.of(region));
        final PoolBudget budget = plan.pool(PoolId.REGION_EXECUTION).orElseThrow();

        assertEquals(12, plan.ceilingCores());
        assertEquals(2, budget.maxCores(), "an elastic region pool must not claim the whole ceiling");
        assertEquals(2, budget.capacity());
    }

    @Test
    void chunkAndIoPermitsComeFromTheirDeductions() {
        final PoolSpec chunk = new PoolSpec(
            PoolId.CHUNK_WORKER, PoolKind.CHUNK, "moonrise", "chunks", 1, -1, CoverageLevel.L3,
            ResourceBudget.defaults()
        );
        final PoolSpec io = new PoolSpec(
            PoolId.CHUNK_IO, PoolKind.IO, "moonrise", "chunk io", 1, -1, CoverageLevel.L3,
            ResourceBudget.defaults()
        );

        final BudgetPlan plan = plan(16, new BudgetDeductions(2, 4, 1), List.of(chunk, io));

        assertEquals(4, plan.pool(PoolId.CHUNK_WORKER).orElseThrow().capacity());
        assertEquals(1, plan.pool(PoolId.CHUNK_IO).orElseThrow().capacity());
    }

    @Test
    void elasticPoolsShareTheUsableRemainderInsteadOfEachTakingAllOfIt() {
        final PoolSpec alpha = new PoolSpec(
            PoolId.of("alpha"), PoolKind.COMPUTE, "test", "alpha", 1, -1, CoverageLevel.L3,
            ResourceBudget.defaults()
        );
        final PoolSpec beta = new PoolSpec(
            PoolId.of("beta"), PoolKind.SERVICE, "test", "beta", 1, -1, CoverageLevel.L3,
            ResourceBudget.defaults()
        );

        final BudgetPlan plan = plan(16, foliaDefaults(16), List.of(alpha, beta));
        final int alphaCores = plan.pool(PoolId.of("alpha")).orElseThrow().capacity();
        final int betaCores = plan.pool(PoolId.of("beta")).orElseThrow().capacity();

        assertEquals(4, plan.usableCores());
        assertEquals(4, alphaCores + betaCores, "the caps together must not exceed the usable remainder");
        assertTrue(alphaCores < 4 && betaCores < 4, "neither pool may take the whole remainder: " + alphaCores);
    }

    @Test
    void overSubscribedSharedCapsAreDegradedWithNumbers() {
        final PoolSpec alpha = new PoolSpec(
            PoolId.of("alpha"), PoolKind.COMPUTE, "test", "alpha", 1, -1, CoverageLevel.L3,
            ResourceBudget.defaults()
        );
        final PoolSpec beta = new PoolSpec(
            PoolId.of("beta"), PoolKind.SERVICE, "test", "beta", 1, -1, CoverageLevel.L3,
            ResourceBudget.defaults()
        );

        final BudgetPlan plan = plan(16, foliaDefaults(16), List.of(alpha, beta));

        assertTrue(
            plan.degradedReasons().stream()
                .anyMatch(reason -> reason.contains("shared pool caps reach 8")
                    && reason.contains("only 4 are usable")),
            () -> "expected an overcommit reason with numbers, got " + plan.degradedReasons()
        );
    }

    @Test
    void reserveIsNeverBelowOne() {
        final ThreadManagementSettings.Resources noReserve =
            new ThreadManagementSettings.Resources(true, 0, -1);
        final BudgetPlanner planner = new BudgetPlanner(
            new FakeTopology(16), noReserve, BudgetDeductions.none()
        );

        final BudgetPlan plan = planner.plan(List.of());

        assertEquals(1, plan.reservedCores());
        assertEquals(11, plan.usableCores());
    }

    @Test
    void oversubscribedReservesAreReported() {
        final PoolSpec alpha = new PoolSpec(
            PoolId.of("alpha"), PoolKind.COMPUTE, "test", "alpha", 1, -1, CoverageLevel.L3,
            ResourceBudget.elastic(1.0d, 2)
        );
        final PoolSpec beta = new PoolSpec(
            PoolId.of("beta"), PoolKind.SERVICE, "test", "beta", 1, -1, CoverageLevel.L3,
            ResourceBudget.elastic(1.0d, 2)
        );

        final BudgetPlan plan = plan(8, foliaDefaults(8), List.of(alpha, beta));

        assertTrue(
            plan.degradedReasons().stream().anyMatch(reason -> reason.contains("not overcommitting")),
            () -> "expected an overcommit reason, got " + plan.degradedReasons()
        );
    }

    @Test
    void planIsComputedExactlyOnce() {
        final BudgetPlanner planner = new BudgetPlanner(
            new FakeTopology(8),
            ThreadManagementSettings.Resources.defaults(),
            BudgetDeductions.none()
        );

        assertTrue(planner.current().isEmpty());
        final BudgetPlan first = planner.plan(List.of(compute(2)));
        assertEquals(first, planner.current().orElseThrow());
        assertThrows(IllegalStateException.class, () -> planner.plan(List.of(compute(2))));
    }

    @Test
    void duplicateIdsAreRejected() {
        final BudgetPlanner planner = new BudgetPlanner(
            new FakeTopology(8),
            ThreadManagementSettings.Resources.defaults(),
            BudgetDeductions.none()
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> planner.plan(List.of(compute(2), compute(2)))
        );
    }

    @Test
    void ledgerLineNamesEveryTerm() {
        final BudgetPlan plan = plan(16, foliaDefaults(16), List.of(compute(2)));

        final String ledger = plan.ledger();

        assertTrue(ledger.contains("cores 16"), ledger);
        assertTrue(ledger.contains("ceiling 12"), ledger);
        assertTrue(ledger.contains("region 2"), ledger);
        assertTrue(ledger.contains("chunk 4"), ledger);
        assertTrue(ledger.contains("io 1"), ledger);
        assertTrue(ledger.contains("reserve 1"), ledger);
        assertTrue(ledger.contains("usable 4"), ledger);
    }

    @Test
    void poolsAreListedInIdOrder() {
        final PoolSpec zulu = new PoolSpec(
            PoolId.of("zulu"), PoolKind.SERVICE, "test", "zulu", 1, -1, CoverageLevel.L3,
            ResourceBudget.defaults()
        );
        final PoolSpec alpha = new PoolSpec(
            PoolId.of("alpha"), PoolKind.SERVICE, "test", "alpha", 1, -1, CoverageLevel.L3,
            ResourceBudget.defaults()
        );

        final BudgetPlan plan = plan(8, foliaDefaults(8), List.of(zulu, alpha));

        assertEquals(PoolId.of("alpha"), plan.pools().get(0).pool());
        assertEquals(PoolId.of("zulu"), plan.pools().get(1).pool());
    }

    @Test
    void networkMinimumIsHeldBackFromTheSharedRemainder() {
        final PoolSpec network = new PoolSpec(
            PoolId.NETWORK, PoolKind.NETWORK, "netty", "event loops", 3, -1, CoverageLevel.L3,
            ResourceBudget.fixed(3, 0)
        );

        final BudgetPlan plan = plan(16, foliaDefaults(16), List.of(network, compute(1)));

        assertEquals(12, plan.ceilingCores());
        assertEquals(3, plan.criticalReserveCores(), "the network minimum is a guarantee, not a wish");
        assertEquals(1, plan.rawUsableCores(), "4 cores were left by the deductions, 3 are reserved");
        assertEquals(3, plan.pool(PoolId.NETWORK).orElseThrow().capacity());
        assertEquals(1, plan.pool(PoolId.COMPUTE).orElseThrow().capacity(),
            "an elastic compute pool must only see what is left after the critical reserve");
        assertTrue(plan.ledger().contains("+ critical 3"), plan.ledger());
    }

    @Test
    void criticalReserveMayTakeTheWholeRemainder() {
        final PoolSpec network = new PoolSpec(
            PoolId.NETWORK, PoolKind.NETWORK, "netty", "event loops", 5, -1, CoverageLevel.L3,
            ResourceBudget.fixed(5, 0)
        );

        final BudgetPlan plan = plan(16, foliaDefaults(16), List.of(network, compute(1)));

        assertEquals(5, plan.criticalReserveCores());
        assertEquals(-1, plan.rawUsableCores());
        assertEquals(0, plan.pool(PoolId.COMPUTE).orElseThrow().capacity(),
            "a shared pool gets zero rather than a core the ledger does not have");
        assertTrue(
            plan.degradedReasons().stream().anyMatch(reason -> reason.contains("non critical pools get none")),
            () -> "expected the critical reserve to be explained, got " + plan.degradedReasons()
        );
    }

    @Test
    void regionMinimumAboveItsThreadDeductionIsReserved() {
        final PoolSpec region = new PoolSpec(
            PoolId.REGION_EXECUTION, PoolKind.REGION_EXECUTION, "folia", "ticks", 3, -1, CoverageLevel.L3,
            ResourceBudget.elastic(1.0d, 0)
        );

        final BudgetPlan plan = plan(16, new BudgetDeductions(1, 4, 1), List.of(region, compute(1)));

        // one tick thread is already deducted, the other two of the declared minimum are not
        assertEquals(2, plan.criticalReserveCores());
        assertEquals(3, plan.pool(PoolId.REGION_EXECUTION).orElseThrow().capacity());
    }

    @Test
    void aDeclaredCriticalReserveCountsAsWell() {
        final PoolSpec network = new PoolSpec(
            PoolId.NETWORK, PoolKind.NETWORK, "netty", "event loops", 1, -1, CoverageLevel.L3,
            ResourceBudget.elastic(1.0d, 2)
        );

        final BudgetPlan plan = plan(16, foliaDefaults(16), List.of(network, compute(1)));

        assertEquals(2, plan.criticalReserveCores(), "the larger of the minimum and the reserve is the promise");
    }

    @Test
    void budgetForUnknownPoolIsEmpty() {
        final BudgetPlan plan = plan(16, foliaDefaults(16), List.of(compute(2)));

        assertTrue(plan.pool(PoolId.NETWORK).isEmpty());
        assertFalse(plan.pool(PoolId.COMPUTE).isEmpty());
    }

    @Test
    void declaredMinimumsAreFundedBeforeTheElasticPool() {
        final PoolSpec virtual = new PoolSpec(
            PoolId.COMPUTE_VIRTUAL, PoolKind.VIRTUAL, "tuff-compute", "virtual lane", 1, 64,
            CoverageLevel.L2, ResourceBudget.fixed(1, 0)
        );
        final PoolSpec service = new PoolSpec(
            PoolId.SERVICE, PoolKind.SERVICE, "tuff", "services", 1, -1,
            CoverageLevel.L2, ResourceBudget.fixed(1, 0)
        );

        final BudgetPlan plan = plan(8, BudgetDeductions.none(), List.of(compute(2), virtual, service));

        assertEquals(5, plan.usableCores(), "8 cores: 6 ceiling, 1 reserve");
        assertEquals(1, plan.pool(PoolId.COMPUTE_VIRTUAL).orElseThrow().capacity(),
            "the lane's pump keeps its core even when the caps do not all fit");
        assertEquals(1, plan.pool(PoolId.SERVICE).orElseThrow().capacity(),
            "the service refresher keeps its core");
        assertEquals(3, plan.pool(PoolId.COMPUTE).orElseThrow().capacity(),
            "compute gets what the fixed claims left");
        assertFalse(plan.degraded(), () -> "proportional division is not a shortfall: " + plan.degradedReasons());
        assertTrue(
            plan.degradedReasons().stream().anyMatch(reason -> reason.contains("funding declared minimums first")),
            () -> "the division has to be explained, got " + plan.degradedReasons()
        );
    }

    @Test
    void aFixedBudgetWinsOverAnElasticMinimumOnATinyMachine() {
        final PoolSpec virtual = new PoolSpec(
            PoolId.COMPUTE_VIRTUAL, PoolKind.VIRTUAL, "tuff-compute", "virtual lane", 1, 64,
            CoverageLevel.L2, ResourceBudget.fixed(1, 0)
        );
        final PoolSpec service = new PoolSpec(
            PoolId.SERVICE, PoolKind.SERVICE, "tuff", "services", 1, -1,
            CoverageLevel.L2, ResourceBudget.fixed(1, 0)
        );

        final BudgetPlan plan = plan(4, BudgetDeductions.none(), List.of(compute(2), virtual, service));

        assertEquals(2, plan.usableCores(), "4 cores: 3 ceiling, 1 reserve");
        assertEquals(1, plan.pool(PoolId.SERVICE).orElseThrow().capacity(),
            "the real thread wins over the elastic minimum");
        assertEquals(1, plan.pool(PoolId.COMPUTE_VIRTUAL).orElseThrow().capacity());
        assertEquals(0, plan.pool(PoolId.COMPUTE).orElseThrow().capacity(),
            "nothing is left, and zero is the honest answer");
        assertFalse(plan.pool(PoolId.COMPUTE).orElseThrow().satisfied());
        assertTrue(plan.degraded(), "an elastic minimum that does not fit is a real shortfall");
        assertTrue(
            plan.degradedReasons().stream().anyMatch(reason -> reason.contains("needs at least 2")),
            () -> "the missing cores have to be named, got " + plan.degradedReasons()
        );
    }

    @Test
    void criticalCapacityAboveUsableIsReportedNotInvented() {
        final PoolSpec network = new PoolSpec(
            PoolId.NETWORK, PoolKind.NETWORK, "netty", "event loops", 1, -1,
            CoverageLevel.L3, ResourceBudget.elastic(1.0d, 0)
        );

        final BudgetPlan plan = plan(16, BudgetDeductions.none(), List.of(network, compute(1)));

        assertEquals(10, plan.usableCores(), "12 ceiling, 1 reserve, 1 critical guarantee");
        assertEquals(12, plan.criticalCapacityCores(), "the critical cap resolves against the ceiling");
        assertTrue(plan.criticalOvercommit());
        assertTrue(
            plan.degradedReasons().stream().anyMatch(reason -> reason.contains("critical pools may lease 12")),
            () -> "the overcommit has to be reported, got " + plan.degradedReasons()
        );
        assertFalse(plan.degraded(), "the guarantees themselves still fit");
    }

    /**
     * Mirrors the core deductions the server actually applies: Folia's tick thread formula, Moonrise's
     * worker formula and one IO thread.
     *
     * @param cores processor count
     * @return the deduction set
     */
    private static BudgetDeductions foliaDefaults(final int cores) {
        final int half = cores / 2;
        final int region = half <= 4 ? 1 : half / 4;
        final int chunk = half <= 4 ? (half <= 3 ? 1 : 2) : half / 2;
        return new BudgetDeductions(region, chunk, 1);
    }

    private static PoolSpec compute(final int minThreads) {
        return new PoolSpec(
            PoolId.COMPUTE,
            PoolKind.COMPUTE,
            "async-compute",
            "background tasks",
            minThreads,
            -1,
            CoverageLevel.L3,
            ResourceBudget.defaults()
        );
    }

    private static BudgetPlan plan(
        final int cores,
        final BudgetDeductions deductions,
        final List<PoolSpec> specs
    ) {
        return new BudgetPlanner(
            new FakeTopology(cores),
            ThreadManagementSettings.Resources.defaults(),
            deductions
        ).plan(specs);
    }
}
