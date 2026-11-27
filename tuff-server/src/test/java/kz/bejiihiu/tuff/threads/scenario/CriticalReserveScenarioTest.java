package kz.bejiihiu.tuff.threads.scenario;

import java.util.ArrayList;
import java.util.List;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.Lease;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.LeaseTimeoutException;
import kz.bejiihiu.tuff.threads.PoolId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scenario 2: the cores reserved for the critical lanes cannot be leased by a shared pool, and the
 * critical pool keeps its minimum even when the arithmetic says otherwise.
 *
 * <p>Three shapes of the same rule: a shared pool that asks for twenty and gets nine, a network pool
 * whose declared minimum is above what the budget allows (overcommit is reported, not hidden), and a
 * machine so small that the reserve eats the whole remainder and the shared pool gets zero.
 */
@Timeout(120)
class CriticalReserveScenarioTest {

    @Test
    void aComputeBurstCannotLeasePastTheCriticalReserve() {
        final ScenarioFixtures.Harness harness = ScenarioFixtures.Harness.create(
            16,
            ScenarioFixtures.settings(true, 1, false, ScenarioFixtures.pressure(false), true),
            ScenarioFixtures.compute(1, -1, 20),
            ScenarioFixtures.network(2, 2)
        );

        assertEquals(2, harness.controller.criticalReserve(), "the network minimum is held back");
        assertEquals(9, harness.controller.usableCores(), "11 left by the deductions minus the 2 core reserve");
        assertEquals(9, harness.controller.budget(PoolId.COMPUTE).orElseThrow().cores(),
            "a cap of twenty is clamped to the nine that are actually left");

        final List<Lease> compute = new ArrayList<>(9);
        for (int core = 0; core < 9; core++) {
            compute.add(harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline())));
        }
        assertThrows(
            LeaseTimeoutException.class,
            () -> harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline())),
            "the tenth core is the network's, a shared pool must not get it"
        );

        final Lease first = harness.controller.acquire(LeaseRequest.immediate(PoolId.NETWORK, harness.deadline()));
        final Lease second = harness.controller.acquire(LeaseRequest.immediate(PoolId.NETWORK, harness.deadline()));
        assertEquals(2, harness.controller.inUse(PoolId.NETWORK), "the network keeps its two cores");

        assertEquals(List.of(), harness.controller.degradedReasons(PoolId.COMPUTE));
        assertEquals(List.of(), harness.controller.degradedReasons(PoolId.NETWORK));

        first.release();
        second.release();
        for (final Lease lease : compute) {
            lease.release();
        }
    }

    @Test
    void anUnsatisfiedNetworkMinimumKeepsItsCoresAndIsReported() {
        final ScenarioFixtures.Harness harness = ScenarioFixtures.Harness.create(
            16,
            ScenarioFixtures.settings(true, 1, false, ScenarioFixtures.pressure(false), true),
            ScenarioFixtures.compute(1, -1, 20),
            ScenarioFixtures.network(3, 2)
        );

        assertEquals(3, harness.controller.criticalReserve(), "the larger of the minimum and the budget");
        assertEquals(8, harness.controller.usableCores(), "the shared pools only see what is left");
        assertEquals(8, harness.controller.budget(PoolId.COMPUTE).orElseThrow().cores());
        assertEquals(3, harness.controller.budget(PoolId.NETWORK).orElseThrow().cores(),
            "a critical pool keeps its minimum even above the budget ceiling");

        assertTrue(harness.controller.degradedReasons(PoolId.NETWORK).contains(DegradedReason.CRITICAL_POOL_PRESSURE),
            "the overcommit must be a state, not a silent raise of the ceiling");
        assertFalse(harness.controller.degradedReasons(PoolId.COMPUTE).contains(DegradedReason.CRITICAL_POOL_PRESSURE));
        assertTrue(harness.controller.budgetLedger().contains("+ critical 3"), harness.controller.budgetLedger());

        final List<Lease> network = new ArrayList<>(3);
        for (int core = 0; core < 3; core++) {
            network.add(harness.controller.acquire(LeaseRequest.immediate(PoolId.NETWORK, harness.deadline())));
        }
        final List<Lease> compute = new ArrayList<>(8);
        for (int core = 0; core < 8; core++) {
            compute.add(harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline())));
        }
        assertThrows(
            LeaseTimeoutException.class,
            () -> harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline())),
            "the shared pool gets the eight that are left, never the three the network keeps"
        );
        for (final Lease lease : compute) {
            lease.release();
        }
        for (final Lease lease : network) {
            lease.release();
        }
    }

    @Test
    void whenTheReserveEatsEverythingTheSharedPoolGetsZeroInsteadOfAnInventedCore() {
        final ScenarioFixtures.Harness harness = ScenarioFixtures.Harness.create(
            4,
            ScenarioFixtures.settings(true, 1, false, ScenarioFixtures.pressure(false), true),
            ScenarioFixtures.compute(1, -1, 5),
            ScenarioFixtures.network(3, 2)
        );

        assertEquals(3, harness.controller.criticalReserve());
        assertEquals(1, harness.controller.usableCores(), "the ledger floors usable at one, the ledger line shows the raw");
        assertEquals(0, harness.controller.budget(PoolId.COMPUTE).orElseThrow().cores(),
            "zero is the honest answer; one would be a core nobody has");

        assertThrows(
            LeaseTimeoutException.class,
            () -> harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline())),
            "a pool with zero permits must fail fast"
        );

        final List<Lease> network = new ArrayList<>(3);
        for (int core = 0; core < 3; core++) {
            network.add(harness.controller.acquire(LeaseRequest.immediate(PoolId.NETWORK, harness.deadline())));
        }
        assertEquals(3, harness.controller.inUse(PoolId.NETWORK), "the critical minimum still works");
        assertTrue(harness.controller.degradedReasons(PoolId.NETWORK).contains(DegradedReason.CRITICAL_POOL_PRESSURE));
        for (final Lease lease : network) {
            lease.release();
        }
    }
}
