package kz.bejiihiu.tuff.threads.scenario;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.bootstrap.ComputeUtmiAdapter;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.Lease;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.LeaseTimeoutException;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.SheddingReport;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.pressure.TickPressureController;
import kz.bejiihiu.tuff.threads.pressure.TickPressureMonitor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scenario 3: shedding and tick pressure act on the same compute pool at the same time, for sixty
 * fake seconds.
 *
 * <p>The critical queue is one held network permit plus one parked request; the compute pool holds
 * every permit it was given, so shedding starts and takes a core every five seconds. The regions lag
 * at the same time, so pressure takes two more. The test checks the arithmetic (never below the pool
 * minimum, never negative), that both reasons are visible, and that neither mechanism oscillates while
 * the conditions stay one-sided.
 */
@Timeout(120)
class SheddingPressureScenarioTest {

    private static final int COMPUTE_CAP = 8;

    @Test
    void sheddingAndPressureShareThePoolWithoutGoingBelowTheMinimumOrFlapping() {
        final ThreadManagementSettings settings = ScenarioFixtures.settings(
            true, 1, true, ScenarioFixtures.pressure(true), true
        );
        final ScenarioFixtures.Harness harness = ScenarioFixtures.Harness.create(
            16, settings, ScenarioFixtures.network(1, 1)
        );
        final ComputeSettings compute = new ComputeSettings(
            true,
            new ComputeSettings.Workers(-1, 1, COMPUTE_CAP, Duration.ofSeconds(30), "Tuff Compute Worker"),
            ComputeSettings.QueueSettings.defaults(),
            ComputeSettings.Scheduling.defaults(),
            ComputeSettings.Consumers.defaults(),
            ComputeSettings.Metrics.defaults(),
            ComputeSettings.Adaptive.defaults(),
            Duration.ofSeconds(30)
        );
        final TickPressureMonitor monitor = new TickPressureMonitor(
            ScenarioFixtures.pressure(true), harness.controller::usableCores
        );
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(harness.runtime, compute, monitor);
        harness.refresh();
        monitor.bind(adapter.pressureSink());
        final ScenarioFixtures.RecordingEngine engine = new ScenarioFixtures.RecordingEngine(compute);
        adapter.bind(engine);

        assertEquals(10, harness.controller.usableCores());
        assertEquals(COMPUTE_CAP, harness.controller.budget(PoolId.COMPUTE).orElseThrow().cores(),
            "the pool cap is what limits it, not the whole remainder");
        assertEquals(COMPUTE_CAP, adapter.processors());

        // every compute permit is held: a shed can only shrink the ceiling, never revoke a lease
        final List<Lease> held = new ArrayList<>(COMPUTE_CAP);
        for (int core = 0; core < COMPUTE_CAP; core++) {
            held.add(harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline())));
        }
        final Lease network = harness.controller.acquire(LeaseRequest.immediate(PoolId.NETWORK, harness.deadline()));
        final Thread waiter = ScenarioFixtures.waiter(
            harness.controller, harness.clock, PoolId.NETWORK, Duration.ofMinutes(1)
        );
        waiter.start();
        ScenarioFixtures.awaitWaiting(harness.controller, PoolId.NETWORK, 1);

        final List<Integer> reserves = new ArrayList<>(61);
        final List<Integer> sheds = new ArrayList<>(61);
        // the state before the first sample: no reserve, nothing shed
        reserves.add(0);
        sheds.add(0);

        // phase A: fifteen lagging seconds with the critical queue parked
        for (int second = 0; second < 15; second++) {
            monitor.onSample(ScenarioFixtures.pass(ScenarioFixtures.lagging()), harness.clock.nanoTime());
            this.triggerComputeAttempt(harness);
            this.assertBudgetRelation(harness, adapter, monitor);
            reserves.add(monitor.reserve());
            sheds.add(harness.controller.sheddingReport().shedCores());
            harness.clock.advanceSeconds(1);
        }

        assertEquals(2, monitor.reserve(), "the policy ceiling is two cores");
        assertTrue(harness.controller.sheddingReport().active());
        assertEquals(3, harness.controller.sheddingReport().shedCores(),
            "one core per cooldown, three in fifteen seconds");
        assertEquals(COMPUTE_CAP - 3, harness.controller.budget(PoolId.COMPUTE).orElseThrow().cores(),
            "shed capacity follows the withheld cores");
        assertEquals(COMPUTE_CAP, harness.controller.inUse(PoolId.COMPUTE),
            "held permits are never revoked by a shed");
        assertTrue(harness.controller.degradedReasons(PoolId.COMPUTE).contains(DegradedReason.LOAD_SHEDDING));
        assertTrue(harness.controller.degradedReasons(PoolId.NETWORK).contains(DegradedReason.CRITICAL_POOL_PRESSURE));
        assertEquals(TickPressureController.Reason.CEILING_REACHED, monitor.snapshot().reason(),
            "pressure is already at its ceiling at second ten");

        // phase B: the critical request is served and forty five calm seconds follow
        network.release();
        awaitFinished(waiter);
        for (int second = 15; second < 60; second++) {
            monitor.onSample(ScenarioFixtures.pass(ScenarioFixtures.calm()), harness.clock.nanoTime());
            this.triggerComputeAttempt(harness);
            this.assertBudgetRelation(harness, adapter, monitor);
            reserves.add(monitor.reserve());
            sheds.add(harness.controller.sheddingReport().shedCores());
            harness.clock.advanceSeconds(1);
        }

        assertEquals(0, monitor.reserve(), "the pressure reserve is fully returned");
        final SheddingReport report = harness.controller.sheddingReport();
        assertFalse(report.active(), "the shed is fully returned");
        assertEquals(0, report.shedCores());
        assertTrue(report.pools().isEmpty());
        assertEquals(COMPUTE_CAP, harness.controller.budget(PoolId.COMPUTE).orElseThrow().cores());
        assertEquals(COMPUTE_CAP, adapter.processors());
        assertEquals(List.of(), harness.controller.degradedReasons(PoolId.COMPUTE));
        assertEquals(List.of(), harness.controller.degradedReasons(PoolId.NETWORK));

        // the no-oscillation part: one-sided conditions must move each number in one direction only
        assertSinglePeak(reserves, "pressure reserve");
        assertSinglePeak(sheds, "shed cores");
        assertEquals(4, countChanges(reserves), "four reserve moves: in, up, down, out");
        assertEquals(6, countChanges(sheds), "three shed moves up and three back down, one per cooldown");

        // a shed is not pushed to the engine budget by itself; the next pressure change reconciles it.
        // this block pins that the pool catches up instead of staying at the stale, smaller number
        for (int second = 0; second < 2; second++) {
            monitor.onSample(ScenarioFixtures.pass(ScenarioFixtures.lagging()), harness.clock.nanoTime());
            harness.clock.advanceSeconds(1);
        }
        assertEquals(1, monitor.reserve());
        for (int second = 0; second < 7; second++) {
            monitor.onSample(ScenarioFixtures.pass(ScenarioFixtures.calm()), harness.clock.nanoTime());
            harness.clock.advanceSeconds(1);
        }
        assertEquals(0, monitor.reserve());
        assertEquals(COMPUTE_CAP, adapter.processors(), "the derivation agrees with the whole capacity");
        assertEquals(COMPUTE_CAP, engine.budgets().getLast(), "the pool is back at its whole budget after the last change");

        for (final Lease lease : held) {
            lease.release();
        }
        assertEquals(0, harness.controller.inUse(PoolId.COMPUTE));
    }

    private void triggerComputeAttempt(final ScenarioFixtures.Harness harness) {
        // a failed attempt is the admission decision that runs the shedding evaluation
        try {
            harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline()));
        } catch (LeaseTimeoutException _) {
            // expected: the pool is at capacity
        }
    }

    private void assertBudgetRelation(
        final ScenarioFixtures.Harness harness,
        final ComputeUtmiAdapter adapter,
        final TickPressureMonitor monitor
    ) {
        final int capacity = harness.controller.budget(PoolId.COMPUTE).orElseThrow().cores();
        final int budget = adapter.processors();
        assertTrue(capacity >= 1, "the pool minimum is never shed");
        assertTrue(budget >= 1, "the engine budget is never zero or negative");
        assertTrue(budget <= COMPUTE_CAP, "no mechanism may raise the pool above its cap");
        assertEquals(Math.max(1, capacity - monitor.reserve()), budget,
            "the applied budget is the effective capacity minus the reserve, floored at one");
    }

    private static void assertSinglePeak(final List<Integer> values, final String name) {
        // one-sided conditions: a single rise, then a single fall, never up-down-up
        int peak = 0;
        for (int index = 1; index < values.size(); index++) {
            if (values.get(index) > values.get(peak)) {
                peak = index;
            }
        }
        for (int index = 1; index <= peak; index++) {
            assertTrue(values.get(index) >= values.get(index - 1),
                name + " fell before the peak at index " + index + ": " + values);
        }
        for (int index = peak + 1; index < values.size(); index++) {
            assertTrue(values.get(index) <= values.get(index - 1),
                name + " rose after the peak at index " + index + ": " + values);
        }
    }

    private static int countChanges(final List<Integer> values) {
        int changes = 0;
        for (int index = 1; index < values.size(); index++) {
            if (!values.get(index).equals(values.get(index - 1))) {
                changes++;
            }
        }
        return changes;
    }

    private static void awaitFinished(final Thread thread) {
        final long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (thread.isAlive()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the served waiter did not finish");
            }
            Thread.yield();
        }
    }
}
