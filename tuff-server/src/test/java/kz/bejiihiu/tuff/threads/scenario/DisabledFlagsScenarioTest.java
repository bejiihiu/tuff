package kz.bejiihiu.tuff.threads.scenario;

import java.time.Duration;
import java.util.List;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.compute.bootstrap.ComputeUtmiAdapter;
import kz.bejiihiu.tuff.threads.DegradedReason;
import kz.bejiihiu.tuff.threads.Lease;
import kz.bejiihiu.tuff.threads.LeaseRequest;
import kz.bejiihiu.tuff.threads.LeaseTimeoutException;
import kz.bejiihiu.tuff.threads.PoolId;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.SheddingReport;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.bootstrap.ThreadsReport;
import kz.bejiihiu.tuff.threads.bootstrap.ThreadsStatus;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.MetricsConfig;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.ResourceSampler;
import kz.bejiihiu.tuff.threads.pressure.TickPressureMonitor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scenario 5: every off switch does nothing and says so.
 *
 * <p>The three flags are independent and all default to off: tick pressure, load shedding and metrics.
 * "Off" has to mean zero applied changes and zero probe work, and the status lines have to state which
 * switch is off instead of showing a healthy looking zero.
 */
@Timeout(120)
class DisabledFlagsScenarioTest {

    @Test
    void aDisabledPressurePolicyNeverReadsTheSignalAndNeverTouchesTheBudget() {
        final ThreadManagementSettings settings = ScenarioFixtures.settings(
            true, 1, false, ScenarioFixtures.pressure(false), true
        );
        final ScenarioFixtures.Harness harness = ScenarioFixtures.Harness.create(
            16, settings, ScenarioFixtures.network(1, 1)
        );
        final ComputeSettings compute = ComputeSettings.defaults();
        final TickPressureMonitor monitor = new TickPressureMonitor(
            ScenarioFixtures.pressure(false), harness.controller::usableCores
        );
        final ComputeUtmiAdapter adapter = ComputeUtmiAdapter.attach(harness.runtime, compute, monitor);
        harness.refresh();
        monitor.bind(adapter.pressureSink());
        final ScenarioFixtures.RecordingEngine engine = new ScenarioFixtures.RecordingEngine(compute);
        adapter.bind(engine);

        final int budget = adapter.processors();
        for (int second = 0; second < 20; second++) {
            monitor.onSample(ScenarioFixtures.pass(ScenarioFixtures.lagging()), harness.clock.nanoTime());
            harness.clock.advanceSeconds(1);
        }

        assertNull(monitor.snapshot(), "a disabled policy publishes no decision");
        assertEquals(List.of(), engine.budgets(), "no reserve change may reach the engine");
        assertEquals(budget, adapter.processors(), "the budget is unchanged");
        assertEquals(0, monitor.reserve());

        final List<String> lines = ThreadsReport.overview(
            ThreadsStatus.running(settings).withSnapshot(ScenarioFixtures.pass(ScenarioFixtures.lagging()), 0, 0L)
        );
        assertTrue(lines.stream().anyMatch(line -> line.contains("pressure: disabled")), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains(
            "thread-management.async-compute.tick-pressure.enabled: false")), lines.toString());
    }

    @Test
    void aDisabledSheddingPolicyNeverWithholdsACoreEvenWhenACriticalQueueWaits() {
        final ThreadManagementSettings settings = ScenarioFixtures.settings(
            true, 1, false, ScenarioFixtures.pressure(false), true
        );
        final ScenarioFixtures.Harness harness = ScenarioFixtures.Harness.create(
            16, settings, ScenarioFixtures.compute(1, -1, 4), ScenarioFixtures.network(1, 1)
        );
        final List<Lease> compute = List.of(
            harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline())),
            harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline())),
            harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline())),
            harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline()))
        );
        final Lease network = harness.controller.acquire(LeaseRequest.immediate(PoolId.NETWORK, harness.deadline()));
        final Thread waiter = ScenarioFixtures.waiter(
            harness.controller, harness.clock, PoolId.NETWORK, Duration.ofMinutes(1)
        );
        waiter.start();
        ScenarioFixtures.awaitWaiting(harness.controller, PoolId.NETWORK, 1);

        for (int attempt = 0; attempt < 10; attempt++) {
            assertThrows(
                LeaseTimeoutException.class,
                () -> harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline()))
            );
            harness.clock.advanceSeconds(1);
        }

        final SheddingReport report = harness.controller.sheddingReport();
        assertFalse(report.enabled());
        assertFalse(report.active());
        assertEquals(0, report.shedCores());
        assertEquals(List.of(), report.pools());
        assertEquals(4, harness.controller.budget(PoolId.COMPUTE).orElseThrow().cores(), "nothing was withheld");
        assertFalse(harness.controller.degradedReasons(PoolId.COMPUTE).contains(DegradedReason.LOAD_SHEDDING));
        assertTrue(report.summary().contains("load shedding off"), report.summary());
        // the critical queue is still a real pressure reason; the off switch only disables the reaction
        assertTrue(harness.controller.degradedReasons(PoolId.NETWORK).contains(DegradedReason.CRITICAL_POOL_PRESSURE));

        harness.controller.close();
        ScenarioFixtures.awaitFinished(waiter);
        network.release();
        for (final Lease lease : compute) {
            lease.release();
        }
    }

    @Test
    void disabledMetricsNeverAskAProbeAndTheSnapshotSaysItIsNotAMeasurement() {
        final ThreadManagementSettings settings = ScenarioFixtures.settings(
            false, 1, false, ScenarioFixtures.pressure(false), false
        );
        final ScenarioFixtures.Harness harness = ScenarioFixtures.Harness.create(
            16, settings, ScenarioFixtures.compute(1, -1, 2), ScenarioFixtures.service()
        );
        final ScenarioFixtures.CountingProbe probe = new ScenarioFixtures.CountingProbe(
            PoolId.COMPUTE, () -> ScenarioFixtures.Reading.of(3L, 2L)
        );
        final ResourceSampler sampler = ScenarioFixtures.sampler(
            harness,
            new MetricsConfig(false, false, false, Duration.ofSeconds(1L), Duration.ofSeconds(30L)),
            List.of(probe),
            new ScenarioFixtures.FakeCounters()
        );

        assertFalse(sampler.enabled());
        final ResourceSnapshot snapshot = sampler.sampleNow();
        assertEquals(0, probe.calls(), "a disabled sampler must not run a probe");
        assertTrue(snapshot.degradedReasons().contains(DegradedReason.METRICS_UNAVAILABLE));
        assertTrue(snapshot.pools().stream().allMatch(pool -> pool.queuedTasks() == -1L),
            "-1 is the honest answer when nothing was read");
        assertTrue(snapshot.pools().stream().allMatch(pool -> pool.cpuLoad() == -1.0d));
        assertEquals(List.of(), snapshot.threads());

        final List<String> lines = ThreadsReport.overview(
            ThreadsStatus.running(settings).withSnapshot(snapshot, -1, 0L)
        );
        assertTrue(lines.stream().anyMatch(line -> line.contains("metrics: disabled")), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains("degraded: unknown (metrics are disabled)")),
            lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains("pressure: disabled")), lines.toString());
        assertTrue(lines.stream().anyMatch(line -> line.contains("load shedding off")), lines.toString());
    }
}
