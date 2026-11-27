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
import kz.bejiihiu.tuff.threads.PoolSnapshot;
import kz.bejiihiu.tuff.threads.ResourceSnapshot;
import kz.bejiihiu.tuff.threads.SheddingReport;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.MetricsConfig;
import kz.bejiihiu.tuff.threads.internal.impl.metrics.ResourceSampler;
import kz.bejiihiu.tuff.threads.pressure.TickPressureMonitor;
import kz.bejiihiu.tuff.threads.pressure.TickPressureSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scenario 6: one snapshot has to agree with the controller, the adapter and itself after the features
 * have moved the numbers.
 *
 * <p>The state under test is the interesting one: the critical reserve is one, shedding has taken one
 * core, pressure is yielding one core, and one pool has a probe while another does not. The snapshot,
 * the controller and the pressure decision must all describe that same moment, and {@code -1} may only
 * appear where the layer genuinely has no source.
 */
@Timeout(120)
class SnapshotConsistencyScenarioTest {

    private static final int COMPUTE_CAP = 8;

    @Test
    void snapshotControllerAndPressureDecisionDescribeTheSameMoment() {
        final ThreadManagementSettings settings = ScenarioFixtures.settings(
            true, 1, true, ScenarioFixtures.pressure(true), true
        );
        final ScenarioFixtures.Harness harness = ScenarioFixtures.Harness.create(
            16, settings, ScenarioFixtures.network(1, 1), ScenarioFixtures.service()
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

        // pressure yields one core, then a failed compute lease triggers one shedding decision
        monitor.onSample(ScenarioFixtures.pass(ScenarioFixtures.lagging()), harness.clock.nanoTime());
        try {
            harness.controller.acquire(LeaseRequest.immediate(PoolId.COMPUTE, harness.deadline()));
        } catch (LeaseTimeoutException _) {
            // expected: all eight permits are held
        }

        final ScenarioFixtures.CountingProbe probe = new ScenarioFixtures.CountingProbe(
            PoolId.COMPUTE, () -> ScenarioFixtures.Reading.of(5L, COMPUTE_CAP)
        );
        harness.clock.advanceSeconds(1);
        final ResourceSampler sampler = ScenarioFixtures.sampler(
            harness,
            new MetricsConfig(true, true, true, Duration.ofSeconds(1L), Duration.ofSeconds(30L)),
            List.of(probe),
            new ScenarioFixtures.FakeCounters()
        );
        sampler.sampleNow();
        harness.clock.advanceSeconds(1);
        final ResourceSnapshot snapshot = sampler.sampleNow();

        // the ledger numbers are one set of facts, not two
        assertEquals(10, harness.controller.usableCores());
        assertEquals(harness.controller.usableCores(), snapshot.usableCores());
        assertEquals(1, snapshot.reservedCores(), "the configured reserve is reported as configured");
        assertEquals(harness.controller.criticalReserve(), snapshot.criticalReserveCores());
        assertEquals(1, snapshot.criticalReserveCores(), "the network minimum");

        final SheddingReport live = harness.controller.sheddingReport();
        final SheddingReport published = snapshot.shedding();
        assertEquals(live.enabled(), published.enabled());
        assertEquals(live.shedCores(), published.shedCores());
        assertEquals(live.pools(), published.pools());
        assertEquals(live.criticalReserveCores(), published.criticalReserveCores());
        assertEquals(1, published.shedCores());
        assertEquals(List.of(PoolId.COMPUTE), published.pools());
        assertTrue(published.active());
        assertEquals(published.summary(), live.summary());

        // capacity, shed and pressure add up and none of them invented a core
        final int capacity = harness.controller.budget(PoolId.COMPUTE).orElseThrow().cores();
        assertEquals(COMPUTE_CAP - published.shedCores(), capacity, "shed capacity is declared minus withheld");
        assertEquals(COMPUTE_CAP, snapshot.pool(PoolId.COMPUTE).orElseThrow().spec().maxThreads());
        final int budget = adapter.processors();
        assertEquals(Math.max(1, capacity - monitor.reserve()), budget, "the applied budget after both levers");
        assertEquals(6, budget, "eight declared, one shed, one yielded");
        assertTrue(budget >= 1);

        final TickPressureSnapshot pressure = monitor.snapshot();
        assertEquals(1, pressure.reserve());
        assertEquals(10, pressure.usableCores());
        assertEquals(2, pressure.maxReserve());
        // documented formula: the command line's budget is usable minus reserve, so it does not see the
        // pool cap or the shed; the applied pool budget above is the authoritative number
        assertEquals(9, pressure.budget());

        // degradation is the union of the per pool reasons the controller reports, and says why
        final PoolSnapshot computePool = snapshot.pool(PoolId.COMPUTE).orElseThrow();
        final PoolSnapshot networkPool = snapshot.pool(PoolId.NETWORK).orElseThrow();
        assertEquals(harness.controller.degradedReasons(PoolId.COMPUTE), computePool.degradedReasons());
        assertEquals(harness.controller.degradedReasons(PoolId.NETWORK), networkPool.degradedReasons());
        assertTrue(computePool.degradedReasons().contains(DegradedReason.LOAD_SHEDDING));
        assertTrue(networkPool.degradedReasons().contains(DegradedReason.CRITICAL_POOL_PRESSURE));
        assertTrue(snapshot.degradedReasons().contains(DegradedReason.LOAD_SHEDDING));
        assertTrue(snapshot.degradedReasons().contains(DegradedReason.CRITICAL_POOL_PRESSURE));
        for (final PoolSnapshot pool : snapshot.pools()) {
            for (final DegradedReason reason : pool.degradedReasons()) {
                assertTrue(snapshot.degradedReasons().contains(reason),
                    "the union must contain " + reason + " from " + pool.id());
            }
        }

        // -1 only where there is no source: a probe gave the compute queue, the probe less pools did not
        assertEquals(5L, computePool.queuedTasks());
        assertEquals(COMPUTE_CAP, computePool.queueCapacity());
        assertNotEquals(-1, computePool.cpuLoad());
        assertEquals(-1L, computePool.submittedTasks(), "the sampler owns no task counters");
        assertEquals(-1L, networkPool.queuedTasks());
        assertEquals(-1L, networkPool.queueCapacity());
        assertEquals(List.of(), snapshot.threads());
        for (final PoolSnapshot pool : snapshot.pools()) {
            assertTrue(pool.aliveThreads() >= 0);
            assertTrue(pool.activeThreads() >= -1);
            assertTrue(pool.idleThreads() >= -1);
            assertTrue(pool.queuedTasks() >= -1L);
            assertTrue(pool.queueCapacity() >= -1L);
            assertTrue(pool.submittedTasks() >= -1L);
        }
        assertFalse(snapshot.degradedReasons().contains(DegradedReason.METRICS_UNAVAILABLE),
            "counters support CPU time here, so the metrics are not unavailable");

        harness.controller.close();
        ScenarioFixtures.awaitFinished(waiter);
        network.release();
        for (final Lease lease : held) {
            lease.release();
        }
    }
}
